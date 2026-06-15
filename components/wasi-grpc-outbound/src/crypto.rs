//! Cryptography (alpha) over gRPC — `EncryptAlpha1`, `DecryptAlpha1`.
//!
//! Both RPCs are bidirectional streams; the WIT contract is one-shot, so
//! each call sends a single request message carrying the options and the
//! whole payload (`seq: 0`), half-closes, and reassembles the response
//! chunks in `seq` order. Streaming RPCs through the Spin h2c path are
//! thus exercised only in this one-shot form here.

use crate::exports::crypto::{CryptoError, DecryptError, EncryptError, EncryptOptions, Guest};
use crate::proto::common as pbc;
use crate::proto::runtime as pb;
use crate::sidecar::{classify, DaprFailure, Sidecar};
use crate::Component;

fn crypto_error(f: DaprFailure) -> CryptoError {
    if f.is_permission() {
        CryptoError::PermissionDenied(f.message)
    } else {
        CryptoError::ComponentNotFound(f.message)
    }
}

fn encrypt_error(f: DaprFailure) -> EncryptError {
    if f.status == 404
        || f.error_code
            .as_deref()
            .is_some_and(|c| c.contains("KEY_NOT_FOUND") || c.contains("NOT_FOUND"))
    {
        return EncryptError::KeyNotFound(f.message);
    }
    if f.status == 400 {
        return EncryptError::UnsupportedAlgorithm(f.message);
    }
    EncryptError::Crypto(crypto_error(f))
}

fn decrypt_error(f: DaprFailure) -> DecryptError {
    if f.status == 404
        || f.error_code
            .as_deref()
            .is_some_and(|c| c.contains("KEY_NOT_FOUND") || c.contains("NOT_FOUND"))
    {
        return DecryptError::KeyNotFound(f.message);
    }
    if f.status == 400 {
        return DecryptError::MalformedCiphertext(f.message);
    }
    DecryptError::Crypto(crypto_error(f))
}

/// Drain the response stream, concatenating the payload chunks in `seq`
/// order (a single gRPC stream already delivers them in order; sorting
/// just enforces the proto contract).
async fn collect_payload<T>(
    mut streaming: tonic::Streaming<T>,
    payload: impl Fn(T) -> Option<pbc::StreamPayload>,
) -> Result<Vec<u8>, tonic::Status> {
    let mut chunks: Vec<(u64, Vec<u8>)> = Vec::new();
    while let Some(message) = streaming.message().await? {
        if let Some(chunk) = payload(message) {
            chunks.push((chunk.seq, chunk.data));
        }
    }
    chunks.sort_by_key(|(seq, _)| *seq);
    Ok(chunks.into_iter().flat_map(|(_, data)| data).collect())
}

impl Guest for Component {
    fn encrypt(
        component_name: String,
        data: Vec<u8>,
        options: EncryptOptions,
    ) -> Result<Vec<u8>, EncryptError> {
        let sidecar = Sidecar::from_env();
        let message = pb::EncryptRequest {
            options: Some(pb::EncryptRequestOptions {
                component_name,
                key_name: options.key_name,
                key_wrap_algorithm: options.key_wrap_algorithm,
                data_encryption_cipher: options.data_encryption_cipher.unwrap_or_default(),
                omit_decryption_key_name: options.omit_decryption_key_name.unwrap_or(false),
                decryption_key_name: options.decryption_key_name.unwrap_or_default(),
            }),
            payload: Some(pbc::StreamPayload { data, seq: 0 }),
        };
        let request = sidecar.request(futures::stream::iter(vec![message]));
        let mut client = sidecar.client();
        spin_executor::run(async move {
            let streaming = client.encrypt_alpha1(request).await?.into_inner();
            collect_payload(streaming, |response: pb::EncryptResponse| response.payload).await
        })
        .map_err(|s| encrypt_error(classify(s)))
    }

    fn decrypt(
        component_name: String,
        data: Vec<u8>,
        key_name: Option<String>,
    ) -> Result<Vec<u8>, DecryptError> {
        let sidecar = Sidecar::from_env();
        let message = pb::DecryptRequest {
            options: Some(pb::DecryptRequestOptions {
                component_name,
                key_name: key_name.unwrap_or_default(),
            }),
            payload: Some(pbc::StreamPayload { data, seq: 0 }),
        };
        let request = sidecar.request(futures::stream::iter(vec![message]));
        let mut client = sidecar.client();
        spin_executor::run(async move {
            let streaming = client.decrypt_alpha1(request).await?.into_inner();
            collect_payload(streaming, |response: pb::DecryptResponse| response.payload).await
        })
        .map_err(|s| decrypt_error(classify(s)))
    }
}
