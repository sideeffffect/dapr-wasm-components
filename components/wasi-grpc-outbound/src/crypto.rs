//! Cryptography (alpha) over gRPC — `EncryptAlpha1`, `DecryptAlpha1`.
//!
//! Both RPCs are bidirectional streams, and daprd streams them for real
//! (`pkg/api/grpc/crypto.go`). This provider does **not**: `wasi-grpc` gives
//! no way to keep one in-flight tonic call alive across several export calls
//! (each call runs its own `spin_executor::run`). So every operation is one
//! RPC: the input is sent as a stream of request messages (options on the
//! first, the payload split into [`CHUNK`]-sized `StreamPayload`s with
//! `seq` 0, 1, …), the client half-closes, and the response chunks are
//! reassembled in `seq` order.
//!
//! The streaming `encryptor`/`decryptor` resources therefore **buffer**: they
//! collect every `write` in provider memory and run the RPC in `finish`;
//! `crypto-output` serves the buffered result. The app still writes and reads
//! in chunks, but the provider holds the whole input and output. Splitting the
//! payload into several messages keeps each one under daprd's per-message
//! limit (`--max-body-size`, default 4Mi), so payloads above 4Mi work.

use std::cell::{Cell, RefCell};

use crate::exports::crypto::{
    CryptoError, CryptoOutput, DecryptError, Decryptor, EncryptError, EncryptOptions, Encryptor,
    Guest, GuestCryptoOutput, GuestDecryptor, GuestEncryptor,
};
use crate::proto::common as pbc;
use crate::proto::runtime as pb;
use crate::sidecar::{classify, DaprFailure, Sidecar};
use crate::Component;

/// Payload bytes per request message (well under daprd's 4Mi default
/// per-message limit).
const CHUNK: usize = 1024 * 1024;

fn crypto_error(f: DaprFailure) -> CryptoError {
    if f.is_permission() {
        CryptoError::PermissionDenied(f.message)
    } else {
        CryptoError::ComponentNotFound(f.message)
    }
}

/// daprd answers an unknown component with `InvalidArgument` and the
/// message "crypto provider <name> not found" (`ErrCryptoProviderNotFound`).
fn is_provider_not_found(f: &DaprFailure) -> bool {
    f.message.contains("crypto provider") && f.message.contains("not found")
}

fn encrypt_error(f: DaprFailure) -> EncryptError {
    if is_provider_not_found(&f) {
        return EncryptError::Crypto(CryptoError::ComponentNotFound(f.message));
    }
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
    if is_provider_not_found(&f) {
        return DecryptError::Crypto(CryptoError::ComponentNotFound(f.message));
    }
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

/// Split `data` into `StreamPayload`s of at most [`CHUNK`] bytes with `seq`
/// 0, 1, … (one empty payload for empty input). Lazy: at most one chunk is
/// copied at a time.
fn payloads(data: Vec<u8>) -> impl Iterator<Item = pbc::StreamPayload> + Send + 'static {
    let count = data.len().div_ceil(CHUNK).max(1);
    (0..count).map(move |i| {
        let start = (i * CHUNK).min(data.len());
        let end = ((i + 1) * CHUNK).min(data.len());
        pbc::StreamPayload {
            data: data[start..end].to_vec(),
            seq: i as u64,
        }
    })
}

fn run_encrypt(
    component_name: String,
    data: Vec<u8>,
    options: EncryptOptions,
) -> Result<Vec<u8>, EncryptError> {
    let sidecar = Sidecar::from_env();
    let mut options = Some(pb::EncryptRequestOptions {
        component_name,
        key_name: options.key_name,
        key_wrap_algorithm: options.key_wrap_algorithm,
        data_encryption_cipher: options.data_encryption_cipher.unwrap_or_default(),
        omit_decryption_key_name: options.omit_decryption_key_name.unwrap_or(false),
        decryption_key_name: options.decryption_key_name.unwrap_or_default(),
    });
    let messages = payloads(data).map(move |payload| pb::EncryptRequest {
        options: options.take(),
        payload: Some(payload),
    });
    let request = sidecar.request(futures::stream::iter(messages));
    let mut client = sidecar.client();
    spin_executor::run(async move {
        let streaming = client.encrypt_alpha1(request).await?.into_inner();
        collect_payload(streaming, |response: pb::EncryptResponse| response.payload).await
    })
    .map_err(|s| encrypt_error(classify(s)))
}

fn run_decrypt(
    component_name: String,
    data: Vec<u8>,
    key_name: Option<String>,
) -> Result<Vec<u8>, DecryptError> {
    let sidecar = Sidecar::from_env();
    let mut options = Some(pb::DecryptRequestOptions {
        component_name,
        key_name: key_name.unwrap_or_default(),
    });
    let messages = payloads(data).map(move |payload| pb::DecryptRequest {
        options: options.take(),
        payload: Some(payload),
    });
    let request = sidecar.request(futures::stream::iter(messages));
    let mut client = sidecar.client();
    spin_executor::run(async move {
        let streaming = client.decrypt_alpha1(request).await?.into_inner();
        collect_payload(streaming, |response: pb::DecryptResponse| response.payload).await
    })
    .map_err(|s| decrypt_error(classify(s)))
}

impl Guest for Component {
    type Encryptor = GrpcEncryptor;
    type Decryptor = GrpcDecryptor;
    type CryptoOutput = GrpcCryptoOutput;

    fn encrypt(
        component_name: String,
        data: Vec<u8>,
        options: EncryptOptions,
    ) -> Result<Vec<u8>, EncryptError> {
        run_encrypt(component_name, data, options)
    }

    fn decrypt(
        component_name: String,
        data: Vec<u8>,
        key_name: Option<String>,
    ) -> Result<Vec<u8>, DecryptError> {
        run_decrypt(component_name, data, key_name)
    }
}

/// Buffering encryptor: `write` collects, `finish` runs one `EncryptAlpha1`.
pub struct GrpcEncryptor {
    component_name: String,
    options: EncryptOptions,
    input: RefCell<Vec<u8>>,
}

impl GuestEncryptor for GrpcEncryptor {
    fn new(component_name: String, options: EncryptOptions) -> Self {
        Self {
            component_name,
            options,
            input: RefCell::new(Vec::new()),
        }
    }

    fn write(&self, chunk: Vec<u8>) -> Result<(), EncryptError> {
        self.input.borrow_mut().extend_from_slice(&chunk);
        Ok(())
    }

    fn finish(this: Encryptor) -> Result<CryptoOutput, EncryptError> {
        let this: GrpcEncryptor = this.into_inner();
        let output = run_encrypt(this.component_name, this.input.into_inner(), this.options)?;
        Ok(CryptoOutput::new(GrpcCryptoOutput::new(output)))
    }
}

/// Buffering decryptor: `write` collects, `finish` runs one `DecryptAlpha1`.
pub struct GrpcDecryptor {
    component_name: String,
    key_name: Option<String>,
    input: RefCell<Vec<u8>>,
}

impl GuestDecryptor for GrpcDecryptor {
    fn new(component_name: String, key_name: Option<String>) -> Self {
        Self {
            component_name,
            key_name,
            input: RefCell::new(Vec::new()),
        }
    }

    fn write(&self, chunk: Vec<u8>) -> Result<(), DecryptError> {
        self.input.borrow_mut().extend_from_slice(&chunk);
        Ok(())
    }

    fn finish(this: Decryptor) -> Result<CryptoOutput, DecryptError> {
        let this: GrpcDecryptor = this.into_inner();
        let output = run_decrypt(this.component_name, this.input.into_inner(), this.key_name)?;
        Ok(CryptoOutput::new(GrpcCryptoOutput::new(output)))
    }
}

/// The buffered output of a finished call, served in chunks.
pub struct GrpcCryptoOutput {
    data: Vec<u8>,
    position: Cell<usize>,
}

impl GrpcCryptoOutput {
    fn new(data: Vec<u8>) -> Self {
        Self {
            data,
            position: Cell::new(0),
        }
    }
}

impl GuestCryptoOutput for GrpcCryptoOutput {
    fn read(&self, max_len: u64) -> Option<Vec<u8>> {
        let start = self.position.get();
        if start >= self.data.len() {
            return None;
        }
        let len = usize::try_from(max_len.max(1)).unwrap_or(usize::MAX);
        let end = start.saturating_add(len).min(self.data.len());
        self.position.set(end);
        Some(self.data[start..end].to_vec())
    }
}
