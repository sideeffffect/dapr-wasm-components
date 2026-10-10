//! Cryptography (alpha) — https://docs.dapr.io/reference/api/cryptography_api/
//!
//! One-shot `encrypt`/`decrypt` send the whole payload in one request. The
//! streaming `encryptor`/`decryptor` resources write the request body chunk
//! by chunk and read the response body chunk by chunk (see `streaming.rs`),
//! so the guest never holds the whole payload. daprd v1.18 itself still
//! buffers the whole output (`io.ReadAll` in `pkg/api/http/crypto.go`) and
//! caps the input at `--max-body-size` (default 4Mi).
//!
//! The streaming resources first check, with `GET /v1.0/metadata`, that the
//! component exists and that the API token is accepted. daprd rejects both
//! *before* it reads the body. Over HTTP/1.1 such an early answer to a large
//! upload is not reliably delivered — the host's HTTP client can report a
//! protocol error instead (seen with wasmtime) — so the provider asks first
//! and never starts an upload that is bound to be rejected.

use wstd::http::Method;

use crate::exports::crypto::{
    CryptoError, CryptoOutput, DecryptError, Decryptor, EncryptError, EncryptOptions, Encryptor,
    Guest, GuestCryptoOutput, GuestDecryptor, GuestEncryptor,
};
use crate::sidecar::{seg, DaprFailure, Sidecar};
use crate::streaming::{StreamingRequest, StreamingResponse};
use crate::Component;

/// daprd's error code for an unknown crypto component (HTTP 400).
const PROVIDER_NOT_FOUND: &str = "ERR_CRYPTO_PROVIDER_NOT_FOUND";

fn is_provider_not_found(f: &DaprFailure) -> bool {
    f.error_code.as_deref() == Some(PROVIDER_NOT_FOUND)
}

/// Map a recoverable failure to the crypto setup/config error.
fn crypto_error(f: DaprFailure) -> CryptoError {
    if f.is_permission() {
        CryptoError::PermissionDenied(f.message)
    } else {
        CryptoError::ComponentNotFound(f.message)
    }
}

/// Map a recoverable failure of an encrypt.
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

/// Map a recoverable failure of a decrypt.
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

fn encrypt_path(component_name: &str) -> String {
    format!("/v1.0-alpha1/crypto/{}/encrypt", seg(component_name))
}

fn decrypt_path(component_name: &str) -> String {
    format!("/v1.0-alpha1/crypto/{}/decrypt", seg(component_name))
}

fn encrypt_headers(options: EncryptOptions) -> Vec<(String, String)> {
    let mut headers = vec![
        (
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        ),
        ("dapr-key-name".to_string(), options.key_name),
        (
            "dapr-key-wrap-algorithm".to_string(),
            options.key_wrap_algorithm,
        ),
    ];
    if let Some(cipher) = options.data_encryption_cipher {
        headers.push(("dapr-data-encryption-cipher".to_string(), cipher));
    }
    if options.omit_decryption_key_name.unwrap_or(false) {
        headers.push((
            "dapr-omit-decryption-key-name".to_string(),
            "true".to_string(),
        ));
    }
    if let Some(key) = options.decryption_key_name {
        headers.push(("dapr-decryption-key-name".to_string(), key));
    }
    headers
}

fn decrypt_headers(key_name: Option<String>) -> Vec<(String, String)> {
    let mut headers = vec![(
        "content-type".to_string(),
        "application/octet-stream".to_string(),
    )];
    if let Some(key) = key_name {
        headers.push(("dapr-key-name".to_string(), key));
    }
    headers
}

impl Guest for Component {
    type Encryptor = HttpEncryptor;
    type Decryptor = HttpDecryptor;
    type CryptoOutput = HttpCryptoOutput;

    fn encrypt(
        component_name: String,
        data: Vec<u8>,
        options: EncryptOptions,
    ) -> Result<Vec<u8>, EncryptError> {
        let response = Sidecar::from_env()
            .expect_success(
                Method::PUT,
                &encrypt_path(&component_name),
                &encrypt_headers(options),
                data,
            )
            .map_err(encrypt_error)?;
        Ok(response.body)
    }

    fn decrypt(
        component_name: String,
        data: Vec<u8>,
        key_name: Option<String>,
    ) -> Result<Vec<u8>, DecryptError> {
        let response = Sidecar::from_env()
            .expect_success(
                Method::PUT,
                &decrypt_path(&component_name),
                &decrypt_headers(key_name),
                data,
            )
            .map_err(decrypt_error)?;
        Ok(response.body)
    }
}

/// A streaming upload, or the failure that stopped it before it started.
enum Upload {
    Rejected(DaprFailure),
    Open(StreamingRequest),
}

impl Upload {
    fn start(component_name: &str, path: &str, headers: &[(String, String)]) -> Self {
        let sidecar = Sidecar::from_env();
        match check_component(&sidecar, component_name) {
            Ok(()) => Upload::Open(StreamingRequest::put(&sidecar, path, headers)),
            Err(failure) => Upload::Rejected(failure),
        }
    }

    fn write(&self, data: &[u8]) -> Result<(), DaprFailure> {
        match self {
            Upload::Rejected(failure) => Err(failure.clone()),
            Upload::Open(request) => request.write(data),
        }
    }

    fn finish(self) -> Result<StreamingResponse, DaprFailure> {
        match self {
            Upload::Rejected(failure) => Err(failure),
            Upload::Open(request) => request.finish(),
        }
    }
}

/// Check that `component_name` is a loaded crypto component (and, on the
/// way, that the API token is accepted) via the metadata API.
fn check_component(sidecar: &Sidecar, component_name: &str) -> Result<(), DaprFailure> {
    let response = sidecar.expect_success(Method::GET, "/v1.0/metadata", &[], Vec::new())?;
    let metadata: serde_json::Value = serde_json::from_slice(&response.body)
        .unwrap_or_else(|e| panic!("malformed sidecar metadata: {e}"));
    let found = metadata["components"].as_array().is_some_and(|components| {
        components.iter().any(|c| {
            c["name"].as_str() == Some(component_name)
                && c["type"].as_str().is_some_and(|t| t.starts_with("crypto."))
        })
    });
    if found {
        Ok(())
    } else {
        Err(DaprFailure {
            status: 400,
            message: format!("crypto provider {component_name} not found"),
            error_code: Some(PROVIDER_NOT_FOUND.to_string()),
        })
    }
}

/// Streaming encryption: one `PUT .../encrypt` whose body is written chunk
/// by chunk.
pub struct HttpEncryptor(Upload);

impl GuestEncryptor for HttpEncryptor {
    fn new(component_name: String, options: EncryptOptions) -> Self {
        Self(Upload::start(
            &component_name,
            &encrypt_path(&component_name),
            &encrypt_headers(options),
        ))
    }

    fn write(&self, chunk: Vec<u8>) -> Result<(), EncryptError> {
        self.0.write(&chunk).map_err(encrypt_error)
    }

    fn finish(this: Encryptor) -> Result<CryptoOutput, EncryptError> {
        let this: HttpEncryptor = this.into_inner();
        let response = this.0.finish().map_err(encrypt_error)?;
        Ok(CryptoOutput::new(HttpCryptoOutput(response)))
    }
}

/// Streaming decryption: one `PUT .../decrypt` whose body is written chunk
/// by chunk.
pub struct HttpDecryptor(Upload);

impl GuestDecryptor for HttpDecryptor {
    fn new(component_name: String, key_name: Option<String>) -> Self {
        Self(Upload::start(
            &component_name,
            &decrypt_path(&component_name),
            &decrypt_headers(key_name),
        ))
    }

    fn write(&self, chunk: Vec<u8>) -> Result<(), DecryptError> {
        self.0.write(&chunk).map_err(decrypt_error)
    }

    fn finish(this: Decryptor) -> Result<CryptoOutput, DecryptError> {
        let this: HttpDecryptor = this.into_inner();
        let response = this.0.finish().map_err(decrypt_error)?;
        Ok(CryptoOutput::new(HttpCryptoOutput(response)))
    }
}

/// The response body of a finished streaming call.
pub struct HttpCryptoOutput(StreamingResponse);

impl GuestCryptoOutput for HttpCryptoOutput {
    fn read(&self, max_len: u64) -> Option<Vec<u8>> {
        self.0.read(max_len)
    }
}
