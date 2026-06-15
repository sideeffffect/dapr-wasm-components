//! Cryptography (alpha) — https://docs.dapr.io/reference/api/cryptography_api/
//! One-shot (non-streaming) encrypt/decrypt.

use wstd::http::Method;

use crate::exports::crypto::{CryptoError, DecryptError, EncryptError, EncryptOptions, Guest};
use crate::sidecar::{seg, DaprFailure, Sidecar};
use crate::Component;

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

impl Guest for Component {
    fn encrypt(
        component_name: String,
        data: Vec<u8>,
        options: EncryptOptions,
    ) -> Result<Vec<u8>, EncryptError> {
        let sidecar = Sidecar::from_env();
        let path = format!("/v1.0-alpha1/crypto/{}/encrypt", seg(&component_name));

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

        let response = sidecar
            .expect_success(Method::PUT, &path, &headers, data)
            .map_err(encrypt_error)?;
        Ok(response.body)
    }

    fn decrypt(
        component_name: String,
        data: Vec<u8>,
        key_name: Option<String>,
    ) -> Result<Vec<u8>, DecryptError> {
        let sidecar = Sidecar::from_env();
        let path = format!("/v1.0-alpha1/crypto/{}/decrypt", seg(&component_name));

        let mut headers = vec![(
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        )];
        if let Some(key) = key_name {
            headers.push(("dapr-key-name".to_string(), key));
        }

        let response = sidecar
            .expect_success(Method::PUT, &path, &headers, data)
            .map_err(decrypt_error)?;
        Ok(response.body)
    }
}
