//! Cryptography (alpha) — https://docs.dapr.io/reference/api/cryptography_api/
//! One-shot (non-streaming) encrypt/decrypt.

use wstd::http::Method;

use crate::exports::crypto::{EncryptOptions, Guest};
use crate::sidecar::{seg, Sidecar};
use crate::types::Error;
use crate::Component;

impl Guest for Component {
    fn encrypt(
        component_name: String,
        data: Vec<u8>,
        options: EncryptOptions,
    ) -> Result<Vec<u8>, Error> {
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

        let response = sidecar.expect_success(Method::PUT, &path, &headers, data)?;
        Ok(response.body)
    }

    fn decrypt(
        component_name: String,
        data: Vec<u8>,
        key_name: Option<String>,
    ) -> Result<Vec<u8>, Error> {
        let sidecar = Sidecar::from_env();
        let path = format!("/v1.0-alpha1/crypto/{}/decrypt", seg(&component_name));

        let mut headers = vec![(
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        )];
        if let Some(key) = key_name {
            headers.push(("dapr-key-name".to_string(), key));
        }

        let response = sidecar.expect_success(Method::PUT, &path, &headers, data)?;
        Ok(response.body)
    }
}
