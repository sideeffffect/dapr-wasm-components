//! Secrets — https://docs.dapr.io/reference/api/secrets_api/

use std::collections::BTreeMap;

use wstd::http::Method;

use crate::exports::secrets::{GetSecretError, Guest, Secret, SecretsError};
use crate::sidecar::{check_status, push_metadata_query, seg, with_query, DaprFailure, Sidecar};
use crate::types::Metadata;
use crate::Component;

/// Map a recoverable failure to the secrets setup/config error.
fn secrets_error(f: DaprFailure) -> SecretsError {
    if f.is_permission() {
        SecretsError::PermissionDenied(f.message)
    } else {
        SecretsError::StoreNotFound(f.message)
    }
}

/// Map a recoverable failure of `get-secret` through the secrets setup/config
/// error. (`secret-not-found` is produced by [`is_secret_not_found`] and the
/// 204 branch.)
fn get_secret_error(f: DaprFailure) -> GetSecretError {
    GetSecretError::Secrets(secrets_error(f))
}

/// daprd reports a store's "no such secret" as a generic `ERR_SECRET_GET`
/// (HTTP 500) — only a store returning no data at all yields a 204. Most
/// stores (e.g. `local.file`: `secret <key> not found`) put "not found" in
/// the store error that daprd appends after
/// `failed getting secret with key <key> from secret store <store>: `, so
/// that suffix is the only signal. Any other `ERR_SECRET_GET` is a store
/// backend failure and still traps.
fn is_secret_not_found(status: u16, body: &[u8], store_name: &str) -> bool {
    if status != 500 {
        return false;
    }
    let Ok(body) = serde_json::from_slice::<serde_json::Value>(body) else {
        return false;
    };
    if body.get("errorCode").and_then(|c| c.as_str()) != Some("ERR_SECRET_GET") {
        return false;
    }
    body.get("message")
        .and_then(|m| m.as_str())
        .is_some_and(|message| store_error_is_not_found(message, store_name))
}

/// Whether the store error daprd appended to an `ERR_SECRET_GET` message
/// says "not found" (case-insensitive).
fn store_error_is_not_found(message: &str, store_name: &str) -> bool {
    let marker = format!("from secret store {store_name}: ");
    message.find(&marker).is_some_and(|at| {
        message[at + marker.len()..]
            .to_ascii_lowercase()
            .contains("not found")
    })
}

impl Guest for Component {
    fn get_secret(
        store_name: String,
        key: String,
        metadata: Metadata,
    ) -> Result<Secret, GetSecretError> {
        let sidecar = Sidecar::from_env();
        let mut query = Vec::new();
        push_metadata_query(&mut query, &metadata);
        let path = with_query(
            format!("/v1.0/secrets/{}/{}", seg(&store_name), seg(&key)),
            query,
        );

        let response = sidecar.request(Method::GET, &path, &[], Vec::new());
        if is_secret_not_found(response.status, &response.body, &store_name) {
            return Err(GetSecretError::SecretNotFound);
        }
        let response = check_status(response).map_err(get_secret_error)?;
        // 204 = the store returned no data for the secret.
        if response.status == 204 {
            return Err(GetSecretError::SecretNotFound);
        }
        let secret: BTreeMap<String, String> = serde_json::from_slice(&response.body)
            .unwrap_or_else(|e| panic!("unexpected secret response: {e}"));
        Ok(secret.into_iter().collect())
    }

    fn get_bulk_secret(
        store_name: String,
        metadata: Metadata,
    ) -> Result<Vec<(String, Secret)>, SecretsError> {
        let sidecar = Sidecar::from_env();
        let mut query = Vec::new();
        push_metadata_query(&mut query, &metadata);
        let path = with_query(format!("/v1.0/secrets/{}/bulk", seg(&store_name)), query);

        let response = sidecar
            .expect_success(Method::GET, &path, &[], Vec::new())
            .map_err(secrets_error)?;
        let secrets: BTreeMap<String, BTreeMap<String, String>> =
            serde_json::from_slice(&response.body)
                .unwrap_or_else(|e| panic!("unexpected bulk secret response: {e}"));
        Ok(secrets
            .into_iter()
            .map(|(name, secret)| (name, secret.into_iter().collect()))
            .collect())
    }
}
