//! Secrets over gRPC — `GetSecret`, `GetBulkSecret`.
//!
//! Divergence: a missing secret surfaces as whatever status daprd returns
//! (typically `internal`, ERR_SECRET_GET) — the gRPC API has no analogue
//! of the HTTP 204 that lets the wasi-http provider report the absence as
//! `secret-not-found`. So `get-secret` here only ever returns the secret or
//! a setup/config error, never `secret-not-found`.

use std::collections::HashMap;

use crate::exports::secrets::{GetSecretError, Guest, Secret, SecretsError};
use crate::proto::runtime as pb;
use crate::sidecar::{metadata_map, DaprFailure, Sidecar};
use crate::types::Metadata;
use crate::Component;

fn secrets_error(f: DaprFailure) -> SecretsError {
    if f.is_permission() {
        SecretsError::PermissionDenied(f.message)
    } else {
        SecretsError::StoreNotFound(f.message)
    }
}

/// Map a recoverable failure of `get-secret` through the secrets setup/config
/// error. The gRPC API cannot signal absence distinctly, so the
/// `secret-not-found` case is never produced here.
fn get_secret_error(f: DaprFailure) -> GetSecretError {
    GetSecretError::Secrets(secrets_error(f))
}

/// Proto maps have no order — sort the secret entries for determinism
/// (the HTTP provider does the same via `BTreeMap`).
fn secret_pairs(map: HashMap<String, String>) -> Secret {
    let mut pairs: Vec<(String, String)> = map.into_iter().collect();
    pairs.sort();
    pairs
}

impl Guest for Component {
    fn get_secret(
        store_name: String,
        key: String,
        metadata: Metadata,
    ) -> Result<Secret, GetSecretError> {
        let sidecar = Sidecar::from_env();
        let response = sidecar
            .unary(
                pb::GetSecretRequest {
                    store_name,
                    key,
                    metadata: metadata_map(&metadata),
                },
                |mut client, request| async move { client.get_secret(request).await },
            )
            .map_err(get_secret_error)?;
        Ok(secret_pairs(response.data))
    }

    fn get_bulk_secret(
        store_name: String,
        metadata: Metadata,
    ) -> Result<Vec<(String, Secret)>, SecretsError> {
        let sidecar = Sidecar::from_env();
        let response = sidecar
            .unary(
                pb::GetBulkSecretRequest {
                    store_name,
                    metadata: metadata_map(&metadata),
                },
                |mut client, request| async move { client.get_bulk_secret(request).await },
            )
            .map_err(secrets_error)?;
        let mut secrets: Vec<(String, Secret)> = response
            .data
            .into_iter()
            .map(|(name, secret)| (name, secret_pairs(secret.secrets)))
            .collect();
        secrets.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(secrets)
    }
}
