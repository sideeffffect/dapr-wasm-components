//! Secrets over gRPC — `GetSecret`, `GetBulkSecret`.
//!
//! Divergence: a missing secret surfaces as whatever status daprd returns
//! (typically `internal`, ERR_SECRET_GET) — the gRPC API has no analogue
//! of the HTTP 204 that lets the wasi-http provider report the absence as
//! `none`. So `get-secret` here only ever returns `some`/error, never `none`.

use std::collections::HashMap;

use crate::exports::secrets::{Guest, Secret, SecretsError};
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
    ) -> Result<Option<Secret>, SecretsError> {
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
            .map_err(secrets_error)?;
        Ok(Some(secret_pairs(response.data)))
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
