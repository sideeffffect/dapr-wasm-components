//! Secrets over gRPC — `GetSecret`, `GetBulkSecret`.
//!
//! A missing secret: daprd reports a store's "no such secret" as a generic
//! `ERR_SECRET_GET` (`internal`) — the same as over HTTP, where it is a 500.
//! Most stores (e.g. `local.file`: `secret <key> not found`) put "not found"
//! in the store error that daprd appends after
//! `failed getting secret with key <key> from secret store <store>: `, so
//! `get-secret` maps that to `secret-not-found`; any other `internal` is a
//! store backend failure and still traps. A store that returns no data at all
//! (a 204 over HTTP) arrives here as an empty map, also `secret-not-found`.

use std::collections::HashMap;

use crate::exports::secrets::{GetSecretError, Guest, Secret, SecretsError};
use crate::proto::runtime as pb;
use crate::sidecar::{classify, metadata_map, DaprFailure, Sidecar};
use crate::types::Metadata;
use crate::Component;

fn secrets_error(f: DaprFailure) -> SecretsError {
    if f.is_permission() {
        SecretsError::PermissionDenied(f.message)
    } else {
        SecretsError::StoreNotFound(f.message)
    }
}

/// Map a failed `get-secret` call: a store "not found" (see the module docs)
/// is `secret-not-found`; anything else goes through [`classify`] (tier-3
/// codes trap) and the secrets setup/config error.
fn get_secret_error(status: tonic::Status, store_name: &str) -> GetSecretError {
    if status.code() == tonic::Code::Internal
        && store_error_is_not_found(status.message(), store_name)
    {
        GetSecretError::SecretNotFound
    } else {
        GetSecretError::Secrets(secrets_error(classify(status)))
    }
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
            .unary_status(
                pb::GetSecretRequest {
                    store_name: store_name.clone(),
                    key,
                    metadata: metadata_map(&metadata),
                },
                |mut client, request| async move { client.get_secret(request).await },
            )
            .map_err(|status| get_secret_error(status, &store_name))?;
        if response.data.is_empty() {
            return Err(GetSecretError::SecretNotFound);
        }
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
