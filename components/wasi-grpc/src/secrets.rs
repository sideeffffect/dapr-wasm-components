//! Secrets over gRPC — `GetSecret`, `GetBulkSecret`.
//!
//! Divergence: a missing secret surfaces as whatever status daprd returns
//! (typically `internal`, ERR_SECRET_GET) — the gRPC API has no analogue
//! of the HTTP 204 that lets the wasi-http provider report `not-found`.

use std::collections::HashMap;

use crate::exports::secrets::{Guest, Secret};
use crate::proto::runtime as pb;
use crate::sidecar::{metadata_map, Sidecar};
use crate::types::{Error, Metadata};
use crate::Component;

/// Proto maps have no order — sort the secret entries for determinism
/// (the HTTP provider does the same via `BTreeMap`).
fn secret_pairs(map: HashMap<String, String>) -> Secret {
    let mut pairs: Vec<(String, String)> = map.into_iter().collect();
    pairs.sort();
    pairs
}

impl Guest for Component {
    fn get_secret(store_name: String, key: String, metadata: Metadata) -> Result<Secret, Error> {
        let sidecar = Sidecar::from_env()?;
        let response = sidecar.unary(
            pb::GetSecretRequest {
                store_name,
                key,
                metadata: metadata_map(&metadata),
            },
            |mut client, request| async move { client.get_secret(request).await },
        )?;
        Ok(secret_pairs(response.data))
    }

    fn get_bulk_secret(
        store_name: String,
        metadata: Metadata,
    ) -> Result<Vec<(String, Secret)>, Error> {
        let sidecar = Sidecar::from_env()?;
        let response = sidecar.unary(
            pb::GetBulkSecretRequest {
                store_name,
                metadata: metadata_map(&metadata),
            },
            |mut client, request| async move { client.get_bulk_secret(request).await },
        )?;
        let mut secrets: Vec<(String, Secret)> = response
            .data
            .into_iter()
            .map(|(name, secret)| (name, secret_pairs(secret.secrets)))
            .collect();
        secrets.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(secrets)
    }
}
