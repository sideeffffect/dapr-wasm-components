//! Secrets — https://docs.dapr.io/reference/api/secrets_api/

use std::collections::BTreeMap;

use wstd::http::Method;

use crate::exports::secrets::{Guest, Secret, SecretsError};
use crate::sidecar::{push_metadata_query, seg, with_query, DaprFailure, Sidecar};
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

impl Guest for Component {
    fn get_secret(
        store_name: String,
        key: String,
        metadata: Metadata,
    ) -> Result<Option<Secret>, SecretsError> {
        let sidecar = Sidecar::from_env();
        let mut query = Vec::new();
        push_metadata_query(&mut query, &metadata);
        let path = with_query(
            format!("/v1.0/secrets/{}/{}", seg(&store_name), seg(&key)),
            query,
        );

        let response = sidecar
            .expect_success(Method::GET, &path, &[], Vec::new())
            .map_err(secrets_error)?;
        // 204 = secret not found.
        if response.status == 204 {
            return Ok(None);
        }
        let secret: BTreeMap<String, String> = serde_json::from_slice(&response.body)
            .unwrap_or_else(|e| panic!("unexpected secret response: {e}"));
        Ok(Some(secret.into_iter().collect()))
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
