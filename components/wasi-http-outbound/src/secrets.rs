//! Secrets — https://docs.dapr.io/reference/api/secrets_api/

use std::collections::BTreeMap;

use wstd::http::Method;

use crate::exports::secrets::{Guest, Secret};
use crate::sidecar::{push_metadata_query, seg, with_query, Sidecar};
use crate::types::{Error, Metadata};
use crate::Component;

impl Guest for Component {
    fn get_secret(
        store_name: String,
        key: String,
        metadata: Metadata,
    ) -> Result<Option<Secret>, Error> {
        let sidecar = Sidecar::from_env();
        let mut query = Vec::new();
        push_metadata_query(&mut query, &metadata);
        let path = with_query(
            format!("/v1.0/secrets/{}/{}", seg(&store_name), seg(&key)),
            query,
        );

        let response = sidecar.expect_success(Method::GET, &path, &[], Vec::new())?;
        // 204 = secret not found.
        if response.status == 204 {
            return Ok(None);
        }
        let secret: BTreeMap<String, String> = serde_json::from_slice(&response.body)
            .map_err(|e| Error::Internal(format!("unexpected secret response: {e}")))?;
        Ok(Some(secret.into_iter().collect()))
    }

    fn get_bulk_secret(
        store_name: String,
        metadata: Metadata,
    ) -> Result<Vec<(String, Secret)>, Error> {
        let sidecar = Sidecar::from_env();
        let mut query = Vec::new();
        push_metadata_query(&mut query, &metadata);
        let path = with_query(format!("/v1.0/secrets/{}/bulk", seg(&store_name)), query);

        let response = sidecar.expect_success(Method::GET, &path, &[], Vec::new())?;
        let secrets: BTreeMap<String, BTreeMap<String, String>> =
            serde_json::from_slice(&response.body)
                .map_err(|e| Error::Internal(format!("unexpected bulk secret response: {e}")))?;
        Ok(secrets
            .into_iter()
            .map(|(name, secret)| (name, secret.into_iter().collect()))
            .collect())
    }
}
