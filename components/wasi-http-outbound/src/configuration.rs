//! Configuration — https://docs.dapr.io/reference/api/configuration_api/

use std::collections::BTreeMap;

use serde::Deserialize;
use wstd::http::Method;

use crate::exports::configuration::{
    ConfigurationError, ConfigurationItem, Guest, UnsubscribeError,
};
use crate::sidecar::{push_metadata_query, seg, with_query, DaprFailure, Sidecar};
use crate::types::Metadata;
use crate::Component;

/// Map a recoverable failure to the configuration setup/config error.
fn configuration_error(f: DaprFailure) -> ConfigurationError {
    if f.is_permission() {
        ConfigurationError::PermissionDenied(f.message)
    } else {
        ConfigurationError::StoreNotFound(f.message)
    }
}

/// Map a recoverable failure of an unsubscribe.
fn unsubscribe_error(f: DaprFailure) -> UnsubscribeError {
    if f.status == 404 {
        UnsubscribeError::NoSuchSubscription(f.message)
    } else {
        UnsubscribeError::Configuration(configuration_error(f))
    }
}

impl Guest for Component {
    fn get(
        store_name: String,
        keys: Vec<String>,
        metadata: Metadata,
    ) -> Result<Vec<(String, ConfigurationItem)>, ConfigurationError> {
        let sidecar = Sidecar::from_env();
        let mut query = Vec::new();
        for key in &keys {
            query.push(format!("key={}", urlencoding::encode(key)));
        }
        push_metadata_query(&mut query, &metadata);
        let path = with_query(format!("/v1.0/configuration/{}", seg(&store_name)), query);

        let response = sidecar
            .expect_success(Method::GET, &path, &[], Vec::new())
            .map_err(configuration_error)?;

        #[derive(Deserialize)]
        struct ItemJson {
            #[serde(default)]
            value: String,
            #[serde(default)]
            version: String,
            #[serde(default)]
            metadata: BTreeMap<String, String>,
        }
        let items: BTreeMap<String, ItemJson> = serde_json::from_slice(&response.body)
            .unwrap_or_else(|e| panic!("unexpected configuration response: {e}"));
        Ok(items
            .into_iter()
            .map(|(key, item)| {
                (
                    key,
                    ConfigurationItem {
                        value: item.value,
                        version: item.version,
                        metadata: item.metadata.into_iter().collect(),
                    },
                )
            })
            .collect())
    }

    fn subscribe(
        store_name: String,
        keys: Vec<String>,
        metadata: Metadata,
    ) -> Result<String, ConfigurationError> {
        let sidecar = Sidecar::from_env();
        let mut query = Vec::new();
        for key in &keys {
            query.push(format!("key={}", urlencoding::encode(key)));
        }
        push_metadata_query(&mut query, &metadata);
        let path = with_query(
            format!("/v1.0/configuration/{}/subscribe", seg(&store_name)),
            query,
        );

        let response = sidecar
            .expect_success(Method::GET, &path, &[], Vec::new())
            .map_err(configuration_error)?;

        #[derive(Deserialize)]
        struct SubscribeResponse {
            id: String,
        }
        let parsed: SubscribeResponse = serde_json::from_slice(&response.body)
            .unwrap_or_else(|e| panic!("unexpected subscribe response: {e}"));
        Ok(parsed.id)
    }

    fn unsubscribe(store_name: String, id: String) -> Result<(), UnsubscribeError> {
        let sidecar = Sidecar::from_env();
        let path = format!(
            "/v1.0/configuration/{}/{}/unsubscribe",
            seg(&store_name),
            seg(&id)
        );
        sidecar
            .expect_success(Method::GET, &path, &[], Vec::new())
            .map_err(unsubscribe_error)?;
        Ok(())
    }
}
