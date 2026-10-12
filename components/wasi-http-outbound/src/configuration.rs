//! Configuration — https://docs.dapr.io/reference/api/configuration_api/

use std::collections::BTreeMap;

use serde::Deserialize;
use wstd::http::Method;

use crate::exports::configuration::{
    ConfigurationError, ConfigurationItem, Guest, GuestSubscription, Subscription,
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

/// The provider side of the `subscription` resource: the sidecar's
/// subscription id plus the store it belongs to (the unsubscribe path needs
/// both).
pub struct ConfigurationSubscription {
    store_name: String,
    id: String,
}

impl GuestSubscription for ConfigurationSubscription {
    fn id(&self) -> String {
        self.id.clone()
    }
}

/// Dropping the handle unsubscribes. A destructor has no way to report a
/// failure, and trapping here would abort the app's instance while it is
/// only cleaning up, so every failure — an error status, or the sidecar
/// being unreachable — is logged to stderr and ignored. The cost is benign:
/// the sidecar keeps pushing updates for `id` to `configuration-callback`
/// (which the app can ignore) until the sidecar stops.
impl Drop for ConfigurationSubscription {
    fn drop(&mut self) {
        let sidecar = Sidecar::from_env();
        let path = format!(
            "/v1.0/configuration/{}/{}/unsubscribe",
            seg(&self.store_name),
            seg(&self.id)
        );
        match sidecar.try_request(Method::GET, &path, &[], Vec::new()) {
            Ok(result) if (200..300).contains(&result.status) => {}
            Ok(result) => eprintln!(
                "dapr-wasm-components: unsubscribing configuration subscription {} on {} \
                 failed: HTTP {}: {}",
                self.id,
                self.store_name,
                result.status,
                String::from_utf8_lossy(&result.body)
            ),
            Err(error) => eprintln!(
                "dapr-wasm-components: unsubscribing configuration subscription {} on {} \
                 failed: {error}",
                self.id, self.store_name
            ),
        }
    }
}

impl Guest for Component {
    type Subscription = ConfigurationSubscription;

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
    ) -> Result<Subscription, ConfigurationError> {
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
        Ok(Subscription::new(ConfigurationSubscription {
            store_name,
            id: parsed.id,
        }))
    }
}
