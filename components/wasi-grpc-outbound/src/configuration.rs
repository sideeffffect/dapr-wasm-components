//! Configuration over gRPC — the stable `GetConfiguration` (not the `Alpha1`
//! variant) for the read side.
//!
//! `subscribe` is unsupported here (it traps, so a `subscription` handle never
//! exists and there is nothing to unsubscribe): gRPC delivers configuration
//! updates over a *server stream* on the `Dapr` service itself (the first
//! message carries the subscription id, subsequent ones carry updates), not
//! through the `AppCallback` app channel. That streaming-delivery model does
//! not map onto the sync, app-channel-based `configuration-callback` inbound
//! interface this provider drives, so we surface a clear error. Use the
//! wasi-http provider for configuration subscriptions.

use crate::exports::configuration::{
    ConfigurationError, ConfigurationItem, Guest, GuestSubscription, Subscription,
};
use crate::proto::common as pbc;
use crate::proto::runtime as pb;
use crate::sidecar::{metadata_map, metadata_pairs, DaprFailure, Sidecar};
use crate::types::Metadata;
use crate::Component;

fn configuration_error(f: DaprFailure) -> ConfigurationError {
    if f.is_permission() {
        ConfigurationError::PermissionDenied(f.message)
    } else {
        ConfigurationError::StoreNotFound(f.message)
    }
}

/// The `subscription` resource of this provider. It has no values: `subscribe`
/// traps before it could create one, and the empty enum states that in the
/// type (no `id`, no drop to implement).
pub enum NoSubscription {}

impl GuestSubscription for NoSubscription {
    fn id(&self) -> String {
        match *self {}
    }
}

fn item(item: pbc::ConfigurationItem) -> ConfigurationItem {
    ConfigurationItem {
        value: item.value,
        version: item.version,
        metadata: metadata_pairs(item.metadata),
    }
}

impl Guest for Component {
    type Subscription = NoSubscription;

    fn get(
        store_name: String,
        keys: Vec<String>,
        metadata: Metadata,
    ) -> Result<Vec<(String, ConfigurationItem)>, ConfigurationError> {
        let sidecar = Sidecar::from_env();
        let response = sidecar
            .unary(
                pb::GetConfigurationRequest {
                    store_name,
                    keys,
                    metadata: metadata_map(&metadata),
                },
                |mut client, request| async move { client.get_configuration(request).await },
            )
            .map_err(configuration_error)?;
        // Proto maps have no order; sort by key for determinism (the HTTP
        // provider gets this implicitly from its BTreeMap).
        let mut items: Vec<(String, ConfigurationItem)> = response
            .items
            .into_iter()
            .map(|(key, value)| (key, item(value)))
            .collect();
        items.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(items)
    }

    fn subscribe(
        _store_name: String,
        _keys: Vec<String>,
        _metadata: Metadata,
    ) -> Result<Subscription, ConfigurationError> {
        panic!(
            "configuration subscribe is not supported by the wasi-grpc provider: gRPC delivers \
             updates over a server stream on the Dapr service, not the AppCallback app channel; \
             use the wasi-http provider"
        );
    }
}
