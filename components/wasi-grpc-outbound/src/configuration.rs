//! Configuration over gRPC — the stable `GetConfiguration` (not the `Alpha1`
//! variant) for the read side.
//!
//! `subscribe`/`unsubscribe` are unsupported here: gRPC delivers configuration
//! updates over a *server stream* on the `Dapr` service itself (the first
//! message carries the subscription id, subsequent ones carry updates), not
//! through the `AppCallback` app channel. That streaming-delivery model does
//! not map onto the sync, app-channel-based `configuration-callback` inbound
//! interface this provider drives, so we surface a clear error. Use the
//! wasi-http provider for configuration subscriptions.

use crate::exports::configuration::{ConfigurationItem, Guest};
use crate::proto::common as pbc;
use crate::proto::runtime as pb;
use crate::sidecar::{metadata_map, metadata_pairs, Sidecar};
use crate::types::{Error, Metadata};
use crate::Component;

fn item(item: pbc::ConfigurationItem) -> ConfigurationItem {
    ConfigurationItem {
        value: item.value,
        version: item.version,
        metadata: metadata_pairs(item.metadata),
    }
}

impl Guest for Component {
    fn get(
        store_name: String,
        keys: Vec<String>,
        metadata: Metadata,
    ) -> Result<Vec<(String, ConfigurationItem)>, Error> {
        let sidecar = Sidecar::from_env()?;
        let response = sidecar.unary(
            pb::GetConfigurationRequest {
                store_name,
                keys,
                metadata: metadata_map(&metadata),
            },
            |mut client, request| async move { client.get_configuration(request).await },
        )?;
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
    ) -> Result<String, Error> {
        Err(Error::Other(
            "configuration subscribe is not supported by the wasi-grpc provider: gRPC delivers \
             updates over a server stream on the Dapr service, not the AppCallback app channel; \
             use the wasi-http provider"
                .to_string(),
        ))
    }

    fn unsubscribe(_store_name: String, _id: String) -> Result<(), Error> {
        Err(Error::Other(
            "configuration unsubscribe is not supported by the wasi-grpc provider; \
             use the wasi-http provider"
                .to_string(),
        ))
    }
}
