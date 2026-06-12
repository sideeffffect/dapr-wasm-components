//! Configuration (read side) over gRPC — the stable `GetConfiguration`
//! (not the `Alpha1` variant). Subscriptions are server-streaming and out
//! of scope for this outbound interface, matching the WIT contract.

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
}
