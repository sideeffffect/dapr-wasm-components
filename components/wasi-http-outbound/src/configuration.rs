//! Configuration — https://docs.dapr.io/reference/api/configuration_api/

use std::collections::BTreeMap;

use serde::Deserialize;
use wstd::http::Method;

use crate::exports::configuration::{ConfigurationItem, Guest};
use crate::sidecar::{push_metadata_query, seg, with_query, Sidecar};
use crate::types::{Error, Metadata};
use crate::Component;

impl Guest for Component {
    fn get(
        store_name: String,
        keys: Vec<String>,
        metadata: Metadata,
    ) -> Result<Vec<(String, ConfigurationItem)>, Error> {
        let sidecar = Sidecar::from_env();
        let mut query = Vec::new();
        for key in &keys {
            query.push(format!("key={}", urlencoding::encode(key)));
        }
        push_metadata_query(&mut query, &metadata);
        let path = with_query(format!("/v1.0/configuration/{}", seg(&store_name)), query);

        let response = sidecar.expect_success(Method::GET, &path, &[], Vec::new())?;

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
            .map_err(|e| Error::Internal(format!("unexpected configuration response: {e}")))?;
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
    ) -> Result<String, Error> {
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

        let response = sidecar.expect_success(Method::GET, &path, &[], Vec::new())?;

        #[derive(Deserialize)]
        struct SubscribeResponse {
            id: String,
        }
        let parsed: SubscribeResponse = serde_json::from_slice(&response.body)
            .map_err(|e| Error::Internal(format!("unexpected subscribe response: {e}")))?;
        Ok(parsed.id)
    }

    fn unsubscribe(store_name: String, id: String) -> Result<(), Error> {
        let sidecar = Sidecar::from_env();
        let path = format!(
            "/v1.0/configuration/{}/{}/unsubscribe",
            seg(&store_name),
            seg(&id)
        );
        sidecar.expect_success(Method::GET, &path, &[], Vec::new())?;
        Ok(())
    }
}
