//! `DaprBackend` implementation backed by the Dapr Rust SDK, talking gRPC
//! to the Dapr sidecar.

use std::collections::HashMap;

use async_trait::async_trait;
use dapr_sdk::client::TonicClient;
use dapr_sdk::dapr::proto::common::v1 as common_v1;

use crate::backend::DaprBackend;
use crate::bindings::dapr::client::{
    bindings as wit_bindings, configuration, invocation, secrets, state, types,
};

pub struct SidecarBackend {
    client: dapr_sdk::Client<TonicClient>,
}

impl SidecarBackend {
    /// Connect to the Dapr sidecar, resolving the address like other Dapr
    /// SDKs: `DAPR_GRPC_ENDPOINT` wins, then `DAPR_GRPC_PORT` on localhost,
    /// then the default `http://127.0.0.1:50001`.
    pub async fn connect() -> Result<Self, dapr_sdk::error::Error> {
        let (addr, port) = resolve_sidecar_address();
        let client = dapr_sdk::Client::<TonicClient>::connect_with_port(addr, port).await?;
        Ok(Self { client })
    }
}

/// Returns `(scheme://host, port)` for `Client::connect_with_port`.
fn resolve_sidecar_address() -> (String, String) {
    if let Ok(endpoint) = std::env::var("DAPR_GRPC_ENDPOINT") {
        if let Some((addr, port)) = endpoint.rsplit_once(':') {
            return (addr.to_string(), port.to_string());
        }
    }
    let port = std::env::var("DAPR_GRPC_PORT").unwrap_or_else(|_| "50001".to_string());
    ("http://127.0.0.1".to_string(), port)
}

fn to_hashmap(metadata: types::Metadata) -> Option<HashMap<String, String>> {
    if metadata.is_empty() {
        None
    } else {
        Some(metadata.into_iter().collect())
    }
}

fn from_hashmap(metadata: HashMap<String, String>) -> types::Metadata {
    metadata.into_iter().collect()
}

fn to_wit_error(error: dapr_sdk::error::Error) -> types::Error {
    use dapr_sdk::error::Error;
    match error {
        Error::TransportError => {
            types::Error::Unavailable("cannot reach the Dapr sidecar (transport error)".to_string())
        }
        // The SDK keeps the gRPC status private, so the code cannot be
        // inspected; the Debug representation still carries code + message.
        Error::GrpcError(e) => types::Error::Internal(format!("{e:?}")),
        other => types::Error::Other(format!("{other:?}")),
    }
}

fn to_sdk_options(options: state::StateOptions) -> common_v1::StateOptions {
    common_v1::StateOptions {
        concurrency: match options.concurrency {
            state::Concurrency::Unspecified => 0,
            state::Concurrency::FirstWrite => 1,
            state::Concurrency::LastWrite => 2,
        },
        consistency: match options.consistency {
            state::Consistency::Unspecified => 0,
            state::Consistency::Eventual => 1,
            state::Consistency::Strong => 2,
        },
    }
}

#[async_trait]
impl DaprBackend for SidecarBackend {
    async fn get_state(
        &mut self,
        store_name: String,
        key: String,
        metadata: types::Metadata,
    ) -> Result<state::GetStateResponse, types::Error> {
        let response = self
            .client
            .get_state(store_name, key, to_hashmap(metadata))
            .await
            .map_err(to_wit_error)?;
        Ok(state::GetStateResponse {
            data: response.data,
            etag: if response.etag.is_empty() {
                None
            } else {
                Some(response.etag)
            },
            metadata: from_hashmap(response.metadata),
        })
    }

    async fn save_state(
        &mut self,
        store_name: String,
        key: String,
        value: Vec<u8>,
        etag: Option<String>,
        metadata: types::Metadata,
        options: Option<state::StateOptions>,
    ) -> Result<(), types::Error> {
        self.client
            .save_state(
                store_name,
                key,
                value,
                etag.map(|value| common_v1::Etag { value }),
                to_hashmap(metadata),
                options.map(to_sdk_options),
            )
            .await
            .map_err(to_wit_error)
    }

    async fn save_bulk_state(
        &mut self,
        store_name: String,
        items: Vec<state::StateItem>,
    ) -> Result<(), types::Error> {
        let items: Vec<common_v1::StateItem> = items
            .into_iter()
            .map(|item| common_v1::StateItem {
                key: item.key,
                value: item.value,
                etag: item.etag.map(|value| common_v1::Etag { value }),
                metadata: item.metadata.into_iter().collect(),
                options: item.options.map(to_sdk_options),
            })
            .collect();
        self.client
            .save_bulk_states(store_name, items)
            .await
            .map_err(to_wit_error)
    }

    async fn delete_state(
        &mut self,
        store_name: String,
        key: String,
        metadata: types::Metadata,
    ) -> Result<(), types::Error> {
        self.client
            .delete_state(store_name, key, to_hashmap(metadata))
            .await
            .map_err(to_wit_error)
    }

    async fn publish(
        &mut self,
        pubsub_name: String,
        topic: String,
        data: Vec<u8>,
        data_content_type: String,
        metadata: types::Metadata,
    ) -> Result<(), types::Error> {
        self.client
            .publish_event(
                pubsub_name,
                topic,
                data_content_type,
                data,
                to_hashmap(metadata),
            )
            .await
            .map_err(to_wit_error)
    }

    async fn get_secret(
        &mut self,
        store_name: String,
        key: String,
    ) -> Result<secrets::Secret, types::Error> {
        let response = self
            .client
            .get_secret(store_name, key)
            .await
            .map_err(to_wit_error)?;
        Ok(from_hashmap(response.data))
    }

    async fn get_bulk_secret(
        &mut self,
        store_name: String,
        metadata: types::Metadata,
    ) -> Result<Vec<(String, secrets::Secret)>, types::Error> {
        let response = self
            .client
            .get_bulk_secret(store_name, to_hashmap(metadata))
            .await
            .map_err(to_wit_error)?;
        Ok(response
            .data
            .into_iter()
            .map(|(name, secret)| (name, from_hashmap(secret.secrets)))
            .collect())
    }

    async fn invoke_binding(
        &mut self,
        name: String,
        operation: String,
        data: Vec<u8>,
        metadata: types::Metadata,
    ) -> Result<wit_bindings::InvokeBindingResponse, types::Error> {
        let response = self
            .client
            .invoke_binding(name, data, operation, to_hashmap(metadata))
            .await
            .map_err(to_wit_error)?;
        Ok(wit_bindings::InvokeBindingResponse {
            data: response.data,
            metadata: from_hashmap(response.metadata),
        })
    }

    async fn invoke_service(
        &mut self,
        app_id: String,
        method: String,
        data: Vec<u8>,
    ) -> Result<invocation::InvocationResponse, types::Error> {
        let payload = prost_types::Any {
            type_url: String::new(),
            value: data,
        };
        let response = self
            .client
            .invoke_service(app_id, method, Some(payload))
            .await
            .map_err(to_wit_error)?;
        Ok(invocation::InvocationResponse {
            data: response.data.map(|any| any.value).unwrap_or_default(),
            content_type: response.content_type,
        })
    }

    async fn get_configuration(
        &mut self,
        store_name: String,
        keys: Vec<String>,
        metadata: types::Metadata,
    ) -> Result<Vec<(String, configuration::ConfigurationItem)>, types::Error> {
        let response = self
            .client
            .get_configuration(store_name, keys, to_hashmap(metadata))
            .await
            .map_err(to_wit_error)?;
        Ok(response
            .items
            .into_iter()
            .map(|(key, item)| {
                (
                    key,
                    configuration::ConfigurationItem {
                        value: item.value,
                        version: item.version,
                        metadata: from_hashmap(item.metadata),
                    },
                )
            })
            .collect())
    }
}
