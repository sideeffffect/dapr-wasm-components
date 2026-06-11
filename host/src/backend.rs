pub mod memory;
pub mod sidecar;

use async_trait::async_trait;

use crate::bindings::dapr::client::{
    bindings as wit_bindings, configuration, invocation, secrets, state, types,
};

/// The bridge between the `dapr:client` WIT interfaces and an actual Dapr
/// implementation. The WIT functions are sync from the guest's point of
/// view; the host awaits these futures while the guest blocks.
#[async_trait]
pub trait DaprBackend: Send {
    async fn get_state(
        &mut self,
        store_name: String,
        key: String,
        metadata: types::Metadata,
    ) -> Result<state::GetStateResponse, types::Error>;

    async fn save_state(
        &mut self,
        store_name: String,
        key: String,
        value: Vec<u8>,
        etag: Option<String>,
        metadata: types::Metadata,
        options: Option<state::StateOptions>,
    ) -> Result<(), types::Error>;

    async fn save_bulk_state(
        &mut self,
        store_name: String,
        items: Vec<state::StateItem>,
    ) -> Result<(), types::Error>;

    async fn delete_state(
        &mut self,
        store_name: String,
        key: String,
        metadata: types::Metadata,
    ) -> Result<(), types::Error>;

    async fn publish(
        &mut self,
        pubsub_name: String,
        topic: String,
        data: Vec<u8>,
        data_content_type: String,
        metadata: types::Metadata,
    ) -> Result<(), types::Error>;

    async fn get_secret(
        &mut self,
        store_name: String,
        key: String,
    ) -> Result<secrets::Secret, types::Error>;

    async fn get_bulk_secret(
        &mut self,
        store_name: String,
        metadata: types::Metadata,
    ) -> Result<Vec<(String, secrets::Secret)>, types::Error>;

    async fn invoke_binding(
        &mut self,
        name: String,
        operation: String,
        data: Vec<u8>,
        metadata: types::Metadata,
    ) -> Result<wit_bindings::InvokeBindingResponse, types::Error>;

    async fn invoke_service(
        &mut self,
        app_id: String,
        method: String,
        data: Vec<u8>,
    ) -> Result<invocation::InvocationResponse, types::Error>;

    async fn get_configuration(
        &mut self,
        store_name: String,
        keys: Vec<String>,
        metadata: types::Metadata,
    ) -> Result<Vec<(String, configuration::ConfigurationItem)>, types::Error>;
}
