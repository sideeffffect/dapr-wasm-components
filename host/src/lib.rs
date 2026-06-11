pub mod backend;
pub mod callback;
pub mod runner;

use wasmtime::component::ResourceTable;
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

use crate::backend::DaprBackend;

pub mod bindings {
    wasmtime::component::bindgen!({
        path: "../wit",
        world: "app",
        imports: { default: async | trappable },
        exports: { default: async },
    });
}

use bindings::dapr::client::{
    bindings as wit_bindings, configuration, invocation, pubsub, secrets, state, types,
};

/// Per-instance store data: WASI context plus the Dapr backend.
pub struct Ctx {
    wasi: WasiCtx,
    table: ResourceTable,
    backend: Box<dyn DaprBackend>,
}

impl Ctx {
    pub fn new(backend: Box<dyn DaprBackend>) -> Self {
        Self {
            wasi: WasiCtxBuilder::new().inherit_stdio().inherit_env().build(),
            table: ResourceTable::new(),
            backend,
        }
    }

    pub fn backend(&self) -> &dyn DaprBackend {
        self.backend.as_ref()
    }
}

impl WasiView for Ctx {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl types::Host for Ctx {}

impl state::Host for Ctx {
    async fn get(
        &mut self,
        store_name: String,
        key: String,
        metadata: types::Metadata,
    ) -> wasmtime::Result<Result<state::GetStateResponse, types::Error>> {
        Ok(self.backend.get_state(store_name, key, metadata).await)
    }

    async fn save(
        &mut self,
        store_name: String,
        key: String,
        value: Vec<u8>,
        etag: Option<String>,
        metadata: types::Metadata,
        options: Option<state::StateOptions>,
    ) -> wasmtime::Result<Result<(), types::Error>> {
        Ok(self
            .backend
            .save_state(store_name, key, value, etag, metadata, options)
            .await)
    }

    async fn save_bulk(
        &mut self,
        store_name: String,
        items: Vec<state::StateItem>,
    ) -> wasmtime::Result<Result<(), types::Error>> {
        Ok(self.backend.save_bulk_state(store_name, items).await)
    }

    async fn delete(
        &mut self,
        store_name: String,
        key: String,
        metadata: types::Metadata,
    ) -> wasmtime::Result<Result<(), types::Error>> {
        Ok(self.backend.delete_state(store_name, key, metadata).await)
    }
}

impl pubsub::Host for Ctx {
    async fn publish(
        &mut self,
        pubsub_name: String,
        topic: String,
        data: Vec<u8>,
        data_content_type: String,
        metadata: types::Metadata,
    ) -> wasmtime::Result<Result<(), types::Error>> {
        Ok(self
            .backend
            .publish(pubsub_name, topic, data, data_content_type, metadata)
            .await)
    }
}

impl secrets::Host for Ctx {
    async fn get_secret(
        &mut self,
        store_name: String,
        key: String,
    ) -> wasmtime::Result<Result<secrets::Secret, types::Error>> {
        Ok(self.backend.get_secret(store_name, key).await)
    }

    async fn get_bulk_secret(
        &mut self,
        store_name: String,
        metadata: types::Metadata,
    ) -> wasmtime::Result<Result<Vec<(String, secrets::Secret)>, types::Error>> {
        Ok(self.backend.get_bulk_secret(store_name, metadata).await)
    }
}

impl wit_bindings::Host for Ctx {
    async fn invoke_binding(
        &mut self,
        name: String,
        operation: String,
        data: Vec<u8>,
        metadata: types::Metadata,
    ) -> wasmtime::Result<Result<wit_bindings::InvokeBindingResponse, types::Error>> {
        Ok(self
            .backend
            .invoke_binding(name, operation, data, metadata)
            .await)
    }
}

impl invocation::Host for Ctx {
    async fn invoke(
        &mut self,
        app_id: String,
        method: String,
        data: Vec<u8>,
    ) -> wasmtime::Result<Result<invocation::InvocationResponse, types::Error>> {
        Ok(self.backend.invoke_service(app_id, method, data).await)
    }
}

impl configuration::Host for Ctx {
    async fn get(
        &mut self,
        store_name: String,
        keys: Vec<String>,
        metadata: types::Metadata,
    ) -> wasmtime::Result<Result<Vec<(String, configuration::ConfigurationItem)>, types::Error>>
    {
        Ok(self
            .backend
            .get_configuration(store_name, keys, metadata)
            .await)
    }
}
