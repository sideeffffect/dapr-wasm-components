//! In-memory `DaprBackend` used by tests and `--backend memory` dry runs.
//! State lives in hash maps; published events and binding invocations are
//! recorded so tests can assert on them.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::backend::DaprBackend;
use crate::bindings::dapr::client::{
    bindings as wit_bindings, configuration, invocation, secrets, state, types,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedEvent {
    pub pubsub_name: String,
    pub topic: String,
    pub data: Vec<u8>,
    pub data_content_type: String,
}

#[derive(Debug, Default)]
pub struct MemoryStore {
    /// store-name -> key -> value
    pub state: HashMap<String, HashMap<String, Vec<u8>>>,
    pub published: Vec<PublishedEvent>,
    /// store-name -> secret-name -> entries
    pub secrets: HashMap<String, HashMap<String, Vec<(String, String)>>>,
    /// store-name -> key -> (value, version)
    pub configuration: HashMap<String, HashMap<String, (String, String)>>,
}

#[derive(Default, Clone)]
pub struct MemoryBackend {
    store: Arc<Mutex<MemoryStore>>,
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }

    /// Shared handle for inspecting the backend after a guest ran.
    pub fn store(&self) -> Arc<Mutex<MemoryStore>> {
        self.store.clone()
    }
}

#[async_trait]
impl DaprBackend for MemoryBackend {
    async fn get_state(
        &mut self,
        store_name: String,
        key: String,
        _metadata: types::Metadata,
    ) -> Result<state::GetStateResponse, types::Error> {
        let store = self.store.lock().unwrap();
        // Dapr semantics: a missing key is not an error, it returns empty data.
        let data = store
            .state
            .get(&store_name)
            .and_then(|kv| kv.get(&key))
            .cloned()
            .unwrap_or_default();
        Ok(state::GetStateResponse {
            data,
            etag: None,
            metadata: Vec::new(),
        })
    }

    async fn save_state(
        &mut self,
        store_name: String,
        key: String,
        value: Vec<u8>,
        _etag: Option<String>,
        _metadata: types::Metadata,
        _options: Option<state::StateOptions>,
    ) -> Result<(), types::Error> {
        let mut store = self.store.lock().unwrap();
        store
            .state
            .entry(store_name)
            .or_default()
            .insert(key, value);
        Ok(())
    }

    async fn save_bulk_state(
        &mut self,
        store_name: String,
        items: Vec<state::StateItem>,
    ) -> Result<(), types::Error> {
        let mut store = self.store.lock().unwrap();
        let kv = store.state.entry(store_name).or_default();
        for item in items {
            kv.insert(item.key, item.value);
        }
        Ok(())
    }

    async fn delete_state(
        &mut self,
        store_name: String,
        key: String,
        _metadata: types::Metadata,
    ) -> Result<(), types::Error> {
        let mut store = self.store.lock().unwrap();
        if let Some(kv) = store.state.get_mut(&store_name) {
            kv.remove(&key);
        }
        Ok(())
    }

    async fn publish(
        &mut self,
        pubsub_name: String,
        topic: String,
        data: Vec<u8>,
        data_content_type: String,
        _metadata: types::Metadata,
    ) -> Result<(), types::Error> {
        let mut store = self.store.lock().unwrap();
        store.published.push(PublishedEvent {
            pubsub_name,
            topic,
            data,
            data_content_type,
        });
        Ok(())
    }

    async fn get_secret(
        &mut self,
        store_name: String,
        key: String,
    ) -> Result<secrets::Secret, types::Error> {
        let store = self.store.lock().unwrap();
        store
            .secrets
            .get(&store_name)
            .and_then(|s| s.get(&key))
            .cloned()
            .ok_or_else(|| types::Error::NotFound(format!("secret {key} in {store_name}")))
    }

    async fn get_bulk_secret(
        &mut self,
        store_name: String,
        _metadata: types::Metadata,
    ) -> Result<Vec<(String, secrets::Secret)>, types::Error> {
        let store = self.store.lock().unwrap();
        Ok(store
            .secrets
            .get(&store_name)
            .map(|s| s.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default())
    }

    async fn invoke_binding(
        &mut self,
        _name: String,
        _operation: String,
        data: Vec<u8>,
        _metadata: types::Metadata,
    ) -> Result<wit_bindings::InvokeBindingResponse, types::Error> {
        // Echo backend: returns the request payload.
        Ok(wit_bindings::InvokeBindingResponse {
            data,
            metadata: Vec::new(),
        })
    }

    async fn invoke_service(
        &mut self,
        _app_id: String,
        _method: String,
        data: Vec<u8>,
    ) -> Result<invocation::InvocationResponse, types::Error> {
        // Echo backend: returns the request payload.
        Ok(invocation::InvocationResponse {
            data,
            content_type: "application/octet-stream".to_string(),
        })
    }

    async fn get_configuration(
        &mut self,
        store_name: String,
        keys: Vec<String>,
        _metadata: types::Metadata,
    ) -> Result<Vec<(String, configuration::ConfigurationItem)>, types::Error> {
        let store = self.store.lock().unwrap();
        let Some(items) = store.configuration.get(&store_name) else {
            return Ok(Vec::new());
        };
        Ok(items
            .iter()
            .filter(|(key, _)| keys.is_empty() || keys.contains(key))
            .map(|(key, (value, version))| {
                (
                    key.clone(),
                    configuration::ConfigurationItem {
                        value: value.clone(),
                        version: version.clone(),
                        metadata: Vec::new(),
                    },
                )
            })
            .collect())
    }
}
