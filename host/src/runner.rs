//! Loads a `dapr:client` app component and drives its exports.

use std::path::Path;

use wasmtime::component::{Component, HasSelf, Linker};
use wasmtime::error::Context as _;
use wasmtime::{Config, Engine, Store};

use crate::backend::DaprBackend;
use crate::bindings::exports::dapr::client::topic_handler::{
    TopicEvent, TopicEventResponse, TopicSubscription,
};
use crate::bindings::App;
use crate::Ctx;

pub struct GuestRunner {
    store: Store<Ctx>,
    instance: App,
}

impl GuestRunner {
    /// Compile and instantiate the component at `path` with the given backend.
    pub async fn load(path: &Path, backend: Box<dyn DaprBackend>) -> wasmtime::Result<Self> {
        // Note: as of wasmtime 45 async support is always available;
        // Config::async_support is deprecated and has no effect.
        let engine = Engine::new(&Config::new())?;

        let component = Component::from_file(&engine, path)
            .with_context(|| format!("failed to load component {}", path.display()))?;

        let mut linker = Linker::<Ctx>::new(&engine);
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
        App::add_to_linker::<_, HasSelf<_>>(&mut linker, |ctx| ctx)?;

        let mut store = Store::new(&engine, Ctx::new(backend));
        let instance = App::instantiate_async(&mut store, &component, &linker).await?;
        Ok(Self { store, instance })
    }

    /// Call the guest's `run` entry point.
    pub async fn run(&mut self) -> wasmtime::Result<Result<String, String>> {
        self.instance.call_run(&mut self.store).await
    }

    /// Ask the guest which pub/sub topics it wants to subscribe to.
    pub async fn list_topic_subscriptions(&mut self) -> wasmtime::Result<Vec<TopicSubscription>> {
        self.instance
            .dapr_client_topic_handler()
            .call_list_topic_subscriptions(&mut self.store)
            .await
    }

    /// Deliver a pub/sub event to the guest.
    pub async fn on_topic_event(
        &mut self,
        event: &TopicEvent,
    ) -> wasmtime::Result<TopicEventResponse> {
        self.instance
            .dapr_client_topic_handler()
            .call_on_topic_event(&mut self.store, event)
            .await
    }
}
