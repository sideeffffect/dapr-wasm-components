//! App-side SDK for `dapr-wasm-components`.
//!
//! An application implements the [`DaprApp`] trait — overriding only the
//! callbacks it actually uses, since every method has a sensible default —
//! and exports it with [`export_app!`]. To call Dapr (the outbound
//! direction), it uses the building-block interfaces re-exported under
//! [`dapr`]. The application never touches HTTP or gRPC; a composed provider
//! (`dapr-wasm-components-wasi-http` or `-wasi-grpc`) bridges the wire on both
//! directions.
//!
//! ```ignore
//! use dapr_app::{dapr, callback, DaprApp};
//!
//! struct App;
//!
//! impl DaprApp for App {
//!     fn list_topic_subscriptions() -> Vec<callback::pubsub_callback::TopicSubscription> {
//!         vec![callback::pubsub_callback::TopicSubscription {
//!             pubsub_name: "pubsub".into(),
//!             topic: "orders".into(),
//!             metadata: vec![],
//!             dead_letter_topic: None,
//!         }]
//!     }
//!     fn on_topic_event(
//!         event: callback::pubsub_callback::TopicEvent,
//!     ) -> callback::pubsub_callback::TopicEventResponse {
//!         // ... handle the typed event, call dapr::state::save(...), etc.
//!         callback::pubsub_callback::TopicEventResponse::Success
//!     }
//! }
//!
//! dapr_app::export_app!(App);
//! ```

// Let the generated `export_app!` macro resolve binding paths as `dapr_app::…`
// both inside this crate and from the application crates that invoke it.
extern crate self as dapr_app;

wit_bindgen::generate!({
    world: "app",
    path: "../../components/wit",
    pub_export_macro: true,
    export_macro_name: "export_app",
    default_bindings_module: "dapr_app",
});

/// The Dapr building-block interfaces an application calls (outbound).
pub use dapr_wasm_components::interfaces as dapr;

/// The callback interfaces an application serves (inbound): types and the
/// generated `Guest` traits the SDK binds to [`DaprApp`].
pub use exports::dapr_wasm_components::interfaces as callback;

use callback::actors_callback as ac;
use callback::bindings_callback as bc;
use callback::configuration_callback as cc;
use callback::invocation_callback as ic;
use callback::jobs_callback as jc;
use callback::pubsub_callback as pc;
use dapr::types::Error;

/// The inbound surface of a Dapr application. Implement this trait and export
/// it with [`export_app!`]. Every method has a default, so an outbound-only
/// app implements nothing and a pub/sub app overrides just the two pub/sub
/// methods.
#[allow(unused_variables)]
pub trait DaprApp {
    // --- service invocation ---

    /// Handle a service-invocation call. Default: 404 Not Found.
    fn on_invoke(request: ic::InvokeRequest) -> ic::HttpResponse {
        ic::HttpResponse {
            status: 404,
            headers: Vec::new(),
            body: b"no service-invocation handler".to_vec(),
        }
    }

    // --- pub/sub ---

    /// Declare the topics this app subscribes to. Default: none.
    fn list_topic_subscriptions() -> Vec<pc::TopicSubscription> {
        Vec::new()
    }

    /// Handle a delivered event. Default: DROP (with no subscriptions
    /// declared, this is never reached).
    fn on_topic_event(event: pc::TopicEvent) -> pc::TopicEventResponse {
        pc::TopicEventResponse::Drop
    }

    // --- input bindings ---

    /// Declare the input binding names this app consumes. Default: none.
    fn list_input_bindings() -> Vec<String> {
        Vec::new()
    }

    /// Handle an input-binding event. Default: acknowledge with no side effects.
    fn on_binding_event(event: bc::BindingEvent) -> Result<bc::BindingEventResponse, Error> {
        Ok(bc::BindingEventResponse {
            store_name: None,
            states: Vec::new(),
            to: Vec::new(),
            data: None,
            concurrency: bc::BindingConcurrency::Sequential,
        })
    }

    // --- jobs ---

    /// Handle a job trigger. Default: acknowledge.
    fn on_job_event(event: jc::JobEvent) -> Result<(), Error> {
        Ok(())
    }

    // --- actors ---

    /// Declare hosted actor types and runtime options. Default: host none.
    fn actor_config() -> ac::ActorConfig {
        ac::ActorConfig {
            entities: Vec::new(),
            actor_idle_timeout: None,
            actor_scan_interval: None,
            drain_ongoing_call_timeout: None,
            drain_rebalanced_actors: None,
            reentrancy_enabled: None,
            reentrancy_max_stack_depth: None,
            reminders_storage_partitions: None,
        }
    }

    /// Invoke an actor method. Default: not found.
    fn on_actor_invoke(
        actor_type: String,
        actor_id: String,
        method: String,
        data: Vec<u8>,
    ) -> Result<Vec<u8>, Error> {
        Err(Error::NotFound(format!(
            "no actor type {actor_type} hosted"
        )))
    }

    /// Fire an actor timer. Default: no-op.
    fn on_actor_timer(event: ac::ActorTimerEvent) -> Result<(), Error> {
        Ok(())
    }

    /// Fire an actor reminder. Default: no-op.
    fn on_actor_reminder(event: ac::ActorReminderEvent) -> Result<(), Error> {
        Ok(())
    }

    /// Deactivate an actor instance. Default: no-op.
    fn deactivate_actor(actor_type: String, actor_id: String) -> Result<(), Error> {
        Ok(())
    }

    // --- configuration updates ---

    /// Handle a configuration update push. Default: ignore.
    fn on_configuration_event(update: cc::ConfigurationUpdate) {}

    // --- health ---

    /// Report application health. Default: healthy.
    fn health_check() -> Result<(), Error> {
        Ok(())
    }
}

// Bridge the generated per-interface `Guest` traits to the single `DaprApp`
// trait via blanket impls, so an app type that implements `DaprApp` satisfies
// every callback export.

impl<T: DaprApp> ic::Guest for T {
    fn on_invoke(request: ic::InvokeRequest) -> ic::HttpResponse {
        <T as DaprApp>::on_invoke(request)
    }
}

impl<T: DaprApp> pc::Guest for T {
    fn list_topic_subscriptions() -> Vec<pc::TopicSubscription> {
        <T as DaprApp>::list_topic_subscriptions()
    }
    fn on_topic_event(event: pc::TopicEvent) -> pc::TopicEventResponse {
        <T as DaprApp>::on_topic_event(event)
    }
}

impl<T: DaprApp> bc::Guest for T {
    fn list_input_bindings() -> Vec<String> {
        <T as DaprApp>::list_input_bindings()
    }
    fn on_binding_event(event: bc::BindingEvent) -> Result<bc::BindingEventResponse, Error> {
        <T as DaprApp>::on_binding_event(event)
    }
}

impl<T: DaprApp> jc::Guest for T {
    fn on_job_event(event: jc::JobEvent) -> Result<(), Error> {
        <T as DaprApp>::on_job_event(event)
    }
}

impl<T: DaprApp> ac::Guest for T {
    fn config() -> ac::ActorConfig {
        <T as DaprApp>::actor_config()
    }
    fn on_invoke(
        actor_type: String,
        actor_id: String,
        method: String,
        data: Vec<u8>,
    ) -> Result<Vec<u8>, Error> {
        <T as DaprApp>::on_actor_invoke(actor_type, actor_id, method, data)
    }
    fn on_timer(event: ac::ActorTimerEvent) -> Result<(), Error> {
        <T as DaprApp>::on_actor_timer(event)
    }
    fn on_reminder(event: ac::ActorReminderEvent) -> Result<(), Error> {
        <T as DaprApp>::on_actor_reminder(event)
    }
    fn deactivate(actor_type: String, actor_id: String) -> Result<(), Error> {
        <T as DaprApp>::deactivate_actor(actor_type, actor_id)
    }
}

impl<T: DaprApp> cc::Guest for T {
    fn on_configuration_event(update: cc::ConfigurationUpdate) {
        <T as DaprApp>::on_configuration_event(update)
    }
}

impl<T: DaprApp> callback::health_callback::Guest for T {
    fn health_check() -> Result<(), Error> {
        <T as DaprApp>::health_check()
    }
}
