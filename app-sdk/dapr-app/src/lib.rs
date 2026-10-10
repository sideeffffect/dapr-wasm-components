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
//!         vec![
//!             // One route for the whole topic.
//!             callback::pubsub_callback::TopicSubscription::new("pubsub", "orders"),
//!             // CEL routing rules: the sidecar picks the route, the app
//!             // sees it in `event.path`.
//!             callback::pubsub_callback::TopicSubscription::new("pubsub", "inventory")
//!                 .with_rule(r#"event.type == "widget""#, "/widgets")
//!                 .with_default_route("/products"),
//!             // Opt in to bulk delivery: batches of up to 10 events, at most
//!             // 100 ms apart. The default `on_bulk_topic_event` still calls
//!             // `on_topic_event` once per entry.
//!             callback::pubsub_callback::TopicSubscription::new("pubsub", "audit")
//!                 .with_bulk_subscribe(Some(10), Some(100)),
//!         ]
//!     }
//!     fn on_topic_event(
//!         event: callback::pubsub_callback::TopicEvent,
//!     ) -> callback::pubsub_callback::TopicEventResponse {
//!         // ... dispatch on event.topic / event.path, call dapr::state::save(...), etc.
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
use dapr::types::AppError;

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

    /// Handle a batch of events for a bulk-subscribed topic. Default: call
    /// [`DaprApp::on_topic_event`] once per entry (see [`bulk_to_single`]), so
    /// an app can opt a subscription in to bulk delivery without new code.
    fn on_bulk_topic_event(event: pc::BulkTopicEvent) -> Vec<pc::BulkTopicEventEntryResponse> {
        bulk_to_single(event, <Self as DaprApp>::on_topic_event)
    }

    // --- input bindings ---

    /// Declare the input binding names this app consumes. Default: none.
    fn list_input_bindings() -> Vec<String> {
        Vec::new()
    }

    /// Handle an input-binding event. Default: acknowledge with no side effects.
    fn on_binding_event(event: bc::BindingEvent) -> Result<bc::BindingEventResponse, AppError> {
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
    fn on_job_event(event: jc::JobEvent) -> Result<(), AppError> {
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
    ) -> Result<Vec<u8>, AppError> {
        Err(AppError {
            message: format!("no actor type {actor_type} hosted"),
        })
    }

    /// Fire an actor timer. Default: no-op.
    fn on_actor_timer(event: ac::ActorTimerEvent) -> Result<(), AppError> {
        Ok(())
    }

    /// Fire an actor reminder. Default: no-op.
    fn on_actor_reminder(event: ac::ActorReminderEvent) -> Result<(), AppError> {
        Ok(())
    }

    /// Deactivate an actor instance. Default: no-op.
    fn deactivate_actor(actor_type: String, actor_id: String) -> Result<(), AppError> {
        Ok(())
    }

    // --- configuration updates ---

    /// Handle a configuration update push. Default: ignore.
    fn on_configuration_event(update: cc::ConfigurationUpdate) {}

    // --- health ---

    /// Report application health. Default: healthy.
    fn health_check() -> Result<(), AppError> {
        Ok(())
    }
}

/// Builder-style constructors for subscriptions, so apps don't spell out every
/// record field (and keep compiling as optional fields are added).
/// Builder-style constructors for subscriptions, so apps don't spell out every
/// record field (and keep compiling as optional fields are added).
impl pc::TopicSubscription {
    /// Subscribe to `topic` on `pubsub_name`: no metadata, no dead-letter
    /// topic, no routing rules (one route for the whole topic), one event per
    /// delivery (no bulk).
    pub fn new(pubsub_name: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            pubsub_name: pubsub_name.into(),
            topic: topic.into(),
            metadata: Vec::new(),
            dead_letter_topic: None,
            routes: None,
            bulk_subscribe: None,
        }
    }

    /// Opt in to bulk delivery (Dapr `bulkSubscribe`). `None` limits use the
    /// Dapr defaults. Batches arrive at [`DaprApp::on_bulk_topic_event`].
    pub fn with_bulk_subscribe(
        mut self,
        max_messages_count: Option<i32>,
        max_await_duration_ms: Option<i32>,
    ) -> Self {
        self.bulk_subscribe = Some(pc::BulkSubscribeConfig {
            enabled: true,
            max_messages_count,
            max_await_duration_ms,
        });
        self
    }

    /// Add one subscription metadata entry (e.g. `rawPayload`).
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.push((key.into(), value.into()));
        self
    }

    /// Send events that fail delivery to `topic`.
    pub fn with_dead_letter_topic(mut self, topic: impl Into<String>) -> Self {
        self.dead_letter_topic = Some(topic.into());
        self
    }

    /// Append a CEL routing rule: events for which `cel` is true are
    /// delivered with `path` in [`pc::TopicEvent::path`]. Rules are evaluated
    /// in the order they are added; the first match wins. Without a
    /// [`with_default_route`](Self::with_default_route), the sidecar drops
    /// events that no rule matches.
    pub fn with_rule(mut self, cel: impl Into<String>, path: impl Into<String>) -> Self {
        self.routes_mut().rules.push(pc::TopicRule {
            match_: cel.into(),
            path: path.into(),
        });
        self
    }

    /// The path for events that no rule matches.
    pub fn with_default_route(mut self, path: impl Into<String>) -> Self {
        self.routes_mut().default = Some(path.into());
        self
    }

    fn routes_mut(&mut self) -> &mut pc::TopicRoutes {
        self.routes.get_or_insert_with(|| pc::TopicRoutes {
            rules: Vec::new(),
            default: None,
        })
    }
}

/// Handle a bulk delivery one entry at a time with a single-event handler,
/// and collect one status per entry. A CloudEvent entry is passed as is. A raw
/// entry becomes a `topic-event` with `id` = the entry id, the raw bytes as
/// `data`, the entry content type, and the batch's topic, pub/sub name, type
/// and path.
pub fn bulk_to_single(
    event: pc::BulkTopicEvent,
    mut handle: impl FnMut(pc::TopicEvent) -> pc::TopicEventResponse,
) -> Vec<pc::BulkTopicEventEntryResponse> {
    event
        .entries
        .into_iter()
        .map(|entry| {
            let single = match entry.event {
                pc::BulkEvent::CloudEvent(cloud_event) => cloud_event,
                pc::BulkEvent::Raw(data) => pc::TopicEvent {
                    id: entry.entry_id.clone(),
                    source: String::new(),
                    type_: event.type_.clone(),
                    spec_version: "1.0".to_string(),
                    data_content_type: entry.content_type,
                    data,
                    topic: event.topic.clone(),
                    pubsub_name: event.pubsub_name.clone(),
                    path: event.path.clone(),
                    extensions: Vec::new(),
                },
            };
            pc::BulkTopicEventEntryResponse {
                entry_id: entry.entry_id,
                status: handle(single),
            }
        })
        .collect()
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
    fn on_bulk_topic_event(event: pc::BulkTopicEvent) -> Vec<pc::BulkTopicEventEntryResponse> {
        <T as DaprApp>::on_bulk_topic_event(event)
    }
}

impl<T: DaprApp> bc::Guest for T {
    fn list_input_bindings() -> Vec<String> {
        <T as DaprApp>::list_input_bindings()
    }
    fn on_binding_event(event: bc::BindingEvent) -> Result<bc::BindingEventResponse, AppError> {
        <T as DaprApp>::on_binding_event(event)
    }
}

impl<T: DaprApp> jc::Guest for T {
    fn on_job_event(event: jc::JobEvent) -> Result<(), AppError> {
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
    ) -> Result<Vec<u8>, AppError> {
        <T as DaprApp>::on_actor_invoke(actor_type, actor_id, method, data)
    }
    fn on_timer(event: ac::ActorTimerEvent) -> Result<(), AppError> {
        <T as DaprApp>::on_actor_timer(event)
    }
    fn on_reminder(event: ac::ActorReminderEvent) -> Result<(), AppError> {
        <T as DaprApp>::on_actor_reminder(event)
    }
    fn deactivate(actor_type: String, actor_id: String) -> Result<(), AppError> {
        <T as DaprApp>::deactivate_actor(actor_type, actor_id)
    }
}

impl<T: DaprApp> cc::Guest for T {
    fn on_configuration_event(update: cc::ConfigurationUpdate) {
        <T as DaprApp>::on_configuration_event(update)
    }
}

impl<T: DaprApp> callback::health_callback::Guest for T {
    fn health_check() -> Result<(), AppError> {
        <T as DaprApp>::health_check()
    }
}
