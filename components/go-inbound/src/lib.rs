//! dapr-wasm-components-go-inbound
//!
//! The pump of the **go** provider family. `dapr-wasm-components-go-outbound`
//! embeds the one Dapr runtime of the composed app and queues every Dapr →
//! app delivery as an `event` on the private `dapr-wasm-components:go/events`
//! interface. This component runs as the composition's `wasi:cli/run`: it
//! attaches (which connects the runtime to the app), then loops — pull an
//! event, call the app's matching `*-callback` export, hand the result back.
//!
//! Pulling instead of pushing keeps the composition acyclic
//! (`go-outbound → app → go-inbound`, plus `go-outbound → go-inbound`), and no
//! instance is ever re-entered: while the pump calls the app, go-outbound is
//! not on the stack, so the app may call the building blocks freely.
//!
//! It holds no Dapr state and is deliberately not written in Go: a Go pump
//! would put a second Go runtime into the composed app.
//!
//! The `events` records are structural copies of the callback records (see
//! `go.wit`), so this crate converts between the two generated type families
//! field by field.

mod wit {
    wit_bindgen::generate!({
        world: "go-inbound",
        path: "../go-outbound/wit",
        default_bindings_module: "crate::wit",
        // The interfaces package is a WIT dependency of go.wit, not the root.
        generate_all,
    });
}

use wit::dapr_wasm_components::go::events as ev;
use wit::dapr_wasm_components::interfaces::{
    actors_callback as actors, bindings_callback as bindings,
    configuration_callback as configuration, health_callback as health,
    invocation_callback as invocation, jobs_callback as jobs, pubsub_callback as pubsub,
};

struct Pump;

impl wit::exports::wasi::cli::run::Guest for Pump {
    fn run() -> Result<(), ()> {
        ev::attach();
        loop {
            let (id, event) = ev::next_event();
            ev::complete_event(id, &dispatch(event));
        }
    }
}

wit::export!(Pump);

/// Call the app's callback for one event and wrap its answer in the
/// matching `event-result` case.
fn dispatch(event: ev::Event) -> ev::EventResult {
    match event {
        ev::Event::ListTopicSubscriptions => ev::EventResult::ListTopicSubscriptions(
            pubsub::list_topic_subscriptions()
                .into_iter()
                .map(subscription)
                .collect(),
        ),
        ev::Event::ListInputBindings => {
            ev::EventResult::ListInputBindings(bindings::list_input_bindings())
        }
        ev::Event::ActorConfig => ev::EventResult::ActorConfig(actor_config(actors::config())),
        ev::Event::HealthCheck => ev::EventResult::HealthCheck(health::health_check()),
        ev::Event::Invoke(r) => {
            ev::EventResult::Invoke(invocation::on_invoke(&invocation::InvokeRequest {
                method: r.method,
                verb: r.verb,
                headers: r.headers,
                query: r.query,
                content_type: r.content_type,
                data: r.data,
            }))
        }
        ev::Event::Topic(e) => {
            ev::EventResult::Topic(topic_status(pubsub::on_topic_event(&topic_event(e))))
        }
        ev::Event::BulkTopic(b) => ev::EventResult::BulkTopic(
            pubsub::on_bulk_topic_event(&bulk_topic_event(b))
                .into_iter()
                .map(|r| ev::BulkTopicEventEntryResponse {
                    entry_id: r.entry_id,
                    status: topic_status(r.status),
                })
                .collect(),
        ),
        ev::Event::Binding(e) => ev::EventResult::Binding(
            bindings::on_binding_event(&bindings::BindingEvent {
                name: e.name,
                data: e.data,
                metadata: e.metadata,
            })
            .map(binding_response),
        ),
        ev::Event::Job(e) => ev::EventResult::Job(jobs::on_job_event(&jobs::JobEvent {
            name: e.name,
            data: e.data,
            content_type: e.content_type,
        })),
        ev::Event::ActorInvoke(a) => ev::EventResult::ActorInvoke(actors::on_invoke(
            &a.actor_type,
            &a.actor_id,
            &a.method,
            &a.data,
        )),
        ev::Event::ActorTimer(t) => {
            ev::EventResult::ActorTimer(actors::on_timer(&actors::ActorTimerEvent {
                actor_type: t.actor_type,
                actor_id: t.actor_id,
                name: t.name,
                due_time: t.due_time,
                period: t.period,
                data: t.data,
                callback: t.callback,
            }))
        }
        ev::Event::ActorReminder(r) => {
            ev::EventResult::ActorReminder(actors::on_reminder(&actors::ActorReminderEvent {
                actor_type: r.actor_type,
                actor_id: r.actor_id,
                name: r.name,
                due_time: r.due_time,
                period: r.period,
                data: r.data,
            }))
        }
        ev::Event::ActorDeactivate(a) => {
            ev::EventResult::ActorDeactivate(actors::deactivate(&a.actor_type, &a.actor_id))
        }
        ev::Event::ConfigurationUpdate(u) => {
            configuration::on_configuration_event(&configuration::ConfigurationUpdate {
                store_name: u.store_name,
                id: u.id,
                items: u.items,
            });
            ev::EventResult::ConfigurationUpdate
        }
    }
}

fn subscription(s: pubsub::TopicSubscription) -> ev::TopicSubscription {
    ev::TopicSubscription {
        pubsub_name: s.pubsub_name,
        topic: s.topic,
        metadata: s.metadata,
        dead_letter_topic: s.dead_letter_topic,
        routes: s.routes.map(|r| ev::TopicRoutes {
            rules: r
                .rules
                .into_iter()
                .map(|rule| ev::TopicRule {
                    match_: rule.match_,
                    path: rule.path,
                })
                .collect(),
            default: r.default,
        }),
        bulk_subscribe: s.bulk_subscribe.map(|b| ev::BulkSubscribeConfig {
            enabled: b.enabled,
            max_messages_count: b.max_messages_count,
            max_await_duration_ms: b.max_await_duration_ms,
        }),
    }
}

fn actor_config(c: actors::ActorConfig) -> ev::ActorConfig {
    ev::ActorConfig {
        entities: c.entities,
        actor_idle_timeout: c.actor_idle_timeout,
        actor_scan_interval: c.actor_scan_interval,
        drain_ongoing_call_timeout: c.drain_ongoing_call_timeout,
        drain_rebalanced_actors: c.drain_rebalanced_actors,
        reentrancy_enabled: c.reentrancy_enabled,
        reentrancy_max_stack_depth: c.reentrancy_max_stack_depth,
        reminders_storage_partitions: c.reminders_storage_partitions,
    }
}

fn topic_event(e: ev::TopicEvent) -> pubsub::TopicEvent {
    pubsub::TopicEvent {
        id: e.id,
        source: e.source,
        type_: e.type_,
        spec_version: e.spec_version,
        data_content_type: e.data_content_type,
        data: e.data,
        topic: e.topic,
        pubsub_name: e.pubsub_name,
        path: e.path,
        extensions: e.extensions,
    }
}

fn bulk_topic_event(b: ev::BulkTopicEvent) -> pubsub::BulkTopicEvent {
    pubsub::BulkTopicEvent {
        id: b.id,
        entries: b
            .entries
            .into_iter()
            .map(|entry| pubsub::BulkTopicEventEntry {
                entry_id: entry.entry_id,
                event: match entry.event {
                    ev::BulkEvent::CloudEvent(e) => pubsub::BulkEvent::CloudEvent(topic_event(e)),
                    ev::BulkEvent::Raw(raw) => pubsub::BulkEvent::Raw(raw),
                },
                content_type: entry.content_type,
                metadata: entry.metadata,
            })
            .collect(),
        metadata: b.metadata,
        topic: b.topic,
        pubsub_name: b.pubsub_name,
        type_: b.type_,
        path: b.path,
    }
}

fn topic_status(s: pubsub::TopicEventResponse) -> ev::TopicEventResponse {
    match s {
        pubsub::TopicEventResponse::Success => ev::TopicEventResponse::Success,
        pubsub::TopicEventResponse::Retry => ev::TopicEventResponse::Retry,
        pubsub::TopicEventResponse::Drop => ev::TopicEventResponse::Drop,
    }
}

fn binding_response(r: bindings::BindingEventResponse) -> ev::BindingEventResponse {
    ev::BindingEventResponse {
        store_name: r.store_name,
        states: r.states,
        to: r.to,
        data: r.data,
        concurrency: match r.concurrency {
            bindings::BindingConcurrency::Sequential => ev::BindingConcurrency::Sequential,
            bindings::BindingConcurrency::Parallel => ev::BindingConcurrency::Parallel,
        },
    }
}
