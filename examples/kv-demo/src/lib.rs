//! Example Dapr app component.
//!
//! On `run`: saves a value to the state store, reads it back, deletes it,
//! and publishes an event announcing the roundtrip. Also subscribes to the
//! `kv-demo` topic and logs every delivered message.
//!
//! Note that all the Dapr calls below are plain synchronous function calls —
//! the host bridges them to the async Dapr Rust SDK behind the scenes.

wit_bindgen::generate!({
    world: "app",
    path: "../../wit",
});

use dapr::client::{pubsub, state};
use exports::dapr::client::topic_handler;

const STATE_STORE: &str = "statestore";
const PUBSUB: &str = "pubsub";
const TOPIC: &str = "kv-demo";

struct Component;

impl Guest for Component {
    fn run() -> Result<String, String> {
        let key = "kv-demo-key";
        let value = b"hello from a wasm component";

        state::save(STATE_STORE, key, value, None, &[], None)
            .map_err(|e| format!("save failed: {e:?}"))?;

        let got = state::get(STATE_STORE, key, &[]).map_err(|e| format!("get failed: {e:?}"))?;
        if got.data != value {
            return Err(format!(
                "state roundtrip mismatch: wrote {value:?}, read {:?}",
                got.data
            ));
        }

        state::delete(STATE_STORE, key, &[]).map_err(|e| format!("delete failed: {e:?}"))?;

        pubsub::publish(
            PUBSUB,
            TOPIC,
            b"{\"message\":\"state roundtrip done\"}",
            "application/json",
            &[],
        )
        .map_err(|e| format!("publish failed: {e:?}"))?;

        Ok(format!(
            "state roundtrip through '{STATE_STORE}' succeeded; published to '{TOPIC}' on '{PUBSUB}'"
        ))
    }
}

impl topic_handler::Guest for Component {
    fn list_topic_subscriptions() -> Vec<topic_handler::TopicSubscription> {
        vec![topic_handler::TopicSubscription {
            pubsub_name: PUBSUB.to_string(),
            topic: TOPIC.to_string(),
            metadata: Vec::new(),
        }]
    }

    fn on_topic_event(event: topic_handler::TopicEvent) -> topic_handler::TopicEventResponse {
        println!(
            "received event {} on topic {}: {}",
            event.id,
            event.topic,
            String::from_utf8_lossy(&event.data),
        );
        topic_handler::TopicEventResponse::Success
    }
}

export!(Component);
