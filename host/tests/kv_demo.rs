//! End-to-end test: runs the kv-demo example component against the
//! in-memory backend and checks the state/pubsub effects.
//!
//! Requires the example component to be built first:
//!   cargo build --release --target wasm32-wasip2 --manifest-path examples/Cargo.toml
//! The path can be overridden with KV_DEMO_COMPONENT.

use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::Mutex;

use dapr_wasm_host::backend::memory::MemoryBackend;
use dapr_wasm_host::bindings::exports::dapr::client::topic_handler::{
    TopicEvent, TopicEventResponse,
};
use dapr_wasm_host::runner::GuestRunner;

fn component_path() -> PathBuf {
    if let Ok(path) = std::env::var("KV_DEMO_COMPONENT") {
        return PathBuf::from(path);
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../examples/target/wasm32-wasip2/release/kv_demo.wasm")
}

#[tokio::test]
async fn kv_demo_roundtrip() {
    let path = component_path();
    assert!(
        path.exists(),
        "example component not found at {} — build it first:\n  \
         cargo build --release --target wasm32-wasip2 --manifest-path examples/Cargo.toml",
        path.display()
    );

    let backend = MemoryBackend::new();
    let store = backend.store();

    let mut runner = GuestRunner::load(&path, Box::new(backend))
        .await
        .expect("failed to load component");

    // The guest subscribes to the kv-demo topic.
    let subscriptions = runner
        .list_topic_subscriptions()
        .await
        .expect("list-topic-subscriptions trapped");
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(subscriptions[0].pubsub_name, "pubsub");
    assert_eq!(subscriptions[0].topic, "kv-demo");

    // run() does a state roundtrip and publishes an event.
    let summary = runner
        .run()
        .await
        .expect("run trapped")
        .expect("run returned an error");
    assert!(
        summary.contains("roundtrip"),
        "unexpected summary: {summary}"
    );

    {
        let store = store.lock().unwrap();
        assert_eq!(
            store.published.len(),
            1,
            "expected exactly one published event"
        );
        let event = &store.published[0];
        assert_eq!(event.pubsub_name, "pubsub");
        assert_eq!(event.topic, "kv-demo");
        assert_eq!(event.data_content_type, "application/json");
        // The guest deleted its key after the roundtrip.
        let empty = store
            .state
            .get("statestore")
            .map(|kv| kv.is_empty())
            .unwrap_or(true);
        assert!(empty, "state store should be empty after the roundtrip");
    }

    // Deliver an event back into the guest's topic-handler export.
    let runner = Arc::new(Mutex::new(runner));
    let response = runner
        .lock()
        .await
        .on_topic_event(&TopicEvent {
            id: "test-1".to_string(),
            source: "test".to_string(),
            event_type: "com.dapr.event.sent".to_string(),
            spec_version: "1.0".to_string(),
            data_content_type: "application/json".to_string(),
            data: b"{\"message\":\"state roundtrip done\"}".to_vec(),
            topic: "kv-demo".to_string(),
            pubsub_name: "pubsub".to_string(),
            path: String::new(),
        })
        .await
        .expect("on-topic-event trapped");
    assert_eq!(response, TopicEventResponse::Success);
}
