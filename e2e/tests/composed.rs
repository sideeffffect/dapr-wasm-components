//! Composition test: plug kv-demo into the wasi-http provider with
//! wac-graph (the programmatic `wac plug`) and run the resulting command
//! component against the mock sidecar — the same artifact a user would run
//! with `wasmtime run -S http`.

use wasmtime::component::Component;
use wasmtime::Store;

use dapr_wasm_components_e2e::mock::MockSidecar;
use dapr_wasm_components_e2e::{
    compose, engine, http_inbound_path, http_outbound_path, kv_demo_path, linker,
    microservice_path, serve_inbound, serve_inbound_with_env, Ctx,
};

#[tokio::test]
async fn composed_kv_demo_runs() {
    let sidecar = MockSidecar::start().await.unwrap();

    let app_bytes = std::fs::read(kv_demo_path()).expect("kv-demo component not built");
    let outbound_bytes =
        std::fs::read(http_outbound_path()).expect("http-outbound provider not built");
    let composed = compose::plug(app_bytes, outbound_bytes).expect("composition failed");

    let engine = engine().unwrap();
    let component = Component::new(&engine, &composed).unwrap();
    let linker = linker(&engine).unwrap();

    let env = vec![("DAPR_HTTP_ENDPOINT".to_string(), sidecar.endpoint.clone())];
    let mut store = Store::new(&engine, Ctx::new(&env));

    let command =
        wasmtime_wasi::p2::bindings::Command::instantiate_async(&mut store, &component, &linker)
            .await
            .unwrap();
    let result = command.wasi_cli_run().call_run(&mut store).await.unwrap();
    assert!(result.is_ok(), "kv-demo exited with failure");

    let recorded = sidecar.recorded.lock().unwrap();
    // kv-demo deleted its key, so state must be empty again...
    assert!(recorded.state.is_empty());
    // ...and exactly one event was published.
    assert_eq!(recorded.published.len(), 1);
    assert_eq!(recorded.published[0].topic, "kv-demo");
}

/// The full bidirectional composition (`outbound → app → inbound`) must encode
/// to a valid server component that exports `wasi:http/incoming-handler`.
#[tokio::test]
async fn full_composition_is_valid() {
    let app = std::fs::read(microservice_path()).expect("microservice component not built");
    let outbound = std::fs::read(http_outbound_path()).expect("http-outbound not built");
    let inbound = std::fs::read(http_inbound_path()).expect("http-inbound not built");

    let composed =
        compose::plug_full(app, outbound, inbound).expect("full composition failed to encode");

    // Component::new fully validates the encoded component bytes.
    let engine = engine().unwrap();
    Component::new(&engine, &composed).expect("composed component is invalid");
}

/// Inbound config-update delivery: a sidecar `POST /configuration/<store>/<key>`
/// carrying Dapr's `UpdateEvent` must reach the app's `on-configuration-event`.
/// The microservice persists each delivered item to state, so we assert the
/// resulting state write landed on the mock sidecar.
#[tokio::test]
async fn inbound_configuration_update_is_delivered() {
    let sidecar = MockSidecar::start().await.unwrap();

    let app = std::fs::read(microservice_path()).expect("microservice component not built");
    let outbound = std::fs::read(http_outbound_path()).expect("http-outbound not built");
    let inbound = std::fs::read(http_inbound_path()).expect("http-inbound not built");
    let composed = compose::plug_full(app, outbound, inbound).expect("full composition failed");

    // Dapr's UpdateEvent: `items` is a map keyed by configuration key.
    let body = serde_json::json!({
        "id": "sub-1",
        "items": {
            "feature-flag": { "value": "enabled", "version": "v2" }
        }
    })
    .to_string()
    .into_bytes();

    let (status, _) = serve_inbound(
        &composed,
        &sidecar.endpoint,
        "POST",
        "/configuration/configstore/feature-flag",
        "application/json",
        body,
    )
    .await
    .expect("inbound handler errored");
    assert_eq!(status, 200, "config-update delivery should return 200");

    let recorded = sidecar.recorded.lock().unwrap();
    let stored = recorded
        .state
        .get(&("statestore".to_string(), "config-feature-flag".to_string()))
        .expect("app did not persist the delivered config item");
    assert_eq!(stored, &serde_json::json!("enabled"));
}

/// The microservice composed `outbound → app → inbound` over wasi-http.
fn composed_microservice() -> Vec<u8> {
    let app = std::fs::read(microservice_path()).expect("microservice component not built");
    let outbound = std::fs::read(http_outbound_path()).expect("http-outbound not built");
    let inbound = std::fs::read(http_inbound_path()).expect("http-inbound not built");
    compose::plug_full(app, outbound, inbound).expect("full composition failed")
}

/// `GET /dapr/subscribe` on the consumer-role microservice, parsed.
async fn discover_subscriptions(composed: &[u8], sidecar_endpoint: &str) -> serde_json::Value {
    let (status, body) = serve_inbound_with_env(
        composed,
        sidecar_endpoint,
        &[("SUBSCRIBE", "1")],
        "GET",
        "/dapr/subscribe",
        "application/json",
        Vec::new(),
    )
    .await
    .expect("inbound handler errored");
    assert_eq!(status, 200);
    serde_json::from_slice(&body).unwrap()
}

fn subscription_for(subscriptions: &serde_json::Value, topic: &str) -> serde_json::Value {
    subscriptions
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["topic"] == topic)
        .unwrap_or_else(|| panic!("no subscription for {topic}: {subscriptions}"))
        .clone()
}

/// Bulk subscribe discovery: a subscription that opts in via
/// `with_bulk_subscribe` is advertised on `GET /dapr/subscribe` with Dapr's
/// `bulkSubscribe` object and routed to the separate bulk route; a plain
/// subscription keeps the single-event route and has no `bulkSubscribe`.
#[tokio::test]
async fn inbound_subscribe_advertises_bulk_subscribe() {
    let sidecar = MockSidecar::start().await.unwrap();
    let subscriptions = discover_subscriptions(&composed_microservice(), &sidecar.endpoint).await;

    let single = subscription_for(&subscriptions, "orders");
    assert!(single.get("bulkSubscribe").is_none(), "{single}");

    let bulk = subscription_for(&subscriptions, "orders-bulk");
    assert_eq!(bulk["pubsubname"], "pubsub");
    assert_eq!(
        bulk["bulkSubscribe"],
        serde_json::json!({ "enabled": true, "maxMessagesCount": 10, "maxAwaitDurationMs": 500 })
    );
    assert_ne!(bulk["route"], single["route"], "bulk needs its own route");
}

/// Bulk delivery: the envelope daprd POSTs to the bulk route (CloudEvent
/// entries carry the parsed CloudEvent object, raw entries a base64 string)
/// reaches `on-bulk-topic-event`; the SDK helper hands each entry to
/// `on-topic-event`, and the per-entry statuses come back as Dapr's
/// `{ "statuses": [...] }`.
#[tokio::test]
async fn inbound_bulk_topic_event_is_delivered() {
    let sidecar = MockSidecar::start().await.unwrap();
    let composed = composed_microservice();

    let subscriptions = discover_subscriptions(&composed, &sidecar.endpoint).await;
    let route = subscription_for(&subscriptions, "orders-bulk")["route"]
        .as_str()
        .expect("bulk subscription has no route")
        .to_string();

    let cloud_event = |id: &str, order: serde_json::Value| {
        serde_json::json!({
            "id": id,
            "source": "publisher",
            "type": "com.dapr.event.sent",
            "specversion": "1.0",
            "datacontenttype": "application/json",
            "data": order,
            "topic": "orders-bulk",
            "pubsubname": "pubsub",
        })
    };
    let envelope = serde_json::json!({
        "id": "bulk-1",
        "type": "com.dapr.event.sent",
        "topic": "orders-bulk",
        "pubsubname": "pubsub",
        "metadata": {},
        "entries": [
            {
                "entryId": "e1",
                "event": cloud_event("ce-1", serde_json::json!({ "orderId": 1 })),
                "contentType": "application/cloudevents+json",
                "metadata": {}
            },
            {
                "entryId": "e2",
                "event": cloud_event("ce-2", serde_json::json!({ "orderId": 2 })),
                "contentType": "application/cloudevents+json",
                "metadata": {}
            },
            {
                // Raw payload entry: base64 of `{"orderId":3}`.
                "entryId": "e3",
                "event": "eyJvcmRlcklkIjozfQ==",
                "contentType": "application/json",
                "metadata": { "rawPayload": "true" }
            },
            {
                // Not an order: the app's handler answers RETRY.
                "entryId": "e4",
                "event": cloud_event("ce-4", serde_json::json!({ "nope": true })),
                "contentType": "application/cloudevents+json",
                "metadata": {}
            }
        ]
    })
    .to_string()
    .into_bytes();

    let (status, body) = serve_inbound(
        &composed,
        &sidecar.endpoint,
        "POST",
        &route,
        "application/json",
        envelope,
    )
    .await
    .expect("inbound handler errored");
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));

    let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        response,
        serde_json::json!({ "statuses": [
            { "entryId": "e1", "status": "SUCCESS" },
            { "entryId": "e2", "status": "SUCCESS" },
            { "entryId": "e3", "status": "SUCCESS" },
            { "entryId": "e4", "status": "RETRY" },
        ]})
    );

    let recorded = sidecar.recorded.lock().unwrap();
    let state = |key: &str| {
        recorded
            .state
            .get(&("statestore".to_string(), key.to_string()))
            .cloned()
    };
    for id in 1..=3 {
        assert_eq!(
            state(&format!("bulk-order-{id}")),
            Some(serde_json::json!({ "orderId": id })),
            "order {id} not processed"
        );
    }
    assert_eq!(state("bulk-processed-count"), Some(serde_json::json!(3)));
    assert_eq!(state("bulk-batch-count"), Some(serde_json::json!(1)));
}
