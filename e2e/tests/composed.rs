//! Composition test: plug kv-demo into the wasi-http provider with
//! wac-graph (the programmatic `wac plug`) and run the resulting command
//! component against the mock sidecar — the same artifact a user would run
//! with `wasmtime run -S http`.

use wasmtime::component::Component;
use wasmtime::Store;

use dapr_wasm_components_e2e::mock::{MockSidecar, MOCK_CRYPTO_COMPONENT};
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

    let env = vec![
        ("DAPR_HTTP_ENDPOINT".to_string(), sidecar.endpoint.clone()),
        (
            "KV_DEMO_CRYPTO_COMPONENT".to_string(),
            MOCK_CRYPTO_COMPONENT.to_string(),
        ),
    ];
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
    // The 3 MiB streaming crypto roundtrip went through as two streamed
    // uploads (encrypt, then decrypt).
    let crypto = &recorded.crypto_requests;
    assert_eq!(crypto.len(), 2, "expected one encrypt and one decrypt");
    assert_eq!(crypto[0].operation, "encrypt");
    assert_eq!(crypto[0].body_len, 3 * 1024 * 1024);
    assert_eq!(crypto[1].operation, "decrypt");
    assert!(crypto.iter().all(|r| r.chunked), "uploads must be streamed");
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

/// The wasi-http inbound provider's reserved pub/sub route prefix.
const PUBSUB_PREFIX: &str = "/__dapr_wasm_components/pubsub";
/// The wasi-http inbound provider's reserved bulk pub/sub route prefix.
const PUBSUB_BULK_PREFIX: &str = "/__dapr_wasm_components/pubsub-bulk";

fn full_http_composition() -> Vec<u8> {
    let app = std::fs::read(microservice_path()).expect("microservice component not built");
    let outbound = std::fs::read(http_outbound_path()).expect("http-outbound not built");
    let inbound = std::fs::read(http_inbound_path()).expect("http-inbound not built");
    compose::plug_full(app, outbound, inbound).expect("full composition failed")
}

/// The microservice's consumer role (`SUBSCRIBE`) declares CEL routing rules;
/// the composed inbound server must advertise them on `GET /dapr/subscribe`
/// in the Dapr HTTP API shape (`routes: {rules: [{match, path}], default}`),
/// with every path mapped under the provider's reserved pub/sub prefix.
#[tokio::test]
async fn inbound_subscribe_advertises_routing_rules() {
    let sidecar = MockSidecar::start().await.unwrap();
    let composed = full_http_composition();

    let subscriptions = discover_subscriptions(&composed, &sidecar.endpoint).await;
    let orders = subscription_for(&subscriptions, "orders");
    assert_eq!(
        orders,
        serde_json::json!({
            "pubsubname": "pubsub",
            "topic": "orders",
            "routes": {
                "rules": [{
                    "match": "has(event.data.priority) && event.data.priority == true",
                    "path": format!("{PUBSUB_PREFIX}/orders/priority"),
                }],
                "default": format!("{PUBSUB_PREFIX}/orders"),
            },
        }),
        "unexpected /dapr/subscribe: {subscriptions}"
    );
}

/// A sidecar POST to a routing-rule path must reach `on-topic-event` with
/// `path` set to the declared rule path (the microservice persists it as
/// `route-<orderId>`); the bare shared route still delivers with no `path`.
#[tokio::test]
async fn inbound_routed_event_is_delivered_with_path() {
    let sidecar = MockSidecar::start().await.unwrap();
    let composed = full_http_composition();

    for (url_path, order_id, expected_route) in [
        (
            format!("{PUBSUB_PREFIX}/orders/priority"),
            100,
            serde_json::json!("/orders/priority"),
        ),
        (
            format!("{PUBSUB_PREFIX}/orders"),
            1,
            serde_json::json!("/orders"),
        ),
        (PUBSUB_PREFIX.to_string(), 2, serde_json::Value::Null),
    ] {
        let event = serde_json::json!({
            "id": format!("evt-{order_id}"),
            "source": "publisher",
            "type": "com.dapr.event.sent",
            "specversion": "1.0",
            "datacontenttype": "application/json",
            "topic": "orders",
            "pubsubname": "pubsub",
            "data": { "orderId": order_id, "priority": order_id == 100 },
        })
        .to_string()
        .into_bytes();

        let (status, body) = serve_inbound_with_env(
            &composed,
            &sidecar.endpoint,
            &[("SUBSCRIBE", "1")],
            "POST",
            &url_path,
            "application/cloudevents+json",
            event,
        )
        .await
        .expect("inbound handler errored");
        assert_eq!(status, 200, "delivery to {url_path} failed");
        let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(response["status"], "SUCCESS", "delivery to {url_path}");

        let recorded = sidecar.recorded.lock().unwrap();
        let route = recorded
            .state
            .get(&("statestore".to_string(), format!("route-{order_id}")))
            .unwrap_or_else(|| panic!("order {order_id} was not processed"));
        assert_eq!(route, &expected_route, "wrong path for {url_path}");
    }
}

/// Inbound input-binding delivery: a sidecar `POST /<binding>` must reach the
/// app's `on-binding-event`, and the reply must be serialized with the field
/// names of Dapr's `bindings.AppResponse` JSON tags (`storeName`, `state`,
/// `to`, `data`, `concurrency` — components-contrib `bindings/responses.go`).
/// The microservice (with `SUBSCRIBE`) declares `ticks`, counts the delivery
/// via the outbound state API, and replies with one `state` item.
#[tokio::test]
async fn inbound_binding_event_reply_shape() {
    let sidecar = MockSidecar::start().await.unwrap();
    let composed = full_http_composition();

    let (status, body) = serve_inbound_with_env(
        &composed,
        &sidecar.endpoint,
        &[("SUBSCRIBE", "1")],
        "POST",
        "/ticks",
        "application/json",
        Vec::new(),
    )
    .await
    .expect("inbound handler errored");
    assert_eq!(status, 200, "binding delivery should return 200");

    let reply: serde_json::Value =
        serde_json::from_slice(&body).expect("binding reply is not JSON");
    assert_eq!(
        reply,
        serde_json::json!({
            "storeName": "statestore",
            "state": [
                { "key": "binding-response", "value": { "from": "binding-response" } }
            ]
        }),
        "unexpected binding reply"
    );

    let recorded = sidecar.recorded.lock().unwrap();
    assert_eq!(
        recorded
            .state
            .get(&("statestore".to_string(), "binding-ticks".to_string())),
        Some(&serde_json::json!(1)),
        "app did not count the delivery"
    );
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
    let subscriptions = discover_subscriptions(&full_http_composition(), &sidecar.endpoint).await;

    let single = subscription_for(&subscriptions, "orders");
    assert!(single.get("bulkSubscribe").is_none(), "{single}");

    let bulk = subscription_for(&subscriptions, "orders-bulk");
    assert_eq!(bulk["pubsubname"], "pubsub");
    assert_eq!(
        bulk["bulkSubscribe"],
        serde_json::json!({ "enabled": true, "maxMessagesCount": 10, "maxAwaitDurationMs": 500 })
    );
    // Bulk entries are envelopes, not CloudEvents, so routes for a bulk
    // subscription live under the provider's separate bulk prefix.
    assert_eq!(
        bulk["routes"],
        serde_json::json!({
            "rules": [],
            "default": format!("{PUBSUB_BULK_PREFIX}/orders-bulk"),
        })
    );
}

/// Bulk delivery: the envelope daprd POSTs to the bulk route (CloudEvent
/// entries carry the parsed CloudEvent object, raw entries a base64 string)
/// reaches `on-bulk-topic-event`; the SDK helper hands each entry to
/// `on-topic-event`, and the per-entry statuses come back as Dapr's
/// `{ "statuses": [...] }`.
#[tokio::test]
async fn inbound_bulk_topic_event_is_delivered() {
    let sidecar = MockSidecar::start().await.unwrap();
    let composed = full_http_composition();

    let subscriptions = discover_subscriptions(&composed, &sidecar.endpoint).await;
    let route = subscription_for(&subscriptions, "orders-bulk")["routes"]["default"]
        .as_str()
        .expect("bulk subscription has no default route")
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
    // The default route was recovered from the URL into every entry's `path`.
    for id in 1..=3 {
        assert_eq!(
            state(&format!("bulk-route-{id}")),
            Some(serde_json::json!("/orders-bulk")),
            "order {id} has the wrong path"
        );
    }
    assert_eq!(state("bulk-processed-count"), Some(serde_json::json!(3)));
    assert_eq!(state("bulk-batch-count"), Some(serde_json::json!(1)));
}
