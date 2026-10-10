//! E2E microservice — a single typed Dapr **reactor**, run as **two
//! instances** (a *publisher* and a *consumer*) so one app exercises the whole
//! Dapr round-trip: outbound state / pub/sub / service-invocation calls *and*
//! the inbound app channel (pub/sub delivery, input bindings, invocation
//! handling). It never
//! touches HTTP — it implements [`DaprApp`] and is composed behind a provider.
//!
//! The **same** binary backs both the wasi-http E2E (composed with the
//! wasi-http providers, served by `wasmtime serve`) and the wasi-grpc E2E
//! (gRPC outbound + HTTP inbound, served by `spin up`) — the two suites run an
//! identical scenario and differ only in provider + runtime.
//!
//! Behaviour is shaped by env (set by the test harness):
//!
//! - `APP_ID` — this instance's Dapr app id (used for self-invocation and
//!   reported by the `smoke` method).
//! - `PEER_APP_ID` — the *other* instance's app id, the target of
//!   `invoke-peer` (cross-app service invocation via the sidecar).
//! - `SUBSCRIBE` — when set, the app subscribes to the `orders` topic (the
//!   consumer role), to the `orders-bulk` topic with bulk delivery, and
//!   declares the `ticks` input binding; when unset, it
//!   subscribes to nothing (the publisher role).
//!   The subscription uses CEL routing rules: orders with `"priority": true`
//!   go to [`PRIORITY_ROUTE`], all others to the default [`ORDERS_ROUTE`].
//!   The consumer records the delivered `path` per order (`route-<orderId>`),
//!   readable via `/route?order=N`.
//!   `orders-bulk` is bulk-subscribed with a default route ([`BULK_ROUTE`]),
//!   so bulk delivery and routing are exercised together; bulk entries record
//!   their delivered `path` as `bulk-route-<orderId>` (`/bulk-route?order=N`).
//!
//! The test harness drives the control methods by HTTP-GETting them on the
//! served app channel (`/smoke`, `/publish?n=N`, `/invoke-self`, …); the
//! inbound provider routes those unknown paths to [`DaprApp::on_invoke`].

use dapr_app::callback::bindings_callback::{
    BindingConcurrency, BindingEvent, BindingEventResponse,
};
use dapr_app::callback::configuration_callback::ConfigurationUpdate;
use dapr_app::callback::invocation_callback::{HttpResponse, InvokeRequest};
use dapr_app::callback::pubsub_callback::{
    BulkTopicEvent, BulkTopicEventEntryResponse, TopicEvent, TopicEventResponse, TopicSubscription,
};
use dapr_app::dapr::types::AppError;
use dapr_app::dapr::{invocation, runtime, secrets, state};
use dapr_app::DaprApp;
use serde_json::json;

const STATE_STORE: &str = "statestore";
const PUBSUB: &str = "pubsub";
const TOPIC: &str = "orders";
const COUNTER_KEY: &str = "processed-count";
/// The bulk-subscribed topic and its counters: orders processed, and bulk
/// deliveries (batches) received.
const BULK_TOPIC: &str = "orders-bulk";
const BULK_COUNTER_KEY: &str = "bulk-processed-count";
const BULK_BATCHES_KEY: &str = "bulk-batch-count";
/// Default route of the `orders-bulk` subscription (bulk + routing together).
pub const BULK_ROUTE: &str = "/orders-bulk";
/// Default route of the `orders` subscription.
pub const ORDERS_ROUTE: &str = "/orders";
/// Route selected by the CEL rule for priority orders.
pub const PRIORITY_ROUTE: &str = "/orders/priority";
/// CEL rule selecting [`PRIORITY_ROUTE`].
const PRIORITY_RULE: &str = "has(event.data.priority) && event.data.priority == true";
/// The `orderId` that `publish-priority` uses.
const PRIORITY_ORDER_ID: u64 = 100;
/// The cron input binding the consumer handles (`bindings.cron`, `@every 1s`).
const TICKS_BINDING: &str = "ticks";
/// Counter the consumer bumps itself (outbound state API) on every tick.
const TICKS_KEY: &str = "binding-ticks";
/// Key the consumer asks *the sidecar* to persist, via the input-binding
/// response (`storeName` + `state`).
const BINDING_RESPONSE_KEY: &str = "binding-response";
const BINDING_RESPONSE_VALUE: &str = r#"{"from":"binding-response"}"#;
const SECRET_STORE: &str = "secretstore";

fn app_id() -> String {
    std::env::var("APP_ID").unwrap_or_else(|_| "microservice".to_string())
}

fn peer_app_id() -> String {
    std::env::var("PEER_APP_ID").unwrap_or_default()
}

fn subscribes() -> bool {
    std::env::var("SUBSCRIBE").is_ok()
}

struct Microservice;

impl DaprApp for Microservice {
    fn list_topic_subscriptions() -> Vec<TopicSubscription> {
        if subscribes() {
            vec![
                TopicSubscription::new(PUBSUB, TOPIC)
                    .with_rule(PRIORITY_RULE, PRIORITY_ROUTE)
                    .with_default_route(ORDERS_ROUTE),
                TopicSubscription::new(PUBSUB, BULK_TOPIC)
                    .with_bulk_subscribe(Some(10), Some(500))
                    .with_default_route(BULK_ROUTE),
            ]
        } else {
            Vec::new()
        }
    }

    fn on_topic_event(event: TopicEvent) -> TopicEventResponse {
        // `event.data` is the domain payload; the envelope is already parsed.
        let (prefix, counter, route_prefix) = if event.topic == BULK_TOPIC {
            ("bulk-order", BULK_COUNTER_KEY, "bulk-route")
        } else {
            ("order", COUNTER_KEY, "route")
        };
        match process_order(
            &event.data,
            event.path.as_deref(),
            prefix,
            counter,
            route_prefix,
        ) {
            Ok(()) => TopicEventResponse::Success,
            Err(error) => {
                eprintln!("order processing failed: {error}");
                TopicEventResponse::Retry
            }
        }
    }

    fn on_bulk_topic_event(event: BulkTopicEvent) -> Vec<BulkTopicEventEntryResponse> {
        // Count the batch (proves delivery came through the bulk path), then
        // let the SDK hand each entry to `on_topic_event`.
        if let Err(error) = increment(BULK_BATCHES_KEY) {
            eprintln!("counting bulk batch failed: {error}");
        }
        dapr_app::bulk_to_single(event, <Self as DaprApp>::on_topic_event)
    }

    fn list_input_bindings() -> Vec<String> {
        if subscribes() {
            vec![TICKS_BINDING.to_string()]
        } else {
            Vec::new()
        }
    }

    fn on_binding_event(event: BindingEvent) -> Result<BindingEventResponse, AppError> {
        if event.name != TICKS_BINDING {
            return Err(AppError {
                message: format!("unexpected input binding {}", event.name),
            });
        }
        // Count the delivery ourselves, so delivery is observable on its own.
        increment(TICKS_KEY).map_err(|message| AppError { message })?;
        // Then ask the sidecar to persist one more item for us.
        Ok(BindingEventResponse {
            store_name: Some(STATE_STORE.to_string()),
            states: vec![state::StateItem {
                key: BINDING_RESPONSE_KEY.to_string(),
                value: BINDING_RESPONSE_VALUE.as_bytes().to_vec(),
                etag: None,
                metadata: Vec::new(),
                options: None,
            }],
            to: Vec::new(),
            data: None,
            concurrency: BindingConcurrency::Sequential,
        })
    }

    fn on_configuration_event(update: ConfigurationUpdate) {
        // Persist each delivered item under `config-<key>` so the round-trip is
        // observable from outside (the harness asserts on the state writes).
        for (key, item) in &update.items {
            if let Err(error) = state::save(
                STATE_STORE,
                &[state::StateItem {
                    key: format!("config-{key}"),
                    value: item.value.clone().into_bytes(),
                    etag: None,
                    metadata: Vec::new(),
                    options: None,
                }],
                &[],
            ) {
                eprintln!("persisting config item {key} failed: {error:?}");
            }
        }
    }

    fn on_invoke(request: InvokeRequest) -> HttpResponse {
        let result = match request.method.as_str() {
            "count" | "summary" => read_counter(COUNTER_KEY).map(|c| json!({ "processed": c })),
            "count-bulk" => read_counter(BULK_COUNTER_KEY).and_then(|processed| {
                let batches = read_counter(BULK_BATCHES_KEY)?;
                Ok(json!({ "processed": processed, "batches": batches }))
            }),
            "publish" => publish_orders(TOPIC, query_n(request.query.as_deref()))
                .map(|n| json!({ "published": n })),
            "publish-bulk" => publish_orders(BULK_TOPIC, query_n(request.query.as_deref()))
                .map(|n| json!({ "published": n })),
            "bulk-route" => read_route_at(
                "bulk-route",
                query_param(request.query.as_deref(), "order").unwrap_or(0),
            ),
            "binding" => binding_status(),
            "publish-priority" => publish_priority_order().map(|id| json!({ "published": id })),
            "route" => read_route(query_param(request.query.as_deref(), "order").unwrap_or(0)),
            "smoke" => smoke(StateValue::Json),
            "smoke-binary" => smoke(StateValue::Binary),
            "invoke-self" => invoke_self(),
            "secrets" => check_secrets(),
            "invoke-peer" => invoke_peer(),
            "echo" => Ok(json!({ "echo": String::from_utf8_lossy(&request.data).into_owned() })),
            _ => return json_response(404, &json!({ "error": "not found" })),
        };
        match result {
            Ok(value) => json_response(200, &value),
            Err(error) => json_response(500, &json!({ "error": error })),
        }
    }
}

dapr_app::export_app!(Microservice);

fn json_response(status: u16, value: &serde_json::Value) -> HttpResponse {
    HttpResponse {
        status,
        headers: vec![("content-type".to_string(), "application/json".to_string())],
        body: value.to_string().into_bytes(),
    }
}

/// Extract `n` from a `n=N` query string (default 1).
fn query_n(query: Option<&str>) -> u64 {
    query_param(query, "n").unwrap_or(1)
}

/// Extract a numeric `name=N` parameter from a query string.
fn query_param(query: Option<&str>, name: &str) -> Option<u64> {
    query
        .unwrap_or("")
        .split('&')
        .find_map(|pair| pair.strip_prefix(name)?.strip_prefix('='))
        .and_then(|n| n.parse().ok())
}

/// Publish one order the CEL rule routes to [`PRIORITY_ROUTE`].
fn publish_priority_order() -> Result<u64, String> {
    dapr_app::dapr::pubsub::publish(
        PUBSUB,
        TOPIC,
        json!({ "orderId": PRIORITY_ORDER_ID, "priority": true })
            .to_string()
            .as_bytes(),
        Some("application/json"),
        &[],
    )
    .map_err(|e| format!("publishing priority order failed: {e:?}"))?;
    Ok(PRIORITY_ORDER_ID)
}

/// The route an order was delivered on (`null` if not delivered yet).
fn read_route(order_id: u64) -> Result<serde_json::Value, String> {
    read_route_at("route", order_id)
}

/// The route stored under `<prefix>-<order_id>` (`null` if not delivered yet).
fn read_route_at(prefix: &str, order_id: u64) -> Result<serde_json::Value, String> {
    match state::get(STATE_STORE, &format!("{prefix}-{order_id}"), None, &[]) {
        Ok(response) if response.data.is_empty() => Ok(json!({ "route": null })),
        Ok(response) => {
            let route: serde_json::Value = serde_json::from_slice(&response.data)
                .map_err(|e| format!("stored route is not JSON: {e}"))?;
            Ok(json!({ "route": route }))
        }
        Err(state::GetError::KeyNotFound) => Ok(json!({ "route": null })),
        Err(other) => Err(format!("reading route failed: {other:?}")),
    }
}

fn publish_orders(topic: &str, n: u64) -> Result<u64, String> {
    for order_id in 1..=n {
        dapr_app::dapr::pubsub::publish(
            PUBSUB,
            topic,
            json!({ "orderId": order_id }).to_string().as_bytes(),
            Some("application/json"),
            &[],
        )
        .map_err(|e| format!("publishing order {order_id} failed: {e:?}"))?;
    }
    Ok(n)
}

enum StateValue {
    /// A valid-JSON value — roundtrips byte-exact on both providers.
    Json,
    /// Deliberately non-JSON, non-UTF-8 bytes — only the gRPC provider (raw
    /// protobuf bytes) returns these byte-exact; the HTTP provider's JSON
    /// envelope cannot.
    Binary,
}

/// State end to end: byte-exact roundtrip, etag CAS (stale write rejected),
/// delete, and a metadata sanity check.
fn smoke(kind: StateValue) -> Result<serde_json::Value, String> {
    let (key, value): (&str, Vec<u8>) = match kind {
        StateValue::Json => ("smoke-json", br#"{"v":42}"#.to_vec()),
        StateValue::Binary => (
            "smoke-binary",
            vec![0x00, 0xFF, 0x9F, 0x92, 0x96, b'"', b'{'],
        ),
    };

    state::save(
        STATE_STORE,
        &[state::StateItem {
            key: key.into(),
            value: value.clone(),
            etag: None,
            metadata: Vec::new(),
            options: None,
        }],
        &[],
    )
    .map_err(|e| format!("save failed: {e:?}"))?;

    let read = state::get(STATE_STORE, key, None, &[]).map_err(|e| match e {
        state::GetError::KeyNotFound => "get returned not-found after save".to_string(),
        other => format!("get failed: {other:?}"),
    })?;
    if read.data != value {
        return Err(format!(
            "value did not roundtrip: wrote {value:?}, read {:?}",
            read.data
        ));
    }

    // CAS: a write with a fresh etag succeeds, a second write with the now
    // stale etag must be rejected.
    let etag = read.etag.clone().ok_or("store returned no etag")?;
    state::save(STATE_STORE, &[cas_item(key, b"second", etag.clone())], &[])
        .map_err(|e| format!("etag save failed: {e:?}"))?;
    let stale = state::save(STATE_STORE, &[cas_item(key, b"third", etag)], &[]);
    if !matches!(stale, Err(state::WriteError::EtagMismatch(_))) {
        return Err(format!("stale etag write was not rejected: {stale:?}"));
    }

    state::delete(STATE_STORE, key, None, None, &[])
        .map_err(|e| format!("delete failed: {e:?}"))?;
    match state::get(STATE_STORE, key, None, &[]) {
        // HTTP surfaces a deleted key as not-found; the gRPC API has no 204
        // analogue, so absence shows up as an empty (Ok) value instead.
        Err(state::GetError::KeyNotFound) => {}
        Ok(read) if read.data.is_empty() => {}
        Ok(_) => return Err("key still present after delete".to_string()),
        Err(other) => return Err(format!("get after delete failed: {other:?}")),
    }

    let metadata = runtime::get_metadata().map_err(|e| format!("get-metadata failed: {e:?}"))?;
    let document: serde_json::Value =
        serde_json::from_str(&metadata).map_err(|e| format!("metadata is not JSON: {e}"))?;
    let id = document["id"].as_str().unwrap_or_default().to_string();
    if id.is_empty() {
        return Err(format!("metadata has no id: {document}"));
    }

    Ok(json!({ "ok": true, "appId": id }))
}

fn cas_item(key: &str, value: &[u8], etag: String) -> state::StateItem {
    state::StateItem {
        key: key.into(),
        value: value.to_vec(),
        etag: Some(etag),
        metadata: Vec::new(),
        options: Some(state::StateOptions {
            concurrency: state::Concurrency::FirstWrite,
            consistency: state::Consistency::Strong,
        }),
    }
}

/// Service invocation of our own `echo` method: out through the provider, back
/// in through our app channel.
/// Read the `local.file` secret store: `db` exists, `missing` must be the
/// typed `secret-not-found` (daprd reports it as a generic ERR_SECRET_GET).
fn check_secrets() -> Result<serde_json::Value, String> {
    let secret = secrets::get_secret(SECRET_STORE, "db", &[])
        .map_err(|e| format!("get-secret db failed: {e:?}"))?;
    if secret != [("db".to_string(), "s3cret".to_string())] {
        return Err(format!("unexpected secret db: {secret:?}"));
    }
    match secrets::get_secret(SECRET_STORE, "missing", &[]) {
        Err(secrets::GetSecretError::SecretNotFound) => Ok(json!({ "ok": true })),
        other => Err(format!(
            "missing secret: expected secret-not-found, got {other:?}"
        )),
    }
}

fn invoke_self() -> Result<serde_json::Value, String> {
    let response = invocation::invoke(
        &app_id(),
        "echo",
        invocation::HttpVerb::Post,
        &[("content-type".to_string(), "text/plain".to_string())],
        None,
        b"ping",
    )
    .map_err(|e| format!("invoke failed: {e:?}"))?;
    if response.status / 100 != 2 {
        return Err(format!(
            "echo returned status {}: {}",
            response.status,
            String::from_utf8_lossy(&response.body)
        ));
    }
    let body: serde_json::Value = serde_json::from_slice(&response.body)
        .map_err(|e| format!("echo response is not JSON: {e}"))?;
    if body["echo"] != json!("ping") {
        return Err(format!("unexpected echo body: {body}"));
    }
    Ok(json!({ "ok": true }))
}

/// Cross-app service invocation of `PEER_APP_ID`'s `summary` (needs the
/// sidecars to resolve each other's app ids).
fn invoke_peer() -> Result<serde_json::Value, String> {
    let peer = peer_app_id();
    if peer.is_empty() {
        return Err("PEER_APP_ID not set".to_string());
    }
    let response = invocation::invoke(&peer, "summary", invocation::HttpVerb::Post, &[], None, &[])
        .map_err(|e| format!("invoking {peer}/summary failed: {e:?}"))?;
    if response.status / 100 != 2 {
        return Err(format!(
            "{peer}/summary returned status {}: {}",
            response.status,
            String::from_utf8_lossy(&response.body)
        ));
    }
    let summary: serde_json::Value = serde_json::from_slice(&response.body)
        .map_err(|e| format!("summary response is not JSON: {e}"))?;
    Ok(json!({ "processed": summary["processed"].as_u64().unwrap_or(0) }))
}

fn process_order(
    order: &[u8],
    path: Option<&str>,
    key_prefix: &str,
    counter_key: &str,
    route_prefix: &str,
) -> Result<(), String> {
    let order: serde_json::Value =
        serde_json::from_slice(order).map_err(|e| format!("invalid order payload: {e}"))?;
    let order_id = order["orderId"]
        .as_u64()
        .ok_or_else(|| format!("order without orderId: {order}"))?;

    state::save(
        STATE_STORE,
        &[
            state::StateItem {
                key: format!("{key_prefix}-{order_id}"),
                value: order.to_string().into_bytes(),
                etag: None,
                metadata: Vec::new(),
                options: None,
            },
            // The route the sidecar's CEL routing picked for this order.
            state::StateItem {
                key: format!("{route_prefix}-{order_id}"),
                value: json!(path).to_string().into_bytes(),
                etag: None,
                metadata: Vec::new(),
                options: None,
            },
        ],
        &[],
    )
    .map_err(|e| format!("saving order failed: {e:?}"))?;

    increment(counter_key)?;
    println!("processed {key_prefix} {order_id}");
    Ok(())
}

/// CAS-increment the counter at `key` (deliveries may be concurrent): read
/// value + etag, write back first-write, retry on conflict.
fn increment(key: &str) -> Result<(), String> {
    for _ in 0..16 {
        let (count, etag) = match state::get(STATE_STORE, key, None, &[]) {
            Ok(response) => (
                String::from_utf8_lossy(&response.data)
                    .parse::<u64>()
                    .unwrap_or(0),
                response.etag,
            ),
            Err(state::GetError::KeyNotFound) => (0, None),
            Err(other) => return Err(format!("reading counter failed: {other:?}")),
        };

        let result = state::save(
            STATE_STORE,
            &[state::StateItem {
                key: key.to_string(),
                value: (count + 1).to_string().into_bytes(),
                etag,
                metadata: Vec::new(),
                options: Some(state::StateOptions {
                    concurrency: state::Concurrency::FirstWrite,
                    consistency: state::Consistency::Strong,
                }),
            }],
            &[],
        );
        match result {
            Ok(()) => return Ok(()),
            Err(state::WriteError::EtagMismatch(_)) => continue,
            Err(other) => return Err(format!("incrementing counter failed: {other:?}")),
        }
    }
    Err("incrementing counter failed: too many etag conflicts".to_string())
}

fn read_counter(key: &str) -> Result<u64, String> {
    match state::get(STATE_STORE, key, None, &[]) {
        Ok(response) => Ok(String::from_utf8_lossy(&response.data)
            .parse::<u64>()
            .unwrap_or(0)),
        Err(state::GetError::KeyNotFound) => Ok(0),
        Err(other) => Err(format!("reading counter failed: {other:?}")),
    }
}

/// Input-binding status: how many `ticks` events were delivered, and what the
/// sidecar persisted from our binding responses (`null` if nothing).
fn binding_status() -> Result<serde_json::Value, String> {
    let ticks = read_counter(TICKS_KEY)?;
    let persisted = match state::get(STATE_STORE, BINDING_RESPONSE_KEY, None, &[]) {
        Ok(response) if !response.data.is_empty() => {
            Some(String::from_utf8_lossy(&response.data).into_owned())
        }
        Ok(_) | Err(state::GetError::KeyNotFound) => None,
        Err(other) => return Err(format!("reading {BINDING_RESPONSE_KEY} failed: {other:?}")),
    };
    Ok(json!({ "ticks": ticks, "responseState": persisted }))
}
