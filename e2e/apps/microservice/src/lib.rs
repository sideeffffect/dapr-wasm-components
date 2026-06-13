//! E2E microservice — a single typed Dapr **reactor**, run as **two
//! instances** (a *publisher* and a *consumer*) so one app exercises the whole
//! Dapr round-trip: outbound state / pub/sub / service-invocation calls *and*
//! the inbound app channel (pub/sub delivery, invocation handling). It never
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
//!   consumer role); when unset, it subscribes to nothing (the publisher role).
//!
//! The test harness drives the control methods by HTTP-GETting them on the
//! served app channel (`/smoke`, `/publish?n=N`, `/invoke-self`, …); the
//! inbound provider routes those unknown paths to [`DaprApp::on_invoke`].

use dapr_app::callback::invocation_callback::{HttpResponse, InvokeRequest};
use dapr_app::callback::pubsub_callback::{TopicEvent, TopicEventResponse, TopicSubscription};
use dapr_app::dapr::{invocation, runtime, state};
use dapr_app::DaprApp;
use serde_json::json;

const STATE_STORE: &str = "statestore";
const PUBSUB: &str = "pubsub";
const TOPIC: &str = "orders";
const COUNTER_KEY: &str = "processed-count";

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
            vec![TopicSubscription {
                pubsub_name: PUBSUB.to_string(),
                topic: TOPIC.to_string(),
                metadata: Vec::new(),
                dead_letter_topic: None,
            }]
        } else {
            Vec::new()
        }
    }

    fn on_topic_event(event: TopicEvent) -> TopicEventResponse {
        // `event.data` is the domain payload; the envelope is already parsed.
        match process_order(&event.data) {
            Ok(()) => TopicEventResponse::Success,
            Err(error) => {
                eprintln!("order processing failed: {error}");
                TopicEventResponse::Retry
            }
        }
    }

    fn on_invoke(request: InvokeRequest) -> HttpResponse {
        let result = match request.method.as_str() {
            "count" | "summary" => read_counter().map(|c| json!({ "processed": c })),
            "publish" => {
                publish_orders(query_n(request.query.as_deref())).map(|n| json!({ "published": n }))
            }
            "smoke" => smoke(StateValue::Json),
            "smoke-binary" => smoke(StateValue::Binary),
            "invoke-self" => invoke_self(),
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
    query
        .unwrap_or("")
        .split('&')
        .find_map(|pair| pair.strip_prefix("n="))
        .and_then(|n| n.parse().ok())
        .unwrap_or(1)
}

fn publish_orders(n: u64) -> Result<u64, String> {
    for order_id in 1..=n {
        dapr_app::dapr::pubsub::publish(
            PUBSUB,
            TOPIC,
            json!({ "orderId": order_id }).to_string().as_bytes(),
            "application/json",
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

    let read = state::get(STATE_STORE, key, None, &[])
        .map_err(|e| format!("get failed: {e:?}"))?
        .ok_or("get returned none after save")?;
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
    if !matches!(
        stale,
        Err(state::Error::Aborted(_)) | Err(state::Error::Internal(_))
    ) {
        return Err(format!("stale etag write was not rejected: {stale:?}"));
    }

    state::delete(STATE_STORE, key, None, None, &[])
        .map_err(|e| format!("delete failed: {e:?}"))?;
    if state::get(STATE_STORE, key, None, &[])
        .map_err(|e| format!("get after delete failed: {e:?}"))?
        .is_some()
    {
        return Err("key still present after delete".to_string());
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

fn process_order(order: &[u8]) -> Result<(), String> {
    let order: serde_json::Value =
        serde_json::from_slice(order).map_err(|e| format!("invalid order payload: {e}"))?;
    let order_id = order["orderId"]
        .as_u64()
        .ok_or_else(|| format!("order without orderId: {order}"))?;

    state::save(
        STATE_STORE,
        &[state::StateItem {
            key: format!("order-{order_id}"),
            value: order.to_string().into_bytes(),
            etag: None,
            metadata: Vec::new(),
            options: None,
        }],
        &[],
    )
    .map_err(|e| format!("saving order failed: {e:?}"))?;

    increment_counter()?;
    println!("processed order {order_id}");
    Ok(())
}

/// CAS-increment the processed-order counter (deliveries may be concurrent):
/// read value + etag, write back first-write, retry on conflict.
fn increment_counter() -> Result<(), String> {
    for _ in 0..16 {
        let current = state::get(STATE_STORE, COUNTER_KEY, None, &[])
            .map_err(|e| format!("reading counter failed: {e:?}"))?;
        let (count, etag) = match &current {
            Some(response) => (
                String::from_utf8_lossy(&response.data)
                    .parse::<u64>()
                    .unwrap_or(0),
                response.etag.clone(),
            ),
            None => (0, None),
        };

        let result = state::save(
            STATE_STORE,
            &[state::StateItem {
                key: COUNTER_KEY.to_string(),
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
            Err(state::Error::Aborted(_)) | Err(state::Error::Internal(_)) => continue,
            Err(other) => return Err(format!("incrementing counter failed: {other:?}")),
        }
    }
    Err("incrementing counter failed: too many etag conflicts".to_string())
}

fn read_counter() -> Result<u64, String> {
    let current = state::get(STATE_STORE, COUNTER_KEY, None, &[])
        .map_err(|e| format!("reading counter failed: {e:?}"))?;
    Ok(current
        .map(|response| {
            String::from_utf8_lossy(&response.data)
                .parse::<u64>()
                .unwrap_or(0)
        })
        .unwrap_or(0))
}
