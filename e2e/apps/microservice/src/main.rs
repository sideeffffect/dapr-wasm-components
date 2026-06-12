//! E2E microservice — a single `wasi:http` server component, run as **two
//! instances** (a *publisher* and a *consumer*) so one app exercises the
//! whole Dapr round-trip: outbound state / pub/sub / service-invocation calls
//! *and* the inbound app channel (pub/sub delivery, invocation handling).
//!
//! The **same** binary backs both the wasi-http E2E (composed with the
//! wasi-http provider, served by `wasmtime serve`) and the wasi-grpc E2E
//! (composed with the wasi-grpc provider, served by `spin up`) — so the two
//! suites run an identical scenario and differ only in provider + runtime.
//!
//! Behaviour is shaped by env (set by the test harness):
//!
//! - `APP_ID` — this instance's Dapr app id (used for self-invocation and
//!   reported by `/smoke`).
//! - `PEER_APP_ID` — the *other* instance's app id, the target of
//!   `/invoke-peer` (cross-app service invocation via the sidecar).
//! - `SUBSCRIBE` — when set, `/dapr/subscribe` declares a subscription to the
//!   `orders` topic (the consumer role); when unset, it declares none (the
//!   publisher role). This mirrors the original two-service split.
//!
//! Routes:
//!
//! - `/dapr/subscribe` — programmatic subscription (consumer role only)
//! - `/orders` — pub/sub delivery: record the order, CAS-increment a counter
//! - `/count`, `/summary` — current processed-order count (the latter is the
//!   service-invocation target)
//! - `/publish?n=N` — publish N orders (default 1) to the `orders` topic
//! - `/smoke` — JSON-value state roundtrip + etag CAS + delete + metadata
//!   (byte-exact on both providers, since a valid-JSON value roundtrips)
//! - `/smoke-binary` — *binary* (non-JSON) state roundtrip; byte-exact only on
//!   the gRPC provider, so only the wasi-grpc suite calls it
//! - `/invoke-self` — service-invoke our own `/echo` (out and back in)
//! - `/invoke-peer` — service-invoke `PEER_APP_ID`'s `/summary`
//! - `/echo`, `/healthz`

use serde_json::json;
use wstd::http::body::Body;
use wstd::http::{Request, Response, StatusCode};

wit_bindgen::generate!({
    world: "dapr-client",
    path: "../../../components/wit",
});

use dapr_wasm_components::interfaces::invocation;
use dapr_wasm_components::interfaces::pubsub;
use dapr_wasm_components::interfaces::runtime;
use dapr_wasm_components::interfaces::state;

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

#[wstd::http_server]
async fn main(mut request: Request<Body>) -> Result<Response<Body>, wstd::http::Error> {
    let path = request.uri().path().to_string();
    let query = request.uri().query().unwrap_or("").to_string();
    let response = match path.as_str() {
        "/dapr/subscribe" => {
            let subs = if subscribes() {
                json!([{ "pubsubname": PUBSUB, "topic": TOPIC, "route": "/orders" }])
            } else {
                json!([])
            };
            json_response(StatusCode::OK, subs)
        }
        "/orders" => {
            let body = request.body_mut().contents().await?;
            match process_order(body) {
                Ok(()) => json_response(StatusCode::OK, json!({ "status": "SUCCESS" })),
                Err(error) => {
                    eprintln!("order processing failed: {error}");
                    json_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        json!({ "status": "RETRY" }),
                    )
                }
            }
        }
        "/count" | "/summary" => fallible(read_counter().map(|c| json!({ "processed": c }))),
        "/publish" => fallible(publish_orders(query_n(&query)).map(|n| json!({ "published": n }))),
        "/smoke" => fallible(smoke(StateValue::Json)),
        "/smoke-binary" => fallible(smoke(StateValue::Binary)),
        "/invoke-self" => fallible(invoke_self()),
        "/invoke-peer" => fallible(invoke_peer()),
        "/echo" => {
            let body = request.body_mut().contents().await?;
            let echoed = String::from_utf8_lossy(body).into_owned();
            json_response(StatusCode::OK, json!({ "echo": echoed }))
        }
        "/" | "/healthz" => json_response(StatusCode::OK, json!({ "status": "ok" })),
        _ => json_response(StatusCode::NOT_FOUND, json!({ "error": "not found" })),
    };
    Ok(response)
}

/// Render a `Result` as a 200 JSON body or a 500 `{"error": …}`.
fn fallible(result: Result<serde_json::Value, String>) -> Response<Body> {
    match result {
        Ok(value) => json_response(StatusCode::OK, value),
        Err(error) => json_response(StatusCode::INTERNAL_SERVER_ERROR, json!({ "error": error })),
    }
}

fn json_response(status: StatusCode, value: serde_json::Value) -> Response<Body> {
    let mut response = Response::new(Body::from(value.to_string().into_bytes()));
    *response.status_mut() = status;
    response.headers_mut().insert(
        "content-type",
        wstd::http::HeaderValue::from_static("application/json"),
    );
    response
}

/// Extract `n` from a `?n=N` query string (default 1).
fn query_n(query: &str) -> u64 {
    query
        .split('&')
        .find_map(|pair| pair.strip_prefix("n="))
        .and_then(|n| n.parse().ok())
        .unwrap_or(1)
}

fn publish_orders(n: u64) -> Result<u64, String> {
    for order_id in 1..=n {
        pubsub::publish(
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
    /// Deliberately non-JSON, non-UTF-8 bytes — only the gRPC provider
    /// (raw protobuf bytes) returns these byte-exact; the HTTP provider's
    /// JSON envelope cannot.
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

/// Service invocation of our own `/echo` route: out through the provider,
/// back in through our app channel.
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

/// Cross-app service invocation of `PEER_APP_ID`'s `/summary` (needs the
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

fn process_order(cloud_event: &[u8]) -> Result<(), String> {
    let event: serde_json::Value =
        serde_json::from_slice(cloud_event).map_err(|e| format!("invalid CloudEvent: {e}"))?;
    let order = &event["data"];
    let order_id = order["orderId"]
        .as_u64()
        .ok_or_else(|| format!("event without orderId: {event}"))?;

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
