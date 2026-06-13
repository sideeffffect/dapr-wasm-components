//! Spin-demo: the typed Dapr application for the wasi-grpc provider's E2E.
//!
//! A reactor component (no HTTP of its own): it implements [`DaprApp`] and is
//! composed behind the wasi-grpc provider, which serves the sidecar's gRPC
//! app channel (`AppCallback`) and turns each call into the typed callbacks
//! below. The test drives it through Dapr **service invocation** — every old
//! control route is now an `on-invoke` method:
//!
//! - `smoke` — state roundtrip over gRPC with *binary* values (byte-exact),
//!   etag CAS, delete, metadata sanity check
//! - `invoke-self` — Dapr service invocation of our own `echo` method (gRPC
//!   `InvokeService` out, `AppCallback.OnInvoke` back in)
//! - `publish` — publish an order to the `orders` topic
//! - `count` — current processed-order count
//! - `echo` — echo the request body
//!
//! and the `orders` topic subscription counts delivered orders.

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
    std::env::var("APP_ID").unwrap_or_else(|_| "spin-demo".to_string())
}

struct SpinDemo;

impl DaprApp for SpinDemo {
    fn list_topic_subscriptions() -> Vec<TopicSubscription> {
        vec![TopicSubscription {
            pubsub_name: PUBSUB.to_string(),
            topic: TOPIC.to_string(),
            metadata: Vec::new(),
            dead_letter_topic: None,
        }]
    }

    fn on_topic_event(event: TopicEvent) -> TopicEventResponse {
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
            "smoke" => smoke(),
            "invoke-self" => invoke_self(),
            "publish" => publish_order().map(|()| json!({ "published": true })),
            "count" => read_counter().map(|count| json!({ "processed": count })),
            "echo" => {
                let echoed = String::from_utf8_lossy(&request.data).into_owned();
                Ok(json!({ "echo": echoed }))
            }
            other => {
                return json_response(404, &json!({ "error": format!("unknown method: {other}") }))
            }
        };
        match result {
            Ok(value) => json_response(200, &value),
            Err(error) => json_response(500, &json!({ "error": error })),
        }
    }
}

dapr_app::export_app!(SpinDemo);

/// State over gRPC, end to end: binary roundtrip, etag CAS, delete.
fn smoke() -> Result<serde_json::Value, String> {
    // Bytes that are deliberately not valid JSON nor UTF-8: the gRPC provider
    // must return them byte-exact (the HTTP provider cannot).
    let binary: Vec<u8> = vec![0x00, 0xFF, 0x9F, 0x92, 0x96, b'"', b'{'];

    state::save(
        STATE_STORE,
        &[state::StateItem {
            key: "smoke-binary".into(),
            value: binary.clone(),
            etag: None,
            metadata: Vec::new(),
            options: None,
        }],
        &[],
    )
    .map_err(|e| format!("save failed: {e:?}"))?;

    let read = state::get(STATE_STORE, "smoke-binary", None, &[])
        .map_err(|e| format!("get failed: {e:?}"))?
        .ok_or("get returned none after save")?;
    if read.data != binary {
        return Err(format!(
            "binary value did not roundtrip: wrote {binary:?}, read {:?}",
            read.data
        ));
    }

    // CAS: writing with a stale etag must be rejected.
    let etag = read.etag.clone().ok_or("store returned no etag")?;
    state::save(
        STATE_STORE,
        &[state::StateItem {
            key: "smoke-binary".into(),
            value: b"second".to_vec(),
            etag: Some(etag.clone()),
            metadata: Vec::new(),
            options: Some(state::StateOptions {
                concurrency: state::Concurrency::FirstWrite,
                consistency: state::Consistency::Strong,
            }),
        }],
        &[],
    )
    .map_err(|e| format!("etag save failed: {e:?}"))?;
    let stale = state::save(
        STATE_STORE,
        &[state::StateItem {
            key: "smoke-binary".into(),
            value: b"third".to_vec(),
            etag: Some(etag),
            metadata: Vec::new(),
            options: Some(state::StateOptions {
                concurrency: state::Concurrency::FirstWrite,
                consistency: state::Consistency::Strong,
            }),
        }],
        &[],
    );
    if !matches!(
        stale,
        Err(state::Error::Aborted(_)) | Err(state::Error::Internal(_))
    ) {
        return Err(format!("stale etag write was not rejected: {stale:?}"));
    }

    state::delete(STATE_STORE, "smoke-binary", None, None, &[])
        .map_err(|e| format!("delete failed: {e:?}"))?;
    if state::get(STATE_STORE, "smoke-binary", None, &[])
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

/// Service invocation of our own `echo` method: out through gRPC
/// `InvokeService`, back in through the app channel (`AppCallback.OnInvoke`).
fn invoke_self() -> Result<serde_json::Value, String> {
    let response = invocation::invoke(
        &app_id(),
        "echo",
        invocation::HttpVerb::Post,
        &[("content-type".to_string(), "text/plain".to_string())],
        None,
        b"ping-over-grpc",
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
    if body["echo"] != json!("ping-over-grpc") {
        return Err(format!("unexpected echo body: {body}"));
    }
    Ok(json!({ "ok": true }))
}

fn publish_order() -> Result<(), String> {
    dapr_app::dapr::pubsub::publish(
        PUBSUB,
        TOPIC,
        json!({ "orderId": 1 }).to_string().as_bytes(),
        "application/json",
        &[],
    )
    .map_err(|e| format!("publish failed: {e:?}"))
}

fn process_order(order: &[u8]) -> Result<(), String> {
    let order: serde_json::Value =
        serde_json::from_slice(order).map_err(|e| format!("invalid order payload: {e}"))?;
    order["orderId"]
        .as_u64()
        .ok_or_else(|| format!("order without orderId: {order}"))?;
    increment_counter()
}

/// CAS-increment the processed-order counter (deliveries may be concurrent).
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

fn json_response(status: u16, value: &serde_json::Value) -> HttpResponse {
    HttpResponse {
        status,
        headers: vec![("content-type".to_string(), "application/json".to_string())],
        body: value.to_string().into_bytes(),
    }
}
