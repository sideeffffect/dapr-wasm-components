//! Spin-demo microservice (a `wasi:http` server component).
//!
//! The E2E app for the wasi-grpc provider: composed with it and served by
//! `spin up`, next to a daprd sidecar whose app channel points back at
//! Spin's listener. Routes:
//!
//! - `/smoke` — state roundtrip over gRPC, with *binary* values
//!   (byte-exact, which the HTTP provider cannot do), etag CAS, delete,
//!   and a metadata sanity check
//! - `/invoke-self` — Dapr service invocation of our own `/echo` (gRPC
//!   `InvokeService` out, app channel back in)
//! - `/publish` — publish an order to the `orders` topic
//! - `/orders` — pub/sub delivery route (counts orders)
//! - `/count` — current processed-order count
//! - `/dapr/subscribe` — programmatic subscription declaration
//! - `/echo`, `/healthz`

use serde_json::json;
use wstd::http::body::Body;
use wstd::http::{Request, Response, StatusCode};

wit_bindgen::generate!({
    world: "imports",
    path: "../../wit",
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
    std::env::var("APP_ID").unwrap_or_else(|_| "spin-demo".to_string())
}

#[wstd::http_server]
async fn main(mut request: Request<Body>) -> Result<Response<Body>, wstd::http::Error> {
    let path = request.uri().path().to_string();
    let response = match path.as_str() {
        "/dapr/subscribe" => json_response(
            StatusCode::OK,
            json!([{ "pubsubname": PUBSUB, "topic": TOPIC, "route": "/orders" }]),
        ),
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
        "/smoke" => match smoke() {
            Ok(value) => json_response(StatusCode::OK, value),
            Err(error) => {
                json_response(StatusCode::INTERNAL_SERVER_ERROR, json!({ "error": error }))
            }
        },
        "/echo" => {
            let body = request.body_mut().contents().await?;
            let echoed = String::from_utf8_lossy(body).into_owned();
            json_response(StatusCode::OK, json!({ "echo": echoed }))
        }
        "/invoke-self" => match invoke_self() {
            Ok(value) => json_response(StatusCode::OK, value),
            Err(error) => {
                json_response(StatusCode::INTERNAL_SERVER_ERROR, json!({ "error": error }))
            }
        },
        "/publish" => match publish_order() {
            Ok(()) => json_response(StatusCode::OK, json!({ "published": true })),
            Err(error) => {
                json_response(StatusCode::INTERNAL_SERVER_ERROR, json!({ "error": error }))
            }
        },
        "/count" => match read_counter() {
            Ok(count) => json_response(StatusCode::OK, json!({ "processed": count })),
            Err(error) => {
                json_response(StatusCode::INTERNAL_SERVER_ERROR, json!({ "error": error }))
            }
        },
        "/" | "/healthz" => json_response(StatusCode::OK, json!({ "status": "ok" })),
        _ => json_response(StatusCode::NOT_FOUND, json!({ "error": "not found" })),
    };
    Ok(response)
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

/// State over gRPC, end to end: binary roundtrip, etag CAS, delete.
fn smoke() -> Result<serde_json::Value, String> {
    // Bytes that are deliberately not valid JSON nor UTF-8: the gRPC
    // provider must return them byte-exact (the HTTP provider cannot).
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

/// Service invocation of our own `/echo` route: out through gRPC
/// `InvokeService`, back in through the app channel.
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
    pubsub::publish(
        PUBSUB,
        TOPIC,
        json!({ "orderId": 1 }).to_string().as_bytes(),
        "application/json",
        &[],
    )
    .map_err(|e| format!("publish failed: {e:?}"))
}

fn process_order(cloud_event: &[u8]) -> Result<(), String> {
    let event: serde_json::Value =
        serde_json::from_slice(cloud_event).map_err(|e| format!("invalid CloudEvent: {e}"))?;
    event["data"]["orderId"]
        .as_u64()
        .ok_or_else(|| format!("event without orderId: {event}"))?;
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
