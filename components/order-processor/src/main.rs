//! Order-processor microservice (a `wasi:http` server component).
//!
//! Serves its Dapr app channel: declares a programmatic subscription to the
//! `orders` topic, processes delivered events by recording each order and
//! CAS-incrementing a counter in the Dapr state store, and answers
//! `summary` service invocations. Compose with the wasi-http provider and
//! serve with `wasmtime serve -S cli`.

use serde_json::json;
use wstd::http::body::Body;
use wstd::http::{Request, Response, StatusCode};

wit_bindgen::generate!({
    world: "imports",
    path: "../../wit",
});

use dapr_wasm_components::interfaces::state;

const STATE_STORE: &str = "statestore";
const PUBSUB: &str = "pubsub";
const TOPIC: &str = "orders";
const COUNTER_KEY: &str = "processed-count";

#[wstd::http_server]
async fn main(mut request: Request<Body>) -> Result<Response<Body>, wstd::http::Error> {
    let path = request.uri().path().to_string();
    let response = match path.as_str() {
        // Dapr fetches this at startup to learn our subscriptions.
        "/dapr/subscribe" => json_response(
            StatusCode::OK,
            json!([{ "pubsubname": PUBSUB, "topic": TOPIC, "route": "/orders" }]),
        ),
        // Pub/sub delivery route (body is a CloudEvent).
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
        // Service-invocation target: report how many orders were processed.
        "/summary" => match read_counter() {
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

/// Increment the counter with optimistic concurrency: read the current
/// value + etag, write back with first-write concurrency, retry on conflict
/// (deliveries may be concurrent).
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
            // Etag conflict: somebody else incremented first — retry.
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
