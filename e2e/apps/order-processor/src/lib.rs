//! Order-processor: a typed Dapr application (a reactor component).
//!
//! It subscribes to the `orders` topic, records each order and CAS-increments
//! a counter in the Dapr state store, and answers `summary` service
//! invocations. It never touches HTTP — it implements [`DaprApp`] callbacks
//! and is composed behind a provider, which serves the sidecar's app channel
//! and turns each delivery into the typed calls below.

use dapr_app::callback::invocation_callback::{HttpResponse, InvokeRequest};
use dapr_app::callback::pubsub_callback::{TopicEvent, TopicEventResponse, TopicSubscription};
use dapr_app::dapr::state;
use dapr_app::DaprApp;
use serde_json::json;

const STATE_STORE: &str = "statestore";
const PUBSUB: &str = "pubsub";
const TOPIC: &str = "orders";
const COUNTER_KEY: &str = "processed-count";

struct OrderProcessor;

impl DaprApp for OrderProcessor {
    fn list_topic_subscriptions() -> Vec<TopicSubscription> {
        vec![TopicSubscription {
            pubsub_name: PUBSUB.to_string(),
            topic: TOPIC.to_string(),
            metadata: Vec::new(),
            dead_letter_topic: None,
        }]
    }

    fn on_topic_event(event: TopicEvent) -> TopicEventResponse {
        // `event.data` is the domain payload — the provider already unwrapped
        // the CloudEvent envelope.
        match process_order(&event.data) {
            Ok(()) => TopicEventResponse::Success,
            Err(error) => {
                eprintln!("order processing failed: {error}");
                TopicEventResponse::Retry
            }
        }
    }

    fn on_invoke(request: InvokeRequest) -> HttpResponse {
        match request.method.as_str() {
            // Report how many orders were processed.
            "summary" => match read_counter() {
                Ok(count) => json_response(200, &json!({ "processed": count })),
                Err(error) => json_response(500, &json!({ "error": error })),
            },
            other => json_response(404, &json!({ "error": format!("unknown method: {other}") })),
        }
    }
}

dapr_app::export_app!(OrderProcessor);

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

/// Increment the counter with optimistic concurrency: read the current value +
/// etag, write back with first-write concurrency, retry on conflict (deliveries
/// may be concurrent).
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

fn json_response(status: u16, value: &serde_json::Value) -> HttpResponse {
    HttpResponse {
        status,
        headers: vec![("content-type".to_string(), "application/json".to_string())],
        body: value.to_string().into_bytes(),
    }
}
