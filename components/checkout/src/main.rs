//! Checkout microservice (a `wasi:cli` command component).
//!
//! Publishes orders to the `orders` topic via Dapr pub/sub, then verifies —
//! through Dapr service invocation of the order-processor — that all of
//! them were processed. Compose with the wasi-http provider and run with
//! `wasmtime run -S http`.

use std::time::Duration;

wit_bindgen::generate!({
    world: "imports",
    path: "../../wit",
});

use dapr_wasm_components::interfaces::invocation;
use dapr_wasm_components::interfaces::pubsub;
use dapr_wasm_components::interfaces::runtime;

const PUBSUB: &str = "pubsub";
const TOPIC: &str = "orders";
const ORDER_PROCESSOR: &str = "order-processor";
const ORDERS: u64 = 3;

fn run() -> Result<(), String> {
    wait_for(runtime::outbound_healthz, "sidecar outbound health")?;

    for order_id in 1..=ORDERS {
        let event = format!(r#"{{"orderId":{order_id}}}"#);
        pubsub::publish(PUBSUB, TOPIC, event.as_bytes(), "application/json", &[])
            .map_err(|e| format!("publishing order {order_id} failed: {e:?}"))?;
        println!("published order {order_id}");
    }

    // Deliveries are asynchronous — poll the order-processor's summary
    // (via Dapr service invocation) until everything arrived.
    wait_for(
        || matches!(processed_count(), Ok(count) if count >= ORDERS),
        "all orders processed",
    )?;

    let count = processed_count()?;
    println!("order-processor reports {count} processed orders");
    Ok(())
}

fn processed_count() -> Result<u64, String> {
    let response = invocation::invoke(
        ORDER_PROCESSOR,
        "summary",
        invocation::HttpVerb::Post,
        &[],
        None,
        &[],
    )
    .map_err(|e| format!("invoking summary failed: {e:?}"))?;
    if response.status / 100 != 2 {
        return Err(format!(
            "summary returned HTTP {}: {}",
            response.status,
            String::from_utf8_lossy(&response.body)
        ));
    }
    let summary: serde_json::Value = serde_json::from_slice(&response.body)
        .map_err(|e| format!("invalid summary response: {e}"))?;
    Ok(summary["processed"].as_u64().unwrap_or(0))
}

fn wait_for(mut condition: impl FnMut() -> bool, what: &str) -> Result<(), String> {
    for _ in 0..60 {
        if condition() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    Err(format!("timed out waiting for {what}"))
}

fn main() {
    if let Err(error) = run() {
        eprintln!("checkout failed: {error}");
        std::process::exit(1);
    }
    println!("checkout finished successfully");
}
