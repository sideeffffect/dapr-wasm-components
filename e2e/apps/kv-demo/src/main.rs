//! Example Dapr application component (a `wasi:cli` command).
//!
//! Compose it with dapr-wasm-components-wasi-http and run it on any WASI 0.2
//! runtime with wasi:http support, next to a Dapr sidecar:
//!
//! ```sh
//! wac plug kv_demo.wasm --plug dapr_wasm_components_wasi_http.wasm -o app.wasm
//! dapr run --app-id kv-demo -- wasmtime run -S http app.wasm
//! ```
//!
//! All the Dapr calls below are plain synchronous function calls; the
//! provider component turns them into wasi:http requests to the sidecar.

use dapr_app::dapr::{lock, pubsub, runtime, state};
use dapr_app::DaprApp;

/// kv-demo is an outbound-only client, so it overrides no callbacks — the
/// `DaprApp` defaults (no subscriptions, healthy, etc.) apply.
struct KvDemo;
impl DaprApp for KvDemo {}
dapr_app::export_app!(KvDemo);

const STATE_STORE: &str = "statestore";
const PUBSUB: &str = "pubsub";
const TOPIC: &str = "kv-demo";
const LOCK_STORE: &str = "lockstore";

fn run() -> Result<(), String> {
    if !runtime::outbound_healthz() {
        return Err("Dapr sidecar is not ready for outbound calls".to_string());
    }
    println!("sidecar is ready");

    let key = "kv-demo-key";
    let value = br#"{"message":"hello from a wasm component"}"#;

    state::save(
        STATE_STORE,
        &[state::StateItem {
            key: key.to_string(),
            value: value.to_vec(),
            etag: None,
            metadata: Vec::new(),
            options: None,
        }],
        &[],
    )
    .map_err(|e| format!("state save failed: {e:?}"))?;
    println!("saved {key}");

    let got = state::get(STATE_STORE, key, None, &[]).map_err(|e| match e {
        state::GetError::KeyNotFound => format!("state key {key} unexpectedly missing"),
        other => format!("state get failed: {other:?}"),
    })?;
    if got.data != value {
        return Err(format!(
            "state roundtrip mismatch: wrote {}, read {}",
            String::from_utf8_lossy(value),
            String::from_utf8_lossy(&got.data),
        ));
    }
    println!("read {key} back (etag: {:?})", got.etag);

    state::delete(STATE_STORE, key, None, None, &[])
        .map_err(|e| format!("state delete failed: {e:?}"))?;
    println!("deleted {key}");

    match lock::try_lock(LOCK_STORE, "kv-demo-resource", "kv-demo-owner", 60) {
        Ok(()) => {
            lock::unlock(LOCK_STORE, "kv-demo-resource", "kv-demo-owner")
                .map_err(|e| format!("unlock failed: {e:?}"))?;
            println!("lock roundtrip done");
        }
        Err(lock::TryLockError::NotAcquired) => {
            println!("lock not acquired (held by someone else?)");
        }
        Err(other) => return Err(format!("lock failed: {other:?}")),
    }

    pubsub::publish(
        PUBSUB,
        TOPIC,
        br#"{"message":"kv-demo roundtrip done"}"#,
        Some("application/json"),
        &[],
    )
    .map_err(|e| format!("publish failed: {e:?}"))?;
    println!("published to {TOPIC} on {PUBSUB}");

    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("kv-demo failed: {error}");
        std::process::exit(1);
    }
    println!("kv-demo finished successfully");
}
