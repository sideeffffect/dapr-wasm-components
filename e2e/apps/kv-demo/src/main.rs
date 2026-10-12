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

use std::io::{Read, Write};

use dapr_app::crypto_io::{DecryptWriter, EncryptWriter};
use dapr_app::dapr::{crypto, lock, pubsub, runtime, state, state_read};
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
/// Env var naming the crypto component for the optional streaming-crypto
/// step (skipped when unset).
const CRYPTO_COMPONENT_ENV: &str = "KV_DEMO_CRYPTO_COMPONENT";
/// Payload size of the streaming-crypto step.
const CRYPTO_PAYLOAD: usize = 3 * 1024 * 1024;
/// Write size of the streaming-crypto step.
const CRYPTO_CHUNK: usize = 64 * 1024;

/// Encrypt and decrypt a few MiB through the streaming crypto resources (via
/// the SDK's `std::io` adapters), writing and reading in chunks, and check
/// the roundtrip.
fn crypto_roundtrip(component: &str) -> Result<(), String> {
    let plaintext: Vec<u8> = (0..CRYPTO_PAYLOAD).map(|i| (i * 31 % 251) as u8).collect();
    let options = crypto::EncryptOptions {
        key_name: "kv-demo-key".to_string(),
        key_wrap_algorithm: "A256KW".to_string(),
        data_encryption_cipher: None,
        omit_decryption_key_name: None,
        decryption_key_name: None,
    };

    let mut encryptor = EncryptWriter::new(component, &options);
    for chunk in plaintext.chunks(CRYPTO_CHUNK) {
        encryptor
            .write_all(chunk)
            .map_err(|e| format!("encrypt write failed: {e}"))?;
    }
    let mut ciphertext = Vec::new();
    encryptor
        .finish()
        .map_err(|e| format!("encrypt failed: {e:?}"))?
        .read_to_end(&mut ciphertext)
        .map_err(|e| format!("reading ciphertext failed: {e}"))?;

    let mut decryptor = DecryptWriter::new(component, None);
    for chunk in ciphertext.chunks(CRYPTO_CHUNK) {
        decryptor
            .write_all(chunk)
            .map_err(|e| format!("decrypt write failed: {e}"))?;
    }
    let mut decrypted = Vec::new();
    decryptor
        .finish()
        .map_err(|e| format!("decrypt failed: {e:?}"))?
        .read_to_end(&mut decrypted)
        .map_err(|e| format!("reading plaintext failed: {e}"))?;

    if decrypted != plaintext {
        return Err("streaming crypto roundtrip mismatch".to_string());
    }
    println!(
        "streaming crypto roundtrip of {} bytes done ({} bytes of ciphertext)",
        plaintext.len(),
        ciphertext.len()
    );
    Ok(())
}

fn run() -> Result<(), String> {
    if !runtime::outbound_healthz() {
        return Err("Dapr sidecar is not ready for outbound calls".to_string());
    }
    println!("sidecar is ready");

    let key = "kv-demo-key";
    let value = br#"{"message":"hello from a wasm component"}"#;

    state::save(
        STATE_STORE,
        &[state_read::StateItem {
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

    let got = state_read::get(STATE_STORE, key, None, &[]).map_err(|e| match e {
        state_read::GetError::KeyNotFound => format!("state key {key} unexpectedly missing"),
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

    if let Ok(component) = std::env::var(CRYPTO_COMPONENT_ENV) {
        crypto_roundtrip(&component)?;
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
