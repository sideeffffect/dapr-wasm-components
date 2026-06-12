//! Real-Dapr end-to-end test for the **wasi-http** provider: two wasm
//! microservices orchestrated through two actual `daprd` sidecars.
//!
//! Topology:
//!
//! - **order-processor** (wasm, `wasi:http` server, composed with the
//!   wasi-http provider) served by `wasmtime serve`; its daprd subscribes it
//!   to the `orders` topic and delivers events to its app channel.
//! - **checkout** (wasm, `wasi:cli` command, composed with the provider)
//!   run by `wasmtime run`; publishes orders via its own daprd and verifies
//!   processing through Dapr service invocation of order-processor.
//! - Pub/sub: Redis (cross-sidecar); state: in-memory; name resolution:
//!   sqlite (shared db file, bind-mounted into both sidecar containers).
//!
//! The mechanical scaffolding (managed processes, daprd testcontainers,
//! resource files, readiness polling) is shared with the wasi-grpc test in
//! [`common`]; this file holds only what is specific to the wasi-http flow.
//!
//! Ignored by default — requires Docker and the `wasmtime` CLI:
//!
//! ```sh
//! cargo build --release --target wasm32-wasip2 --manifest-path components/Cargo.toml
//! cargo test --test dapr -- --ignored
//! ```
//!
//! Overrides: WASMTIME_BIN, DAPRD_IMAGE_TAG (default 1.18.0),
//! REDIS_HOST (skips the Redis testcontainer).

mod common;

use std::process::Command;
use std::time::Duration;

use testcontainers_modules::testcontainers::runners::SyncRunner;

use common::{
    binary, relax_permissions, wait_http_ok, write_resource, Daprd, DaprdConfig, DaprdPorts,
    Service, DAPRD_MOUNT, STATESTORE_IN_MEMORY,
};
use dapr_wasm_components_e2e::{app_path, compose};

const ORDER_PROCESSOR_APP_PORT: u16 = 8091;
const OP_DAPR_HTTP: u16 = 3551;
const CHECKOUT_DAPR_HTTP: u16 = 3552;

/// Write the resource definitions and config both sidecars share: an
/// in-memory statestore, a Redis pub/sub broker (cross-sidecar), and a
/// sqlite name-resolution config (deterministic in CI, unlike mDNS).
fn write_dapr_resources(dir: &std::path::Path, redis_host: &str) {
    write_resource(dir, "statestore.yaml", STATESTORE_IN_MEMORY);
    write_resource(
        dir,
        "pubsub.yaml",
        &format!(
            r#"apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: pubsub
spec:
  type: pubsub.redis
  version: v1
  metadata:
  - name: redisHost
    value: "{redis_host}"
  - name: redisPassword
    value: ""
"#
        ),
    );
    std::fs::write(
        dir.join("config.yaml"),
        format!(
            r#"apiVersion: dapr.io/v1alpha1
kind: Configuration
metadata:
  name: e2e-config
spec:
  nameResolution:
    component: "sqlite"
    version: "v1"
    configuration:
      connectionString: "{DAPRD_MOUNT}/nameresolution.db"
"#
        ),
    )
    .unwrap();
    relax_permissions(dir);
}

#[test]
#[ignore = "requires Docker and the wasmtime CLI (see module docs)"]
fn microservices_through_real_dapr() {
    let wasmtime = binary("WASMTIME_BIN", "wasmtime");

    // Redis backs cross-sidecar pub/sub. Started via testcontainers unless
    // REDIS_HOST points at an existing instance. The container handle must
    // stay alive for the duration of the test.
    let mut _redis_container = None;
    let redis_host = match std::env::var("REDIS_HOST") {
        Ok(host) => host,
        Err(_) => {
            let container = testcontainers_modules::redis::Redis::default()
                .start()
                .expect("failed to start Redis testcontainer (is Docker running?)");
            let port = container
                .get_host_port_ipv4(6379)
                .expect("no mapped Redis port");
            _redis_container = Some(container);
            format!("127.0.0.1:{port}")
        }
    };

    // Compose both microservices with the wasi-http provider.
    let provider = std::fs::read(dapr_wasm_components_e2e::provider_path())
        .expect("provider component not built");
    let order_processor = std::fs::read(app_path(
        "ORDER_PROCESSOR_COMPONENT",
        "order-processor.wasm",
    ))
    .expect("order-processor component not built");
    let checkout = std::fs::read(app_path("CHECKOUT_COMPONENT", "checkout.wasm"))
        .expect("checkout component not built");

    let dir = tempfile::tempdir().unwrap();
    let op_composed = dir.path().join("order-processor-composed.wasm");
    std::fs::write(
        &op_composed,
        compose::plug(order_processor, provider.clone()).unwrap(),
    )
    .unwrap();
    let checkout_composed = dir.path().join("checkout-composed.wasm");
    std::fs::write(
        &checkout_composed,
        compose::plug(checkout, provider).unwrap(),
    )
    .unwrap();

    write_dapr_resources(dir.path(), &redis_host);

    // 1. The order-processor wasm service, served over its app channel.
    let _op_server = Service::spawn(
        "wasmtime serve (order-processor)",
        Command::new(&wasmtime)
            .arg("serve")
            .arg("-S")
            .arg("cli")
            .arg("--addr")
            .arg(format!("127.0.0.1:{ORDER_PROCESSOR_APP_PORT}"))
            .arg("--env")
            .arg(format!("DAPR_HTTP_PORT={OP_DAPR_HTTP}"))
            .arg(&op_composed),
    );
    wait_http_ok(
        &format!("http://127.0.0.1:{ORDER_PROCESSOR_APP_PORT}/healthz"),
        "order-processor app",
        Duration::from_secs(30),
    );

    // 2. Its daprd sidecar (registers the topic subscription).
    let _op_daprd = Daprd::start(DaprdConfig {
        name: "daprd (order-processor)",
        app_id: "order-processor",
        ports: DaprdPorts {
            http: OP_DAPR_HTTP,
            grpc: 50051,
            internal_grpc: 48051,
        },
        app_port: Some(ORDER_PROCESSOR_APP_PORT),
        config_file: Some("config.yaml"),
        shared_dir: dir.path(),
    });
    wait_http_ok(
        &format!("http://127.0.0.1:{OP_DAPR_HTTP}/v1.0/metadata"),
        "order-processor daprd",
        Duration::from_secs(60),
    );

    // 3. The checkout sidecar (no app channel needed).
    let _checkout_daprd = Daprd::start(DaprdConfig {
        name: "daprd (checkout)",
        app_id: "checkout",
        ports: DaprdPorts {
            http: CHECKOUT_DAPR_HTTP,
            grpc: 50052,
            internal_grpc: 48052,
        },
        app_port: None,
        config_file: Some("config.yaml"),
        shared_dir: dir.path(),
    });
    wait_http_ok(
        &format!("http://127.0.0.1:{CHECKOUT_DAPR_HTTP}/v1.0/healthz/outbound"),
        "checkout daprd",
        Duration::from_secs(60),
    );

    // 4. Run the checkout microservice to completion: it publishes orders
    //    and polls the order-processor's summary via service invocation.
    let output = Command::new(&wasmtime)
        .arg("run")
        .arg("-S")
        .arg("http")
        .arg("--env")
        .arg(format!("DAPR_HTTP_PORT={CHECKOUT_DAPR_HTTP}"))
        .arg(&checkout_composed)
        .output()
        .expect("failed to run checkout");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    println!("checkout stdout:\n{stdout}");
    assert!(
        output.status.success(),
        "checkout failed (status {:?})\nstderr:\n{stderr}",
        output.status
    );
    assert!(stdout.contains("checkout finished successfully"));

    // 5. Independent verification: invoke the order-processor through the
    //    checkout sidecar from the outside.
    let summary: serde_json::Value = ureq::post(&format!(
        "http://127.0.0.1:{CHECKOUT_DAPR_HTTP}/v1.0/invoke/order-processor/method/summary"
    ))
    .call()
    .expect("summary invocation failed")
    .into_json()
    .expect("summary is not JSON");
    assert_eq!(summary["processed"], 3, "unexpected summary: {summary}");
}
