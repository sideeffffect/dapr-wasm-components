//! Real-Dapr end-to-end test for the **go** provider family: Dapr itself
//! embedded in the composed app — no daprd sidecar.
//!
//! Runs the shared [`common::run_mirrored_scenario`] — the *same* scenario the
//! wasi-http (`dapr.rs`) and wasi-grpc (`spin.rs`) suites run. Topology:
//!
//! - two instances of the `microservice` app, each composed as
//!   `go-outbound → microservice → go-inbound` and run by `wasmtime run`: a
//!   `publisher` and a `consumer`. Each instance *is* its own Dapr runtime,
//!   configured with daprd's flags on the `wasmtime run` command line;
//! - pub/sub: Redis (cross-runtime); state: in-memory (per runtime); name
//!   resolution: Consul (the embedded runtimes register themselves; the
//!   sqlite resolver the sidecar suites use does not build for wasm).
//!
//! There is no app port: the scenario drives each app through its embedded
//! runtime's own Dapr HTTP API (`/v1.0/invoke/<self>/method/...`), which
//! reaches the app with in-process Go calls and the go-inbound pump.
//!
//! Ignored by default — requires Docker and the `wasmtime` CLI:
//!
//! ```sh
//! components/go-outbound/scripts/build.sh
//! cargo build --release --target wasm32-wasip2 --manifest-path components/Cargo.toml
//! cargo build --release --target wasm32-wasip2 --manifest-path e2e/apps/Cargo.toml
//! cargo test --test go -- --ignored
//! ```
//!
//! Overrides: WASMTIME_BIN, REDIS_HOST (skips the Redis testcontainer).

mod common;

use std::process::Command;
use std::time::{Duration, Instant};

use common::{
    binary, get_json, run_mirrored_scenario, start_redis, wait_http_ok,
    write_cross_sidecar_components, Endpoints, Service, CONFIG_FILE, DAPRD_MOUNT,
};
use dapr_wasm_components_e2e::{compose, go_inbound_path, go_outbound_path, microservice_path};
use testcontainers_modules::testcontainers::runners::SyncRunner;
use testcontainers_modules::testcontainers::{Container, GenericImage, ImageExt};

// All ports below 32768 to avoid Docker's ephemeral range.
const PUB_DAPR_HTTP: u16 = 13595;
const PUB_DAPR_GRPC: u16 = 15595;
const PUB_DAPR_INTERNAL: u16 = 14595;
const CONS_DAPR_HTTP: u16 = 13596;
const CONS_DAPR_GRPC: u16 = 15596;
const CONS_DAPR_INTERNAL: u16 = 14596;
const CONSUL_HTTP: u16 = 18500;

/// A dev-mode Consul agent on the host network (HTTP API on `CONSUL_HTTP`),
/// the name resolver both embedded runtimes register with.
fn start_consul() -> Container<GenericImage> {
    GenericImage::new("hashicorp/consul", "1.20")
        .with_cmd([
            "agent",
            "-dev",
            "-client",
            "127.0.0.1",
            "-bind",
            "127.0.0.1",
            "-http-port",
            &CONSUL_HTTP.to_string(),
            // Non-default ports (and no DNS) so a local Consul agent on the
            // host network does not clash.
            "-dns-port=-1",
            "-server-port=18300",
            "-serf-lan-port=18301",
            "-serf-wan-port=18302",
        ])
        .with_network("host")
        .start()
        .expect("failed to start Consul testcontainer (is Docker running?)")
}

/// Poll Consul until `app_id` has an instance with a passing health check.
fn wait_consul_passing(app_id: &str, timeout: Duration) {
    let url = format!("http://127.0.0.1:{CONSUL_HTTP}/v1/health/service/{app_id}?passing=true");
    let start = Instant::now();
    loop {
        if get_json(&url).as_array().is_some_and(|a| !a.is_empty()) {
            return;
        }
        assert!(
            start.elapsed() < timeout,
            "{app_id} never became healthy in Consul"
        );
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// The `Configuration` both runtimes load: Consul name resolution, with each
/// runtime registering itself (its internal gRPC port, at DAPR_HOST_IP).
fn write_consul_config(dir: &std::path::Path) {
    std::fs::write(
        dir.join(CONFIG_FILE),
        format!(
            r#"apiVersion: dapr.io/v1alpha1
kind: Configuration
metadata:
  name: e2e-config
spec:
  nameResolution:
    component: "consul"
    version: "v1"
    configuration:
      client:
        address: "127.0.0.1:{CONSUL_HTTP}"
      selfRegister: true
      queryOptions:
        useCache: false
"#
        ),
    )
    .unwrap();
}

/// Run one `microservice` instance: the composed app under `wasmtime run`,
/// with the shared dir preopened at `DAPRD_MOUNT` (so the resource files
/// resolve at the same paths as in the sidecar suites) and daprd's flags as
/// the program arguments.
fn run(
    name: &'static str,
    wasmtime: &str,
    composed: &std::path::Path,
    shared_dir: &std::path::Path,
    app_id: &str,
    ports: (u16, u16, u16),
    env: &[(&str, &str)],
) -> Service {
    let (http, grpc, internal) = ports;
    let mut command = Command::new(wasmtime);
    command
        .arg("run")
        .args([
            "-S",
            "inherit-network",
            "-S",
            "tcp",
            "-S",
            "allow-ip-name-lookup",
        ])
        .arg("--dir")
        .arg(format!("{}::{DAPRD_MOUNT}", shared_dir.display()))
        // The embedded runtime cannot enumerate interfaces in wasm; it
        // registers itself with Consul at this address.
        .args(["--env", "DAPR_HOST_IP=127.0.0.1"]);
    for (key, value) in env {
        command.arg("--env").arg(format!("{key}={value}"));
    }
    command.arg(composed).args([
        "--app-id",
        app_id,
        "--resources-path",
        &format!("{DAPRD_MOUNT}/resources"),
        "--config",
        &format!("{DAPRD_MOUNT}/{CONFIG_FILE}"),
        "--dapr-http-port",
        &http.to_string(),
        "--dapr-grpc-port",
        &grpc.to_string(),
        "--dapr-internal-grpc-port",
        &internal.to_string(),
        "--log-level",
        "info",
    ]);
    Service::spawn(name, &mut command)
}

#[test]
#[ignore = "needs Docker + wasmtime CLI; runs in the go-e2e CI job (see module docs)"]
fn microservices_through_embedded_dapr() {
    let wasmtime = binary("WASMTIME_BIN", "wasmtime");
    let redis = start_redis();
    let _consul = start_consul();
    wait_http_ok(
        &format!("http://127.0.0.1:{CONSUL_HTTP}/v1/status/leader"),
        "consul",
        Duration::from_secs(60),
    );

    // One composed artifact (go-outbound → microservice → go-inbound); both
    // instances run it, differing only by arguments and env.
    let outbound = std::fs::read(go_outbound_path()).expect("go-outbound not built");
    let inbound = std::fs::read(go_inbound_path()).expect("go-inbound not built");
    let app = std::fs::read(microservice_path()).expect("microservice component not built");
    let dir = tempfile::tempdir().unwrap();
    let composed = dir.path().join("composed.wasm");
    std::fs::write(
        &composed,
        compose::plug_full(app, outbound, inbound).unwrap(),
    )
    .unwrap();
    write_cross_sidecar_components(dir.path(), &redis.host);
    write_consul_config(dir.path());

    let _publisher = run(
        "wasmtime run (publisher)",
        &wasmtime,
        &composed,
        dir.path(),
        "publisher",
        (PUB_DAPR_HTTP, PUB_DAPR_GRPC, PUB_DAPR_INTERNAL),
        &[("APP_ID", "publisher"), ("PEER_APP_ID", "consumer")],
    );
    let _consumer = run(
        "wasmtime run (consumer)",
        &wasmtime,
        &composed,
        dir.path(),
        "consumer",
        (CONS_DAPR_HTTP, CONS_DAPR_GRPC, CONS_DAPR_INTERNAL),
        &[
            ("APP_ID", "consumer"),
            ("PEER_APP_ID", "publisher"),
            ("SUBSCRIBE", "1"),
        ],
    );
    // Full healthz waits for the runtime *and* the app channel. The first
    // start compiles a large component, hence the generous timeout.
    for (port, what) in [
        (PUB_DAPR_HTTP, "publisher runtime"),
        (CONS_DAPR_HTTP, "consumer runtime"),
    ] {
        wait_http_ok(
            &format!("http://127.0.0.1:{port}/v1.0/healthz"),
            what,
            Duration::from_secs(300),
        );
    }
    // Dapr resolves every invocation target — its own app id included —
    // through the name resolver, and Consul only returns instances whose
    // health check passed (its first run comes up to one interval, 15s,
    // after registration).
    for app_id in ["publisher", "consumer"] {
        wait_consul_passing(app_id, Duration::from_secs(60));
    }

    run_mirrored_scenario(Endpoints {
        publisher_base: &format!("http://127.0.0.1:{PUB_DAPR_HTTP}/v1.0/invoke/publisher/method"),
        consumer_base: &format!("http://127.0.0.1:{CONS_DAPR_HTTP}/v1.0/invoke/consumer/method"),
        // In-process: values are raw bytes, no JSON envelope.
        binary_state_exact: true,
        self_invocation: false,
    });
}
