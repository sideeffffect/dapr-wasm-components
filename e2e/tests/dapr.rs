//! Real-Dapr end-to-end test for the **wasi-http** provider.
//!
//! Runs the shared [`common::run_mirrored_scenario`] — the *same* scenario the
//! wasi-grpc suite (`spin.rs`) runs — so the two differ only in provider and
//! runtime. Topology:
//!
//! - two instances of the `microservice` app (composed with the wasi-http
//!   provider, served by `wasmtime serve`): a `publisher` and a `consumer`,
//!   each behind its own `daprd` sidecar (host networking);
//! - pub/sub: Redis (cross-sidecar); state: in-memory (per sidecar); name
//!   resolution: sqlite (shared db, bind-mounted into both sidecars).
//!
//! The scenario then drives state + CAS + delete, service invocation (self and
//! cross-app), and a cross-sidecar publish→deliver→count loop, all from the
//! app's HTTP surface. Mechanical scaffolding is shared in [`common`].
//!
//! Ignored by default — requires Docker and the `wasmtime` CLI:
//!
//! ```sh
//! cargo build --release --target wasm32-wasip2 --manifest-path components/Cargo.toml
//! cargo build --release --target wasm32-wasip2 --manifest-path e2e/apps/Cargo.toml
//! cargo test --test dapr -- --ignored
//! ```
//!
//! Overrides: WASMTIME_BIN, DAPRD_IMAGE_TAG (default 1.18.0),
//! REDIS_HOST (skips the Redis testcontainer).

mod common;

use std::process::Command;
use std::time::Duration;

use common::{
    binary, run_mirrored_scenario, start_redis, wait_http_ok, write_cross_sidecar_resources, Daprd,
    DaprdConfig, DaprdPorts, Endpoints, Service, CONFIG_FILE,
};
use dapr_wasm_components_e2e::{compose, microservice_path, provider_path};

// All ports below 32768 to avoid Docker's ephemeral range (where stray
// docker-proxy mappings can make daprd fail to bind).
const PUB_APP: u16 = 18091;
const PUB_DAPR_HTTP: u16 = 13591;
const PUB_DAPR_GRPC: u16 = 15591;
const PUB_DAPR_INTERNAL: u16 = 14591;
const CONS_APP: u16 = 18092;
const CONS_DAPR_HTTP: u16 = 13592;
const CONS_DAPR_GRPC: u16 = 15592;
const CONS_DAPR_INTERNAL: u16 = 14592;

/// Serve one `microservice` instance on `addr_port` with `wasmtime serve`,
/// composed with the wasi-http provider. `env` carries APP_ID / PEER_APP_ID /
/// SUBSCRIBE and the sidecar's DAPR_HTTP_PORT.
fn serve(
    name: &'static str,
    wasmtime: &str,
    composed: &std::path::Path,
    addr_port: u16,
    env: &[(&str, String)],
) -> Service {
    let mut command = Command::new(wasmtime);
    command
        .arg("serve")
        .arg("-S")
        .arg("cli")
        .arg("--addr")
        .arg(format!("127.0.0.1:{addr_port}"));
    for (key, value) in env {
        command.arg("--env").arg(format!("{key}={value}"));
    }
    command.arg(composed);
    Service::spawn(name, &mut command)
}

#[test]
#[ignore = "requires Docker and the wasmtime CLI (see module docs)"]
fn microservices_through_real_dapr() {
    let wasmtime = binary("WASMTIME_BIN", "wasmtime");
    let redis = start_redis();

    // One composed artifact (microservice + wasi-http provider); both
    // instances run it, differing only by env.
    let provider = std::fs::read(provider_path()).expect("provider component not built");
    let app = std::fs::read(microservice_path()).expect("microservice component not built");
    let dir = tempfile::tempdir().unwrap();
    let composed = dir.path().join("composed.wasm");
    std::fs::write(&composed, compose::plug(app, provider).unwrap()).unwrap();
    write_cross_sidecar_resources(dir.path(), &redis.host);

    // 1. The two app instances (their own app channels).
    let _publisher = serve(
        "wasmtime serve (publisher)",
        &wasmtime,
        &composed,
        PUB_APP,
        &[
            ("DAPR_HTTP_PORT", PUB_DAPR_HTTP.to_string()),
            ("APP_ID", "publisher".to_string()),
            ("PEER_APP_ID", "consumer".to_string()),
        ],
    );
    let _consumer = serve(
        "wasmtime serve (consumer)",
        &wasmtime,
        &composed,
        CONS_APP,
        &[
            ("DAPR_HTTP_PORT", CONS_DAPR_HTTP.to_string()),
            ("APP_ID", "consumer".to_string()),
            ("PEER_APP_ID", "publisher".to_string()),
            ("SUBSCRIBE", "1".to_string()),
        ],
    );
    wait_http_ok(
        &format!("http://127.0.0.1:{PUB_APP}/healthz"),
        "publisher app",
        Duration::from_secs(30),
    );
    wait_http_ok(
        &format!("http://127.0.0.1:{CONS_APP}/healthz"),
        "consumer app",
        Duration::from_secs(30),
    );

    // 2. Each instance's daprd sidecar (registers subscriptions, full healthz
    //    waits for the app channel too).
    let _pub_daprd = Daprd::start(DaprdConfig {
        name: "daprd (publisher)",
        app_id: "publisher",
        ports: DaprdPorts {
            http: PUB_DAPR_HTTP,
            grpc: PUB_DAPR_GRPC,
            internal_grpc: PUB_DAPR_INTERNAL,
        },
        app_port: Some(PUB_APP),
        config_file: Some(CONFIG_FILE),
        shared_dir: dir.path(),
    });
    let _cons_daprd = Daprd::start(DaprdConfig {
        name: "daprd (consumer)",
        app_id: "consumer",
        ports: DaprdPorts {
            http: CONS_DAPR_HTTP,
            grpc: CONS_DAPR_GRPC,
            internal_grpc: CONS_DAPR_INTERNAL,
        },
        app_port: Some(CONS_APP),
        config_file: Some(CONFIG_FILE),
        shared_dir: dir.path(),
    });
    for (port, what) in [
        (PUB_DAPR_HTTP, "publisher daprd"),
        (CONS_DAPR_HTTP, "consumer daprd"),
    ] {
        wait_http_ok(
            &format!("http://127.0.0.1:{port}/v1.0/healthz"),
            what,
            Duration::from_secs(60),
        );
    }

    // 3. The shared scenario (HTTP inbound to the apps; outbound over HTTP).
    run_mirrored_scenario(Endpoints {
        publisher_base: &format!("http://127.0.0.1:{PUB_APP}"),
        consumer_base: &format!("http://127.0.0.1:{CONS_APP}"),
        binary_state_exact: false,
    });
}
