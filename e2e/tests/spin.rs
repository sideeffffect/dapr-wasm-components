//! Real-Dapr end-to-end test for the **wasi-grpc** provider: one wasm
//! microservice served by **Spin**, talking to an actual `daprd` sidecar
//! over its **gRPC** API.
//!
//! Topology:
//!
//! - **spin-demo** (wasm, `wasi:http` server, composed with the wasi-grpc
//!   provider) served by `spin up` — the only runtime with outbound
//!   cleartext HTTP/2 today (`SPIN_OUTBOUND_H2C_PRIOR_KNOWLEDGE`, set on
//!   the *Spin host process*, must equal the `DAPR_GRPC_ENDPOINT`
//!   authority byte-for-byte).
//! - One daprd sidecar (testcontainer, host networking) whose app channel
//!   points back at Spin's listener; state and pub/sub are in-memory.
//!
//! Asserted, all through gRPC: binary byte-exact state roundtrip + etag
//! CAS + delete (`/smoke`), service invocation out-and-back-in
//! (`/invoke-self`), and a pub/sub publish→deliver→count loop
//! (`/publish` → `/count`).
//!
//! The mechanical scaffolding (managed processes, the daprd testcontainer,
//! resource files, readiness polling) is shared with the wasi-http test in
//! [`common`]; this file holds only what is specific to the Spin/gRPC flow.
//!
//! Ignored by default — requires Docker and the `spin` CLI (>= 3.4):
//!
//! ```sh
//! cargo build --release --target wasm32-wasip2 --manifest-path components/Cargo.toml
//! cargo test --test spin -- --ignored
//! ```
//!
//! Overrides: SPIN_BIN, DAPRD_IMAGE_TAG (default 1.18.0).

mod common;

use std::process::Command;
use std::time::{Duration, Instant};

use common::{
    binary, get_json, relax_permissions, wait_http_ok, write_resource, Daprd, DaprdConfig,
    DaprdPorts, Service, PUBSUB_IN_MEMORY, STATESTORE_IN_MEMORY,
};
use dapr_wasm_components_e2e::{app_path, compose, grpc_provider_path};

// Below 32768: Docker's ephemeral port range (32768+) is where stray
// docker-proxy mappings (e.g. testcontainers ryuk) land — daprd would fail
// to bind a clashing port.
const SPIN_LISTEN_PORT: u16 = 18093;
const DAPR_HTTP_PORT: u16 = 13553;
const DAPR_GRPC_PORT: u16 = 15553;
const DAPR_INTERNAL_GRPC_PORT: u16 = 14553;

/// In-memory statestore and pub/sub — enough for a single sidecar.
fn write_dapr_resources(dir: &std::path::Path) {
    write_resource(dir, "statestore.yaml", STATESTORE_IN_MEMORY);
    write_resource(dir, "pubsub.yaml", PUBSUB_IN_MEMORY);
    relax_permissions(dir);
}

fn write_spin_manifest(dir: &std::path::Path) -> std::path::PathBuf {
    let manifest = dir.join("spin.toml");
    std::fs::write(
        &manifest,
        format!(
            r#"spin_manifest_version = 2

[application]
name = "spin-demo"
version = "0.1.0"

[[trigger.http]]
route = "/..."
component = "spin-demo"

[component.spin-demo]
source = "composed.wasm"
allowed_outbound_hosts = ["http://127.0.0.1:{DAPR_GRPC_PORT}"]
environment = {{ DAPR_GRPC_ENDPOINT = "http://127.0.0.1:{DAPR_GRPC_PORT}", APP_ID = "spin-demo" }}
"#
        ),
    )
    .unwrap();
    manifest
}

/// GET a path on the Spin-served app and parse the JSON body.
fn app_json(path: &str) -> serde_json::Value {
    get_json(&format!("http://127.0.0.1:{SPIN_LISTEN_PORT}{path}"))
}

#[test]
#[ignore = "requires Docker and the spin CLI >= 3.4 (see module docs)"]
fn microservice_through_real_dapr_grpc_on_spin() {
    let spin = binary("SPIN_BIN", "spin");

    // Compose the demo app with the wasi-grpc provider.
    let provider = std::fs::read(grpc_provider_path()).expect("wasi-grpc provider not built");
    let app = std::fs::read(app_path("SPIN_DEMO_COMPONENT", "spin-demo.wasm"))
        .expect("spin-demo component not built");

    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("composed.wasm"),
        compose::plug(app, provider).unwrap(),
    )
    .unwrap();
    let manifest = write_spin_manifest(dir.path());
    write_dapr_resources(dir.path());

    // 1. Spin serves the composed component (it is also the Dapr app
    //    channel). H2c prior knowledge is a host-process env var and must
    //    textually match the DAPR_GRPC_ENDPOINT authority.
    let _spin = Service::spawn(
        "spin up",
        Command::new(&spin)
            .arg("up")
            .arg("-f")
            .arg(&manifest)
            .arg("--listen")
            .arg(format!("127.0.0.1:{SPIN_LISTEN_PORT}"))
            .env(
                "SPIN_OUTBOUND_H2C_PRIOR_KNOWLEDGE",
                format!("127.0.0.1:{DAPR_GRPC_PORT}"),
            ),
    );
    wait_http_ok(
        &format!("http://127.0.0.1:{SPIN_LISTEN_PORT}/.well-known/spin/health"),
        "spin",
        Duration::from_secs(60),
    );

    // 2. The daprd sidecar; full healthz waits for the app channel too, so
    //    the topic subscription is registered before we publish.
    let _daprd = Daprd::start(DaprdConfig {
        name: "daprd",
        app_id: "spin-demo",
        ports: DaprdPorts {
            http: DAPR_HTTP_PORT,
            grpc: DAPR_GRPC_PORT,
            internal_grpc: DAPR_INTERNAL_GRPC_PORT,
        },
        app_port: Some(SPIN_LISTEN_PORT),
        config_file: None,
        shared_dir: dir.path(),
    });
    wait_http_ok(
        &format!("http://127.0.0.1:{DAPR_HTTP_PORT}/v1.0/healthz"),
        "daprd",
        Duration::from_secs(60),
    );

    // 3. State over gRPC: binary byte-exact roundtrip, etag CAS, delete,
    //    metadata.
    let smoke = app_json("/smoke");
    assert_eq!(smoke["ok"], true, "smoke failed: {smoke}");
    assert_eq!(smoke["appId"], "spin-demo", "unexpected smoke: {smoke}");

    // 4. Service invocation over gRPC, back in through the app channel.
    let invoked = app_json("/invoke-self");
    assert_eq!(invoked["ok"], true, "invoke-self failed: {invoked}");

    // 5. Pub/sub over gRPC: publish three orders, the in-memory pubsub
    //    delivers them to /orders via the app channel, the counter grows.
    for _ in 0..3 {
        let published = app_json("/publish");
        assert_eq!(published["published"], true, "publish failed: {published}");
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let count = app_json("/count");
        if count["processed"] == 3 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "orders were not all processed in time: {count}"
        );
        std::thread::sleep(Duration::from_millis(500));
    }
}
