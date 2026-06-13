//! Real-Dapr end-to-end test for the **wasi-grpc** provider.
//!
//! Runs the *same* [`common::run_mirrored_scenario`] as the wasi-http suite
//! (`dapr.rs`) — the two differ only in provider and runtime. Topology, the
//! gRPC mirror of `dapr.rs`:
//!
//! - two instances of the `microservice` app (composed with the wasi-grpc
//!   provider, served by `spin up` — the only runtime with outbound cleartext
//!   HTTP/2, which gRPC needs): a `publisher` and a `consumer`, each behind its
//!   own `daprd` sidecar (host networking, app channel back to Spin over HTTP);
//! - pub/sub: Redis (cross-sidecar); state: in-memory; name resolution: sqlite.
//!
//! Each Spin process carries `SPIN_OUTBOUND_H2C_PRIOR_KNOWLEDGE` = its own
//! sidecar's gRPC authority (must match `DAPR_GRPC_ENDPOINT` byte-for-byte).
//! The scenario additionally asserts a binary byte-exact state roundtrip —
//! something only the gRPC provider can do.
//!
//! Ignored by default — requires Docker and the `spin` CLI (>= 3.4):
//!
//! ```sh
//! cargo build --release --target wasm32-wasip2 --manifest-path components/Cargo.toml
//! cargo build --release --target wasm32-wasip2 --manifest-path e2e/apps/Cargo.toml
//! cargo test --test spin -- --ignored
//! ```
//!
//! Overrides: SPIN_BIN, DAPRD_IMAGE_TAG (default 1.18.0),
//! REDIS_HOST (skips the Redis testcontainer).

mod common;

use std::path::Path;
use std::process::Command;
use std::time::Duration;

use common::{
    binary, run_mirrored_scenario, start_redis, wait_http_ok, write_cross_sidecar_resources, Daprd,
    DaprdConfig, DaprdPorts, Endpoints, Service, CONFIG_FILE,
};
use dapr_wasm_components_e2e::{compose, grpc_outbound_path, http_inbound_path, microservice_path};

// All ports below 32768 (Docker's ephemeral range, where stray docker-proxy
// mappings can make daprd fail to bind), and distinct from the dapr.rs block.
const PUB_LISTEN: u16 = 18093;
const PUB_DAPR_HTTP: u16 = 13593;
const PUB_DAPR_GRPC: u16 = 15593;
const PUB_DAPR_INTERNAL: u16 = 14593;
const CONS_LISTEN: u16 = 18094;
const CONS_DAPR_HTTP: u16 = 13594;
const CONS_DAPR_GRPC: u16 = 15594;
const CONS_DAPR_INTERNAL: u16 = 14594;

/// Write a per-instance Spin manifest. `subscribe` adds `SUBSCRIBE` for the
/// consumer role. `composed` is referenced by absolute path so every instance
/// shares the one composed artifact.
fn write_manifest(
    path: &Path,
    composed: &Path,
    grpc_port: u16,
    app_id: &str,
    peer_app_id: &str,
    subscribe: bool,
) {
    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let subscribe_env = if subscribe {
        r#", SUBSCRIBE = "1""#
    } else {
        ""
    };
    std::fs::write(
        path,
        format!(
            r#"spin_manifest_version = 2

[application]
name = "microservice"
version = "0.1.0"

[[trigger.http]]
route = "/..."
component = "microservice"

[component.microservice]
source = "{source}"
allowed_outbound_hosts = ["{endpoint}"]
environment = {{ DAPR_GRPC_ENDPOINT = "{endpoint}", APP_ID = "{app_id}", PEER_APP_ID = "{peer_app_id}"{subscribe_env} }}
"#,
            source = composed.to_str().unwrap(),
        ),
    )
    .unwrap();
}

/// Serve one `microservice` instance with `spin up`. h2c prior knowledge is a
/// host-process env var that must textually match `DAPR_GRPC_ENDPOINT`'s
/// authority; each instance runs from its own working dir so two Spin
/// processes don't share runtime state.
fn serve(
    name: &'static str,
    spin: &str,
    manifest: &Path,
    work_dir: &Path,
    listen_port: u16,
    grpc_port: u16,
) -> Service {
    let mut command = Command::new(spin);
    command
        .current_dir(work_dir)
        .arg("up")
        .arg("-f")
        .arg(manifest)
        .arg("--listen")
        .arg(format!("127.0.0.1:{listen_port}"))
        .env(
            "SPIN_OUTBOUND_H2C_PRIOR_KNOWLEDGE",
            format!("127.0.0.1:{grpc_port}"),
        );
    Service::spawn(name, &mut command)
}

#[test]
#[ignore = "requires Docker and the spin CLI >= 3.4 (see module docs)"]
fn microservices_through_real_dapr_grpc_on_spin() {
    let spin = binary("SPIN_BIN", "spin");
    let redis = start_redis();

    // One composed artifact, used by both instances: the microservice with the
    // wasi-grpc **outbound** provider (gRPC to the sidecar) and the wasi-http
    // **inbound** provider (the app channel is HTTP — Spin serves it, daprd is
    // `--app-protocol http`). The two directions are independent: gRPC out,
    // HTTP in.
    let outbound = std::fs::read(grpc_outbound_path()).expect("wasi-grpc outbound not built");
    let inbound = std::fs::read(http_inbound_path()).expect("wasi-http inbound not built");
    let app = std::fs::read(microservice_path()).expect("microservice component not built");
    let dir = tempfile::tempdir().unwrap();
    let composed = dir.path().join("composed.wasm");
    std::fs::write(
        &composed,
        compose::plug_full(app, outbound, inbound).unwrap(),
    )
    .unwrap();
    write_cross_sidecar_resources(dir.path(), &redis.host);

    let pub_dir = dir.path().join("publisher");
    let cons_dir = dir.path().join("consumer");
    std::fs::create_dir_all(&pub_dir).unwrap();
    std::fs::create_dir_all(&cons_dir).unwrap();
    let pub_manifest = pub_dir.join("spin.toml");
    let cons_manifest = cons_dir.join("spin.toml");
    write_manifest(
        &pub_manifest,
        &composed,
        PUB_DAPR_GRPC,
        "publisher",
        "consumer",
        false,
    );
    write_manifest(
        &cons_manifest,
        &composed,
        CONS_DAPR_GRPC,
        "consumer",
        "publisher",
        true,
    );

    // 1. The two app instances, each served by its own Spin process.
    let _publisher = serve(
        "spin up (publisher)",
        &spin,
        &pub_manifest,
        &pub_dir,
        PUB_LISTEN,
        PUB_DAPR_GRPC,
    );
    let _consumer = serve(
        "spin up (consumer)",
        &spin,
        &cons_manifest,
        &cons_dir,
        CONS_LISTEN,
        CONS_DAPR_GRPC,
    );
    for (port, what) in [
        (PUB_LISTEN, "publisher spin"),
        (CONS_LISTEN, "consumer spin"),
    ] {
        wait_http_ok(
            &format!("http://127.0.0.1:{port}/.well-known/spin/health"),
            what,
            Duration::from_secs(60),
        );
    }

    // 2. Each instance's daprd sidecar (app channel over HTTP back to Spin;
    //    full healthz waits for it, so subscriptions are registered).
    let _pub_daprd = Daprd::start(DaprdConfig {
        name: "daprd (publisher)",
        app_id: "publisher",
        ports: DaprdPorts {
            http: PUB_DAPR_HTTP,
            grpc: PUB_DAPR_GRPC,
            internal_grpc: PUB_DAPR_INTERNAL,
        },
        app_port: Some(PUB_LISTEN),
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
        app_port: Some(CONS_LISTEN),
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

    // 3. The shared scenario, plus the gRPC-only binary byte-exact roundtrip.
    run_mirrored_scenario(Endpoints {
        publisher_base: &format!("http://127.0.0.1:{PUB_LISTEN}"),
        consumer_base: &format!("http://127.0.0.1:{CONS_LISTEN}"),
        binary_state_exact: true,
    });
}
