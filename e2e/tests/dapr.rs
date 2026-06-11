//! Real-Dapr end-to-end test: two wasm microservices orchestrated through
//! two actual `daprd` sidecars.
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
//!   sqlite (shared db file).
//!
//! Ignored by default — requires `daprd`, `wasmtime`, and Docker (Redis is
//! started automatically via testcontainers):
//!
//! ```sh
//! cargo build --release --target wasm32-wasip2 --manifest-path components/Cargo.toml
//! cargo test --test dapr -- --ignored
//! ```
//!
//! Overrides: DAPRD_BIN, WASMTIME_BIN, REDIS_HOST (skips the testcontainer).

use std::io::Read;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dapr_wasm_components_e2e::{component_path, compose};

const ORDER_PROCESSOR_APP_PORT: u16 = 8091;
const OP_DAPR_HTTP: u16 = 3551;
const CHECKOUT_DAPR_HTTP: u16 = 3552;

/// Kills the process on drop, dumping its output on test failure.
/// Output pipes are drained continuously by background threads — a child
/// with an undrained pipe blocks once the pipe buffer fills up.
struct Service {
    name: &'static str,
    child: Child,
    output: Arc<Mutex<String>>,
}

impl Service {
    fn spawn(name: &'static str, command: &mut Command) -> Self {
        let mut child = command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("failed to start {name}: {e}"));
        let output = Arc::new(Mutex::new(String::new()));
        for stream in [
            child
                .stdout
                .take()
                .map(|s| Box::new(s) as Box<dyn Read + Send>),
            child
                .stderr
                .take()
                .map(|s| Box::new(s) as Box<dyn Read + Send>),
        ]
        .into_iter()
        .flatten()
        {
            let output = output.clone();
            std::thread::spawn(move || {
                let mut stream = stream;
                let mut buffer = [0u8; 4096];
                loop {
                    match stream.read(&mut buffer) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => output
                            .lock()
                            .unwrap()
                            .push_str(&String::from_utf8_lossy(&buffer[..n])),
                    }
                }
            });
        }
        Self {
            name,
            child,
            output,
        }
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if std::thread::panicking() {
            let output = self.output.lock().unwrap();
            if !output.trim().is_empty() {
                eprintln!("===== {} output =====\n{output}", self.name);
            }
        }
    }
}

fn binary(env_var: &str, default: &str) -> String {
    std::env::var(env_var).unwrap_or_else(|_| default.to_string())
}

fn wait_http_ok(url: &str, what: &str, timeout: Duration) {
    let start = Instant::now();
    loop {
        match ureq::get(url).timeout(Duration::from_secs(2)).call() {
            Ok(_) => return,
            Err(error) => {
                if start.elapsed() > timeout {
                    panic!("timed out waiting for {what} at {url}: {error}");
                }
                std::thread::sleep(Duration::from_millis(250));
            }
        }
    }
}

fn write_dapr_resources(dir: &std::path::Path, redis_host: &str) -> PathBuf {
    let resources = dir.join("resources");
    std::fs::create_dir_all(&resources).unwrap();
    std::fs::write(
        resources.join("statestore.yaml"),
        r#"apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: statestore
spec:
  type: state.in-memory
  version: v1
"#,
    )
    .unwrap();
    std::fs::write(
        resources.join("pubsub.yaml"),
        format!(
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
    )
    .unwrap();
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
      connectionString: "{}"
"#,
            dir.join("nameresolution.db").display()
        ),
    )
    .unwrap();
    resources
}

struct DaprdPorts {
    http: u16,
    grpc: u16,
    internal_grpc: u16,
}

fn daprd_command(
    daprd: &str,
    app_id: &str,
    ports: DaprdPorts,
    app_port: Option<u16>,
    resources: &std::path::Path,
    config: &std::path::Path,
) -> Command {
    let mut command = Command::new(daprd);
    command
        .arg("--app-id")
        .arg(app_id)
        .arg("--dapr-http-port")
        .arg(ports.http.to_string())
        .arg("--dapr-grpc-port")
        .arg(ports.grpc.to_string())
        .arg("--dapr-internal-grpc-port")
        .arg(ports.internal_grpc.to_string())
        .arg("--resources-path")
        .arg(resources)
        .arg("--config")
        .arg(config)
        .arg("--enable-metrics=false")
        .arg("--log-level")
        .arg("info");
    if let Some(port) = app_port {
        command
            .arg("--app-port")
            .arg(port.to_string())
            .arg("--app-protocol")
            .arg("http");
    }
    command
}

#[test]
#[ignore = "requires daprd, wasmtime CLI, and a local Redis (see module docs)"]
fn microservices_through_real_dapr() {
    let daprd = binary("DAPRD_BIN", "daprd");
    let wasmtime = binary("WASMTIME_BIN", "wasmtime");

    // Redis backs cross-sidecar pub/sub. Started via testcontainers unless
    // REDIS_HOST points at an existing instance. The container handle must
    // stay alive for the duration of the test.
    let mut _redis_container = None;
    let redis_host = match std::env::var("REDIS_HOST") {
        Ok(host) => host,
        Err(_) => {
            use testcontainers_modules::testcontainers::runners::SyncRunner;
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
    let order_processor = std::fs::read(component_path(
        "ORDER_PROCESSOR_COMPONENT",
        "order-processor.wasm",
    ))
    .expect("order-processor component not built");
    let checkout = std::fs::read(component_path("CHECKOUT_COMPONENT", "checkout.wasm"))
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

    let resources = write_dapr_resources(dir.path(), &redis_host);
    let config = dir.path().join("config.yaml");

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
    let _op_daprd = Service::spawn(
        "daprd (order-processor)",
        &mut daprd_command(
            &daprd,
            "order-processor",
            DaprdPorts {
                http: OP_DAPR_HTTP,
                grpc: 50051,
                internal_grpc: 48051,
            },
            Some(ORDER_PROCESSOR_APP_PORT),
            &resources,
            &config,
        ),
    );
    wait_http_ok(
        &format!("http://127.0.0.1:{OP_DAPR_HTTP}/v1.0/metadata"),
        "order-processor daprd",
        Duration::from_secs(60),
    );

    // 3. The checkout sidecar (no app channel needed).
    let _checkout_daprd = Service::spawn(
        "daprd (checkout)",
        &mut daprd_command(
            &daprd,
            "checkout",
            DaprdPorts {
                http: CHECKOUT_DAPR_HTTP,
                grpc: 50052,
                internal_grpc: 48052,
            },
            None,
            &resources,
            &config,
        ),
    );
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
