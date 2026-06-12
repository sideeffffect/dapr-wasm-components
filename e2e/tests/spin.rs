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
//! Ignored by default — requires Docker and the `spin` CLI (>= 3.4):
//!
//! ```sh
//! cargo build --release --target wasm32-wasip2 --manifest-path components/Cargo.toml
//! cargo test --test spin -- --ignored
//! ```
//!
//! Overrides: SPIN_BIN, DAPRD_IMAGE_TAG (default 1.18.0).

use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use testcontainers_modules::testcontainers::core::Mount;
use testcontainers_modules::testcontainers::runners::SyncRunner;
use testcontainers_modules::testcontainers::{Container, GenericImage, ImageExt};

use dapr_wasm_components_e2e::{component_path, compose, grpc_provider_path};

/// Where the resources directory is bind-mounted inside the daprd container.
const DAPRD_MOUNT: &str = "/dapr-e2e";

// Below 32768: Docker's ephemeral port range (32768+) is where stray
// docker-proxy mappings (e.g. testcontainers ryuk) land — daprd would fail
// to bind a clashing port.
const SPIN_LISTEN_PORT: u16 = 18093;
const DAPR_HTTP_PORT: u16 = 13553;
const DAPR_GRPC_PORT: u16 = 15553;
const DAPR_INTERNAL_GRPC_PORT: u16 = 14553;

/// Kills the process on drop, dumping its output on test failure.
/// Output pipes are drained continuously by background threads — a child
/// with an undrained pipe blocks once the pipe buffer fills up.
/// The child gets its own process group, killed as a whole: `spin up`
/// runs the actual server as a `spin trigger http` subprocess that would
/// survive its parent and keep the port.
struct Service {
    name: &'static str,
    child: Child,
    output: Arc<Mutex<String>>,
}

impl Service {
    fn spawn(name: &'static str, command: &mut Command) -> Self {
        let mut child = std::os::unix::process::CommandExt::process_group(command, 0)
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
        // Kill the whole process group (negative pid), then reap the child.
        let _ = Command::new("kill")
            .args(["-9", "--", &format!("-{}", self.child.id())])
            .status();
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

fn write_dapr_resources(dir: &std::path::Path) {
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
        r#"apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: pubsub
spec:
  type: pubsub.in-memory
  version: v1
"#,
    )
    .unwrap();
    // The daprd container runs as a non-root user (65532) and must read
    // the resource definitions in this directory.
    for path in [dir.to_path_buf(), resources] {
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o777);
        std::fs::set_permissions(&path, permissions).unwrap();
    }
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

/// The daprd sidecar as a testcontainer (host networking so it can dial
/// the Spin-served app channel). Dumps its logs when the test fails.
struct Daprd {
    container: Container<GenericImage>,
}

impl Daprd {
    fn start(shared_dir: &std::path::Path) -> Self {
        let tag = std::env::var("DAPRD_IMAGE_TAG").unwrap_or_else(|_| "1.18.0".to_string());
        let cmd = vec![
            "/daprd".to_string(),
            "--app-id".to_string(),
            "spin-demo".to_string(),
            "--app-port".to_string(),
            SPIN_LISTEN_PORT.to_string(),
            "--app-protocol".to_string(),
            "http".to_string(),
            "--dapr-http-port".to_string(),
            DAPR_HTTP_PORT.to_string(),
            "--dapr-grpc-port".to_string(),
            DAPR_GRPC_PORT.to_string(),
            "--dapr-internal-grpc-port".to_string(),
            DAPR_INTERNAL_GRPC_PORT.to_string(),
            "--resources-path".to_string(),
            format!("{DAPRD_MOUNT}/resources"),
            "--enable-metrics=false".to_string(),
            "--log-level".to_string(),
            "info".to_string(),
        ];
        let container = GenericImage::new("daprio/daprd", &tag)
            .with_cmd(cmd)
            .with_network("host")
            .with_mount(Mount::bind_mount(
                shared_dir.to_str().unwrap().to_string(),
                DAPRD_MOUNT,
            ))
            .start()
            .expect("failed to start daprd container (is Docker running?)");
        Self { container }
    }
}

impl Drop for Daprd {
    fn drop(&mut self) {
        if std::thread::panicking() {
            for (label, result) in [
                ("stdout", self.container.stdout_to_vec()),
                ("stderr", self.container.stderr_to_vec()),
            ] {
                if let Ok(output) = result {
                    let output = String::from_utf8_lossy(&output);
                    if !output.trim().is_empty() {
                        eprintln!("===== daprd {label} =====\n{output}");
                    }
                }
            }
        }
    }
}

fn get_json(path: &str) -> serde_json::Value {
    let url = format!("http://127.0.0.1:{SPIN_LISTEN_PORT}{path}");
    let response = ureq::get(&url)
        .timeout(Duration::from_secs(30))
        .call()
        .unwrap_or_else(|e| panic!("GET {path} failed: {e}"));
    response
        .into_json()
        .unwrap_or_else(|e| panic!("GET {path} returned non-JSON: {e}"))
}

#[test]
#[ignore = "requires Docker and the spin CLI >= 3.4 (see module docs)"]
fn microservice_through_real_dapr_grpc_on_spin() {
    let spin = std::env::var("SPIN_BIN").unwrap_or_else(|_| "spin".to_string());

    // Compose the demo app with the wasi-grpc provider.
    let provider = std::fs::read(grpc_provider_path()).expect("wasi-grpc provider not built");
    let app = std::fs::read(component_path("SPIN_DEMO_COMPONENT", "spin-demo.wasm"))
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
    let _daprd = Daprd::start(dir.path());
    wait_http_ok(
        &format!("http://127.0.0.1:{DAPR_HTTP_PORT}/v1.0/healthz"),
        "daprd",
        Duration::from_secs(60),
    );

    // 3. State over gRPC: binary byte-exact roundtrip, etag CAS, delete,
    //    metadata.
    let smoke = get_json("/smoke");
    assert_eq!(smoke["ok"], true, "smoke failed: {smoke}");
    assert_eq!(smoke["appId"], "spin-demo", "unexpected smoke: {smoke}");

    // 4. Service invocation over gRPC, back in through the app channel.
    let invoked = get_json("/invoke-self");
    assert_eq!(invoked["ok"], true, "invoke-self failed: {invoked}");

    // 5. Pub/sub over gRPC: publish three orders, the in-memory pubsub
    //    delivers them to /orders via the app channel, the counter grows.
    for _ in 0..3 {
        let published = get_json("/publish");
        assert_eq!(published["published"], true, "publish failed: {published}");
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let count = get_json("/count");
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
