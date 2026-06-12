//! Shared scaffolding for the real-Dapr end-to-end tests (`dapr.rs` for the
//! wasi-http provider, `spin.rs` for the wasi-grpc provider).
//!
//! The two tests differ only in which component(s) they compose, how they
//! drive them (`wasmtime serve`/`run` vs `spin up`), the pub/sub backend,
//! and what they assert. Everything mechanical — managed child processes,
//! the `daprd` testcontainer, resource files, and HTTP readiness polling —
//! lives here so the tests stay down to their essence.

// Each test binary compiles this module independently and uses only a
// subset of it; the rest is legitimately unused for that binary.
#![allow(dead_code)]

use std::io::Read;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use testcontainers_modules::testcontainers::core::Mount;
use testcontainers_modules::testcontainers::runners::SyncRunner;
use testcontainers_modules::testcontainers::{Container, GenericImage, ImageExt};

/// Where the shared directory (resources, config, name-resolution db) is
/// bind-mounted inside the daprd containers.
pub const DAPRD_MOUNT: &str = "/dapr-e2e";

/// An env override (used in CI) or a default for a CLI binary path.
pub fn binary(env_var: &str, default: &str) -> String {
    std::env::var(env_var).unwrap_or_else(|_| default.to_string())
}

/// A managed child process — an app server (`wasmtime serve`) or `spin up`.
///
/// Its output is drained continuously by background threads (a child with an
/// undrained pipe blocks once the pipe buffer fills), and the whole process
/// group is killed on drop: `spin up` runs the actual server as a `spin
/// trigger http` subprocess that would otherwise survive its parent and hold
/// the port. `wasmtime serve` spawns no children, so for it the group kill is
/// equivalent to killing the process directly.
pub struct Service {
    name: &'static str,
    child: Child,
    output: Arc<Mutex<String>>,
}

impl Service {
    pub fn spawn(name: &'static str, command: &mut Command) -> Self {
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

/// Poll `url` until it answers with any HTTP response, or `timeout` elapses
/// (then panic). Used to wait for app channels and daprd readiness probes.
pub fn wait_http_ok(url: &str, what: &str, timeout: Duration) {
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

/// GET `url` and parse the body as JSON, panicking with context on failure.
pub fn get_json(url: &str) -> serde_json::Value {
    let response = ureq::get(url)
        .timeout(Duration::from_secs(30))
        .call()
        .unwrap_or_else(|e| panic!("GET {url} failed: {e}"));
    response
        .into_json()
        .unwrap_or_else(|e| panic!("GET {url} returned non-JSON: {e}"))
}

/// The API/internal-gRPC/HTTP ports a daprd sidecar listens on.
pub struct DaprdPorts {
    pub http: u16,
    pub grpc: u16,
    pub internal_grpc: u16,
}

/// Specification for one daprd sidecar testcontainer.
pub struct DaprdConfig<'a> {
    /// Label used when dumping the container's logs on failure.
    pub name: &'static str,
    pub app_id: &'a str,
    pub ports: DaprdPorts,
    /// The app channel port, when this sidecar fronts an app (so it can
    /// register subscriptions, deliver events, and be an invocation target).
    pub app_port: Option<u16>,
    /// A `Configuration` file name under `DAPRD_MOUNT` (e.g. `"config.yaml"`),
    /// when the sidecar needs one — for instance to pick a name-resolution
    /// component. `None` uses daprd's defaults.
    pub config_file: Option<&'a str>,
    pub shared_dir: &'a Path,
}

/// A daprd sidecar as a testcontainer (daprio/daprd image, host networking so
/// it can dial the wasm app served on the host). Dumps its logs when the test
/// fails. Override the image tag with `DAPRD_IMAGE_TAG` (default 1.18.0).
pub struct Daprd {
    name: &'static str,
    container: Container<GenericImage>,
}

impl Daprd {
    pub fn start(config: DaprdConfig) -> Self {
        let DaprdConfig {
            name,
            app_id,
            ports,
            app_port,
            config_file,
            shared_dir,
        } = config;
        let tag = std::env::var("DAPRD_IMAGE_TAG").unwrap_or_else(|_| "1.18.0".to_string());
        let mut cmd = vec![
            "/daprd".to_string(),
            "--app-id".to_string(),
            app_id.to_string(),
            "--dapr-http-port".to_string(),
            ports.http.to_string(),
            "--dapr-grpc-port".to_string(),
            ports.grpc.to_string(),
            "--dapr-internal-grpc-port".to_string(),
            ports.internal_grpc.to_string(),
            "--resources-path".to_string(),
            format!("{DAPRD_MOUNT}/resources"),
            "--enable-metrics=false".to_string(),
            "--log-level".to_string(),
            "info".to_string(),
        ];
        if let Some(file) = config_file {
            cmd.extend(["--config".to_string(), format!("{DAPRD_MOUNT}/{file}")]);
        }
        if let Some(port) = app_port {
            cmd.extend([
                "--app-port".to_string(),
                port.to_string(),
                "--app-protocol".to_string(),
                "http".to_string(),
            ]);
        }
        let container = GenericImage::new("daprio/daprd", &tag)
            .with_cmd(cmd)
            .with_network("host")
            .with_mount(Mount::bind_mount(
                shared_dir.to_str().unwrap().to_string(),
                DAPRD_MOUNT,
            ))
            .start()
            .unwrap_or_else(|e| {
                panic!("failed to start {name} container (is Docker running?): {e}")
            });
        Self { name, container }
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
                        eprintln!("===== {} {label} =====\n{output}", self.name);
                    }
                }
            }
        }
    }
}

/// The `state.in-memory` statestore both tests use, named `statestore`.
pub const STATESTORE_IN_MEMORY: &str = r#"apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: statestore
spec:
  type: state.in-memory
  version: v1
"#;

/// The `pubsub.in-memory` pub/sub broker, named `pubsub` — fine for a single
/// sidecar (the wasi-grpc test); cross-sidecar pub/sub needs a real broker.
pub const PUBSUB_IN_MEMORY: &str = r#"apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: pubsub
spec:
  type: pubsub.in-memory
  version: v1
"#;

/// Write `<dir>/resources/<name>` (creating the `resources/` subdir as
/// needed) — daprd loads every component definition under `--resources-path`.
pub fn write_resource(dir: &Path, name: &str, yaml: &str) {
    let resources = dir.join("resources");
    std::fs::create_dir_all(&resources).unwrap();
    std::fs::write(resources.join(name), yaml).unwrap();
}

/// daprd containers run as a non-root user (uid 65532); make the shared dir
/// and its `resources/` subdir world-accessible so the sidecar can read the
/// configs and write the name-resolution db. Call after writing resources.
pub fn relax_permissions(dir: &Path) {
    for path in [dir.to_path_buf(), dir.join("resources")] {
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o777);
        std::fs::set_permissions(&path, permissions).unwrap();
    }
}
