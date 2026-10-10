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

use testcontainers_modules::redis::Redis;
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
        match ureq::get(url)
            .config()
            .timeout_global(Some(Duration::from_secs(2)))
            .build()
            .call()
        {
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
    let mut response = ureq::get(url)
        .config()
        .timeout_global(Some(Duration::from_secs(30)))
        .build()
        .call()
        .unwrap_or_else(|e| panic!("GET {url} failed: {e}"));
    response
        .body_mut()
        .read_json()
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
/// fails. Override the image tag with `DAPRD_IMAGE_TAG` (default 1.18.7).
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
        let tag = std::env::var("DAPRD_IMAGE_TAG").unwrap_or_else(|_| "1.18.7".to_string());
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

/// A `bindings.cron` input binding named `ticks` that fires every second — a
/// deterministic input-binding trigger with no external dependency. Only the
/// app instance that declares `ticks` (the consumer) receives the events.
pub const BINDING_CRON_TICKS: &str = r#"apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: ticks
spec:
  type: bindings.cron
  version: v1
  metadata:
  - name: schedule
    value: "@every 1s"
  - name: direction
    value: "input"
"#;

/// Write a `secretstores.local.file` store named `secretstore` and its
/// `secrets.json` (one secret, `db` = `s3cret`) under the bind mount.
pub fn write_secret_store(dir: &Path) {
    write_resource(
        dir,
        "secretstore.yaml",
        &format!(
            r#"apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: secretstore
spec:
  type: secretstores.local.file
  version: v1
  metadata:
  - name: secretsFile
    value: "{DAPRD_MOUNT}/secrets.json"
"#
        ),
    );
    std::fs::write(dir.join("secrets.json"), r#"{"db":"s3cret"}"#).unwrap();
}

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

/// A Redis broker for cross-sidecar pub/sub. Started via testcontainers unless
/// `REDIS_HOST` points at an existing instance; the container handle must stay
/// alive for the duration of the test.
pub struct RedisBroker {
    _container: Option<Container<Redis>>,
    pub host: String,
}

pub fn start_redis() -> RedisBroker {
    match std::env::var("REDIS_HOST") {
        Ok(host) => RedisBroker {
            _container: None,
            host,
        },
        Err(_) => {
            let container = Redis::default()
                .start()
                .expect("failed to start Redis testcontainer (is Docker running?)");
            let port = container
                .get_host_port_ipv4(6379)
                .expect("no mapped Redis port");
            RedisBroker {
                _container: Some(container),
                host: format!("127.0.0.1:{port}"),
            }
        }
    }
}

/// The config file name (under `DAPRD_MOUNT`) written by
/// [`write_cross_sidecar_resources`].
pub const CONFIG_FILE: &str = "config.yaml";

/// Write the resources + config that the two-sidecar scenario needs, shared by
/// both real-Dapr suites: an in-memory statestore (per sidecar), a
/// `local.file` secret store, a Redis
/// pub/sub broker (so events cross sidecars), and a sqlite name-resolution
/// `Configuration` (deterministic cross-sidecar invocation, unlike mDNS) whose
/// db both sidecars share via the bind mount.
pub fn write_cross_sidecar_resources(dir: &Path, redis_host: &str) {
    write_resource(dir, "statestore.yaml", STATESTORE_IN_MEMORY);
    write_secret_store(dir);
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
    write_resource(dir, "ticks.yaml", BINDING_CRON_TICKS);
    std::fs::write(
        dir.join(CONFIG_FILE),
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

/// The two app instances of the mirrored scenario, addressed by their app
/// channel base URLs (e.g. `http://127.0.0.1:8091`).
pub struct Endpoints<'a> {
    /// Instance with app id `publisher` (does not subscribe): publishes,
    /// invokes, and runs the state smoke checks.
    pub publisher_base: &'a str,
    /// Instance with app id `consumer` (subscribes to `orders`, and to
    /// `orders-bulk` with bulk delivery): processes deliveries into counters.
    pub consumer_base: &'a str,
    /// Also assert a *binary* (non-JSON) byte-exact state roundtrip. Only the
    /// gRPC provider can do that (raw protobuf bytes), so only the wasi-grpc
    /// suite sets this; the HTTP provider's JSON envelope can't.
    pub binary_state_exact: bool,
}

/// The scenario both real-Dapr suites run, identical regardless of provider or
/// runtime: state roundtrip + etag CAS + delete, secrets, service invocation
/// (self and cross-app), cross-sidecar pub/sub publish→deliver→count loops
/// (single with CEL routing rules, and bulk subscribe with a default route),
/// and a cron input binding delivered to the consumer. Panics (with context)
/// on the first failed assertion.
pub fn run_mirrored_scenario(endpoints: Endpoints) {
    let Endpoints {
        publisher_base,
        consumer_base,
        binary_state_exact,
    } = endpoints;

    // 1. State: JSON-value roundtrip + etag CAS + delete + metadata.
    let smoke = get_json(&format!("{publisher_base}/smoke"));
    assert_eq!(smoke["ok"], true, "smoke failed: {smoke}");
    assert_eq!(smoke["appId"], "publisher", "unexpected smoke: {smoke}");

    // 2. (gRPC only) the same, with binary bytes — byte-exact.
    if binary_state_exact {
        let smoke = get_json(&format!("{publisher_base}/smoke-binary"));
        assert_eq!(smoke["ok"], true, "binary smoke failed: {smoke}");
    }

    // 3. Secrets (`local.file`): an existing secret, and a missing one as the
    //    typed `secret-not-found`.
    let secrets = get_json(&format!("{publisher_base}/secrets"));
    assert_eq!(secrets["ok"], true, "secrets failed: {secrets}");

    // 4. Service invocation out and back into the same instance.
    let invoked = get_json(&format!("{publisher_base}/invoke-self"));
    assert_eq!(invoked["ok"], true, "invoke-self failed: {invoked}");

    // 5. Publish three orders.
    let published = get_json(&format!("{publisher_base}/publish?n=3"));
    assert_eq!(published["published"], 3, "publish failed: {published}");

    // 6. Cross-sidecar pub/sub delivery: the consumer's counter reaches 3.
    wait_for_processed(consumer_base, 3);

    // 7. CEL routing rules: plain orders took the subscription's default
    //    route; a `"priority": true` order is routed by daprd to the rule path
    //    and the app sees that path in `topic-event.path`.
    assert_eq!(
        get_json(&format!("{consumer_base}/route?order=1"))["route"],
        "/orders",
        "plain order did not take the default route"
    );
    let published = get_json(&format!("{publisher_base}/publish-priority"));
    assert_eq!(published["published"], 100, "publish failed: {published}");
    wait_for_processed(consumer_base, 4);
    assert_eq!(
        get_json(&format!("{consumer_base}/route?order=100"))["route"],
        "/orders/priority",
        "priority order was not routed by the CEL rule"
    );

    // 8. Bulk subscribe: the consumer subscribes to `orders-bulk` with
    //    `bulkSubscribe` enabled and a default route, so daprd delivers the
    //    three orders in batches to `on-bulk-topic-event` (the app counts
    //    each batch) on that route, which the app sees in `path`.
    let published = get_json(&format!("{publisher_base}/publish-bulk?n=3"));
    assert_eq!(
        published["published"], 3,
        "bulk publish failed: {published}"
    );
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let count = get_json(&format!("{consumer_base}/count-bulk"));
        if count["processed"] == 3 {
            let batches = count["batches"].as_u64().unwrap_or(0);
            assert!(
                (1..=3).contains(&batches),
                "bulk orders did not arrive through bulk delivery: {count}"
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "bulk orders were not all processed in time: {count}"
        );
        std::thread::sleep(Duration::from_millis(500));
    }
    assert_eq!(
        get_json(&format!("{consumer_base}/bulk-route?order=1"))["route"],
        "/orders-bulk",
        "bulk order was not delivered on the subscription's default route"
    );

    // 9. Cross-app service invocation: publisher → consumer's /summary (needs
    //    the sidecars to resolve each other's app ids).
    let peer = get_json(&format!("{publisher_base}/invoke-peer"));
    assert_eq!(peer["processed"], 4, "cross-app invocation failed: {peer}");

    // 10. Input binding: the cron `ticks` binding (every 1s) reaches the
    //    consumer's `on-binding-event` through the inbound provider; the app
    //    counts each delivery itself (outbound state API).
    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        let status = get_json(&format!("{consumer_base}/binding"));
        if status["ticks"].as_u64().unwrap_or(0) >= 3 {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "input binding events were not delivered in time: {status}"
        );
        std::thread::sleep(Duration::from_millis(500));
    };

    // 11. Each reply also asked the sidecar to persist `binding-response` via
    //    `storeName` + `state`. daprd (checked up to 1.18) ignores the body of
    //    an HTTP app-channel binding reply — `sendBindingEventToApp` never
    //    parses it — so nothing is persisted, even after 3+ replies. This pins
    //    that upstream behaviour: if it starts failing, daprd now honours the
    //    reply — flip this to a positive check and update the docs.
    assert!(
        status["responseState"].is_null(),
        "daprd now applies the input-binding reply over the HTTP app channel \
         (update the docs and this check): {status}"
    );
}

/// Poll the consumer until its processed-order counter reaches `expected`.
/// Generous deadline — a transient app↔sidecar connection blip makes the app
/// return RETRY, and daprd redelivers with backoff; give it room.
fn wait_for_processed(consumer_base: &str, expected: u64) {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let count = get_json(&format!("{consumer_base}/count"));
        if count["processed"] == expected {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "orders were not all processed in time (want {expected}): {count}"
        );
        std::thread::sleep(Duration::from_millis(500));
    }
}
