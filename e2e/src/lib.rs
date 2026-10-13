//! Test harness for dapr-wasm-components: a mock Dapr sidecar (HTTP),
//! wasmtime instantiation of the wasi-http provider, and wac composition.

pub mod compose;
pub mod mock;

use std::path::PathBuf;

use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpCtxView, WasiHttpView};

pub mod bindings {
    wasmtime::component::bindgen!({
        path: "../components/wit",
        world: "outbound",
        exports: { default: async },
    });
}

pub struct Ctx {
    wasi: WasiCtx,
    http: WasiHttpCtx,
    table: ResourceTable,
}

impl Ctx {
    pub fn new(env: &[(String, String)]) -> Self {
        let mut wasi = WasiCtxBuilder::new();
        wasi.inherit_stdout().inherit_stderr();
        for (key, value) in env {
            wasi.env(key, value);
        }
        Self {
            wasi: wasi.build(),
            http: WasiHttpCtx::new(),
            table: ResourceTable::new(),
        }
    }
}

impl WasiView for Ctx {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl WasiHttpView for Ctx {
    fn http(&mut self) -> WasiHttpCtxView<'_> {
        WasiHttpCtxView {
            ctx: &mut self.http,
            table: &mut self.table,
            hooks: Default::default(),
        }
    }
}

/// Resolve a built wasm artifact: an env override (used in CI) wins,
/// otherwise join `relative` (from the e2e crate's manifest dir) with `file`.
fn resolve(env_var: &str, relative: &str, file: &str) -> PathBuf {
    if let Ok(path) = std::env::var(env_var) {
        return PathBuf::from(path);
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(relative)
        .join(file)
}

/// Path to a provider/interface module built in the `components/` workspace.
fn provider_artifact(env_var: &str, file: &str) -> PathBuf {
    resolve(env_var, "../components/target/wasm32-wasip2/release", file)
}

/// Path to a demo/fixture app built in the `e2e/apps/` workspace, overridable
/// via env (used in CI). The demo apps are the e2e suite's own fixtures.
pub fn app_path(env_var: &str, file_name: &str) -> PathBuf {
    resolve(env_var, "apps/target/wasm32-wasip2/release", file_name)
}

/// The wasi-http **outbound** provider (exports the building blocks).
pub fn http_outbound_path() -> PathBuf {
    provider_artifact(
        "HTTP_OUTBOUND_COMPONENT",
        "dapr_wasm_components_wasi_http_outbound.wasm",
    )
}

/// The wasi-http **inbound** provider (exports `wasi:http/incoming-handler`,
/// imports the callbacks). Composes with any outbound provider — including the
/// gRPC one — since the two directions are independent.
pub fn http_inbound_path() -> PathBuf {
    provider_artifact(
        "HTTP_INBOUND_COMPONENT",
        "dapr_wasm_components_wasi_http_inbound.wasm",
    )
}

/// The wasi-grpc **outbound** provider (exports the building blocks over gRPC).
pub fn grpc_outbound_path() -> PathBuf {
    provider_artifact(
        "GRPC_OUTBOUND_COMPONENT",
        "dapr_wasm_components_wasi_grpc_outbound.wasm",
    )
}

/// The go family's outbound component: the embedded Dapr runtime (exports
/// the building blocks plus the private go-events queue). Built by
/// `components/go-outbound/scripts/build.sh` into the components target dir.
pub fn go_outbound_path() -> PathBuf {
    provider_artifact(
        "GO_OUTBOUND_COMPONENT",
        "dapr_wasm_components_go_outbound.wasm",
    )
}

/// The go family's inbound pump (exports `wasi:cli/run`, imports go-events
/// and the callbacks).
pub fn go_inbound_path() -> PathBuf {
    provider_artifact(
        "GO_INBOUND_COMPONENT",
        "dapr_wasm_components_go_inbound.wasm",
    )
}

pub fn kv_demo_path() -> PathBuf {
    app_path("KV_DEMO_COMPONENT", "kv-demo.wasm")
}

/// The unified E2E microservice app (run as two instances by the real-Dapr
/// suites). Composed with whichever provider is under test.
pub fn microservice_path() -> PathBuf {
    app_path("MICROSERVICE_COMPONENT", "microservice.wasm")
}

/// The state-reader fixture app: it only reads state (authority test).
pub fn state_reader_path() -> PathBuf {
    app_path("STATE_READER_COMPONENT", "state_reader.wasm")
}

pub fn engine() -> wasmtime::Result<Engine> {
    Engine::new(&Config::new())
}

/// Drive a composed **inbound** server (one that exports
/// `wasi:http/incoming-handler`, i.e. `outbound → app → inbound`) with a single
/// synthetic app-channel request — exactly what a Dapr sidecar would POST. The
/// app's outbound calls during handling hit the real `sidecar_endpoint` (the
/// mock), so a test can assert on what the delivery caused. Returns the app
/// channel's HTTP status and response body.
pub async fn serve_inbound(
    component_bytes: &[u8],
    sidecar_endpoint: &str,
    method: &str,
    path: &str,
    content_type: &str,
    body: Vec<u8>,
) -> anyhow::Result<(u16, Vec<u8>)> {
    serve_inbound_with_env(
        component_bytes,
        sidecar_endpoint,
        &[],
        method,
        path,
        content_type,
        body,
    )
    .await
}

/// [`serve_inbound`] with extra guest environment variables (e.g. the
/// microservice's `SUBSCRIBE`), on top of `DAPR_HTTP_ENDPOINT`.
pub async fn serve_inbound_with_env(
    component_bytes: &[u8],
    sidecar_endpoint: &str,
    extra_env: &[(&str, &str)],
    method: &str,
    path: &str,
    content_type: &str,
    body: Vec<u8>,
) -> anyhow::Result<(u16, Vec<u8>)> {
    use http_body_util::{BodyExt, Full};
    use wasmtime_wasi_http::p2::bindings::http::types::{ErrorCode, Scheme};
    use wasmtime_wasi_http::p2::bindings::ProxyPre;

    let engine = engine()?;
    let component = Component::new(&engine, component_bytes)?;
    let linker = linker(&engine)?;
    let pre = ProxyPre::new(linker.instantiate_pre(&component)?)?;

    let mut env = vec![(
        "DAPR_HTTP_ENDPOINT".to_string(),
        sidecar_endpoint.to_string(),
    )];
    env.extend(
        extra_env
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string())),
    );
    let mut store = Store::new(&engine, Ctx::new(&env));

    let request = hyper::Request::builder()
        .method(method)
        .uri(format!("http://app{path}"))
        .header("content-type", content_type)
        .body(
            Full::new(bytes::Bytes::from(body))
                .map_err(|e: std::convert::Infallible| -> ErrorCode { match e {} }),
        )?;

    let (sender, receiver) = tokio::sync::oneshot::channel();
    let req = store
        .data_mut()
        .http()
        .new_incoming_request(Scheme::Http, request)?;
    let out = store.data_mut().http().new_response_outparam(sender)?;

    let task = tokio::task::spawn(async move {
        let proxy = pre.instantiate_async(&mut store).await?;
        proxy
            .wasi_http_incoming_handler()
            .call_handle(&mut store, req, out)
            .await
    });

    let response = match receiver.await {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => anyhow::bail!("inbound handler returned an error: {error:?}"),
        Err(_) => {
            // The guest never set the response outparam; surface the trap.
            task.await??;
            anyhow::bail!("guest never produced a response");
        }
    };
    let status = response.status().as_u16();
    let bytes = response.into_body().collect().await?.to_bytes().to_vec();
    Ok((status, bytes))
}

pub fn linker(engine: &Engine) -> wasmtime::Result<Linker<Ctx>> {
    let mut linker = Linker::<Ctx>::new(engine);
    wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
    wasmtime_wasi_http::p2::add_only_http_to_linker_async(&mut linker)?;
    Ok(linker)
}

/// Instantiate the wasi-http outbound provider with `DAPR_HTTP_ENDPOINT`
/// pointing at the given mock sidecar address. The `outbound` world has
/// no app-facing imports, so it instantiates standalone.
pub async fn load_provider(
    sidecar_endpoint: &str,
) -> wasmtime::Result<(Store<Ctx>, bindings::Outbound)> {
    let engine = engine()?;
    let component = Component::from_file(&engine, http_outbound_path())?;
    let linker = linker(&engine)?;
    let env = vec![(
        "DAPR_HTTP_ENDPOINT".to_string(),
        sidecar_endpoint.to_string(),
    )];
    let mut store = Store::new(&engine, Ctx::new(&env));
    let provider = bindings::Outbound::instantiate_async(&mut store, &component, &linker).await?;
    Ok((store, provider))
}
