//! Test harness for dapr-wasm-components: a mock Dapr sidecar (HTTP),
//! wasmtime instantiation of the wasi-http provider, and wac composition.

pub mod compose;
pub mod mock;

use std::path::PathBuf;

use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};
use wasmtime_wasi_http::p2::{WasiHttpCtxView, WasiHttpView};
use wasmtime_wasi_http::WasiHttpCtx;

pub mod bindings {
    wasmtime::component::bindgen!({
        path: "../components/wit",
        world: "dapr-outbound",
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

pub fn kv_demo_path() -> PathBuf {
    app_path("KV_DEMO_COMPONENT", "kv-demo.wasm")
}

/// The unified E2E microservice app (run as two instances by the real-Dapr
/// suites). Composed with whichever provider is under test.
pub fn microservice_path() -> PathBuf {
    app_path("MICROSERVICE_COMPONENT", "microservice.wasm")
}

pub fn engine() -> wasmtime::Result<Engine> {
    Engine::new(&Config::new())
}

pub fn linker(engine: &Engine) -> wasmtime::Result<Linker<Ctx>> {
    let mut linker = Linker::<Ctx>::new(engine);
    wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
    wasmtime_wasi_http::p2::add_only_http_to_linker_async(&mut linker)?;
    Ok(linker)
}

/// Instantiate the wasi-http outbound provider with `DAPR_HTTP_ENDPOINT`
/// pointing at the given mock sidecar address. The `dapr-outbound` world has
/// no app-facing imports, so it instantiates standalone.
pub async fn load_provider(
    sidecar_endpoint: &str,
) -> wasmtime::Result<(Store<Ctx>, bindings::DaprOutbound)> {
    let engine = engine()?;
    let component = Component::from_file(&engine, http_outbound_path())?;
    let linker = linker(&engine)?;
    let env = vec![(
        "DAPR_HTTP_ENDPOINT".to_string(),
        sidecar_endpoint.to_string(),
    )];
    let mut store = Store::new(&engine, Ctx::new(&env));
    let provider =
        bindings::DaprOutbound::instantiate_async(&mut store, &component, &linker).await?;
    Ok((store, provider))
}
