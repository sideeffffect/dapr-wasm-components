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
        path: "../wit",
        world: "provider",
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

/// Path to a built component, overridable via env (used in CI).
pub fn component_path(env_var: &str, file_name: &str) -> PathBuf {
    if let Ok(path) = std::env::var(env_var) {
        return PathBuf::from(path);
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../components/target/wasm32-wasip2/release")
        .join(file_name)
}

pub fn provider_path() -> PathBuf {
    component_path("PROVIDER_COMPONENT", "dapr_wasm_components_wasi_http.wasm")
}

pub fn kv_demo_path() -> PathBuf {
    component_path("KV_DEMO_COMPONENT", "kv-demo.wasm")
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

/// Instantiate the wasi-http provider with `DAPR_HTTP_ENDPOINT` pointing at
/// the given mock sidecar address.
pub async fn load_provider(
    sidecar_endpoint: &str,
) -> wasmtime::Result<(Store<Ctx>, bindings::Provider)> {
    let engine = engine()?;
    let component = Component::from_file(&engine, provider_path())?;
    let linker = linker(&engine)?;
    let env = vec![(
        "DAPR_HTTP_ENDPOINT".to_string(),
        sidecar_endpoint.to_string(),
    )];
    let mut store = Store::new(&engine, Ctx::new(&env));
    let provider = bindings::Provider::instantiate_async(&mut store, &component, &linker).await?;
    Ok((store, provider))
}
