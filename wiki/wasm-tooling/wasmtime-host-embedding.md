# Wasmtime Host Embedding — sync WIT, async host

> Sources: Bytecode Alliance (docs.rs/wasmtime, docs.wasmtime.dev), 2026-06-11 survey
> Raw: [component-ecosystem-survey-2026-06](../../raw/wasm-tooling/component-ecosystem-survey-2026-06.md)

## Overview

Wasmtime (45.x as of June 2026) embeds components in Rust via `wasmtime::component::bindgen!`. The crucial fact for this project: **a sync WIT function can be implemented by an async Rust host function**. The guest blocks; the runtime awaits the host future. This lets tokio-based clients (like the Dapr SDK) sit behind sync guest-facing interfaces — no need for WASI 0.3 async.

## Modern bindgen! syntax (wasmtime ≥ ~34; 45.x current)

The old `async: true` option was **replaced** by per-function flag maps:

```rust
bindgen!({
    path: "wit",
    world: "my-world",
    imports: { default: async | trappable },   // host trait methods become async fn
    exports: { default: async },               // guest exports called via async API
});
```

Flags: `async`, `store`, `tracing`, `verbose_tracing`, `trappable`, `ignore_wit`, `exact`; specify per function (`"ns:pkg/iface#func": async`) or as `default:`.

## Runtime requirements

- `Config::async_support(true)` — **deprecated in wasmtime 45 and a no-op**: async support is always on; just use `Engine::new(&Config::new())`. (Older docs/examples still show it as required.)
- wasmtime 45 has its own `wasmtime::Error`/`wasmtime::Result` (anyhow-like, 99% API-compatible); `From<wasmtime::Error> for anyhow::Error` exists behind the default-on `anyhow` feature, and `wasmtime::error::Context` replaces `anyhow::Context` for `.with_context(...)` on wasmtime results.
- Generated export bindings live under `bindings::exports::<ns>::<pkg>::<interface>`; world-level exports get `call_<name>` on the instance type, interface exports via accessor methods like `instance.dapr_client_topic_handler()`.
- `wasmtime-wasi` 45 re-exports `WasiCtx`, `WasiCtxBuilder`, `WasiView`, `WasiCtxView` at the **crate root** (not `p2::`); only `add_to_linker_async` lives in `p2`.
- Use `_async` variants throughout: `instantiate_async`, generated `call_*` wrappers are async.
- Futures are `Send` when store data is `Send`; tune `Config::async_stack_size`.
- WASI 0.2 imports: `wasmtime_wasi::add_to_linker_async`.

## Guest side (wit-bindgen 0.58)

`wit_bindgen::generate!({ world, path: "wit", generate_all })` + `export!(Component)`; `crate-type = ["cdylib"]`; build with `cargo build --target wasm32-wasip2` (tier-2 since Rust 1.82, produces a component directly — cargo-component not needed and in maintenance mode).

## WASI 0.2 vs 0.3 (June 2026)

WASI 0.3.0 final released 2026-06-11 (native async ABI, `stream`/`future`); wasmtime 45.0.1 still tracks an RC snapshot; cross-component async has ~3.5x call overhead pre-1.0. **WASI 0.2.x remains the production target**; sync WIT + async host covers the Dapr use case fully.

## See Also

- [WIT Format](../wasm-component-model/wit-format.md)
- [wasm-pkg-tools (wkg)](wasm-pkg-tools-wkg.md)
- [Dapr Rust SDK](../dapr/dapr-rust-sdk.md)
