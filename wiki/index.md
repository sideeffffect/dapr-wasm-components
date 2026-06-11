# Knowledge Base Index

## wasm-component-model

The WebAssembly Component Model specification: WIT IDL, worlds, canonical ABI, async.

| Article | Summary | Updated |
|---------|---------|---------|
| [Component Model Overview](wasm-component-model/component-model-overview.md) | What the component model is, spec repo layout, WASI 0.2 vs 0.3 status | 2026-06-11 |
| [WIT Format](wasm-component-model/wit-format.md) | WIT packages/interfaces/worlds, type system, sync vs async functions, filesystem conventions | 2026-06-11 |

## dapr

Dapr distributed application runtime, with focus on the Rust SDK and Wasm integration.

| Article | Summary | Updated |
|---------|---------|---------|
| [Dapr Rust SDK](dapr/dapr-rust-sdk.md) | Alpha async (tokio/tonic) client: versions, full building-block matrix, app-callback server, key types | 2026-06-11 |
| [Dapr × Wasm Prior Art](dapr/dapr-wasm-prior-art.md) | Existing Dapr+Wasm work (http-wasm middleware, DeisLabs, WasmEdge) — none uses WIT; space is open | 2026-06-11 |
| [dapr-wasm-components Architecture](dapr/dapr-wasm-components-architecture.md) | This repo's design: sync WIT + async host, backend trait, workspace split, OCI publishing, exclusions | 2026-06-11 |

## wasm-tooling

Tooling around building, packaging, and distributing Wasm components.

| Article | Summary | Updated |
|---------|---------|---------|
| [wasm-pkg-tools (wkg)](wasm-tooling/wasm-pkg-tools-wkg.md) | wkg CLI: config, OCI naming, wkg.toml/wkg.lock, publish commands, ghcr.io auth | 2026-06-11 |
| [Wasmtime Host Embedding](wasm-tooling/wasmtime-host-embedding.md) | bindgen! flag maps, async_support, sync-WIT-with-async-host pattern, wit-bindgen guest side | 2026-06-11 |
