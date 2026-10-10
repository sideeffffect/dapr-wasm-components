# Knowledge Base Index

## wasm-component-model

The WebAssembly Component Model specification: WIT IDL, worlds, canonical ABI, async.

| Article | Summary | Updated |
|---------|---------|---------|
| [Component Model Overview](wasm-component-model/component-model-overview.md) | What the component model is, spec repo layout, WASI 0.2 vs 0.3 status, why it fits provider composition | 2026-10-10 |
| [WIT Format](wasm-component-model/wit-format.md) | WIT packages/interfaces/worlds, type system, sync vs async functions, filesystem conventions | 2026-06-11 |

## dapr

Dapr distributed application runtime: HTTP API, Rust SDK, and Wasm integration.

| Article | Summary | Updated |
|---------|---------|---------|
| [Dapr HTTP API](dapr/dapr-http-api.md) | Sidecar REST API: version-prefix map (stable/alpha1/alpha2), field-naming traps, crypto buffering/limits (v1.18 source) | 2026-10-10 |
| [dapr-wasm-components Architecture](dapr/dapr-wasm-components-architecture.md) | Interface package `@0.6.0` + four split pure-wasm providers (http/grpc × outbound/inbound), `dapr-app` SDK, composition, error model, streaming crypto, OCI publishing (5 packages), real-Dapr E2Es | 2026-10-10 |
| [dapr-wasm-components Inbound Design](dapr/dapr-wasm-components-inbound-design.md) | Design (implemented): Dapr→app inbound as typed `*-callback` WIT (all six flows + health), app never touches the wire; `app`/`outbound`/`inbound` worlds, wasi-http + wasi-grpc inbound providers | 2026-10-10 |
| [Dapr Rust SDK](dapr/dapr-rust-sdk.md) | Alpha async (tokio/tonic) client: versions, building-block matrix, app-callback server (no longer used by this project) | 2026-06-11 |
| [gRPC to Dapr from Wasm](dapr/dapr-grpc-from-wasm.md) | The HTTP/2-from-wasm crux, transport options, why the wasi-grpc crate is Spin-locked — the shipped Spin-only PoC outcome, and the 2026-10 landscape (wasmCloud v2 host-side gRPC, tokio wasip2 net) | 2026-10-10 |
| [Dapr × Wasm Prior Art](dapr/dapr-wasm-prior-art.md) | Existing Dapr+Wasm work (http-wasm middleware, DeisLabs, WasmEdge) — none uses WIT; space is open | 2026-06-11 |

## wasm-tooling

Tooling around building, packaging, and distributing Wasm components.

| Article | Summary | Updated |
|---------|---------|---------|
| [wasm-pkg-tools (wkg)](wasm-tooling/wasm-pkg-tools-wkg.md) | wkg CLI: config, OCI naming, wkg.toml/wkg.lock, publish vs oci push, ghcr.io auth, current CI usage | 2026-10-10 |
| [Wasmtime Host Embedding](wasm-tooling/wasmtime-host-embedding.md) | bindgen! flag maps, sync-WIT-with-async-host (v1), wasmtime-wasi/-http view traits, wit-bindgen guest side; now e2e-harness-only | 2026-10-10 |
