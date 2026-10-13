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
| [Dapr HTTP API](dapr/dapr-http-api.md) | Sidecar REST API: version-prefix map (stable/alpha1/alpha2), field-naming traps, input-binding reply ignored by daprd, crypto buffering/limits (v1.18 source), workflow get response keys/status strings | 2026-10-12 |
| [dapr-wasm-components Architecture](dapr/dapr-wasm-components-architecture.md) | Interface package `@0.7.1` + six split pure-wasm providers (http/grpc/go × outbound/inbound), `dapr-app` SDK, composition, error model, streaming crypto, 0.7.0 authority split (`<x>-read`/`<x>`, import list = authority manifest), subscription resource, typed workflow-instance, OCI publishing (5 packages), real-Dapr E2Es | 2026-10-12 |
| [dapr-wasm-components Inbound Design](dapr/dapr-wasm-components-inbound-design.md) | Design (implemented): Dapr→app inbound as typed `*-callback` WIT (all six flows + health), app never touches the wire; `app`/`outbound`/`inbound` worlds, wasi-http + wasi-grpc inbound providers; what daprd really does with a binding reply; pub/sub CEL routing rules (0.6.0); bulk subscribe (0.6.0); config subscription resource (0.7.0) | 2026-10-12 |
| [dapr-wasm-components go Family](dapr/dapr-wasm-components-go-family.md) | Dapr itself (github.com/dapr/dapr) embedded via componentize-go: pull-based inbound pump (acyclic, one runtime), wasi:sockets net overlay, in-process API + metadata interceptor, GC-in-cabi_realloc netpoll patch, curated wasip1 components, single-instance limits (self-invocation fails fast), Consul NR | 2026-10-12 |
| [Dapr Rust SDK](dapr/dapr-rust-sdk.md) | Alpha async (tokio/tonic) client: versions, building-block matrix, app-callback server (no longer used by this project) | 2026-06-11 |
| [gRPC to Dapr from Wasm](dapr/dapr-grpc-from-wasm.md) | The HTTP/2-from-wasm crux, transport options, why the wasi-grpc crate is Spin-locked — the shipped Spin-only PoC outcome, and the 2026-10 landscape (wasmCloud v2 host-side gRPC, tokio wasip2 net) | 2026-10-10 |
| [Dapr × Wasm Prior Art](dapr/dapr-wasm-prior-art.md) | Existing Dapr+Wasm work (http-wasm middleware, DeisLabs, WasmEdge) — none uses WIT; space is open | 2026-06-11 |

## wasm-tooling

Tooling around building, packaging, and distributing Wasm components.

| Article | Summary | Updated |
|---------|---------|---------|
| [wasm-pkg-tools (wkg)](wasm-tooling/wasm-pkg-tools-wkg.md) | wkg CLI: config, OCI naming, wkg.toml/wkg.lock, publish vs oci push, ghcr.io auth, current CI usage | 2026-10-10 |
| [Wasmtime Host Embedding](wasm-tooling/wasmtime-host-embedding.md) | bindgen! flag maps, sync-WIT-with-async-host (v1), wasmtime-wasi/-http view traits, wit-bindgen guest side; now e2e-harness-only | 2026-10-10 |
