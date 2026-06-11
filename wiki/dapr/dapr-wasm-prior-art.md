# Dapr × WebAssembly — Prior Art

> Sources: Dapr docs, DeisLabs, Second State, dapr/dapr issues; 2026-06-11 survey
> Raw: [component-ecosystem-survey-2026-06](../../raw/wasm-tooling/component-ecosystem-survey-2026-06.md)

## Overview

As of June 2026, **no shipped project exposes Dapr building blocks (state, pub/sub, bindings…) as WASI-0.2 WIT interfaces to component guests** — the space dapr-wasm-components targets is open. Existing Dapr×Wasm work predates or sidesteps the component model.

## Existing work

- **Dapr Wasm HTTP middleware** (`middleware.http.wasm`) — production, in-tree: runs wasm in daprd's HTTP pipeline via the http-wasm Handler ABI on embedded wazero. Core-module ABI, not WIT/components.
- **deislabs/dapr-wasm-exp** — DeisLabs experiment compiling Dapr capabilities (state store plugin) to wasm via the Go plugin system; contemplated the component model but never implemented it. Archived 2025-07-14.
- **second-state/dapr-wasm** — WasmEdge functions as Dapr sidecar microservices; pre-component-model.
- **dapr/dapr#3496, #5619** — proposals for wasm state/pubsub/actors; deferred (WASI immaturity); pluggable components went gRPC/Unix-socket instead.

## Design vocabulary worth borrowing

wasmCloud's capability model over WIT: `wasi:keyvalue`, `wasi:messaging`, `wasi:http` — the closest adjacent art for shaping `dapr:*` interfaces.

## See Also

- [Dapr Rust SDK](dapr-rust-sdk.md)
- [Component Model Overview](../wasm-component-model/component-model-overview.md)
