# WebAssembly Component Model — Overview

> Sources: W3C WebAssembly Community Group / Bytecode Alliance, Unknown
> Raw: [component-model-readme](../../raw/wasm-component-model/component-model-readme.md); [wit-design-doc](../../raw/wasm-component-model/wit-design-doc.md)

## Overview

The Component Model is the standardization effort (W3C WebAssembly CG, repo `WebAssembly/component-model`) that layers a language-agnostic composition and interface system on top of core WebAssembly. Components describe their imports and exports in a typed IDL called WIT; the Canonical ABI defines how those high-level types lower to core wasm. The model was stabilized incrementally through WASI Preview 2 (a.k.a. WASI 0.2); WASI 0.3.0 (final released 2026-06-11) adds the concurrency model — native `async` functions, `future<T>` and `stream<T>` types. As of June 2026, WASI 0.2.x (latest 0.2.12) remains the production target: wasmtime 45.0.1 still tracks a 0.3 RC snapshot and cross-component async carries ~3.5x call overhead pre-Component-Model-1.0. (Update 2026-10-10: WASI 0.3.1 shipped 2026-08-11 and wasmtime 46+ enables P3 by default — see [Wasmtime Host Embedding](../wasm-tooling/wasmtime-host-embedding.md#wasi-02-vs-03-june-2026). This project still targets WASI 0.2.)

## Key parts of the spec repo

- **WIT** (`design/mvp/WIT.md`) — the IDL: packages, interfaces, worlds.
- **Explainer / Binary format** — text and binary encodings of components.
- **Concurrency model** (`design/mvp/Concurrency.md`) — async ABI (Preview 3).
- **Canonical ABI** — lifting/lowering between WIT types and core wasm.

User-facing documentation lives at <https://component-model.bytecodealliance.org/>.

## Why it matters for this project

Dapr building blocks (state, pub/sub, secrets, …) are natural WIT interfaces. The project's package `dapr-wasm-components:interfaces` (WASI 0.2, sync functions only) defines them. An app component imports the building blocks and exports `*-callback` interfaces. Pure-wasm provider components implement both sides: outbound providers export the building blocks, and inbound providers import the callbacks. Composition (`wac`) links them as `outbound → app → inbound`, so no custom host is needed. The providers keep the WIT sync by blocking inside the guest (`wstd` `block_on` or `spin_executor::run`).

The first design (v1, 2026-06-11) used a wasmtime host that implemented sync WIT imports with async Rust functions over the Dapr Rust SDK. It was replaced the same day; see [dapr-wasm-components Architecture](../dapr/dapr-wasm-components-architecture.md).

## See Also

- [WIT Format](wit-format.md)
- [Wasmtime Host Embedding](../wasm-tooling/wasmtime-host-embedding.md)
- [wasm-pkg-tools (wkg)](../wasm-tooling/wasm-pkg-tools-wkg.md)
- [dapr-wasm-components Architecture](../dapr/dapr-wasm-components-architecture.md)
