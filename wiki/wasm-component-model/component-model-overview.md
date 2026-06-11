# WebAssembly Component Model — Overview

> Sources: W3C WebAssembly Community Group / Bytecode Alliance, Unknown
> Raw: [component-model-readme](../../raw/wasm-component-model/component-model-readme.md); [wit-design-doc](../../raw/wasm-component-model/wit-design-doc.md)

## Overview

The Component Model is the standardization effort (W3C WebAssembly CG, repo `WebAssembly/component-model`) that layers a language-agnostic composition and interface system on top of core WebAssembly. Components describe their imports and exports in a typed IDL called WIT; the Canonical ABI defines how those high-level types lower to core wasm. The model was stabilized incrementally through WASI Preview 2 (a.k.a. WASI 0.2); the WASI Preview 3 / 0.3 milestone adds the concurrency model — native `async` functions, `future<T>` and `stream<T>` types.

## Key parts of the spec repo

- **WIT** (`design/mvp/WIT.md`) — the IDL: packages, interfaces, worlds.
- **Explainer / Binary format** — text and binary encodings of components.
- **Concurrency model** (`design/mvp/Concurrency.md`) — async ABI (Preview 3).
- **Canonical ABI** — lifting/lowering between WIT types and core wasm.

User-facing documentation lives at <https://component-model.bytecodealliance.org/>.

## Why it matters for this project

Dapr building blocks (state, pub/sub, secrets, …) are natural WIT interfaces: a guest component imports `dapr:*` interfaces, and a host (wasmtime embedding) implements them by delegating to the Dapr sidecar via the Dapr Rust SDK. Sync WIT functions can be backed by async host implementations in wasmtime, so the guest-facing API can stay sync (the user's preference) even though the Rust SDK is tokio-based.

## See Also

- [WIT Format](wit-format.md)
