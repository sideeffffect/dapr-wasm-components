# WIT Format

> Sources: W3C WebAssembly Community Group, Unknown
> Raw: [wit-design-doc](../../raw/wasm-component-model/wit-design-doc.md)

## Overview

WIT (Wasm Interface Type) is the IDL of the component model. A WIT **package** is a collection of **interfaces** (named bags of types and functions) and **worlds** (a component type: what a component imports and exports), identified as `namespace:name@semver` (e.g. `dapr:client@0.1.0`). Files use `.wit` extension; a package can span multiple files in one directory.

## Core constructs

- `package dapr:client@0.1.0;` — package declaration with semver.
- `interface state { ... }` — functions + type definitions.
- `world component { import state; export run; }` — a component's contract.
- `use other-interface.{type-a, type-b};` — cross-interface type reuse; top-level `use wasi:io/streams@0.2.0;` for cross-package.
- Types: `record` (struct), `variant` (sum), `enum`, `flags`, `option<T>`, `result<T, E>`, `list<T>`, `tuple<...>`, `string`, `bool`, `u8…u64`, `s8…s64`, `f32/f64`, `resource` (handle with methods, constructor, statics).
- Identifiers are kebab-case; `%` prefix escapes keywords.
- Feature gates: `@since(version = ...)`, `@unstable(feature = ...)`, `@deprecated(...)`.

## Functions and async

```wit
func-type ::= 'async'? 'func' param-list result-list
```

Functions are sync by default. The optional `async` prefix (WASI 0.3 / Preview 3 concurrency model) marks the callee as potentially blocking, switching to the async ABI and async source-language bindings. `future<T>` and `stream<T>` types exist for asynchronous values/sequences.

**Important nuance**: a *sync* WIT function only constrains the guest-visible ABI. A host embedding (e.g. wasmtime with `Config::async_support(true)` and `bindgen!` async host traits) may implement a sync-WIT import with an async Rust function; the guest simply blocks until the host future resolves. This is the standard way to keep guest interfaces sync while the host uses tokio-based clients.

## Filesystem conventions

- A project keeps its WIT in a `wit/` directory; dependencies go to `wit/deps/<package>/`.
- `wkg wit fetch`/`wkg wit build` resolve and vendor dependencies (see [wasm-pkg-tools](../wasm-tooling/wasm-pkg-tools-wkg.md)).

## See Also

- [Component Model Overview](component-model-overview.md)
- [wasm-pkg-tools (wkg)](../wasm-tooling/wasm-pkg-tools-wkg.md)
- [Wasmtime Host Embedding](../wasm-tooling/wasmtime-host-embedding.md)
