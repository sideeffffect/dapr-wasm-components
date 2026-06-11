# AGENTS.md

Dapr building blocks exposed as WebAssembly component (WIT) interfaces, plus a wasmtime host bridging them to the Dapr Rust SDK. See [README.md](README.md) for the full picture.

## llm-wiki

Use the karpathy-llm-wiki SKILLs to interact with the llm-wiki in this repository (`wiki/` and `raw/`, start at [wiki/index.md](wiki/index.md)).
Learn from this wiki and save all what you learn continually during each session.
Architectural decisions and their rationale live in [wiki/dapr/dapr-wasm-components-architecture.md](wiki/dapr/dapr-wasm-components-architecture.md) — read it before changing interfaces or the host.

## Layout

- `wit/` — the `dapr:client` WIT package (sync functions only; WASI 0.2 target).
- `host/` — `dapr-wasm-host`: wasmtime host, async host functions behind the sync WIT imports; `DaprBackend` trait with `sidecar` (Dapr SDK) and `memory` (tests) impls.
- `examples/` — guest components; a **separate cargo workspace** (guest cdylibs don't build for native targets — keep it that way).
- `.github/workflows/ci.yml` — checks + wkg publish of both modules to ghcr.io.

## Checks (run after every change, in this order)

```sh
cargo fmt --all && cargo fmt --all --manifest-path examples/Cargo.toml
wasm-tools component wit wit/                                              # WIT still valid
cargo build --release --target wasm32-wasip2 --manifest-path examples/Cargo.toml
cargo clippy --all-targets -- -D warnings
cargo clippy --target wasm32-wasip2 --manifest-path examples/Cargo.toml -- -D warnings
cargo test                                                                 # e2e: runs kv-demo against the memory backend
```

## Conventions

- WIT changes must stay sync (no `async` functions, no `stream`/`future`) and must be mirrored in: host `Host` impls + `DaprBackend` trait + both backends, the kv-demo example, and the README interface table. Bump the package version in `wit/types.wit` (CI derives the OCI tag from it).
- Only expose what the Dapr Rust SDK actually implements — check the wiki's [Dapr Rust SDK](wiki/dapr/dapr-rust-sdk.md) support matrix first.
- The `dapr` crate is renamed to `dapr-sdk` in `host/Cargo.toml` to avoid clashing with the bindgen-generated `dapr` module; keep `tonic`/`prost-types` versions in lockstep with the SDK's.
- Diagrams and charts in markdown must be [Mermaid](https://mermaid.js.org/) (` ```mermaid ` blocks) — never ASCII art or manually drawn box diagrams.
