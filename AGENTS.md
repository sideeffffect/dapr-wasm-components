# AGENTS.md

Dapr building blocks as WebAssembly component (WIT) interfaces, implemented by a pure-wasm `wasi:http` component that talks to the Dapr sidecar's HTTP API. No native host, no Dapr Rust SDK. See [README.md](README.md) for the full picture.

## llm-wiki

Use the karpathy-llm-wiki SKILLs to interact with the llm-wiki in this repository (`wiki/` and `raw/`, start at [wiki/index.md](wiki/index.md)).
Learn from this wiki and save all what you learn continually during each session.
Architectural decisions and their rationale live in [wiki/dapr/dapr-wasm-components-architecture.md](wiki/dapr/dapr-wasm-components-architecture.md) — read it before changing interfaces or the implementation.

## Layout

- `wit/` — the `dapr-wasm-components:interfaces` WIT package (sync functions only; WASI 0.2; published to OCI as `dapr-wasm-components-interface`).
- `components/wasi-http/` — the implementation component (published as `dapr-wasm-components-wasi-http`); `components/kv-demo/` — example app. A **separate cargo workspace** (wasm-only crates — keep it that way).
- `e2e/` — native test harness (root workspace): mock Dapr HTTP sidecar (axum) + wasmtime + wac-graph composition, plus the real-Dapr E2E (`tests/dapr.rs`, ignored by default) orchestrating `components/order-processor` + `components/checkout` through two actual daprd sidecars with Redis pub/sub.
- `.github/workflows/ci.yml` — checks + `wkg oci push` of both modules to ghcr.io (`latest` on main, semver on GitHub releases; release tag must match the WIT package version).

## Checks (run after every change, in this order)

```sh
cargo fmt --all && cargo fmt --all --manifest-path components/Cargo.toml
wasm-tools component wit wit/                                              # WIT still valid
cargo build --release --target wasm32-wasip2 --manifest-path components/Cargo.toml
cargo clippy --all-targets -- -D warnings
cargo clippy --target wasm32-wasip2 --manifest-path components/Cargo.toml -- -D warnings
cargo test                                                                 # e2e: provider + composed kv-demo vs mock sidecar
# real-Dapr E2E (needs Docker + wasmtime CLI — daprd sidecars and Redis come from testcontainers):
cargo test --test dapr -- --ignored
```

## Conventions

- WIT stays **sync** (no `async` functions, no `stream`/`future`) and outbound-only; inbound flows go through the app's `wasi:http/incoming-handler`. Interface changes must be mirrored in: the wasi-http implementation, the e2e mock + tests, kv-demo if relevant, and the README interface table. Bump the package version in `wit/types.wit` (CI checks it against release tags).
- The implementation maps the Dapr **HTTP API** exactly — verify request/response shapes against https://docs.dapr.io/reference/api/ (captured in [wiki/dapr/dapr-http-api.md](wiki/dapr/dapr-http-api.md)), not against the gRPC SDKs.
- HTTP client is `wstd` (blocking via `block_on` over wasi:http); JSON via serde_json; values that aren't valid JSON are sent as JSON strings (UTF-8 lossy).
- Diagrams and charts in markdown must be [Mermaid](https://mermaid.js.org/) (` ```mermaid ` blocks) — never ASCII art or manually drawn box diagrams.
