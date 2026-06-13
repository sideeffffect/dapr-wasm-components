# AGENTS.md

Dapr building blocks as WebAssembly component (WIT) interfaces, implemented by two pure-wasm provider components: `wasi-http` (talks to the sidecar's HTTP API; runs on any WASI 0.2 runtime) and `wasi-grpc` (talks to the sidecar's gRPC API via tonic over wasi:http outgoing HTTP/2; requires Spin >= 3.4 with `SPIN_OUTBOUND_H2C_PRIOR_KNOWLEDGE`). No native host, no Dapr Rust SDK. **Both directions are typed** (since `@0.2.0`): a provider is a two-way adapter — apps *import* the building blocks (outbound) and *export* the `*-callback` interfaces (inbound), and never touch the wire. Apps use the `dapr-app` SDK (`app-sdk/`). See [README.md](README.md) and [wiki/dapr/dapr-wasm-components-inbound-design.md](wiki/dapr/dapr-wasm-components-inbound-design.md) for the full picture.

## llm-wiki

Use the karpathy-llm-wiki SKILLs to interact with the llm-wiki in this repository (`wiki/` and `raw/`, start at [wiki/index.md](wiki/index.md)).
Learn from this wiki and save all what you learn continually during each session.
Architectural decisions and their rationale live in [wiki/dapr/dapr-wasm-components-architecture.md](wiki/dapr/dapr-wasm-components-architecture.md) — read it before changing interfaces or the implementation.

## Layout

- **`components/` holds only the three published things** (interface + two providers); everything that exists to test them is under `e2e/`. Three Cargo workspaces in total, two of them wasm-only — keep them separate (guest crates don't build natively).
- `components/wit/` — the `dapr-wasm-components:interfaces` WIT package (sync functions only; WASI 0.2; published to OCI as `dapr-wasm-components-interface`). Worlds: `dapr-client` (what an app imports) and `dapr-server` (what a provider exports).
- `components/wasi-http/` — the portable implementation component (published as `dapr-wasm-components-wasi-http`); `components/wasi-grpc/` — the gRPC implementation (published as `dapr-wasm-components-wasi-grpc`; vendored Dapr v1.18 protos in `proto/`, checked-in tonic codegen in `src/proto/` — regen instructions in `proto/README.md`). The two providers form the **`components/` workspace** (wasm-only).
- `e2e/` — native test harness (root workspace): mock Dapr HTTP sidecar (axum) + wasmtime + wac-graph composition, plus two real-Dapr E2Es (ignored by default): `tests/dapr.rs` orchestrating `e2e/apps/order-processor` + `e2e/apps/checkout` through two actual daprd sidecars with Redis pub/sub, and `tests/spin.rs` running `e2e/apps/spin-demo` + the wasi-grpc provider under the Spin CLI against a daprd's gRPC API. Shared E2E scaffolding lives in `e2e/tests/common/`.
- `e2e/apps/` — the demo/fixture app components the E2Es compose and drive (`kv-demo`, `order-processor`, `checkout`, `spin-demo`): a **separate wasm-only workspace**, never published.
- `.github/workflows/ci.yml` — checks + `wkg oci push` of the modules to ghcr.io (`latest` on main, semver on GitHub releases; release tag must match the WIT package version).

## Checks (run after every change, in this order)

```sh
cargo fmt --all && cargo fmt --all --manifest-path components/Cargo.toml && cargo fmt --all --manifest-path app-sdk/Cargo.toml && cargo fmt --all --manifest-path e2e/apps/Cargo.toml
wasm-tools component wit components/wit/                                   # WIT still valid
cargo build --release --target wasm32-wasip2 --manifest-path components/Cargo.toml   # providers
cargo build --release --target wasm32-wasip2 --manifest-path app-sdk/Cargo.toml      # app SDK
cargo build --release --target wasm32-wasip2 --manifest-path e2e/apps/Cargo.toml     # demo apps
cargo clippy --all-targets -- -D warnings
cargo clippy --target wasm32-wasip2 --manifest-path components/Cargo.toml -- -D warnings
cargo clippy --target wasm32-wasip2 --manifest-path app-sdk/Cargo.toml -- -D warnings
cargo clippy --target wasm32-wasip2 --manifest-path e2e/apps/Cargo.toml -- -D warnings
cargo test                                                                 # e2e: provider + composed kv-demo vs mock sidecar
# real-Dapr E2E (needs Docker + wasmtime CLI — daprd sidecars and Redis come from testcontainers):
cargo test --test dapr -- --ignored
# wasi-grpc E2E (needs Docker + spin CLI >= 3.4, override with SPIN_BIN):
cargo test --test spin -- --ignored
```

## Conventions

- WIT stays **sync** (no `async` functions, no `stream`/`future`). Two worlds: **`app`** (imports building blocks, exports `*-callback`) and **`dapr`** (exports building blocks, imports `*-callback`); inbound is now typed through the providers, not the app's own `wasi:http/incoming-handler`. Interface changes must be mirrored in: **both** providers (wasi-http and wasi-grpc), the **`dapr-app` SDK** (`app-sdk/`), the e2e mock + tests, the demo apps, and the README interface tables. Bump the package version in `components/wit/types.wit` (CI checks it against release tags).
- The wasi-http implementation maps the Dapr **HTTP API** exactly — verify request/response shapes against https://docs.dapr.io/reference/api/ (captured in [wiki/dapr/dapr-http-api.md](wiki/dapr/dapr-http-api.md)), not against the gRPC SDKs. The wasi-grpc implementation maps `service Dapr` from the vendored protos — the checked-in generated code in `components/wasi-grpc/src/proto/` is the ground truth for shapes.
- wasi-http: HTTP client is `wstd` (blocking via `block_on` over wasi:http); JSON via serde_json; values that aren't valid JSON are sent as JSON strings (UTF-8 lossy). wasi-grpc: tonic generated client over `wasi-grpc`/`wasi-hyperium` (blocking via `spin_executor::run`); values are raw protobuf bytes (byte-exact).
- Diagrams and charts in markdown must be [Mermaid](https://mermaid.js.org/) (` ```mermaid ` blocks) — never ASCII art or manually drawn box diagrams.
