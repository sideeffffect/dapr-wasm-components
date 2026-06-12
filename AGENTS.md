# AGENTS.md

Dapr building blocks as WebAssembly component (WIT) interfaces, implemented by two pure-wasm provider components: `wasi-http` (talks to the sidecar's HTTP API; runs on any WASI 0.2 runtime) and `wasi-grpc` (talks to the sidecar's gRPC API via tonic over wasi:http outgoing HTTP/2; requires Spin >= 3.4 with `SPIN_OUTBOUND_H2C_PRIOR_KNOWLEDGE`). No native host, no Dapr Rust SDK. See [README.md](README.md) for the full picture.

## llm-wiki

Use the karpathy-llm-wiki SKILLs to interact with the llm-wiki in this repository (`wiki/` and `raw/`, start at [wiki/index.md](wiki/index.md)).
Learn from this wiki and save all what you learn continually during each session.
Architectural decisions and their rationale live in [wiki/dapr/dapr-wasm-components-architecture.md](wiki/dapr/dapr-wasm-components-architecture.md) — read it before changing interfaces or the implementation.

## Layout

- **`components/` holds only the three published things** (interface + two providers); everything that exists to test them is under `e2e/`. Three Cargo workspaces in total, two of them wasm-only — keep them separate (guest crates don't build natively).
- `components/wit/` — the `dapr-wasm-components:interfaces` WIT package (sync functions only; WASI 0.2; published to OCI as `dapr-wasm-components-interface`). Worlds: `dapr-client` (what an app imports) and `dapr-server` (what a provider exports).
- `components/wasi-http/` — the portable implementation component (published as `dapr-wasm-components-wasi-http`); `components/wasi-grpc/` — the gRPC implementation (published as `dapr-wasm-components-wasi-grpc`; vendored Dapr v1.18 protos in `proto/`, checked-in tonic codegen in `src/proto/` — regen instructions in `proto/README.md`). The two providers form the **`components/` workspace** (wasm-only).
- `e2e/` — native test harness (root workspace): mock Dapr HTTP sidecar (axum) + wasmtime + wac-graph composition, plus two real-Dapr E2Es (ignored by default) that run the **same shared scenario** (`tests/common/run_mirrored_scenario`) and differ only in provider + runtime: `tests/dapr.rs` (wasi-http, `wasmtime serve`) and `tests/spin.rs` (wasi-grpc, `spin up`). Both run the `microservice` app as two instances (publisher + consumer) through two actual daprd sidecars with Redis pub/sub + sqlite name resolution. Shared scaffolding lives in `e2e/tests/common/`.
- `e2e/apps/` — the demo/fixture app components (a **separate wasm-only workspace**, never published): `kv-demo` (the `wasi:cli` command used by the mock composition test) and `microservice` (the real-Dapr E2E app, run as two instances by both suites).
- `.github/workflows/ci.yml` — checks + `wkg oci push` of the modules to ghcr.io (`latest` on main, semver on GitHub releases; release tag must match the WIT package version).

## Checks (run after every change, in this order)

```sh
cargo fmt --all && cargo fmt --all --manifest-path components/Cargo.toml && cargo fmt --all --manifest-path e2e/apps/Cargo.toml
wasm-tools component wit components/wit/                                   # WIT still valid
cargo build --release --target wasm32-wasip2 --manifest-path components/Cargo.toml   # providers
cargo build --release --target wasm32-wasip2 --manifest-path e2e/apps/Cargo.toml     # demo apps
cargo clippy --all-targets -- -D warnings
cargo clippy --target wasm32-wasip2 --manifest-path components/Cargo.toml -- -D warnings
cargo clippy --target wasm32-wasip2 --manifest-path e2e/apps/Cargo.toml -- -D warnings
cargo test                                                                 # e2e: provider + composed kv-demo vs mock sidecar
# real-Dapr E2E (needs Docker + wasmtime CLI — daprd sidecars and Redis come from testcontainers):
cargo test --test dapr -- --ignored
# wasi-grpc E2E (needs Docker + spin CLI >= 3.4, override with SPIN_BIN):
cargo test --test spin -- --ignored
```

## Conventions

- WIT stays **sync** (no `async` functions, no `stream`/`future`) and outbound-only (the `dapr-client`/`dapr-server` worlds); inbound flows go through the app's `wasi:http/incoming-handler`, independent of which provider is composed in. Interface changes must be mirrored in: **both** implementations (wasi-http and wasi-grpc), the e2e mock + tests, kv-demo if relevant, and the README interface table. Bump the package version in `components/wit/types.wit` (CI checks it against release tags).
- The wasi-http implementation maps the Dapr **HTTP API** exactly — verify request/response shapes against https://docs.dapr.io/reference/api/ (captured in [wiki/dapr/dapr-http-api.md](wiki/dapr/dapr-http-api.md)), not against the gRPC SDKs. The wasi-grpc implementation maps `service Dapr` from the vendored protos — the checked-in generated code in `components/wasi-grpc/src/proto/` is the ground truth for shapes.
- wasi-http: HTTP client is `wstd` (blocking via `block_on` over wasi:http); JSON via serde_json; values that aren't valid JSON are sent as JSON strings (UTF-8 lossy). wasi-grpc: tonic generated client over `wasi-grpc`/`wasi-hyperium` (blocking via `spin_executor::run`); values are raw protobuf bytes (byte-exact).
- Diagrams and charts in markdown must be [Mermaid](https://mermaid.js.org/) (` ```mermaid ` blocks) — never ASCII art or manually drawn box diagrams.
