# AGENTS.md

Dapr building blocks as WebAssembly component (WIT) interfaces, implemented by pure-wasm provider components. **Both directions are typed** (since `@0.2.0`): apps *import* the building blocks (outbound) and *export* the `*-callback` interfaces (inbound), and never touch the wire — providers translate it. No native host, no Dapr Rust SDK. Per transport the provider is **split into an outbound and an inbound component** so the composition graph `outbound → app → inbound` stays acyclic (a single bidirectional provider forms an illegal app↔provider cycle): `wasi-http-outbound`/`wasi-grpc-outbound` talk to the sidecar's HTTP/gRPC API; `wasi-http-inbound` exports `wasi:http/incoming-handler` and dispatches the HTTP app channel to the callbacks; `wasi-grpc-inbound` serves Dapr's `AppCallback` gRPC service over `wasi:http/incoming-handler` (Spin inbound h2c). wasi-grpc needs Spin ≥ 3.4 with `SPIN_OUTBOUND_H2C_PRIOR_KNOWLEDGE`. Apps use the `dapr-app` SDK (`app-sdk/`). See [README.md](README.md) and [wiki/dapr/dapr-wasm-components-inbound-design.md](wiki/dapr/dapr-wasm-components-inbound-design.md) for the full picture.

## llm-wiki

Use the karpathy-llm-wiki SKILLs to interact with the llm-wiki in this repository (`wiki/` and `raw/`, start at [wiki/index.md](wiki/index.md)).
Learn from this wiki and save all what you learn continually during each session.
**Continually lint the llm-wiki** (the karpathy-llm-wiki lint flow): keep the index, internal links, raw references, and See Also cross-references consistent, and surface factual drift — especially after ingesting new sources or restructuring.
Architectural decisions and their rationale live in [wiki/dapr/dapr-wasm-components-architecture.md](wiki/dapr/dapr-wasm-components-architecture.md) — read it before changing interfaces or the implementation.

## Layout

- **`components/` holds only the published things** (interface + the provider components); everything that exists to test them is under `e2e/`. Four Cargo workspaces in total (`components/`, `app-sdk/`, `e2e/apps/` wasm-only; `e2e/` native) — keep them separate (guest crates don't build natively).
- `components/wit/` — the `dapr-wasm-components:interfaces` WIT package (sync functions only; WASI 0.2; published to OCI as `dapr-wasm-components-interfaces`). Worlds: `app` (what an app targets — imports building blocks, exports `*-callback`), `outbound` (a provider exporting the building blocks), `inbound` (a provider importing the callbacks).
- `components/wasi-http-outbound/` + `components/wasi-http-inbound/` — the portable provider (outbound HTTP API client; inbound `wasi:http/incoming-handler` router). `components/wasi-grpc-outbound/` — the gRPC outbound provider (vendored Dapr v1.18 protos in `proto/`, checked-in tonic codegen in `src/proto/` — regen instructions in `proto/README.md`); a gRPC inbound provider is not yet built. These form the **`components/` workspace** (wasm-only).
- `app-sdk/dapr-app/` — the app-side SDK (own wasm-only workspace): the `DaprApp` trait (defaulted callbacks) + `export_app!` + re-exported building-block imports.
- `e2e/` — native test harness (root workspace): mock Dapr HTTP sidecar (axum) + wasmtime + wac-graph composition, plus two real-Dapr E2Es (ignored by default) that run the **same shared scenario** (`tests/common/run_mirrored_scenario`) and differ only in provider + runtime: `tests/dapr.rs` (wasi-http, `wasmtime serve`) and `tests/spin.rs` (wasi-grpc, `spin up`). Both run the `microservice` app as two instances (publisher + consumer) through two actual daprd sidecars with Redis pub/sub + sqlite name resolution. Shared scaffolding lives in `e2e/tests/common/`.
- `e2e/apps/` — the demo/fixture app components (a **separate wasm-only workspace**, never published): `kv-demo` (the `wasi:cli` command used by the mock composition test) and `microservice` (the real-Dapr E2E app, run as two instances by both suites).
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

## Git

- **Always keep pushing to `main` — directly, without asking.** This is a solo, trunk-based repo: commit your finished work and push it straight to `origin/main` as you go. Don't ask for permission to commit or push, and don't open a PR or use a side branch — push directly to `main` even when working from a worktree branch (push your `HEAD` to `main`). Don't leave finished changes sitting uncommitted in a worktree. Rebase onto the latest `origin/main` before pushing. (Wiki/`raw` edits count too.)
- **Cut releases as GitHub releases, never bare git tags.** Use `gh release create` (which creates the underlying tag for you) so each release has notes and is what CI keys off — don't `git tag`/`git push --tags` by hand. The release tag must match the WIT package version (see [Conventions](#conventions)).

## Conventions

- WIT stays **sync** (no `async` functions, no `stream`/`future`). Worlds: **`app`** (imports building blocks, exports `*-callback`), **`outbound`** (exports building blocks) and **`inbound`** (imports `*-callback`); inbound is now typed through the inbound provider, not the app's own `wasi:http/incoming-handler`. Interface changes must be mirrored in: the relevant provider components (`wasi-http-outbound`/`wasi-http-inbound`/`wasi-grpc-outbound`), the **`dapr-app` SDK** (`app-sdk/`), the e2e mock + tests, the demo apps, and the README interface tables. Bump the package version in `components/wit/types.wit` (CI checks it against release tags).
- The wasi-http implementation maps the Dapr **HTTP API** exactly — verify request/response shapes against https://docs.dapr.io/reference/api/ (captured in [wiki/dapr/dapr-http-api.md](wiki/dapr/dapr-http-api.md)), not against the gRPC SDKs. The wasi-grpc implementation maps `service Dapr` from the vendored protos — the checked-in generated code in `components/wasi-grpc-outbound/src/proto/` is the ground truth for shapes.
- wasi-http: HTTP client is `wstd` (blocking via `block_on` over wasi:http); JSON via serde_json; values that aren't valid JSON are sent as JSON strings (UTF-8 lossy). wasi-grpc: tonic generated client over `wasi-grpc`/`wasi-hyperium` (blocking via `spin_executor::run`); values are raw protobuf bytes (byte-exact).
- Diagrams and charts in markdown must be [Mermaid](https://mermaid.js.org/) (` ```mermaid ` blocks) — never ASCII art or manually drawn box diagrams.
