# dapr-wasm-components — Architecture & Decisions

> Sources: this repository (wit/, components/, e2e/, .github/), 2026-06-11 (v2 redesign, same day as v1)

## Overview

Two pure-wasm modules: the **interface** WIT package `dapr-wasm-components:interfaces` (`wit/`, published to OCI as `dapr-wasm-components-interface`) and the **implementation** component `dapr-wasm-components-wasi-http` (`components/wasi-http/`), which exports every interface and implements them by calling the Dapr sidecar's HTTP API via `wasi:http` outgoing requests. Apps import the interfaces, get composed with the implementation (`wac plug`), and run on any WASI 0.2 runtime with wasi:http — next to a Dapr sidecar.

## v1 → v2: why the redesign

v1 (same day) used a native wasmtime host bridging to the Dapr Rust SDK. v2 replaced it on request: the implementation must be pure wasm so *anyone* can compose against it without a custom host. The Rust SDK (tokio/tonic gRPC) cannot compile to wasm32-wasip2, so it was dropped entirely — the HTTP protocol is implemented directly. Bonus: no longer limited to what the SDK supports; the **whole** outbound API surface including alpha (lock, crypto, state query/transactions, jobs, conversation, workflow management, actors client) is covered.

## Key decisions

- **Sync WIT, blocking wasi:http.** Interfaces stay synchronous (user preference, WASI 0.2). The implementation uses `wstd`'s HTTP client with `wstd::runtime::block_on` inside sync exports — wasi:http polling blocks the guest, no async ABI needed.
- **WIT package name is `dapr-wasm-components:interfaces`** — `interface` (singular) is a reserved WIT keyword and cannot be a package name. OCI artifact names are exact (`dapr-wasm-components-interface`, `dapr-wasm-components-wasi-http`) because publishing uses `wkg oci push` with explicit references (also required for the `latest` tag — `wkg publish` only accepts semver tags).
- **Outbound only.** Inbound flows (pub/sub delivery, input bindings, job triggers, actor hosting, config watches) arrive on the app's HTTP app channel → apps export `wasi:http/incoming-handler`. No callback interfaces in the WIT.
- **`provider` world exports `types` explicitly** so composition leaves no dangling `types` import in the composed component.
- **HTTP-faithful interface shapes**: invocation exposes verb/headers/status passthrough; state has transactions and query (possible over HTTP, impossible via the Rust SDK); errors map HTTP status classes to the `error` variant (400 invalid-argument, 401/403 permission-denied, 404 not-found, 409/412 aborted, 5xx internal, transport unavailable).
- **JSON value convention**: HTTP state/bindings APIs carry values as JSON. Bytes that parse as JSON are embedded as-is; otherwise sent as a JSON string (UTF-8 lossy). Response strings are returned unquoted so text roundtrips.
- **Config via env** (read inside the component through `wasi:cli/environment`): `DAPR_HTTP_ENDPOINT` → `DAPR_HTTP_PORT` → `http://127.0.0.1:3500`; `DAPR_API_TOKEN` → `dapr-api-token` header.
- **Workspace split**: root native workspace holds `e2e/` only; `components/` is a separate wasm-only workspace (guest crates don't build natively).
- **Testing without Dapr**: `e2e/` runs the real provider in wasmtime (wasmtime-wasi-http supplies real outgoing HTTP) against a mock axum sidecar, asserting recorded requests; plus a composition test that plugs kv-demo into the provider with **wac-graph** (programmatic `wac plug`) and runs the composed command via `wasmtime_wasi::p2::bindings::Command`.
- **Testing with real Dapr** (`e2e/tests/dapr.rs`, `#[ignore]`d, CI job `dapr-e2e`): two wasm microservices through two actual daprd 1.18 sidecars — `checkout` (command) publishes to Redis pub/sub; `order-processor` (wstd `#[wstd::http_server]` component served by `wasmtime serve -S cli --env ...`) receives deliveries on its app channel, declares programmatic subscriptions via `/dapr/subscribe`, CAS-increments a state counter (etag + first-write), and answers `summary` service invocations. Cross-sidecar name resolution: the **sqlite** nameresolution component (shared db file via daprd `--config`) — deterministic in CI, unlike mDNS. State: `state.in-memory` per sidecar. daprd quirks handled: per-sidecar `--dapr-grpc-port`/`--dapr-internal-grpc-port`, `--enable-metrics=false` (default metrics port would clash between two daprds). **Trap**: spawning daprd with piped stdio and not draining the pipes deadlocks daprd mid-startup once the pipe buffer fills — drain with reader threads. All infrastructure comes from **testcontainers**: Redis (`testcontainers-modules` redis + blocking feature) and both daprd sidecars (`GenericImage` `daprio/daprd:<tag>` — no entrypoint, binary at `/daprd`, runs as user 65532). Containerized daprd works with `.with_network("host")` (so it can dial the host-side `wasmtime serve` app channel and our test can reach its API ports) plus a bind mount of the shared dir (resources, config, sqlite nameres db) — the dir must be chmod 0777 for the non-root container user, and the sqlite connectionString must use the **container** path. wasmtime stays a host CLI (it runs the wasm under test). Future testcontainers candidates: `daprio/placement` (would unlock actors E2E) and `daprio/scheduler` (jobs triggers), both needing more app-channel protocol in the wasm services. **Trap**: the `wasmtime.dev/install.sh` script can fail resolving "latest" *and* exit 0 — pin the version and download the release tarball directly in CI.
- **OCI publishing**: `wkg oci push ghcr.io/sideeffffect/<module>:<tag>` with `org.opencontainers.image.source` annotation for repo linking. Tag `latest` on pushes to main; on GitHub releases the tag is the release version, and CI fails if it doesn't match the WIT package version in `wit/types.wit`.

## Known limitations

- Crypto is one-shot (no streaming); conversation is the alpha2 text subset (no tool calls); configuration subscribe not exposed.
- Alpha Dapr APIs can break between Dapr releases; the version-prefix map lives in [Dapr HTTP API](dapr-http-api.md).
- Binary (non-JSON) state values don't roundtrip byte-exact (JSON-string encoding).

## See Also

- [Dapr HTTP API](dapr-http-api.md)
- [Wasmtime Host Embedding](../wasm-tooling/wasmtime-host-embedding.md)
- [wasm-pkg-tools (wkg)](../wasm-tooling/wasm-pkg-tools-wkg.md)
- [Dapr Rust SDK](dapr-rust-sdk.md)
- [Dapr × Wasm Prior Art](dapr-wasm-prior-art.md)
