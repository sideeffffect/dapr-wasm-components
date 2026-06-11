# dapr-wasm-components — Architecture & Decisions

> Sources: this repository (wit/, host/, examples/, .github/), 2026-06-11

## Overview

This project exposes Dapr building blocks to WebAssembly components. Three parts: the `dapr:client@0.1.0` WIT package (`wit/`), the native `dapr-wasm-host` (wasmtime 45 + Dapr Rust SDK 0.17) that implements those interfaces (`host/`), and guest components like `examples/kv-demo` (wit-bindgen 0.58, `wasm32-wasip2`).

## Key decisions

- **Sync WIT, async host.** All WIT functions are sync (WASI 0.2 target). The host implements them with async Rust via `bindgen!({ imports: { default: async | trappable }, exports: { default: async } })`; the guest blocks while the host awaits the Dapr SDK future. WASI 0.3 async was deliberately avoided (preview-quality, ~3.5x overhead as of June 2026).
- **Host-side implementation, not guest-side.** The Dapr Rust SDK is tokio/tonic gRPC and cannot compile to wasm32-wasip2, so the SDK lives in the native host. (A future alternative: a guest-side implementation component speaking the sidecar's HTTP API over wasi:http.)
- **Interfaces scoped to what SDK 0.17 delivers**: state (no transactions), pubsub publish, secrets, output bindings, invocation, configuration get. Pub/sub delivery via the guest's exported `topic-handler`, forwarded from the host's app-callback gRPC server (tonic 0.12 to match dapr 0.17). Excluded: distributed lock + state transactions (absent from SDK), input bindings + bulk subscribe (`todo!()` stubs in SDK), configuration subscribe (needs streams), actors/jobs/crypto/workflow (later).
- **`DaprBackend` trait** decouples the WIT bridge from the SDK: `SidecarBackend` (real gRPC) vs `MemoryBackend` (tests, `--backend memory`). The e2e test runs the real kv-demo component against `MemoryBackend` — no sidecar needed in CI.
- **Two cargo workspaces** (root: host; `examples/`): guest cdylib crates don't build for native targets, so keeping them out of the root workspace keeps plain `cargo build`/`cargo test`/`cargo clippy` clean.
- **OCI publishing**: CI publishes both modules with wkg on every push to main — `ghcr.io/sideeffffect/dapr-wasm-components/dapr/client:<v>` (WIT) and `.../dapr/kv-demo:<v>` (component), namespace mapping in `.wkg/config.toml`, auth via docker/login-action + GITHUB_TOKEN.

## Operational notes

- Sidecar address resolution mirrors other SDKs: `DAPR_GRPC_ENDPOINT` → `DAPR_GRPC_PORT` → `http://127.0.0.1:50001` (dapr 0.17's `Client::connect` requires `DAPR_GRPC_PORT`, so the host resolves and uses `connect_with_port`).
- dapr 0.17's `GrpcError` keeps the tonic `Status` private — WIT error mapping can only classify `TransportError` → `unavailable` and carry Debug text for the rest.
- Run under Dapr: `dapr run --app-id x --app-port 50051 --app-protocol grpc -- dapr-wasm-host component.wasm`; the app-callback server starts only when the guest subscribes to topics.
- Guest calls are serialized through a `tokio::sync::Mutex<GuestRunner>` (wasmtime `Store` is single-threaded by design).

## See Also

- [Dapr Rust SDK](dapr-rust-sdk.md)
- [Wasmtime Host Embedding](../wasm-tooling/wasmtime-host-embedding.md)
- [wasm-pkg-tools (wkg)](../wasm-tooling/wasm-pkg-tools-wkg.md)
- [Dapr × Wasm Prior Art](dapr-wasm-prior-art.md)
