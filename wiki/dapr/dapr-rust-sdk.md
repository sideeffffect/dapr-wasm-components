# Dapr Rust SDK

> Sources: Dapr project (dapr/rust-sdk README), Unknown; Dapr docs v1.18, Unknown
> Raw: [dapr-rust-sdk-readme](../../raw/dapr/dapr-rust-sdk-readme.md); [dapr-rust-sdk-docs-v1-18](../../raw/dapr/dapr-rust-sdk-docs-v1-18.md)

## Overview

The Dapr Rust SDK (`dapr` crate, README references `0.19`; GitHub latest release tag v0.17.0, docs pages still show `0.16`) is the **Alpha**-status client library for talking to a Dapr sidecar over gRPC (tonic). It is fully **async (tokio)** — there is no sync API. It aims to cover all public Dapr APIs; not all building blocks are implemented yet. Requires Rust ≥ 1.88. License: Apache-2.0.

## Connecting

`dapr::Client::new().await` reads `DAPR_GRPC_ENDPOINT` / `DAPR_GRPC_PORT` (default `http://127.0.0.1:50001`) / `DAPR_API_TOKEN` / `DAPR_CLIENT_TIMEOUT_SECONDS` (default 5 s) from the environment. Programmatic config via `dapr::client::ClientOptions` (`with_address`, `with_api_token`, `with_timeout`) and `Client::from_options`. Older `Client::<TonicClient>::connect(addr)` / `connect_with_port` are deprecated in 0.19, removed in 0.20; `Client::connect_with_address(addr)` overrides just the address.

## Building blocks (client side)

- **State**: `save_state(store, key, value, etag?, metadata?, options?)`, `get_state(store, key, metadata?)`, `delete_state(store, key, metadata?)`, `save_bulk_states`.
- **Pub/sub publish**: `publish_event(pubsub_name, topic, content_type, data, metadata?)`.
- **Service invocation (gRPC)**: `invoke_service(app_id, method, data?)`.
- **Metadata**: `get_metadata()`.
- **Workflows**: default-on `workflow` cargo feature, `dapr::workflow` module (durable task style).
- Others (bindings, secrets, configuration, actors, crypto…) — see the per-method details captured in the article when the API survey lands; the SDK ships gRPC protos for the full runtime API (`dapr::dapr::proto::runtime::v1`).

## Server side (app callback)

For pub/sub subscriptions and input bindings the *sidecar calls the app*: the SDK provides `dapr::appcallback::AppCallbackService` + `AppCallbackServer` (tonic gRPC server the app hosts). Inbound auth via `APP_API_TOKEN` and `AppApiTokenLayer` (no-op when env var unset).

## Implications for dapr-wasm-components

- The SDK being async-only means a host embedding wasm components must bridge: wasmtime async host functions (or `block_on`) behind **sync WIT** imports.
- Outbound building blocks (state, pubsub publish, invoke, secrets, bindings out) map naturally to WIT *imports*; app-callback (topic subscribe, input binding events) maps to WIT *exports* the guest implements and the host's AppCallback gRPC server forwards into.

## See Also

- [Component Model Overview](../wasm-component-model/component-model-overview.md)
