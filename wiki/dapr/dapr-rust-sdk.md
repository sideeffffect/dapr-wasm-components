# Dapr Rust SDK

> Sources: Dapr project (dapr/rust-sdk README), Unknown; Dapr docs v1.18, Unknown; API survey (crates.io/docs.rs/source), 2026-06-11
> Raw: [dapr-rust-sdk-readme](../../raw/dapr/dapr-rust-sdk-readme.md); [dapr-rust-sdk-docs-v1-18](../../raw/dapr/dapr-rust-sdk-docs-v1-18.md); [dapr-rust-sdk-api-survey](../../raw/dapr/dapr-rust-sdk-api-survey.md)

## Overview

The Dapr Rust SDK (`dapr` crate) is the **Alpha**-status client library for talking to a Dapr sidecar over gRPC (tonic). Latest stable is **0.17.0**; **0.19.0-rc.1** (2026-06-02) is a release candidate that adds `Client::new()` env-driven construction, workflow support, and jobs improvements. It is fully **async (tokio)** — no sync API exists; callers need `#[tokio::main]` or `Runtime::block_on`. MSRV 1.88, edition 2024 (on 0.19); Apache-2.0. Companion crates: `dapr-macros` (`#[actor]`, `#[topic]`), `dapr-durabletask`.

## Connecting

0.19+: `dapr::Client::new().await` resolves the sidecar address as (1) `DAPR_GRPC_ENDPOINT` (full endpoint, supports `https://...?tls=true` and `unix:///...`), (2) `http://127.0.0.1:$DAPR_GRPC_PORT`, (3) default `http://127.0.0.1:50001`. `DAPR_API_TOKEN` is attached as `dapr-api-token` gRPC metadata; `DAPR_CLIENT_TIMEOUT_SECONDS` defaults to 5. Programmatic config: `ClientOptions` + `Client::from_options`; address-only: `Client::connect_with_address`. **0.17 only has** `Client::<TonicClient>::connect(addr)` / `connect_with_port(addr, port)` (deprecated in 0.19, removed in 0.20).

## Building-block support matrix (client)

| Building block | Status | Key methods |
|---|---|---|
| Service invocation | ✅ | `invoke_service(app_id, method, Option<prost_types::Any>)` |
| State | ✅ (no transactions) | `get_state`, `save_state(store, key, value, etag?, metadata?, options?)`, `delete_state`, `save_bulk_states`, `delete_bulk_state`, `query_state_alpha1` |
| Pub/sub publish | ✅ (no bulk, no streaming subscribe) | `publish_event(pubsub, topic, content_type, data, metadata?)` |
| Output bindings | ✅ | `invoke_binding(name, data, operation, metadata?)` |
| Secrets | ✅ | `get_secret(store, key)`, `get_bulk_secret(store, metadata?)` |
| Configuration | ✅ incl. subscribe stream | `get_configuration`, `subscribe_configuration` (server-stream), `unsubscribe_configuration` |
| Actors (invoke) | ✅ | `invoke_actor(actor_type, actor_id, method, input, metadata?)` — JSON in/out |
| Crypto (alpha) | ✅ | `encrypt(stream, options)`, `decrypt(payloads, options)` |
| Jobs/scheduler | ✅ | `schedule_job`, `get_job`, `delete_job` (+ `list_jobs`, `delete_jobs_by_prefix` 0.19+) |
| Conversation (LLM) | ✅ alpha | `converse_alpha1` (+ `converse_alpha2` 0.19+) |
| Workflow | ✅ 0.19+ | `dapr::workflow`, `new_workflow_client()`, durable-task style |
| Distributed lock | ❌ | absent from client |
| State transactions | ❌ (client level) | only actor-scoped `execute_actor_state_transaction` |

`get_metadata`/`set_metadata` also exist.

## Server side (sidecar → app)

- **AppCallback gRPC server** (`dapr::appcallback::AppCallbackService` + `AppCallbackServer`): the app hosts a tonic server; sidecar calls `ListTopicSubscriptions` at startup, then `OnTopicEvent` per message. Handlers registered via `add_handler(Handler { pub_sub_name, topic, handler })`; `#[dapr_macros::topic]` generates a `HandlerMethod` from an `async fn`. Implemented: topic subscriptions, `on_topic_event`, `on_job_event`. **Stubbed `todo!()`**: `on_invoke`, `list_input_bindings`, `on_binding_event`, `on_bulk_topic_event` — gRPC-side input bindings will panic if called. Inbound auth: `APP_API_TOKEN` + `AppApiTokenLayer`.
- **Actor runtime** is an **HTTP (axum) server** (`dapr::server::DaprHttpServer`), not gRPC; `ActorContextClient` provides actor state (incl. transactions), reminders, timers. `APP_PORT` default 8080.

## Key types

`StateItem { key, value, etag: Option<Etag>, metadata, options }`; `StateOptions { concurrency, consistency }` with enums `StateConcurrency { Unspecified=0, FirstWrite=1, LastWrite=2 }`, `StateConsistency { Unspecified=0, Eventual=1, Strong=2 }`; `GetStateResponse { data: Vec<u8>, etag: String, metadata }`. Metadata is always `HashMap<String, String>`. `TopicEventRequest` is CloudEvents-v1.0-shaped. Deps: tonic 0.14, prost 0.14, tokio 1.39, axum 0.7.

## Implications for dapr-wasm-components

- Async-only SDK + sync-preferred WIT ⇒ the host embeds wasmtime with async support and implements sync-WIT imports via async host functions (or `block_on`).
- Outbound blocks (state, pubsub publish, invoke, secrets, bindings out, configuration get) map to WIT *imports*; app-callback (topic events, job events) maps to WIT *exports* the guest implements, forwarded from the host's AppCallback gRPC server.
- Distributed lock and client-level state transactions cannot be offered in WIT yet (SDK gap); gRPC input-binding events are stubbed in the SDK and likewise unavailable.

## See Also

- [Component Model Overview](../wasm-component-model/component-model-overview.md)
