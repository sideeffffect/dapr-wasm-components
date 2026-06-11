# Dapr Rust SDK — API surface survey (v0.17.0 / v0.19.0-rc.1)

> Source: Research synthesis compiled 2026-06-11 from https://crates.io/crates/dapr , https://docs.rs/dapr , https://github.com/dapr/rust-sdk (source clone), https://v1-18.docs.dapr.io/developing-applications/sdks/rust/
> Collected: 2026-06-11
> Published: Unknown

## Crates & versions

| Crate | Latest published | Latest stable | Notes |
|---|---|---|---|
| `dapr` | 0.19.0-rc.1 (2026-06-02) | 0.17.0 | main SDK |
| `dapr-macros` | 0.19.0-rc.1 | 0.17.0 | proc macros `#[actor]`, `#[topic]` |
| `dapr-durabletask` | 0.0.2 | — | durable-task engine for `workflow` feature |

Status: Alpha. "Not all building blocks are currently implemented... will likely involve breaking changes."

## Client API surface (`dapr::Client<T>`, v0.19.0-rc.1)

Client is `Client<T> where T: DaprInterface`; concrete instantiation `Client<TonicClientWithAuth>`:

```rust
pub type TonicClient = dapr_v1::dapr_client::DaprClient<TonicChannel>;
pub type TonicClientWithAuth = DaprClient<InterceptedService<Channel, ApiTokenInterceptor>>;
```

### Constructors

```rust
pub async fn new() -> Result<Self, Error>                                  // env-driven (0.19+)
pub async fn from_options(opts: ClientOptions) -> Result<Self, Error>      // 0.19+
pub async fn connect_with_address(address: impl Into<String>) -> Result<Self, Error>
#[deprecated] pub async fn connect(addr: String) -> Result<Self, Error>            // removed in 0.20
#[deprecated] pub async fn connect_with_port(addr: String, port: String) -> Result<Self, Error>
```

In 0.17.0 (current stable) only `connect`/`connect_with_port` exist.

### Service invocation

```rust
pub async fn invoke_service<I, M>(&mut self, app_id: I, method_name: M, data: Option<prost_types::Any>)
    -> Result<InvokeServiceResponse, Error>
```

### Bindings (output)

```rust
pub async fn invoke_binding<S>(&mut self, name: S, data: Vec<u8>, operation: S,
    metadata: Option<HashMap<String, String>>) -> Result<InvokeBindingResponse, Error>
pub async fn invoke_output_binding<S>(&mut self, name: S, operation: S) -> Result<(), Error>  // 0.19+
```

### Pub/Sub (publish only; no bulk publish, no streaming subscribe)

```rust
pub async fn publish_event<S>(&mut self, pubsub_name: S, topic: S, data_content_type: S,
    data: Vec<u8>, metadata: Option<HashMap<String, String>>) -> Result<(), Error>
```

### State store

```rust
pub async fn get_state<S>(&mut self, store_name: S, key: S, metadata: Option<HashMap<String,String>>)
    -> Result<GetStateResponse, Error>
pub async fn save_state<S>(&mut self, store_name: S, key: S, value: Vec<u8>, etag: Option<Etag>,
    metadata: Option<HashMap<String,String>>, options: Option<StateOptions>) -> Result<(), Error>
pub async fn save_bulk_states<S, I>(&mut self, store_name: S, items: I) -> Result<(), Error>
    where I: Into<Vec<StateItem>>
pub async fn delete_state<S>(&mut self, store_name: S, key: S, metadata: Option<HashMap<String,String>>)
    -> Result<(), Error>
pub async fn delete_bulk_state<I, K>(&mut self, store_name: K, states: I) -> Result<(), Error>
    where I: IntoIterator<Item = (K, Vec<u8>)>
pub async fn query_state_alpha1<S>(&mut self, store_name: S, query: serde_json::Value,
    metadata: Option<HashMap<String,String>>) -> Result<QueryStateResponse, Error>
```

No `execute_state_transaction` — client-level state transactions are NOT supported (only actor-scoped transactions exist).

### Secrets

```rust
pub async fn get_secret<S>(&mut self, store_name: S, key: S) -> Result<GetSecretResponse, Error>
pub async fn get_bulk_secret<S>(&mut self, store_name: S, metadata: Option<HashMap<String,String>>)
    -> Result<GetBulkSecretResponse, Error>
```

### Configuration

```rust
pub async fn get_configuration<S, K>(&mut self, store_name: S, keys: Vec<K>,
    metadata: Option<HashMap<String,String>>) -> Result<GetConfigurationResponse, Error>
pub async fn subscribe_configuration<S>(&mut self, store_name: S, keys: Vec<S>,
    metadata: Option<HashMap<String,String>>) -> Result<Streaming<SubscribeConfigurationResponse>, Error>
pub async fn unsubscribe_configuration<S>(&mut self, store_name: S, id: S)
    -> Result<UnsubscribeConfigurationResponse, Error>
```

### Actors (client side)

```rust
pub async fn invoke_actor<I, M, TInput, TOutput>(&mut self, actor_type: I, actor_id: I,
    method_name: M, input: TInput, metadata: Option<HashMap<String,String>>) -> Result<TOutput, Error>
// JSON-serializes input, forces Content-Type: application/json
```

### Cryptography (alpha)

```rust
pub async fn encrypt<R>(&mut self, payload: ReaderStream<R>, request_options: EncryptRequestOptions)
    -> Result<Vec<StreamPayload>, Status>
pub async fn decrypt(&mut self, encrypted: Vec<StreamPayload>, options: DecryptRequestOptions)
    -> Result<Vec<u8>, Status>
```

### Jobs / distributed scheduler

```rust
pub async fn schedule_job(&mut self, job: Job, overwrite: Option<bool>) -> Result<ScheduleJobResponse, Error>
pub async fn get_job(&mut self, name: &str) -> Result<GetJobResponse, Error>
pub async fn delete_job(&mut self, name: &str) -> Result<DeleteJobResponse, Error>
pub async fn delete_jobs_by_prefix(&mut self, prefix: Option<&str>) -> Result<DeleteJobsByPrefixResponse, Error>  // 0.19+
pub async fn list_jobs(&mut self) -> Result<ListJobsResponse, Error>                                              // 0.19+
```

Plus `JobBuilder` and `JobFailurePolicyBuilder`. (`*_alpha1` variants are deprecated aliases in 0.19; only `*_alpha1` exist in 0.17.)

### Conversation (LLM)

```rust
pub async fn converse_alpha1(&mut self, request: ConversationRequest) -> Result<ConversationResponse, Error>
pub async fn converse_alpha2(&mut self, request: ConversationRequestAlpha2) -> Result<ConversationResponseAlpha2, Error>  // 0.19+
```

### Metadata / workflow handle

```rust
pub async fn set_metadata<S>(&mut self, key: S, value: S) -> Result<(), Error>
pub async fn get_metadata(&mut self) -> Result<GetMetadataResponse, Error>
pub async fn new_workflow_client(&self) -> workflow::Result<workflow::WorkflowClient>   // 0.19+
```

### Building-block support matrix

| Building block | Supported? |
|---|---|
| Service invocation | Yes (gRPC, `Any` payload) |
| State (get/save/delete/bulk/query) | Yes; no transactions at client level |
| PubSub publish | Yes; no bulk publish, no programmatic/streaming subscribe (declarative + AppCallback only) |
| Output bindings | Yes |
| Input bindings (server) | Proto types exist, but `on_binding_event`/`list_input_bindings` are `todo!()` stubs |
| Secrets | Yes |
| Configuration | Yes incl. subscribe stream |
| Actors | Yes (client invoke + full server runtime) |
| Crypto | Yes (alpha) |
| Jobs/scheduler | Yes |
| Workflow | Yes (0.19+, default-on `workflow` feature via `dapr-durabletask`) |
| Conversation | Yes (alpha1 + alpha2) |
| Distributed lock | No — no `try_lock`/`unlock` anywhere in the client |

## Async model

Fully async on tokio — every client method is `async fn`; transport is tonic gRPC. No sync wrapper exists in the SDK; you must use `#[tokio::main]` / `Runtime::block_on`.

## Server side

### a) AppCallback (pubsub subscriptions, input bindings) — gRPC server the sidecar calls

`dapr::appcallback::AppCallbackService` implements the generated `AppCallback` + `AppCallbackAlpha` tonic services. You run it as your own tonic gRPC server; the sidecar calls `ListTopicSubscriptions` at startup, then `OnTopicEvent` per message:

```rust
pub struct Handler { pub pub_sub_name: String, pub topic: String, pub handler: Box<dyn HandlerMethod> }
impl AppCallbackService {
    pub fn new() -> AppCallbackService
    pub fn add_handler(&mut self, handler: Handler)
    pub fn add_job_handler(&mut self, job_name: String, handler: Box<dyn JobHandlerMethod>)
}
#[tonic::async_trait] pub trait HandlerMethod: Send + Sync + 'static {
    async fn handler(&self, request: TopicEventRequest) -> Result<Response<TopicEventResponse>, Status>;
}
```

Serving:

```rust
Server::builder()
    .add_service(AppCallbackServer::new(callback_service))
    .serve("127.0.0.1:50051".parse()?).await?;
```

Implemented callbacks: `list_topic_subscriptions`, `on_topic_event`, `on_job_event` (+ alpha1 aliases). Stubbed with `todo!()`: `on_invoke`, `list_input_bindings`, `on_binding_event`, `on_bulk_topic_event`. Inbound auth: `AppApiTokenLayer::from_env()`.

### b) Actor runtime — HTTP server (axum), not gRPC

`dapr::server::DaprHttpServer` hosts actors over HTTP. `#[dapr_macros::actor]` derives the axum plumbing. Per-instance `ActorContextClient` has `get_actor_state`, `execute_actor_state_transaction`, `register/unregister_actor_reminder`, `register/unregister_actor_timer`.

### c) Workflow (0.19+, `dapr::workflow`)

`WorkflowClient::new()/new_with_address()`, `registry_mut()`, `start_worker()`, `scheduling_client()`; ops: `schedule_workflow`, `suspend/resume/terminate_workflow(_recursive)`, `raise_event`, `fetch_workflow_metadata`, `wait_for_workflow_start/_completion`, `purge_workflow_state`. Authoring re-exports from `dapr-durabletask`: `OrchestrationContext`, `when_all`, `when_any`, `RetryPolicy`.

## Sidecar connection (0.19 `Client::new()` resolution order)

1. `DAPR_GRPC_ENDPOINT` — full endpoint, e.g. `http://127.0.0.1:50001`, `https://my-sidecar:443?tls=true`, `unix:///var/run/dapr.sock`
2. `http://127.0.0.1:$DAPR_GRPC_PORT` if set
3. default `http://127.0.0.1:50001`

Other env: `DAPR_API_TOKEN` (gRPC metadata `dapr-api-token`), `DAPR_CLIENT_TIMEOUT_SECONDS` (default 5), `APP_API_TOKEN`, `APP_PORT` (actor HTTP server, default 8080).

## Key types (prost-generated, `dapr::dapr::proto::{common,runtime}::v1`)

```rust
pub struct StateItem { pub key: String, pub value: Vec<u8>, pub etag: Option<Etag>,
                       pub metadata: HashMap<String,String>, pub options: Option<StateOptions> }
pub struct Etag { pub value: String }
pub struct StateOptions { pub concurrency: i32, pub consistency: i32 }
enum StateConcurrency { ConcurrencyUnspecified=0, ConcurrencyFirstWrite=1, ConcurrencyLastWrite=2 }
enum StateConsistency { ConsistencyUnspecified=0, ConsistencyEventual=1, ConsistencyStrong=2 }
pub struct GetStateResponse { pub data: Vec<u8>, pub etag: String, pub metadata: HashMap<String,String> }
```

Metadata everywhere is `HashMap<String, String>`. `TopicEventRequest` is CloudEvents v1.0-compatible. Errors: `dapr::error::Error` (gRPC paths) and raw `tonic::Status` for crypto.

## Toolchain & dependencies (workspace, main @ 0.19.0-rc.1)

- MSRV 1.88.0, edition 2024 (0.16–0.18: MSRV 1.78, edition 2021). License Apache-2.0.
- tonic 0.14.6, prost 0.14, tokio 1.39 (rt/sync/time), axum 0.7, tower 0.5, async-trait 0.1, serde/serde_json (re-exported as `dapr::serde`), futures 0.3, chrono 0.4, dapr-durabletask 0.0.2 (optional, default-on `workflow` feature).
- protoc not required unless regenerating protos (generated code checked in under `dapr/src/dapr/`).
