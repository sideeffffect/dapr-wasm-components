//! A minimal mock of the Dapr sidecar's HTTP API, recording requests so
//! tests can assert on what the provider actually sent.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::json;

#[derive(Debug, Clone)]
pub struct PublishedEvent {
    pub pubsub: String,
    pub topic: String,
    pub content_type: String,
    pub body: Vec<u8>,
}

/// One crypto request the mock received.
#[derive(Debug, Clone)]
pub struct CryptoRequest {
    /// `encrypt` or `decrypt`.
    pub operation: String,
    pub component: String,
    /// Total request-body bytes.
    pub body_len: usize,
    /// Whether the body came with `transfer-encoding: chunked` (no
    /// `content-length`) — i.e. the provider streamed it.
    pub chunked: bool,
}

#[derive(Default)]
pub struct Recorded {
    /// (store, key) -> stored JSON value
    pub state: HashMap<(String, String), serde_json::Value>,
    pub published: Vec<PublishedEvent>,
    pub scheduled_jobs: HashMap<String, serde_json::Value>,
    pub metadata_labels: HashMap<String, String>,
    /// Raw converse request bodies, in order.
    pub converse_requests: Vec<serde_json::Value>,
    /// Crypto requests, in order.
    pub crypto_requests: Vec<CryptoRequest>,
    /// Configuration subscriptions handed out, as (store, id).
    pub configuration_subscriptions: Vec<(String, String)>,
    /// Configuration unsubscribe requests received, as (store, id).
    pub configuration_unsubscribes: Vec<(String, String)>,
}

pub struct MockSidecar {
    pub endpoint: String,
    pub recorded: Arc<Mutex<Recorded>>,
}

type Shared = Arc<Mutex<Recorded>>;

impl MockSidecar {
    /// Bind on an ephemeral local port and serve the mock API.
    pub async fn start() -> anyhow::Result<Self> {
        let recorded: Shared = Arc::new(Mutex::new(Recorded::default()));

        let app = Router::new()
            .route("/v1.0/healthz", get(|| async { StatusCode::NO_CONTENT }))
            .route(
                "/v1.0/healthz/outbound",
                get(|| async { StatusCode::NO_CONTENT }),
            )
            .route(
                "/v1.0/metadata",
                get(|| async {
                    Json(json!({
                        "id": "mock-sidecar",
                        "components": [{
                            "name": MOCK_CRYPTO_COMPONENT,
                            "type": "crypto.mock",
                            "version": "v1",
                        }],
                    }))
                }),
            )
            .route(
                "/v1.0/metadata/{key}",
                axum::routing::put(put_metadata_label),
            )
            .route("/v1.0/state/{store}", post(save_state))
            .route("/v1.0/state/{store}/bulk", post(get_bulk_state))
            .route(
                "/v1.0/state/{store}/{key}",
                get(get_state).delete(delete_state),
            )
            .route("/v1.0/publish/{pubsub}/{topic}", post(publish))
            .route("/v1.0/secrets/{store}/{key}", get(get_secret))
            .route(
                "/v1.0/actors/{actor_type}/{actor_id}/timers/{name}",
                post(register_timer),
            )
            .route("/v1.0-alpha1/lock/{store}", post(try_lock))
            .route("/v1.0-alpha1/unlock/{store}", post(unlock))
            .route(
                "/v1.0/invoke/{app}/method/{*path}",
                post(invoke_echo).get(invoke_echo),
            )
            .route(
                // Only the built-in `dapr` workflow component is routed: the
                // providers must always target it (the WIT takes no component).
                "/v1.0/workflows/dapr/{name}/start",
                post(|| async { (StatusCode::ACCEPTED, Json(json!({"instanceID": "wf-123"}))) }),
            )
            .route(
                "/v1.0/configuration/{store}/subscribe",
                get(subscribe_configuration),
            )
            .route(
                "/v1.0/configuration/{store}/{id}/unsubscribe",
                get(unsubscribe_configuration),
            )
            .route("/v1.0/workflows/dapr/{instance}", get(get_workflow))
            .route(
                // The path daprd 1.14 – 1.18 all serve (daprd <= 1.17 has no
                // `/v1.0/jobs`), so the mock serves only this one.
                "/v1.0-alpha1/jobs/{name}",
                post(schedule_job).get(get_job).delete(delete_job),
            )
            .route(
                "/v1.0-alpha2/conversation/{component}/converse",
                post(converse),
            )
            .route(
                "/v1.0-alpha1/crypto/{component}/{operation}",
                axum::routing::put(crypto),
            )
            .with_state(recorded.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        Ok(Self { endpoint, recorded })
    }
}

async fn save_state(
    State(recorded): State<Shared>,
    Path(store): Path<String>,
    Json(items): Json<Vec<serde_json::Value>>,
) -> StatusCode {
    let mut recorded = recorded.lock().unwrap();
    for item in items {
        let key = item["key"].as_str().unwrap_or_default().to_string();
        recorded
            .state
            .insert((store.clone(), key), item["value"].clone());
    }
    StatusCode::NO_CONTENT
}

async fn get_state(
    State(recorded): State<Shared>,
    Path((store, key)): Path<(String, String)>,
) -> Response {
    let recorded = recorded.lock().unwrap();
    match recorded.state.get(&(store, key)) {
        Some(value) => (
            StatusCode::OK,
            [("etag", "42")],
            serde_json::to_vec(value).unwrap_or_default(),
        )
            .into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    }
}

async fn delete_state(
    State(recorded): State<Shared>,
    Path((store, key)): Path<(String, String)>,
) -> StatusCode {
    recorded.lock().unwrap().state.remove(&(store, key));
    StatusCode::NO_CONTENT
}

async fn get_bulk_state(
    State(recorded): State<Shared>,
    Path(store): Path<String>,
    Json(request): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let recorded = recorded.lock().unwrap();
    let keys: Vec<String> = request["keys"]
        .as_array()
        .map(|keys| {
            keys.iter()
                .filter_map(|k| k.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    let items: Vec<serde_json::Value> = keys
        .into_iter()
        .map(
            |key| match recorded.state.get(&(store.clone(), key.clone())) {
                Some(value) => json!({"key": key, "value": value, "etag": "42"}),
                None => json!({"key": key}),
            },
        )
        .collect();
    Json(serde_json::Value::Array(items))
}

async fn publish(
    State(recorded): State<Shared>,
    Path((pubsub, topic)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    recorded.lock().unwrap().published.push(PublishedEvent {
        pubsub,
        topic,
        content_type: headers
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string(),
        body: body.to_vec(),
    });
    StatusCode::NO_CONTENT
}

/// Acquire a lock. Keyed off `resourceId`: `contended` reports the lock as
/// already held (`success: false`); anything else acquires it.
async fn try_lock(Json(request): Json<serde_json::Value>) -> Json<serde_json::Value> {
    let acquired = request["resourceId"].as_str() != Some("contended");
    Json(json!({ "success": acquired }))
}

/// Release a lock. Keyed off `resourceId` so tests can drive each unlock
/// status: `missing` -> LOCK_DOES_NOT_EXIST (1), `others` ->
/// LOCK_BELONGS_TO_OTHERS (2); anything else -> SUCCESS (0).
async fn unlock(Json(request): Json<serde_json::Value>) -> Json<serde_json::Value> {
    let status = match request["resourceId"].as_str() {
        Some("missing") => 1,
        Some("others") => 2,
        _ => 0,
    };
    Json(json!({ "status": status }))
}

async fn get_secret(Path((store, key)): Path<(String, String)>) -> Response {
    // How real daprd reports a store error: ERR_SECRET_GET with the store's
    // own message appended (`local.file` says `secret <key> not found`).
    let secret_get = |store_error: &str| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "errorCode": "ERR_SECRET_GET",
                "message": format!(
                    "failed getting secret with key {key} from secret store {store}: {store_error}"
                ),
            })),
        )
            .into_response()
    };
    match key.as_str() {
        "missing" => StatusCode::NO_CONTENT.into_response(),
        "missing-in-store" => secret_get(&format!("secret {key} not found")),
        "store-down" => secret_get("connection refused"),
        _ => Json(json!({"password": "hunter2"})).into_response(),
    }
}

/// The one actor type the mock "hosts" (timers on it succeed).
pub const MOCK_HOSTED_ACTOR_TYPE: &str = "HostedActor";
/// An actor type whose timer storage fails.
pub const MOCK_BROKEN_TIMER_ACTOR_TYPE: &str = "BrokenTimerActor";

/// How real daprd (checked on 1.17.7, same code in 1.18) answers a timer
/// registration: 204 for a hosted actor type, otherwise HTTP 500
/// `ERR_ACTOR_TIMER_CREATE` with the timer error appended — `actor type not
/// registered` for an actor type this app does not host.
async fn register_timer(
    Path((actor_type, actor_id, _name)): Path<(String, String, String)>,
) -> Response {
    let timer_create = |timer_error: String| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "errorCode": "ERR_ACTOR_TIMER_CREATE",
                "message": format!("error creating actor timer: {timer_error}"),
            })),
        )
            .into_response()
    };
    match actor_type.as_str() {
        MOCK_HOSTED_ACTOR_TYPE => StatusCode::NO_CONTENT.into_response(),
        MOCK_BROKEN_TIMER_ACTOR_TYPE => timer_create("timer storage unavailable".to_string()),
        _ => timer_create(format!(
            "can't create timer for actor {actor_type}||{actor_id}: actor type not registered"
        )),
    }
}

/// Record the converse request and return a canned alpha2 response that
/// exercises every result field (choices, tool calls, usage, context id).
async fn converse(
    State(recorded): State<Shared>,
    Path(_component): Path<String>,
    Json(request): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    recorded.lock().unwrap().converse_requests.push(request);
    // Shaped like daprd's answer: daprd renders it with protojson, which
    // writes the proto `int64`/`uint64` fields (the choice index, the token
    // counts) as JSON strings.
    Json(json!({
        "contextId": "ctx-1",
        "outputs": [{
            "choices": [{
                "finishReason": "tool_calls",
                "index": "0",
                "message": {
                    "content": "hello",
                    "toolCalls": [{
                        "id": "call-1",
                        "function": { "name": "lookup", "arguments": "{\"q\":1}" }
                    }]
                }
            }],
            "model": "gpt-test",
            "usage": {
                "promptTokens": "3",
                "completionTokens": "5",
                "totalTokens": "8",
                "promptTokensDetails": { "audioTokens": "0", "cachedTokens": "2" },
                "completionTokensDetails": {
                    "acceptedPredictionTokens": "0",
                    "audioTokens": "0",
                    "reasoningTokens": "4",
                    "rejectedPredictionTokens": "0"
                }
            }
        }]
    }))
}

/// Echo back what we received — used to test invocation passthrough,
/// including non-2xx statuses.
async fn invoke_echo(
    Path((app, path)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let status = headers
        .get("x-mock-status")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(200);
    (
        StatusCode::from_u16(status).unwrap_or(StatusCode::OK),
        [("x-echo-app", app), ("x-echo-path", path)],
        body,
    )
        .into_response()
}

async fn put_metadata_label(
    State(recorded): State<Shared>,
    Path(key): Path<String>,
    body: String,
) -> StatusCode {
    recorded.lock().unwrap().metadata_labels.insert(key, body);
    StatusCode::NO_CONTENT
}

async fn schedule_job(
    State(recorded): State<Shared>,
    Path(name): Path<String>,
    Json(job): Json<serde_json::Value>,
) -> StatusCode {
    recorded.lock().unwrap().scheduled_jobs.insert(name, job);
    StatusCode::NO_CONTENT
}

async fn get_job(State(recorded): State<Shared>, Path(name): Path<String>) -> Response {
    let recorded = recorded.lock().unwrap();
    match recorded.scheduled_jobs.get(&name) {
        Some(job) => Json(job.clone()).into_response(),
        None => StatusCode::BAD_REQUEST.into_response(),
    }
}

/// The configuration store whose unsubscribe always fails with HTTP 500.
pub const MOCK_BROKEN_CONFIG_STORE: &str = "broken-config";

/// `GET /v1.0/configuration/{store}/subscribe`: hands out `sub-<n>`.
async fn subscribe_configuration(
    State(recorded): State<Shared>,
    Path(store): Path<String>,
) -> Response {
    let mut recorded = recorded.lock().unwrap();
    let id = format!("sub-{}", recorded.configuration_subscriptions.len() + 1);
    recorded
        .configuration_subscriptions
        .push((store, id.clone()));
    Json(json!({ "id": id })).into_response()
}

/// `GET /v1.0/configuration/{store}/{id}/unsubscribe`: records the request;
/// daprd's `{"ok": true}` on success, a 500 for [`MOCK_BROKEN_CONFIG_STORE`].
async fn unsubscribe_configuration(
    State(recorded): State<Shared>,
    Path((store, id)): Path<(String, String)>,
) -> Response {
    let broken = store == MOCK_BROKEN_CONFIG_STORE;
    recorded
        .lock()
        .unwrap()
        .configuration_unsubscribes
        .push((store, id));
    if broken {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "errorCode": "ERR_CONFIGURATION_UNSUBSCRIBE",
                "message": "store is down",
            })),
        )
            .into_response()
    } else {
        Json(json!({ "ok": true })).into_response()
    }
}

/// `GET /v1.0/workflows/dapr/{instance}` in daprd's shape: the typed
/// values arrive as flat `dapr.workflow.*` properties. `wf-failed` is a failed
/// instance with input, failure details and a custom status; `wf-stalled`
/// has a status string daprd falls back to (`UNKNOWN`); anything else is
/// daprd's 404 `ERR_INSTANCE_ID_NOT_FOUND`.
async fn get_workflow(Path(instance): Path<String>) -> Response {
    match instance.as_str() {
        "wf-failed" => Json(json!({
            "instanceID": "wf-failed",
            "workflowName": "order-processing",
            "createdAt": "2026-10-12T10:00:00Z",
            "lastUpdatedAt": "2026-10-12T10:00:05Z",
            "runtimeStatus": "FAILED",
            "properties": {
                "dapr.workflow.input": "{\"id\":1}",
                "dapr.workflow.custom_status": "\"charging\"",
                "dapr.workflow.failure.error_type": "PaymentDeclined",
                "dapr.workflow.failure.error_message": "card declined",
                "dapr.workflow.failure.stack_trace": "at charge()",
            },
        }))
        .into_response(),
        "wf-stalled" => Json(json!({
            "instanceID": "wf-stalled",
            "workflowName": "order-processing",
            "createdAt": "2026-10-12T10:00:00Z",
            "lastUpdatedAt": "2026-10-12T10:00:00Z",
            "runtimeStatus": "UNKNOWN",
            "properties": { "dapr.workflow.output": "42" },
        }))
        .into_response(),
        _ => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "errorCode": "ERR_INSTANCE_ID_NOT_FOUND",
                "message": format!("unable to find workflow with the provided instance ID: {instance}"),
            })),
        )
            .into_response(),
    }
}

async fn delete_job(State(recorded): State<Shared>, Path(name): Path<String>) -> StatusCode {
    recorded.lock().unwrap().scheduled_jobs.remove(&name);
    StatusCode::NO_CONTENT
}

/// The only crypto component the mock knows.
pub const MOCK_CRYPTO_COMPONENT: &str = "mockcrypto";
/// Prefix of the mock's "encrypted document".
const MOCK_CRYPTO_MAGIC: &[u8] = b"MOCKENC1:";

/// A reversible stand-in for Dapr's encryption scheme: the document is
/// `MOCKENC1:<key-name>\n` followed by the payload XOR 0x5a. Like daprd, an
/// unknown component is a 400 `ERR_CRYPTO_PROVIDER_NOT_FOUND`, sent without
/// reading the body. The body is
/// consumed frame by frame (no size limit), and the output is sent whole
/// (daprd buffers its output too).
async fn crypto(
    State(recorded): State<Shared>,
    Path((component, operation)): Path<(String, String)>,
    headers: HeaderMap,
    body: axum::body::Body,
) -> Response {
    use http_body_util::BodyExt;

    let chunked = headers
        .get("transfer-encoding")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("chunked"));
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };

    // Like daprd, reject an unknown component before reading the body: the
    // provider must cope with an answer that arrives mid-upload.
    if component != MOCK_CRYPTO_COMPONENT {
        recorded
            .lock()
            .unwrap()
            .crypto_requests
            .push(CryptoRequest {
                operation,
                component: component.clone(),
                body_len: 0,
                chunked,
            });
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "errorCode": "ERR_CRYPTO_PROVIDER_NOT_FOUND",
                "message": format!("crypto provider {component} not found"),
            })),
        )
            .into_response();
    }

    let mut input = Vec::new();
    let mut body = body;
    while let Some(frame) = body.frame().await {
        match frame {
            Ok(frame) => {
                if let Some(data) = frame.data_ref() {
                    input.extend_from_slice(data);
                }
            }
            Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        }
    }

    recorded
        .lock()
        .unwrap()
        .crypto_requests
        .push(CryptoRequest {
            operation: operation.clone(),
            component: component.clone(),
            body_len: input.len(),
            chunked,
        });

    let xor = |bytes: &[u8]| bytes.iter().map(|b| b ^ 0x5a).collect::<Vec<u8>>();
    match operation.as_str() {
        "encrypt" => {
            let Some(key) = header("dapr-key-name") else {
                return (StatusCode::BAD_REQUEST, "missing dapr-key-name").into_response();
            };
            let mut output = MOCK_CRYPTO_MAGIC.to_vec();
            output.extend_from_slice(key.as_bytes());
            output.push(b'\n');
            output.extend(xor(&input));
            (StatusCode::OK, output).into_response()
        }
        "decrypt" => {
            let malformed = || {
                (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"errorCode": "ERR_CRYPTO", "message": "malformed document"})),
                )
                    .into_response()
            };
            let Some(rest) = input.strip_prefix(MOCK_CRYPTO_MAGIC) else {
                return malformed();
            };
            let Some(newline) = rest.iter().position(|b| *b == b'\n') else {
                return malformed();
            };
            (StatusCode::OK, xor(&rest[newline + 1..])).into_response()
        }
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}
