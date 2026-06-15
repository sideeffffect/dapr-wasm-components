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

#[derive(Default)]
pub struct Recorded {
    /// (store, key) -> stored JSON value
    pub state: HashMap<(String, String), serde_json::Value>,
    pub published: Vec<PublishedEvent>,
    pub scheduled_jobs: HashMap<String, serde_json::Value>,
    pub metadata_labels: HashMap<String, String>,
    /// Raw converse request bodies, in order.
    pub converse_requests: Vec<serde_json::Value>,
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
                get(|| async { Json(json!({"id": "mock-sidecar"})) }),
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
            .route("/v1.0-alpha1/lock/{store}", post(try_lock))
            .route("/v1.0-alpha1/unlock/{store}", post(unlock))
            .route(
                "/v1.0/invoke/{app}/method/{*path}",
                post(invoke_echo).get(invoke_echo),
            )
            .route(
                "/v1.0/workflows/{component}/{name}/start",
                post(|| async { (StatusCode::ACCEPTED, Json(json!({"instanceID": "wf-123"}))) }),
            )
            .route(
                "/v1.0/jobs/{name}",
                post(schedule_job).get(get_job).delete(delete_job),
            )
            .route(
                "/v1.0-alpha2/conversation/{component}/converse",
                post(converse),
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

async fn get_secret(Path((_store, key)): Path<(String, String)>) -> Response {
    if key == "missing" {
        return StatusCode::NO_CONTENT.into_response();
    }
    Json(json!({"password": "hunter2"})).into_response()
}

/// Record the converse request and return a canned alpha2 response that
/// exercises every result field (choices, tool calls, usage, context id).
async fn converse(
    State(recorded): State<Shared>,
    Path(_component): Path<String>,
    Json(request): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    recorded.lock().unwrap().converse_requests.push(request);
    Json(json!({
        "contextId": "ctx-1",
        "outputs": [{
            "choices": [{
                "finishReason": "tool_calls",
                "index": 0,
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
                "promptTokens": 3,
                "completionTokens": 5,
                "totalTokens": 8,
                "promptTokensDetails": { "audioTokens": 0, "cachedTokens": 2 },
                "completionTokensDetails": {
                    "acceptedPredictionTokens": 0,
                    "audioTokens": 0,
                    "reasoningTokens": 4,
                    "rejectedPredictionTokens": 0
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

async fn delete_job(State(recorded): State<Shared>, Path(name): Path<String>) -> StatusCode {
    recorded.lock().unwrap().scheduled_jobs.remove(&name);
    StatusCode::NO_CONTENT
}
