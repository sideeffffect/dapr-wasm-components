//! dapr-wasm-components-wasi-http-inbound
//!
//! A pure-wasm Dapr provider component for the **inbound** direction. It
//! exports `wasi:http/incoming-handler` so the Dapr sidecar's HTTP app
//! channel reaches a composed application, and translates every app-channel
//! request into a typed call on the imported callback interfaces (world
//! `inbound`). The application never sees HTTP.
//!
//! It is a separate component from `dapr-wasm-components-wasi-http-outbound`
//! so the composition graph (`outbound → app → inbound`) stays acyclic: this
//! component imports the callbacks the app exports and instantiates last.
//!
//! Routing (first match wins):
//!
//! | request                                   | callback                              |
//! |-------------------------------------------|---------------------------------------|
//! | `GET /healthz`                            | `health-callback.health-check`        |
//! | `GET /dapr/subscribe`                     | `pubsub-callback.list-topic-subscriptions` |
//! | `GET /dapr/config`                        | `actors-callback.config`              |
//! | `OPTIONS /<input-binding>`                | (binding verification, 200)           |
//! | `POST <pubsub route>`                     | `pubsub-callback.on-topic-event`      |
//! | `POST /<input-binding>`                   | `bindings-callback.on-binding-event`  |
//! | `POST /job/<name>`                        | `jobs-callback.on-job-event`          |
//! | `POST /configuration/<store>/<key>`       | `configuration-callback.on-configuration-event` |
//! | `PUT /actors/<t>/<id>/method/timer/<n>`   | `actors-callback.on-timer`            |
//! | `PUT /actors/<t>/<id>/method/remind/<n>`  | `actors-callback.on-reminder`         |
//! | `PUT /actors/<t>/<id>/method/<m>`         | `actors-callback.on-invoke`           |
//! | `DELETE /actors/<t>/<id>`                 | `actors-callback.deactivate`          |
//! | anything else                             | `invocation-callback.on-invoke`       |

use serde::Deserialize;
use serde_json::{json, Value};
use wstd::http::body::Body;
use wstd::http::{Method, Request, Response, StatusCode};

mod wit {
    wit_bindgen::generate!({
        world: "inbound",
        path: "../wit",
        default_bindings_module: "crate::wit",
    });
}

/// The callback interfaces this provider imports and calls on the app.
pub(crate) use wit::dapr_wasm_components::interfaces as imports;

use imports::invocation::{HttpResponse, HttpVerb};
use imports::types::AppError;
use imports::{
    actors_callback, bindings_callback, configuration_callback, invocation_callback, jobs_callback,
    pubsub_callback,
};

/// Render bytes as a JSON value: a value that already parses as JSON is kept
/// as-is, otherwise it is wrapped as a (UTF-8 lossy) JSON string — the same
/// convention the wasi-http outbound provider uses.
fn value_to_json(value: &[u8]) -> Value {
    serde_json::from_slice(value)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(value).into_owned()))
}

/// The app-channel path Dapr posts every pub/sub delivery to. It is returned
/// as the `route` of every programmatic subscription; the topic and pub/sub
/// name travel inside the CloudEvent, so a single shared route suffices.
const PUBSUB_ROUTE: &str = "/__dapr_wasm_components/pubsub";

#[wstd::http_server]
async fn main(mut request: Request<Body>) -> Result<Response<Body>, wstd::http::Error> {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let query = request.uri().query().map(str::to_string);
    let headers: Vec<(String, String)> = request
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_string(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect();
    let body = request.body_mut().contents().await?.to_vec();

    Ok(route(&method, &path, query, headers, body))
}

fn route(
    method: &Method,
    path: &str,
    query: Option<String>,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
) -> Response<Body> {
    // App health probe.
    if method == Method::GET && path == "/healthz" {
        return match health_check() {
            Ok(()) => text(StatusCode::OK, "ok"),
            Err(error) => text(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("unhealthy: {error:?}"),
            ),
        };
    }

    // Subscription discovery.
    if method == Method::GET && path == "/dapr/subscribe" {
        return json_response(StatusCode::OK, &subscribe_response());
    }

    // Actor runtime configuration discovery.
    if method == Method::GET && path == "/dapr/config" {
        return json_response(StatusCode::OK, &actor_config_response());
    }

    // Input-binding verification: Dapr sends OPTIONS to the binding path at
    // startup; a 2xx means the app consumes it.
    if method == Method::OPTIONS {
        let name = path.trim_start_matches('/');
        if bindings_callback::list_input_bindings()
            .iter()
            .any(|b| b == name)
        {
            return text(StatusCode::OK, "");
        }
        // Otherwise fall through to service invocation (OPTIONS is a valid verb).
    }

    // Pub/sub delivery.
    if method == Method::POST && path == PUBSUB_ROUTE {
        return deliver_topic_event(body);
    }

    // Job trigger.
    if method == Method::POST {
        if let Some(name) = path.strip_prefix("/job/") {
            return deliver_job(decode(name), &headers, body);
        }
    }

    // Configuration update push: `POST /configuration/<store>/<key>`. The
    // sidecar sends one POST per changed key, each carrying the whole update.
    if method == Method::POST {
        if let Some(rest) = path.strip_prefix("/configuration/") {
            if let Some(store) = rest.split('/').next().filter(|s| !s.is_empty()) {
                return deliver_config_update(decode(store), body);
            }
        }
    }

    // Actor hosting.
    if let Some(rest) = path.strip_prefix("/actors/") {
        if let Some(response) = route_actor(method, rest, &body) {
            return response;
        }
    }

    // Input-binding delivery (POST to a declared binding name).
    if method == Method::POST {
        let name = path.trim_start_matches('/');
        if bindings_callback::list_input_bindings()
            .iter()
            .any(|b| b == name)
        {
            return deliver_binding(name.to_string(), &headers, body);
        }
    }

    // Fallback: service invocation.
    deliver_invocation(method, path, query, headers, body)
}

// --- health ---------------------------------------------------------------

fn health_check() -> Result<(), AppError> {
    imports::health_callback::health_check()
}

// --- pub/sub ---------------------------------------------------------------

fn subscribe_response() -> Value {
    let subscriptions = pubsub_callback::list_topic_subscriptions();
    let entries: Vec<Value> = subscriptions
        .iter()
        .map(|sub| {
            let mut entry = json!({
                "pubsubname": sub.pubsub_name,
                "topic": sub.topic,
                "route": PUBSUB_ROUTE,
            });
            if let Some(dlq) = &sub.dead_letter_topic {
                entry["deadLetterTopic"] = json!(dlq);
            }
            if !sub.metadata.is_empty() {
                entry["metadata"] = metadata_object(&sub.metadata);
            }
            entry
        })
        .collect();
    Value::Array(entries)
}

fn deliver_topic_event(body: Vec<u8>) -> Response<Body> {
    let event = match parse_cloud_event(&body) {
        Ok(event) => event,
        // A malformed envelope is unexpected; ask Dapr to drop it.
        Err(error) => {
            eprintln!("dropping unparseable CloudEvent: {error}");
            return json_response(StatusCode::OK, &json!({ "status": "DROP" }));
        }
    };
    let status = match pubsub_callback::on_topic_event(&event) {
        pubsub_callback::TopicEventResponse::Success => "SUCCESS",
        pubsub_callback::TopicEventResponse::Retry => "RETRY",
        pubsub_callback::TopicEventResponse::Drop => "DROP",
    };
    json_response(StatusCode::OK, &json!({ "status": status }))
}

/// Parse a Dapr-delivered CloudEvent (structured JSON content mode) into the
/// typed `topic-event`. The domain payload is returned in `data` as bytes:
/// JSON `data` is re-serialized; `data_base64` is decoded.
fn parse_cloud_event(body: &[u8]) -> Result<pubsub_callback::TopicEvent, String> {
    let value: Value =
        serde_json::from_slice(body).map_err(|e| format!("invalid CloudEvent JSON: {e}"))?;
    let object = value
        .as_object()
        .ok_or_else(|| "CloudEvent is not a JSON object".to_string())?;

    let take_str = |key: &str| object.get(key).and_then(Value::as_str).map(str::to_string);

    let (data, data_content_type) =
        if let Some(b64) = object.get("data_base64").and_then(Value::as_str) {
            (base64_decode(b64)?, take_str("datacontenttype"))
        } else if let Some(data) = object.get("data") {
            let bytes = match data {
                Value::String(text) => text.clone().into_bytes(),
                other => serde_json::to_vec(other).unwrap_or_default(),
            };
            (
                bytes,
                take_str("datacontenttype").or_else(|| Some("application/json".to_string())),
            )
        } else {
            (Vec::new(), take_str("datacontenttype"))
        };

    // Everything that isn't a standard attribute becomes a CloudEvent extension.
    const STANDARD: &[&str] = &[
        "id",
        "source",
        "type",
        "specversion",
        "datacontenttype",
        "subject",
        "time",
        "topic",
        "pubsubname",
        "data",
        "data_base64",
        "path",
        "traceid",
        "traceparent",
        "tracestate",
    ];
    let extensions: Vec<(String, String)> = object
        .iter()
        .filter(|(key, _)| !STANDARD.contains(&key.as_str()))
        .map(|(key, value)| {
            let text = value
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| value.to_string());
            (key.clone(), text)
        })
        .collect();

    Ok(pubsub_callback::TopicEvent {
        id: take_str("id").unwrap_or_default(),
        source: take_str("source").unwrap_or_default(),
        type_: take_str("type").unwrap_or_default(),
        spec_version: take_str("specversion").unwrap_or_else(|| "1.0".to_string()),
        data_content_type,
        data,
        topic: take_str("topic").unwrap_or_default(),
        pubsub_name: take_str("pubsubname").unwrap_or_default(),
        path: take_str("path"),
        extensions,
    })
}

// --- input bindings --------------------------------------------------------

fn deliver_binding(name: String, headers: &[(String, String)], body: Vec<u8>) -> Response<Body> {
    // Component-set metadata arrives as `metadata.<key>` headers.
    let metadata: Vec<(String, String)> = headers
        .iter()
        .filter_map(|(key, value)| {
            key.strip_prefix("metadata.")
                .map(|stripped| (stripped.to_string(), value.clone()))
        })
        .collect();

    let event = bindings_callback::BindingEvent {
        name,
        data: body,
        metadata,
    };
    match bindings_callback::on_binding_event(&event) {
        Ok(response) => binding_response(&response),
        Err(error) => error_response(&error),
    }
}

fn binding_response(response: &bindings_callback::BindingEventResponse) -> Response<Body> {
    // The Dapr input-binding response JSON instructs the sidecar to persist
    // state and/or forward `data` to output bindings. Field names follow the
    // bindings API reference; values use the provider's JSON convention.
    let mut object = serde_json::Map::new();
    if let Some(store) = &response.store_name {
        object.insert("storeName".to_string(), json!(store));
    }
    if !response.states.is_empty() {
        let states: Vec<Value> = response
            .states
            .iter()
            .map(|item| {
                json!({
                    "key": item.key,
                    "value": value_to_json(&item.value),
                })
            })
            .collect();
        object.insert("state".to_string(), Value::Array(states));
    }
    if !response.to.is_empty() {
        object.insert("to".to_string(), json!(response.to));
        object.insert(
            "concurrency".to_string(),
            json!(match response.concurrency {
                bindings_callback::BindingConcurrency::Sequential => "sequential",
                bindings_callback::BindingConcurrency::Parallel => "parallel",
            }),
        );
    }
    if let Some(data) = &response.data {
        object.insert("data".to_string(), value_to_json(data));
    }
    json_response(StatusCode::OK, &Value::Object(object))
}

// --- jobs ------------------------------------------------------------------

fn deliver_job(name: String, headers: &[(String, String)], body: Vec<u8>) -> Response<Body> {
    let content_type = header(headers, "content-type");
    let event = jobs_callback::JobEvent {
        name,
        data: body,
        content_type,
    };
    match jobs_callback::on_job_event(&event) {
        Ok(()) => text(StatusCode::OK, ""),
        Err(error) => error_response(&error),
    }
}

// --- configuration ---------------------------------------------------------

/// Deliver a sidecar configuration-update push to `on-configuration-event`.
/// The body is Dapr's `UpdateEvent`: `{ "id", "items": { <key>: { value,
/// version, metadata } } }` (items is a map keyed by configuration key).
fn deliver_config_update(store_name: String, body: Vec<u8>) -> Response<Body> {
    #[derive(Deserialize, Default)]
    struct ItemJson {
        #[serde(default)]
        value: String,
        #[serde(default)]
        version: String,
        #[serde(default)]
        metadata: std::collections::BTreeMap<String, String>,
    }
    #[derive(Deserialize, Default)]
    struct UpdateJson {
        #[serde(default)]
        id: String,
        #[serde(default)]
        items: std::collections::BTreeMap<String, ItemJson>,
    }

    let raw: UpdateJson = match serde_json::from_slice(&body) {
        Ok(raw) => raw,
        Err(error) => {
            eprintln!("dropping unparseable configuration update: {error}");
            return text(StatusCode::OK, "");
        }
    };

    let items = raw
        .items
        .into_iter()
        .map(|(key, item)| {
            (
                key,
                configuration_callback::ConfigurationItem {
                    value: item.value,
                    version: item.version,
                    metadata: item.metadata.into_iter().collect(),
                },
            )
        })
        .collect();

    let update = configuration_callback::ConfigurationUpdate {
        store_name,
        id: raw.id,
        items,
    };
    configuration_callback::on_configuration_event(&update);
    text(StatusCode::OK, "")
}

// --- actors ----------------------------------------------------------------

/// Route `/actors/<rest>` requests. Returns `None` if `rest` does not match a
/// known actor app-channel shape (so the caller can fall through).
fn route_actor(method: &Method, rest: &str, body: &[u8]) -> Option<Response<Body>> {
    let segments: Vec<String> = rest
        .split('/')
        .filter(|s| !s.is_empty())
        .map(decode)
        .collect();
    if segments.len() < 2 {
        return None;
    }
    let actor_type = segments[0].clone();
    let actor_id = segments[1].clone();

    // DELETE /actors/<type>/<id> — deactivate.
    if method == Method::DELETE && segments.len() == 2 {
        return Some(match actors_callback::deactivate(&actor_type, &actor_id) {
            Ok(()) => text(StatusCode::OK, ""),
            Err(error) => error_response(&error),
        });
    }

    // .../method/...
    if segments.len() >= 4 && segments[2] == "method" {
        match segments[3].as_str() {
            "timer" if segments.len() >= 5 => {
                return Some(fire_timer(actor_type, actor_id, segments[4].clone(), body));
            }
            "remind" if segments.len() >= 5 => {
                return Some(fire_reminder(
                    actor_type,
                    actor_id,
                    segments[4].clone(),
                    body,
                ));
            }
            _ => {
                let method_name = segments[3..].join("/");
                return Some(
                    match actors_callback::on_invoke(&actor_type, &actor_id, &method_name, body) {
                        Ok(data) => bytes(StatusCode::OK, "application/json", data),
                        Err(error) => error_response(&error),
                    },
                );
            }
        }
    }

    None
}

fn fire_timer(actor_type: String, actor_id: String, name: String, body: &[u8]) -> Response<Body> {
    let fields = TimerFields::parse(body);
    let event = actors_callback::ActorTimerEvent {
        actor_type,
        actor_id,
        name,
        due_time: fields.due_time,
        period: fields.period,
        data: fields.data,
        callback: fields.callback,
    };
    match actors_callback::on_timer(&event) {
        Ok(()) => text(StatusCode::OK, ""),
        Err(error) => error_response(&error),
    }
}

fn fire_reminder(
    actor_type: String,
    actor_id: String,
    name: String,
    body: &[u8],
) -> Response<Body> {
    let fields = TimerFields::parse(body);
    let event = actors_callback::ActorReminderEvent {
        actor_type,
        actor_id,
        name,
        due_time: fields.due_time,
        period: fields.period,
        data: fields.data,
    };
    match actors_callback::on_reminder(&event) {
        Ok(()) => text(StatusCode::OK, ""),
        Err(error) => error_response(&error),
    }
}

/// The shared body of a timer/reminder fire: `{dueTime, period, data, callback}`.
struct TimerFields {
    due_time: Option<String>,
    period: Option<String>,
    callback: Option<String>,
    data: Vec<u8>,
}

impl TimerFields {
    fn parse(body: &[u8]) -> Self {
        #[derive(Deserialize, Default)]
        struct Raw {
            #[serde(rename = "dueTime")]
            due_time: Option<String>,
            period: Option<String>,
            callback: Option<String>,
            data: Option<Value>,
        }
        let raw: Raw = serde_json::from_slice(body).unwrap_or_default();
        let data = match raw.data {
            Some(Value::String(text)) => text.into_bytes(),
            Some(other) => serde_json::to_vec(&other).unwrap_or_default(),
            None => Vec::new(),
        };
        TimerFields {
            due_time: raw.due_time,
            period: raw.period,
            callback: raw.callback,
            data,
        }
    }
}

fn actor_config_response() -> Value {
    let config = actors_callback::config();
    let mut object = serde_json::Map::new();
    object.insert("entities".to_string(), json!(config.entities));
    let mut put = |key: &str, value: Option<Value>| {
        if let Some(value) = value {
            object.insert(key.to_string(), value);
        }
    };
    put(
        "actorIdleTimeout",
        config.actor_idle_timeout.map(|v| json!(v)),
    );
    put(
        "actorScanInterval",
        config.actor_scan_interval.map(|v| json!(v)),
    );
    put(
        "drainOngoingCallTimeout",
        config.drain_ongoing_call_timeout.map(|v| json!(v)),
    );
    put(
        "drainRebalancedActors",
        config.drain_rebalanced_actors.map(|v| json!(v)),
    );
    put(
        "remindersStoragePartitions",
        config.reminders_storage_partitions.map(|v| json!(v)),
    );
    if config.reentrancy_enabled.is_some() || config.reentrancy_max_stack_depth.is_some() {
        let mut reentrancy = serde_json::Map::new();
        if let Some(enabled) = config.reentrancy_enabled {
            reentrancy.insert("enabled".to_string(), json!(enabled));
        }
        if let Some(depth) = config.reentrancy_max_stack_depth {
            reentrancy.insert("maxStackDepth".to_string(), json!(depth));
        }
        object.insert("reentrancy".to_string(), Value::Object(reentrancy));
    }
    Value::Object(object)
}

// --- service invocation ----------------------------------------------------

fn deliver_invocation(
    method: &Method,
    path: &str,
    query: Option<String>,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
) -> Response<Body> {
    let content_type = header(&headers, "content-type").unwrap_or_default();
    let request = invocation_callback::InvokeRequest {
        method: path.trim_start_matches('/').to_string(),
        verb: map_verb(method),
        headers,
        query,
        content_type,
        data: body,
    };
    let response = invocation_callback::on_invoke(&request);
    from_http_response(response)
}

fn map_verb(method: &Method) -> HttpVerb {
    match *method {
        Method::GET => HttpVerb::Get,
        Method::POST => HttpVerb::Post,
        Method::PUT => HttpVerb::Put,
        Method::DELETE => HttpVerb::Delete,
        Method::PATCH => HttpVerb::Patch,
        Method::HEAD => HttpVerb::Head,
        Method::OPTIONS => HttpVerb::Options,
        // CONNECT/TRACE and anything else have no WIT verb; treat as POST.
        _ => HttpVerb::Post,
    }
}

fn from_http_response(response: HttpResponse) -> Response<Body> {
    let mut builder = Response::builder().status(response.status);
    for (name, value) in &response.headers {
        builder = builder.header(name, value);
    }
    builder
        .body(Body::from(response.body))
        .unwrap_or_else(|_| text(StatusCode::INTERNAL_SERVER_ERROR, "invalid app response"))
}

// --- helpers ---------------------------------------------------------------

fn header(headers: &[(String, String)], name: &str) -> Option<String> {
    headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.clone())
}

fn metadata_object(metadata: &[(String, String)]) -> Value {
    Value::Object(
        metadata
            .iter()
            .map(|(k, v)| (k.clone(), json!(v)))
            .collect(),
    )
}

fn decode(segment: &str) -> String {
    urlencoding::decode(segment)
        .map(|s| s.into_owned())
        .unwrap_or_else(|_| segment.to_string())
}

fn text(status: StatusCode, message: &str) -> Response<Body> {
    bytes(status, "text/plain", message.as_bytes().to_vec())
}

fn json_response(status: StatusCode, value: &Value) -> Response<Body> {
    bytes(status, "application/json", value.to_string().into_bytes())
}

fn bytes(status: StatusCode, content_type: &str, body: Vec<u8>) -> Response<Body> {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    if let Ok(value) = wstd::http::HeaderValue::from_str(content_type) {
        response.headers_mut().insert("content-type", value);
    }
    response
}

/// Map an `app-error` reported by the app to the HTTP reply the sidecar
/// expects. The app's recoverable failures are no longer typed by HTTP
/// status — an `app-error` carries only a message — so a graceful callback
/// failure is reported as a 500 with that message.
fn error_response(error: &AppError) -> Response<Body> {
    json_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        &json!({ "errorCode": "ERR_APP", "message": error.message }),
    )
}

fn base64_decode(input: &str) -> Result<Vec<u8>, String> {
    // Minimal standard-alphabet base64 decoder (avoids a new dependency).
    fn val(c: u8) -> Result<u8, String> {
        match c {
            b'A'..=b'Z' => Ok(c - b'A'),
            b'a'..=b'z' => Ok(c - b'a' + 26),
            b'0'..=b'9' => Ok(c - b'0' + 52),
            b'+' => Ok(62),
            b'/' => Ok(63),
            _ => Err(format!("invalid base64 character: {c:#x}")),
        }
    }
    let input: Vec<u8> = input.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    let mut out = Vec::with_capacity(input.len() / 4 * 3);
    for chunk in input.chunks(4) {
        let pad = chunk.iter().filter(|&&c| c == b'=').count();
        let mut acc = 0u32;
        for &c in chunk {
            acc = (acc << 6) | if c == b'=' { 0 } else { val(c)? } as u32;
        }
        // Each 4-char group decodes to 3 bytes, minus the number of pad chars.
        let bytes = [(acc >> 16) as u8, (acc >> 8) as u8, acc as u8];
        out.extend_from_slice(&bytes[..3 - pad]);
    }
    Ok(out)
}
