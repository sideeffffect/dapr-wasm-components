//! dapr-wasm-components-wasi-grpc-inbound
//!
//! A pure-wasm Dapr provider for the **inbound** direction over **gRPC**: it
//! serves Dapr's `AppCallback` (+ `AppCallbackHealthCheck`) service so a
//! sidecar configured with `--app-protocol grpc` can reach a composed
//! application, and translates each call into a typed call on the imported
//! callback interfaces (world `dapr-inbound`). The application never sees gRPC.
//!
//! The `wasi-grpc` crate is client-only, so the server side is implemented by
//! hand on top of `wasi:http/incoming-handler`: the host (Spin, which accepts
//! inbound h2c) hands us the HTTP/2 request; we parse the gRPC length-prefixed
//! frame, decode the protobuf with the messages generated for the outbound
//! provider, dispatch, and reply with a length-prefixed protobuf body plus the
//! `grpc-status` HTTP/2 **trailer** gRPC requires.
//!
//! Like the wasi-http inbound provider, this is split from the outbound
//! provider so the composition graph `outbound → app → inbound` stays acyclic.

use bytes::Bytes;
use http::HeaderMap;
use http_body_util::{BodyExt, Full};
use prost::Message;
use wstd::http::body::Body;
use wstd::http::{Request, Response, StatusCode};

// Reuse the protobuf messages generated (and checked in) for the outbound
// provider — the same `dapr.proto.*` types, no second codegen.
#[path = "../../wasi-grpc-outbound/src/proto/mod.rs"]
mod proto;

use proto::common as pbc;
use proto::runtime as pb;

mod wit {
    wit_bindgen::generate!({
        world: "dapr-inbound",
        path: "../wit",
        default_bindings_module: "crate::wit",
    });
}

pub(crate) use wit::dapr_wasm_components::interfaces as imports;

use imports::invocation::HttpVerb;
use imports::{
    bindings_callback, health_callback, invocation, invocation_callback, jobs_callback,
    pubsub_callback,
};

#[wstd::http_server]
async fn main(mut request: Request<Body>) -> Result<Response<Body>, wstd::http::Error> {
    let path = request.uri().path().to_string();
    let body = request.body_mut().contents().await?.to_vec();
    Ok(dispatch(&path, &body))
}

/// Route a gRPC call by its `/<package>.<Service>/<Method>` path.
fn dispatch(path: &str, body: &[u8]) -> Response<Body> {
    let payload = match unframe(body) {
        Ok(payload) => payload,
        Err(message) => return grpc_error(GRPC_INTERNAL, &message),
    };
    match path {
        "/dapr.proto.runtime.v1.AppCallback/OnInvoke" => on_invoke(payload),
        "/dapr.proto.runtime.v1.AppCallback/ListTopicSubscriptions" => list_topic_subscriptions(),
        "/dapr.proto.runtime.v1.AppCallback/OnTopicEvent" => on_topic_event(payload),
        "/dapr.proto.runtime.v1.AppCallback/ListInputBindings" => list_input_bindings(),
        "/dapr.proto.runtime.v1.AppCallback/OnBindingEvent" => on_binding_event(payload),
        "/dapr.proto.runtime.v1.AppCallback/OnJobEvent" => on_job_event(payload),
        "/dapr.proto.runtime.v1.AppCallbackHealthCheck/HealthCheck" => health_check(),
        _ => grpc_error(GRPC_UNIMPLEMENTED, &format!("unimplemented method: {path}")),
    }
}

// --- method handlers -------------------------------------------------------

fn on_invoke(payload: &[u8]) -> Response<Body> {
    let request = match pbc::InvokeRequest::decode(payload) {
        Ok(request) => request,
        Err(error) => return grpc_error(GRPC_INTERNAL, &format!("bad InvokeRequest: {error}")),
    };
    let (verb, query) = match &request.http_extension {
        Some(ext) => (
            verb_from_proto(ext.verb),
            non_empty(ext.querystring.clone()),
        ),
        None => (HttpVerb::Post, None),
    };
    let wit_request = invocation_callback::InvokeRequest {
        method: request.method,
        verb,
        headers: Vec::new(),
        query,
        content_type: request.content_type.clone(),
        data: request.data.map(|any| any.value).unwrap_or_default(),
    };
    let response = invocation_callback::on_invoke(&wit_request);
    // gRPC's InvokeResponse cannot carry an HTTP status; a non-2xx app
    // response surfaces as a gRPC error (the documented gRPC gap).
    if response.status / 100 != 2 {
        return grpc_error(
            GRPC_INTERNAL,
            &format!("app returned status {}", response.status),
        );
    }
    let content_type = header(&response, "content-type").unwrap_or(request.content_type);
    grpc_ok(&pbc::InvokeResponse {
        data: Some(prost_types::Any {
            type_url: String::new(),
            value: response.body,
        }),
        content_type,
    })
}

fn list_topic_subscriptions() -> Response<Body> {
    let subscriptions = pubsub_callback::list_topic_subscriptions()
        .into_iter()
        .map(|sub| pb::TopicSubscription {
            pubsub_name: sub.pubsub_name,
            topic: sub.topic,
            metadata: sub.metadata.into_iter().collect(),
            routes: None,
            dead_letter_topic: sub.dead_letter_topic.unwrap_or_default(),
            bulk_subscribe: None,
        })
        .collect();
    grpc_ok(&pb::ListTopicSubscriptionsResponse { subscriptions })
}

fn on_topic_event(payload: &[u8]) -> Response<Body> {
    let event = match pb::TopicEventRequest::decode(payload) {
        Ok(event) => event,
        Err(error) => return grpc_error(GRPC_INTERNAL, &format!("bad TopicEventRequest: {error}")),
    };
    let wit_event = pubsub_callback::TopicEvent {
        id: event.id,
        source: event.source,
        type_: event.r#type,
        spec_version: event.spec_version,
        data_content_type: non_empty(event.data_content_type),
        data: event.data,
        topic: event.topic,
        pubsub_name: event.pubsub_name,
        path: non_empty(event.path),
        extensions: Vec::new(),
    };
    let status = match pubsub_callback::on_topic_event(&wit_event) {
        pubsub_callback::TopicEventResponse::Success => {
            pb::topic_event_response::TopicEventResponseStatus::Success
        }
        pubsub_callback::TopicEventResponse::Retry => {
            pb::topic_event_response::TopicEventResponseStatus::Retry
        }
        pubsub_callback::TopicEventResponse::Drop => {
            pb::topic_event_response::TopicEventResponseStatus::Drop
        }
    };
    grpc_ok(&pb::TopicEventResponse {
        status: status as i32,
    })
}

fn list_input_bindings() -> Response<Body> {
    grpc_ok(&pb::ListInputBindingsResponse {
        bindings: bindings_callback::list_input_bindings(),
    })
}

fn on_binding_event(payload: &[u8]) -> Response<Body> {
    let event = match pb::BindingEventRequest::decode(payload) {
        Ok(event) => event,
        Err(error) => {
            return grpc_error(GRPC_INTERNAL, &format!("bad BindingEventRequest: {error}"))
        }
    };
    let wit_event = bindings_callback::BindingEvent {
        name: event.name,
        data: event.data,
        metadata: event.metadata.into_iter().collect(),
    };
    match bindings_callback::on_binding_event(&wit_event) {
        Ok(response) => grpc_ok(&pb::BindingEventResponse {
            store_name: response.store_name.unwrap_or_default(),
            states: response
                .states
                .into_iter()
                .map(|item| pbc::StateItem {
                    key: item.key,
                    value: item.value,
                    etag: item.etag.map(|value| pbc::Etag { value }),
                    metadata: Default::default(),
                    options: None,
                })
                .collect(),
            to: response.to,
            data: response.data.unwrap_or_default(),
            concurrency: match response.concurrency {
                bindings_callback::BindingConcurrency::Sequential => 0,
                bindings_callback::BindingConcurrency::Parallel => 1,
            },
        }),
        Err(error) => grpc_error(GRPC_INTERNAL, &format!("{error:?}")),
    }
}

fn on_job_event(payload: &[u8]) -> Response<Body> {
    let event = match pb::JobEventRequest::decode(payload) {
        Ok(event) => event,
        Err(error) => return grpc_error(GRPC_INTERNAL, &format!("bad JobEventRequest: {error}")),
    };
    let wit_event = jobs_callback::JobEvent {
        name: event.name,
        data: event.data.map(|any| any.value).unwrap_or_default(),
        content_type: non_empty(event.content_type),
    };
    match jobs_callback::on_job_event(&wit_event) {
        Ok(()) => grpc_ok(&pb::JobEventResponse {}),
        Err(error) => grpc_error(GRPC_INTERNAL, &format!("{error:?}")),
    }
}

fn health_check() -> Response<Body> {
    match health_callback::health_check() {
        Ok(()) => grpc_ok(&pb::HealthCheckResponse {}),
        Err(error) => grpc_error(GRPC_INTERNAL, &format!("{error:?}")),
    }
}

// --- gRPC framing & helpers ------------------------------------------------

// gRPC status codes used here.
const GRPC_INTERNAL: u8 = 13;
const GRPC_UNIMPLEMENTED: u8 = 12;

/// Strip the 5-byte gRPC length-prefix framing (`flag:u8, len:u32-be`) from a
/// unary request body and return the message bytes.
fn unframe(body: &[u8]) -> Result<&[u8], String> {
    if body.is_empty() {
        // An empty body is a zero-length message (e.g. google.protobuf.Empty).
        return Ok(body);
    }
    if body.len() < 5 {
        return Err("gRPC frame shorter than its 5-byte header".to_string());
    }
    if body[0] != 0 {
        return Err("compressed gRPC frames are not supported".to_string());
    }
    let len = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;
    body.get(5..5 + len)
        .ok_or_else(|| "gRPC frame length exceeds body".to_string())
}

/// Frame a message as a single gRPC length-prefixed payload.
fn frame(message: &[u8]) -> Bytes {
    let mut out = Vec::with_capacity(5 + message.len());
    out.push(0); // not compressed
    out.extend_from_slice(&(message.len() as u32).to_be_bytes());
    out.extend_from_slice(message);
    Bytes::from(out)
}

/// A successful unary gRPC response: HTTP 200, `application/grpc`, the framed
/// message, and a `grpc-status: 0` trailer.
fn grpc_ok<M: Message>(message: &M) -> Response<Body> {
    grpc_response(frame(&message.encode_to_vec()), 0, None)
}

/// A gRPC error: HTTP 200, no message, `grpc-status`/`grpc-message` trailers.
fn grpc_error(code: u8, message: &str) -> Response<Body> {
    eprintln!("AppCallback error ({code}): {message}");
    grpc_response(Bytes::new(), code, Some(message.to_string()))
}

fn grpc_response(body: Bytes, status: u8, message: Option<String>) -> Response<Body> {
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", status.to_string().parse().unwrap());
    if let Some(message) = message {
        if let Ok(value) = http::HeaderValue::from_str(&message) {
            trailers.insert("grpc-message", value);
        }
    }
    let body = Body::from_http_body(
        Full::new(body)
            .with_trailers(async move { Some(Ok::<_, std::convert::Infallible>(trailers)) }),
    );
    let mut response = Response::new(body);
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        "content-type",
        http::HeaderValue::from_static("application/grpc"),
    );
    response
}

fn verb_from_proto(verb: i32) -> HttpVerb {
    match pbc::http_extension::Verb::try_from(verb) {
        Ok(pbc::http_extension::Verb::Get) => HttpVerb::Get,
        Ok(pbc::http_extension::Verb::Head) => HttpVerb::Head,
        Ok(pbc::http_extension::Verb::Put) => HttpVerb::Put,
        Ok(pbc::http_extension::Verb::Delete) => HttpVerb::Delete,
        Ok(pbc::http_extension::Verb::Options) => HttpVerb::Options,
        Ok(pbc::http_extension::Verb::Patch) => HttpVerb::Patch,
        // NONE/POST/CONNECT/TRACE → POST (no WIT verb for the last two).
        _ => HttpVerb::Post,
    }
}

fn non_empty(value: String) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

fn header(response: &invocation::HttpResponse, name: &str) -> Option<String> {
    response
        .headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.clone())
}
