//! Service invocation over gRPC — `InvokeService`.
//!
//! The WIT contract has HTTP semantics; they map onto the gRPC call like
//! this: verb and query string go into `HTTPExtension`, the `content-type`
//! header becomes the dedicated `InvokeRequest.content_type` field, the
//! remaining request headers travel as gRPC request metadata, and the
//! target app's response headers come back as gRPC initial metadata.
//!
//! Divergence from the wasi-http provider: daprd does not convey the app's
//! exact status code over gRPC. Success is reported as status 200, and a
//! non-2xx app response surfaces as a gRPC error from daprd — returned
//! here as `error`, not as a normal `http-response` like the WIT comment
//! promises for the HTTP transport.

use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue, KeyAndValueRef};

use crate::exports::invocation::{Guest, HttpResponse, HttpVerb};
use crate::proto::common::{self as pbc, http_extension};
use crate::proto::runtime as pb;
use crate::sidecar::{opt_string, status_to_error, Sidecar};
use crate::types::{Error, Metadata};
use crate::Component;

fn verb_pb(verb: HttpVerb) -> http_extension::Verb {
    match verb {
        HttpVerb::Get => http_extension::Verb::Get,
        HttpVerb::Post => http_extension::Verb::Post,
        HttpVerb::Put => http_extension::Verb::Put,
        HttpVerb::Delete => http_extension::Verb::Delete,
        HttpVerb::Patch => http_extension::Verb::Patch,
        HttpVerb::Head => http_extension::Verb::Head,
        HttpVerb::Options => http_extension::Verb::Options,
    }
}

/// Headers of the gRPC transport itself, not of the app's response —
/// dropped so the returned headers describe the invoked app only
/// (`content-type` is replaced by `InvokeResponse.content_type`).
fn is_transport_header(name: &str) -> bool {
    name == "content-type" || name.starts_with("grpc-")
}

impl Guest for Component {
    fn invoke(
        app_id: String,
        method_path: String,
        verb: HttpVerb,
        headers: Metadata,
        query: Option<String>,
        body: Vec<u8>,
    ) -> Result<HttpResponse, Error> {
        let sidecar = Sidecar::from_env()?;

        // `content-type` rides in a dedicated proto field; everything else
        // is passed through as gRPC request metadata.
        let mut content_type = String::new();
        let mut pass_through: Vec<&(String, String)> = Vec::new();
        for header in &headers {
            if header.0.eq_ignore_ascii_case("content-type") {
                content_type = header.1.clone();
            } else {
                pass_through.push(header);
            }
        }

        let message = pb::InvokeServiceRequest {
            id: app_id,
            message: Some(pbc::InvokeRequest {
                method: method_path.trim_start_matches('/').to_string(),
                // An unset type_url makes Dapr treat the value as raw bytes.
                data: Some(prost_types::Any {
                    type_url: String::new(),
                    value: body,
                }),
                content_type,
                http_extension: Some(pbc::HttpExtension {
                    verb: verb_pb(verb) as i32,
                    querystring: query.unwrap_or_default(),
                }),
            }),
        };

        // Not `sidecar.unary`: the app's response headers come back as gRPC
        // initial metadata, which `Response::into_inner` would drop.
        let mut request = sidecar.request(message)?;
        for (name, value) in pass_through {
            let key: AsciiMetadataKey = name.to_ascii_lowercase().parse().map_err(|e| {
                Error::InvalidArgument(format!("header name {name:?} is not valid here: {e}"))
            })?;
            let value: AsciiMetadataValue = value.parse().map_err(|e| {
                Error::InvalidArgument(format!("header {name:?} has a non-ascii value: {e}"))
            })?;
            request.metadata_mut().append(key, value);
        }

        let mut client = sidecar.client();
        let response = spin_executor::run(async move { client.invoke_service(request).await })
            .map_err(status_to_error)?;

        let mut response_headers: Metadata = response
            .metadata()
            .iter()
            .filter_map(|entry| match entry {
                KeyAndValueRef::Ascii(key, value) if !is_transport_header(key.as_str()) => value
                    .to_str()
                    .ok()
                    .map(|v| (key.as_str().to_string(), v.to_string())),
                _ => None,
            })
            .collect();
        let inner = response.into_inner();
        if let Some(content_type) = opt_string(inner.content_type) {
            response_headers.push(("content-type".to_string(), content_type));
        }

        Ok(HttpResponse {
            status: 200,
            headers: response_headers,
            body: inner.data.map(|any| any.value).unwrap_or_default(),
        })
    }
}
