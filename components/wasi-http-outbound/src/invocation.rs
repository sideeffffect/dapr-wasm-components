//! Service invocation — https://docs.dapr.io/reference/api/service_invocation_api/

use wstd::http::Method;

use crate::exports::invocation::{Guest, HttpResponse, HttpVerb, InvokeError};
use crate::sidecar::{seg, Sidecar};
use crate::types::Metadata;
use crate::Component;

impl Guest for Component {
    fn invoke(
        app_id: String,
        method_path: String,
        verb: HttpVerb,
        headers: Metadata,
        query: Option<String>,
        body: Vec<u8>,
    ) -> Result<HttpResponse, InvokeError> {
        let sidecar = Sidecar::from_env();
        let method = match verb {
            HttpVerb::Get => Method::GET,
            HttpVerb::Post => Method::POST,
            HttpVerb::Put => Method::PUT,
            HttpVerb::Delete => Method::DELETE,
            HttpVerb::Patch => Method::PATCH,
            HttpVerb::Head => Method::HEAD,
            HttpVerb::Options => Method::OPTIONS,
        };

        // The method path is used as-is (it may contain multiple segments).
        let mut path = format!(
            "/v1.0/invoke/{}/method/{}",
            seg(&app_id),
            method_path.trim_start_matches('/')
        );
        if let Some(query) = query {
            if !query.is_empty() {
                path.push('?');
                path.push_str(&query);
            }
        }

        // A non-2xx status from the target app is a valid response here,
        // so the raw request result is returned without mapping to error.
        let response = sidecar.request(method, &path, &headers, body);
        Ok(HttpResponse {
            status: response.status,
            headers: response.headers,
            body: response.body,
        })
    }
}
