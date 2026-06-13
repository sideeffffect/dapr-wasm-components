//! Shared HTTP plumbing: sidecar address resolution, request execution,
//! and mapping of HTTP failures to the WIT `error` variant.

use wstd::http::{Body, Client, Method, Request};
use wstd::runtime::block_on;

use crate::types::{Error, Metadata};

/// A response from the sidecar (any status code).
pub struct HttpResult {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

pub struct Sidecar {
    base: String,
    api_token: Option<String>,
}

impl Sidecar {
    /// Resolve the sidecar address like the Dapr SDKs do:
    /// `DAPR_HTTP_ENDPOINT`, then `http://127.0.0.1:$DAPR_HTTP_PORT`,
    /// then the default `http://127.0.0.1:3500`.
    pub fn from_env() -> Self {
        let base = std::env::var("DAPR_HTTP_ENDPOINT")
            .ok()
            .filter(|v| !v.is_empty())
            .or_else(|| {
                std::env::var("DAPR_HTTP_PORT")
                    .ok()
                    .filter(|v| !v.is_empty())
                    .map(|port| format!("http://127.0.0.1:{port}"))
            })
            .unwrap_or_else(|| "http://127.0.0.1:3500".to_string());
        Self {
            base: base.trim_end_matches('/').to_string(),
            api_token: std::env::var("DAPR_API_TOKEN")
                .ok()
                .filter(|v| !v.is_empty()),
        }
    }

    /// Execute a request against the sidecar. `path_and_query` must start
    /// with `/`. Returns the raw response, whatever the status code.
    ///
    /// The sidecar is a localhost hop, but a *transient* connect failure still
    /// happens — daprd resetting a fresh connection under a delivery burst, or
    /// not yet accepting on a port. Such a blip surfaces from `send` as an
    /// error (no response was produced), so we retry it a few times with a
    /// short backoff rather than immediately returning `Unavailable`. Without
    /// this the caller's only recourse is to fail the operation and lean on
    /// redelivery — which, for pub/sub over Redis Streams, Dapr does *not* do
    /// reliably; it flaked the e2e suite. The gRPC provider doesn't need this
    /// (tonic multiplexes one persistent h2 connection).
    ///
    /// Only the connect/transport `send` failure is retried — never an HTTP
    /// response (any status is returned as-is). A reset means the sidecar
    /// never saw a complete request/response, so re-issuing is safe; the few
    /// non-idempotent paths (publish, save) sit behind Dapr's already
    /// at-least-once / CAS semantics.
    pub fn request(
        &self,
        method: Method,
        path_and_query: &str,
        headers: &[(String, String)],
        body: Vec<u8>,
    ) -> Result<HttpResult, Error> {
        const MAX_ATTEMPTS: u32 = 4;
        const BACKOFF: std::time::Duration = std::time::Duration::from_millis(50);

        let url = format!("{}{}", self.base, path_and_query);

        let build_request = || {
            let mut builder = Request::builder().method(method.clone()).uri(&url);
            if let Some(token) = &self.api_token {
                builder = builder.header("dapr-api-token", token);
            }
            for (name, value) in headers {
                builder = builder.header(name.as_str(), value.as_str());
            }
            builder
                .body(Body::from(body.clone()))
                .map_err(|e| Error::InvalidArgument(format!("invalid request for {url}: {e}")))
        };

        block_on(async {
            let mut attempt = 0;
            let response = loop {
                attempt += 1;
                match Client::new().send(build_request()?).await {
                    Ok(response) => break response,
                    Err(_) if attempt < MAX_ATTEMPTS => {
                        wstd::task::sleep((BACKOFF * attempt).into()).await;
                    }
                    Err(e) => {
                        return Err(Error::Unavailable(format!(
                            "cannot reach the Dapr sidecar at {} after {attempt} attempts: {e}",
                            self.base
                        )));
                    }
                }
            };

            let status = response.status().as_u16();
            let headers = response
                .headers()
                .iter()
                .map(|(k, v)| {
                    (
                        k.as_str().to_string(),
                        String::from_utf8_lossy(v.as_bytes()).into_owned(),
                    )
                })
                .collect();
            let mut body = response.into_body();
            let bytes = body
                .contents()
                .await
                .map_err(|e| {
                    Error::Unavailable(format!("failed to read sidecar response body: {e}"))
                })?
                .to_vec();

            Ok(HttpResult {
                status,
                headers,
                body: bytes,
            })
        })
    }

    /// Like `request`, but treats every non-2xx status as a WIT error.
    pub fn expect_success(
        &self,
        method: Method,
        path_and_query: &str,
        headers: &[(String, String)],
        body: Vec<u8>,
    ) -> Result<HttpResult, Error> {
        let result = self.request(method, path_and_query, headers, body)?;
        if result.status / 100 == 2 {
            Ok(result)
        } else {
            Err(status_to_error(result.status, &result.body))
        }
    }

    /// JSON-request convenience: serializes `body`, sets the content type,
    /// and fails on non-2xx.
    pub fn json<T: serde::Serialize>(
        &self,
        method: Method,
        path_and_query: &str,
        body: &T,
    ) -> Result<HttpResult, Error> {
        let bytes = serde_json::to_vec(body)
            .map_err(|e| Error::InvalidArgument(format!("failed to serialize request: {e}")))?;
        self.expect_success(
            method,
            path_and_query,
            &[("content-type".to_string(), "application/json".to_string())],
            bytes,
        )
    }
}

/// Map a non-2xx sidecar status to the WIT `error` variant.
pub fn status_to_error(status: u16, body: &[u8]) -> Error {
    let message = {
        let text = String::from_utf8_lossy(body);
        let text = text.trim();
        if text.is_empty() {
            format!("HTTP {status}")
        } else {
            format!("HTTP {status}: {text}")
        }
    };
    match status {
        400 => Error::InvalidArgument(message),
        401 | 403 => Error::PermissionDenied(message),
        404 => Error::NotFound(message),
        409 | 412 => Error::Aborted(message),
        500..=599 => Error::Internal(message),
        _ => Error::Other(message),
    }
}

/// Append Dapr `metadata.<key>=<value>` pairs to a query string under
/// construction (`parts` holds `key=value` strings, percent-encoded).
pub fn push_metadata_query(parts: &mut Vec<String>, metadata: &Metadata) {
    for (key, value) in metadata {
        parts.push(format!(
            "metadata.{}={}",
            urlencoding::encode(key),
            urlencoding::encode(value)
        ));
    }
}

/// Build a path with an optional query from collected `key=value` parts.
pub fn with_query(path: String, parts: Vec<String>) -> String {
    if parts.is_empty() {
        path
    } else {
        format!("{path}?{}", parts.join("&"))
    }
}

/// Percent-encode a path segment.
pub fn seg(value: &str) -> String {
    urlencoding::encode(value).into_owned()
}
