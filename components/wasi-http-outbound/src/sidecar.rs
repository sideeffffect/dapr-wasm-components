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
    pub fn request(
        &self,
        method: Method,
        path_and_query: &str,
        headers: &[(String, String)],
        body: Vec<u8>,
    ) -> Result<HttpResult, Error> {
        let url = format!("{}{}", self.base, path_and_query);

        let mut builder = Request::builder().method(method).uri(&url);
        if let Some(token) = &self.api_token {
            builder = builder.header("dapr-api-token", token);
        }
        for (name, value) in headers {
            builder = builder.header(name.as_str(), value.as_str());
        }
        let request = builder
            .body(Body::from(body))
            .map_err(|e| Error::InvalidArgument(format!("invalid request for {url}: {e}")))?;

        block_on(async {
            let response = Client::new().send(request).await.map_err(|e| {
                Error::Unavailable(format!(
                    "cannot reach the Dapr sidecar at {}: {e}",
                    self.base
                ))
            })?;

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
