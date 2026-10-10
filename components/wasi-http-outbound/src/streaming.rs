//! Streaming requests to the sidecar over raw `wasi:http` 0.2.
//!
//! [`crate::sidecar::Sidecar::request`] sends a whole body and reads a whole
//! response. The streaming crypto resources need more: the request body is
//! written chunk by chunk across *several* export calls, and the response
//! body is read chunk by chunk across several more. So this module keeps the
//! raw `wasi:http` handles (outgoing body stream, response future, incoming
//! body stream) in the resource and drives them with the blocking
//! `wasi:io` calls — no async executor has to survive between export calls.
//!
//! Ordering: the request is handed to `outgoing-handler.handle` *before* any
//! body byte is written, so the host sends it with a streaming
//! (`transfer-encoding: chunked`) body while the guest writes. The response
//! is awaited only after the body is finished — write-all-then-read, which is
//! what daprd's crypto handlers need (they answer after the input ends).
//!
//! Error model (as in `sidecar.rs`): transport failures and 5xx trap; a
//! non-2xx/non-5xx answer becomes a recoverable [`DaprFailure`]. When the
//! sidecar answers *early* (e.g. an unknown component, before it reads the
//! body), the body stream breaks; the failing `write` then waits for that
//! early answer and returns it.

use std::cell::RefCell;

use wasip2::http::outgoing_handler;
use wasip2::http::types::{
    Fields, FutureIncomingResponse, IncomingBody, IncomingResponse, Method, OutgoingBody,
    OutgoingRequest, Scheme,
};
use wasip2::io::streams::{InputStream, OutputStream, StreamError};

use crate::sidecar::{check_status, DaprFailure, HttpResult, Sidecar};

/// An in-flight `PUT` whose body is still being written.
pub struct StreamingRequest {
    url: String,
    // Field order is drop order: the body stream is a child of the body and
    // must be dropped first.
    stream: Option<OutputStream>,
    body: Option<OutgoingBody>,
    response: FutureIncomingResponse,
    /// Set once the sidecar answered early with a recoverable failure; later
    /// calls return it again.
    failed: RefCell<Option<DaprFailure>>,
}

impl StreamingRequest {
    /// Start a `PUT path` to the sidecar with the given headers and a body
    /// to be written with [`write`](Self::write).
    pub fn put(sidecar: &Sidecar, path: &str, headers: &[(String, String)]) -> Self {
        let url = format!("{}{}", sidecar.base(), path);
        let (scheme, authority, prefix) = split_base(sidecar.base());

        let mut entries: Vec<(String, Vec<u8>)> = headers
            .iter()
            .map(|(name, value)| (name.clone(), value.as_bytes().to_vec()))
            .collect();
        if let Some(token) = sidecar.api_token() {
            entries.push(("dapr-api-token".to_string(), token.as_bytes().to_vec()));
        }
        let fields = Fields::from_list(&entries)
            .unwrap_or_else(|e| panic!("invalid request headers for {url}: {e:?}"));

        let request = OutgoingRequest::new(fields);
        let path_with_query = format!("{prefix}{path}");
        request
            .set_method(&Method::Put)
            .and_then(|()| request.set_scheme(Some(&scheme)))
            .and_then(|()| request.set_authority(Some(&authority)))
            .and_then(|()| request.set_path_with_query(Some(&path_with_query)))
            .unwrap_or_else(|()| panic!("invalid request for {url}"));

        let body = request
            .body()
            .unwrap_or_else(|()| panic!("request body for {url} already taken"));
        let stream = body
            .write()
            .unwrap_or_else(|()| panic!("request body stream for {url} already taken"));
        // Send the request now, so the host streams the body while we write it.
        let response = outgoing_handler::handle(request, None)
            .unwrap_or_else(|e| panic!("cannot send request to the Dapr sidecar at {url}: {e:?}"));

        Self {
            url,
            stream: Some(stream),
            body: Some(body),
            response,
            failed: RefCell::new(None),
        }
    }

    /// Append `data` to the request body. Blocks while the host's buffer is
    /// full (back-pressure from the sidecar).
    pub fn write(&self, mut data: &[u8]) -> Result<(), DaprFailure> {
        if let Some(failure) = self.failed.borrow().as_ref() {
            return Err(failure.clone());
        }
        let stream = self
            .stream
            .as_ref()
            .unwrap_or_else(|| panic!("write after finish on {}", self.url));
        while !data.is_empty() {
            // The sidecar may answer before it reads the whole body (e.g. an
            // unknown component). Stop uploading as soon as an answer is
            // there, before the connection is torn down and the answer lost.
            if self.response.subscribe().ready() {
                return Err(self.early_answer("answered during upload"));
            }
            let permitted = match stream.check_write() {
                Ok(0) => {
                    stream.subscribe().block();
                    continue;
                }
                Ok(n) => usize::try_from(n).unwrap_or(usize::MAX).min(data.len()),
                Err(e) => return Err(self.early_failure(e)),
            };
            if let Err(e) = stream.write(&data[..permitted]) {
                return Err(self.early_failure(e));
            }
            data = &data[permitted..];
        }
        Ok(())
    }

    /// End the request body and wait for the response. A 2xx yields a
    /// [`StreamingResponse`] to read the body from.
    pub fn finish(mut self) -> Result<StreamingResponse, DaprFailure> {
        if let Some(failure) = self.failed.take() {
            return Err(failure);
        }
        let stream = self.stream.take().expect("stream present until finish");
        let flushed = stream.blocking_flush();
        drop(stream);
        if let Err(e) = flushed {
            return Err(self.early_failure(e));
        }
        let body = self.body.take().expect("body present until finish");
        if let Err(e) = OutgoingBody::finish(body, None) {
            // The sidecar may already have answered; prefer its answer.
            return Err(self.answer_or_panic(&format!("{e:?}")));
        }

        let response = self.await_response();
        let status = response.status();
        if !(200..300).contains(&status) {
            let headers = header_pairs(&response);
            let body = read_all(&response, &self.url);
            return Err(
                match check_status(HttpResult {
                    status,
                    headers,
                    body,
                }) {
                    Err(failure) => failure,
                    Ok(_) => unreachable!("non-2xx status classified as success"),
                },
            );
        }
        let body = response
            .consume()
            .unwrap_or_else(|()| panic!("response body for {} already taken", self.url));
        let stream = body
            .stream()
            .unwrap_or_else(|()| panic!("response body stream for {} already taken", self.url));
        Ok(StreamingResponse {
            url: self.url,
            stream: RefCell::new(Some(stream)),
            _body: body,
            _response: response,
        })
    }

    /// The body stream failed: the sidecar has answered (or the connection
    /// broke). Return its recoverable failure, or trap.
    fn early_failure(&self, error: StreamError) -> DaprFailure {
        let detail = match error {
            StreamError::Closed => "stream closed".to_string(),
            StreamError::LastOperationFailed(e) => e.to_debug_string(),
        };
        self.early_answer(&detail)
    }

    /// Record and return the sidecar's early (recoverable) answer, or trap.
    fn early_answer(&self, detail: &str) -> DaprFailure {
        let failure = self.answer_or_panic(detail);
        *self.failed.borrow_mut() = Some(failure.clone());
        failure
    }

    fn answer_or_panic(&self, detail: &str) -> DaprFailure {
        let response = self.await_response();
        let status = response.status();
        let headers = header_pairs(&response);
        let body = read_all(&response, &self.url);
        match check_status(HttpResult {
            status,
            headers,
            body,
        }) {
            Err(failure) => failure,
            Ok(_) => panic!(
                "Dapr sidecar answered {status} to {} before the request body ended ({detail})",
                self.url
            ),
        }
    }

    fn await_response(&self) -> IncomingResponse {
        self.response.subscribe().block();
        match self.response.get() {
            Some(Ok(Ok(response))) => response,
            Some(Ok(Err(e))) => panic!("request to the Dapr sidecar at {} failed: {e:?}", self.url),
            Some(Err(())) => panic!("response from {} already consumed", self.url),
            None => panic!("response from {} not ready after block", self.url),
        }
    }
}

/// The body of a 2xx response, read chunk by chunk.
pub struct StreamingResponse {
    url: String,
    // Field order is drop order: stream, then body, then response.
    stream: RefCell<Option<InputStream>>,
    _body: IncomingBody,
    _response: IncomingResponse,
}

impl StreamingResponse {
    /// The next non-empty chunk of at most `max_len` bytes, or `None` at the
    /// end of the body.
    pub fn read(&self, max_len: u64) -> Option<Vec<u8>> {
        let mut slot = self.stream.borrow_mut();
        let stream = slot.as_ref()?;
        loop {
            match stream.blocking_read(max_len.max(1)) {
                Ok(bytes) if bytes.is_empty() => continue,
                Ok(bytes) => return Some(bytes),
                Err(StreamError::Closed) => {
                    *slot = None;
                    return None;
                }
                Err(StreamError::LastOperationFailed(e)) => panic!(
                    "failed to read sidecar response body from {}: {}",
                    self.url,
                    e.to_debug_string()
                ),
            }
        }
    }
}

/// Read a whole (error) response body.
fn read_all(response: &IncomingResponse, url: &str) -> Vec<u8> {
    let Ok(body) = response.consume() else {
        return Vec::new();
    };
    let stream = body
        .stream()
        .unwrap_or_else(|()| panic!("response body stream for {url} already taken"));
    let mut bytes = Vec::new();
    loop {
        match stream.blocking_read(64 * 1024) {
            Ok(chunk) => bytes.extend_from_slice(&chunk),
            Err(StreamError::Closed) => break,
            Err(StreamError::LastOperationFailed(e)) => panic!(
                "failed to read sidecar response body from {url}: {}",
                e.to_debug_string()
            ),
        }
    }
    drop(stream);
    drop(body);
    bytes
}

fn header_pairs(response: &IncomingResponse) -> Vec<(String, String)> {
    response
        .headers()
        .entries()
        .into_iter()
        .map(|(name, value)| (name, String::from_utf8_lossy(&value).into_owned()))
        .collect()
}

/// Split `scheme://authority[/prefix]` into its parts.
fn split_base(base: &str) -> (Scheme, String, String) {
    let (scheme, rest) = match base.split_once("://") {
        Some(("http", rest)) => (Scheme::Http, rest),
        Some(("https", rest)) => (Scheme::Https, rest),
        Some((other, rest)) => (Scheme::Other(other.to_string()), rest),
        None => (Scheme::Http, base),
    };
    match rest.split_once('/') {
        Some((authority, prefix)) => (
            scheme,
            authority.to_string(),
            format!("/{}", prefix.trim_end_matches('/')),
        ),
        None => (scheme, rest.to_string(), String::new()),
    }
}
