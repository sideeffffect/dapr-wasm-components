//! The gRPC transport: a `tower::Service` that sends tonic requests with
//! `wstd`'s `wasi:http` client.
//!
//! HTTP/2 is the host's job, not this component's. The request carries
//! `content-type: application/grpc`, and the host must send it over HTTP/2:
//! Spin does so for the authority in `SPIN_OUTBOUND_H2C_PRIOR_KNOWLEDGE`,
//! wasmCloud v2 for every `application/grpc` request.
//!
//! The request body is collected before sending (the provider only makes
//! calls whose input is complete up front); the response body streams, and
//! `wstd` passes its trailers through, so tonic sees `grpc-status`.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::BodyExt;

/// The error type of [`WasiGrpcService`].
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Sends each tonic request to `endpoint` (scheme + authority), keeping the
/// request's path.
#[derive(Clone)]
pub struct WasiGrpcService {
    endpoint: http::Uri,
}

impl WasiGrpcService {
    pub fn new(endpoint: http::Uri) -> Self {
        Self { endpoint }
    }
}

impl tower_service::Service<http::Request<tonic::body::Body>> for WasiGrpcService {
    type Response = http::Response<UnsyncBoxBody<bytes::Bytes, Error>>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let endpoint = self.endpoint.clone();
        Box::pin(async move {
            let (mut head, body) = request.into_parts();
            let mut parts = endpoint.into_parts();
            parts.path_and_query = head.uri.path_and_query().cloned();
            head.uri = http::Uri::from_parts(parts)?;

            let body = body.collect().await?.to_bytes();
            let request = http::Request::from_parts(head, wstd::http::Body::from(body));
            let response = wstd::http::Client::new().send(request).await?;

            let (head, body) = response.into_parts();
            let body = UnsyncBoxBody::new(body.into_boxed_body().map_err(Error::from));
            Ok(http::Response::from_parts(head, body))
        })
    }
}
