//! Shared gRPC plumbing: sidecar address resolution, the sync-export ↔
//! async-tonic bridge, and mapping of `tonic::Status` to the WIT `error`
//! variant.
//!
//! Each WIT export runs one unary call: build the tonic request, drive the
//! future to completion with `spin_executor::run` (pure `wasi:io` polling —
//! the blocking-sync trick this project also uses in the wasi-http
//! provider, just with Spin's executor instead of wstd's).

use std::future::Future;

use tonic::metadata::MetadataValue;
use wasi_grpc::WasiGrpcEndpoint;

use crate::proto::runtime::dapr_client::DaprClient;
use crate::types::Error;

/// The tonic Dapr client over the wasi-grpc (wasi:http outgoing) transport.
pub type Client = DaprClient<WasiGrpcEndpoint>;

pub struct Sidecar {
    endpoint: http::Uri,
    api_token: Option<String>,
}

impl Sidecar {
    /// Resolve the sidecar address like the Dapr SDKs do:
    /// `DAPR_GRPC_ENDPOINT`, then `http://127.0.0.1:$DAPR_GRPC_PORT`,
    /// then the default `http://127.0.0.1:50001`.
    ///
    /// Note: with Spin's h2c prior knowledge, the authority of this
    /// endpoint must equal `SPIN_OUTBOUND_H2C_PRIOR_KNOWLEDGE` (set on the
    /// Spin host process) byte-for-byte.
    pub fn from_env() -> Result<Self, Error> {
        let endpoint = std::env::var("DAPR_GRPC_ENDPOINT")
            .ok()
            .filter(|v| !v.is_empty())
            .or_else(|| {
                std::env::var("DAPR_GRPC_PORT")
                    .ok()
                    .filter(|v| !v.is_empty())
                    .map(|port| format!("http://127.0.0.1:{port}"))
            })
            .unwrap_or_else(|| "http://127.0.0.1:50001".to_string());
        let endpoint = if endpoint.contains("://") {
            endpoint
        } else {
            format!("http://{endpoint}")
        };
        let endpoint: http::Uri = endpoint
            .parse()
            .map_err(|e| Error::InvalidArgument(format!("invalid sidecar endpoint: {e}")))?;
        Ok(Self {
            endpoint,
            api_token: std::env::var("DAPR_API_TOKEN")
                .ok()
                .filter(|v| !v.is_empty()),
        })
    }

    /// Execute one unary RPC against the sidecar: `message` is wrapped in a
    /// `tonic::Request` (with the `dapr-api-token` metadata when set), and
    /// `call` picks the client method, e.g.
    /// `sidecar.unary(req, |mut c, r| async move { c.get_state(r).await })`.
    pub fn unary<Req, Res, Fut>(
        &self,
        message: Req,
        call: impl FnOnce(Client, tonic::Request<Req>) -> Fut,
    ) -> Result<Res, Error>
    where
        Fut: Future<Output = Result<tonic::Response<Res>, tonic::Status>>,
    {
        let request = self.request(message)?;
        let client = self.client();
        spin_executor::run(call(client, request))
            .map(tonic::Response::into_inner)
            .map_err(status_to_error)
    }

    /// A bare client, for call shapes `unary` does not fit (streaming).
    pub fn client(&self) -> Client {
        DaprClient::new(WasiGrpcEndpoint::new(self.endpoint.clone()))
    }

    /// Wrap a message in a `tonic::Request`, attaching `dapr-api-token`
    /// metadata when the env var is set.
    pub fn request<Req>(&self, message: Req) -> Result<tonic::Request<Req>, Error> {
        let mut request = tonic::Request::new(message);
        if let Some(token) = &self.api_token {
            let value: MetadataValue<_> = token
                .parse()
                .map_err(|e| Error::InvalidArgument(format!("invalid DAPR_API_TOKEN: {e}")))?;
            request.metadata_mut().insert("dapr-api-token", value);
        }
        Ok(request)
    }
}

/// Map a gRPC status to the WIT `error` variant. Transport failures (the
/// sidecar unreachable, no HTTP/2 host support) surface from tonic as
/// `unknown`/`unavailable`/`internal` depending on where they break; codes
/// the sidecar actually sets map 1:1.
pub fn status_to_error(status: tonic::Status) -> Error {
    let message = format!("{}: {}", status.code(), status.message());
    match status.code() {
        tonic::Code::InvalidArgument | tonic::Code::OutOfRange => Error::InvalidArgument(message),
        tonic::Code::NotFound => Error::NotFound(message),
        tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => {
            Error::PermissionDenied(message)
        }
        tonic::Code::Aborted | tonic::Code::AlreadyExists | tonic::Code::FailedPrecondition => {
            Error::Aborted(message)
        }
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded | tonic::Code::Cancelled => {
            Error::Unavailable(message)
        }
        tonic::Code::Internal | tonic::Code::DataLoss | tonic::Code::ResourceExhausted => {
            Error::Internal(message)
        }
        _ => Error::Other(message),
    }
}

/// Convert WIT metadata pairs to the proto `map<string, string>`.
pub fn metadata_map(
    metadata: &crate::types::Metadata,
) -> std::collections::HashMap<String, String> {
    metadata.iter().cloned().collect()
}

/// Convert a proto `map<string, string>` back to WIT metadata pairs,
/// sorted for determinism (proto maps have no order).
pub fn metadata_pairs(map: std::collections::HashMap<String, String>) -> crate::types::Metadata {
    let mut pairs: Vec<(String, String)> = map.into_iter().collect();
    pairs.sort();
    pairs
}

/// proto3 keeps optional strings as `""`; WIT models them as `option`.
pub fn opt_string(value: String) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}
