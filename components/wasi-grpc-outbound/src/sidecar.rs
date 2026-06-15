//! Shared gRPC plumbing: sidecar address resolution, the sync-export ↔
//! async-tonic bridge, and classification of `tonic::Status`.
//!
//! Each WIT export runs one unary call: build the tonic request, drive the
//! future to completion with `spin_executor::run` (pure `wasi:io` polling —
//! the blocking-sync trick this project also uses in the wasi-http
//! provider, just with Spin's executor instead of wstd's).
//!
//! Error model: unrecoverable failures (transport, internal sidecar errors,
//! timeouts, serialization) `panic!` — they are not in any WIT return type
//! and surface to the app as a trap. Recoverable failures are returned as a
//! [`DaprFailure`] the per-block code maps to its own function error type.

use std::future::Future;

use tonic::metadata::MetadataValue;
use wasi_grpc::WasiGrpcEndpoint;

use crate::proto::runtime::dapr_client::DaprClient;

/// The tonic Dapr client over the wasi-grpc (wasi:http outgoing) transport.
pub type Client = DaprClient<WasiGrpcEndpoint>;

/// A recoverable sidecar failure. `status` carries the equivalent HTTP status
/// so the per-block mapping can be shared with the wasi-http provider; the
/// original gRPC code is retained in `message`. Tier-3 codes never produce a
/// `DaprFailure` — they panic in [`classify`].
pub struct DaprFailure {
    pub status: u16,
    pub message: String,
    pub error_code: Option<String>,
}

impl DaprFailure {
    /// Whether this failure is a permission/authorization problem.
    pub fn is_permission(&self) -> bool {
        matches!(self.status, 401 | 403)
            || self
                .error_code
                .as_deref()
                .is_some_and(|c| c.contains("PERMISSION") || c.contains("FORBIDDEN"))
    }
}

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
    ///
    /// An unparseable endpoint is a misconfiguration that cannot be recovered
    /// from per call, so it panics.
    pub fn from_env() -> Self {
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
            .unwrap_or_else(|e| panic!("invalid sidecar endpoint: {e}"));
        Self {
            endpoint,
            api_token: std::env::var("DAPR_API_TOKEN")
                .ok()
                .filter(|v| !v.is_empty()),
        }
    }

    /// Execute one unary RPC against the sidecar: `message` is wrapped in a
    /// `tonic::Request` (with the `dapr-api-token` metadata when set), and
    /// `call` picks the client method, e.g.
    /// `sidecar.unary(req, |mut c, r| async move { c.get_state(r).await })`.
    ///
    /// Tier-3 statuses panic inside [`classify`]; recoverable ones are
    /// returned as a [`DaprFailure`].
    pub fn unary<Req, Res, Fut>(
        &self,
        message: Req,
        call: impl FnOnce(Client, tonic::Request<Req>) -> Fut,
    ) -> Result<Res, DaprFailure>
    where
        Fut: Future<Output = Result<tonic::Response<Res>, tonic::Status>>,
    {
        let request = self.request(message);
        let client = self.client();
        spin_executor::run(call(client, request))
            .map(tonic::Response::into_inner)
            .map_err(classify)
    }

    /// A bare client, for call shapes `unary` does not fit (streaming).
    pub fn client(&self) -> Client {
        DaprClient::new(WasiGrpcEndpoint::new(self.endpoint.clone()))
    }

    /// Wrap a message in a `tonic::Request`, attaching `dapr-api-token`
    /// metadata when the env var is set. An invalid token is a
    /// misconfiguration and panics.
    pub fn request<Req>(&self, message: Req) -> tonic::Request<Req> {
        let mut request = tonic::Request::new(message);
        if let Some(token) = &self.api_token {
            let value: MetadataValue<_> = token
                .parse()
                .unwrap_or_else(|e| panic!("invalid DAPR_API_TOKEN: {e}"));
            request.metadata_mut().insert("dapr-api-token", value);
        }
        request
    }
}

/// Classify a gRPC status. Tier-3 codes (the sidecar unreachable/not ready,
/// an internal sidecar error, a timeout, resource exhaustion, ...) are
/// unrecoverable and `panic!`. Recoverable codes become a [`DaprFailure`]
/// with the equivalent HTTP status, which the per-block code maps to its own
/// error type.
pub fn classify(status: tonic::Status) -> DaprFailure {
    let message = format!("{}: {}", status.code(), status.message());
    use tonic::Code;
    match status.code() {
        Code::Unavailable
        | Code::DeadlineExceeded
        | Code::Cancelled
        | Code::Internal
        | Code::DataLoss
        | Code::ResourceExhausted
        | Code::Unknown
        | Code::Unimplemented => {
            panic!("Dapr sidecar error: {message}");
        }
        code => {
            // NotFound↔404, PermissionDenied/Unauthenticated↔403,
            // Aborted/FailedPrecondition↔409/412, AlreadyExists↔409,
            // InvalidArgument/OutOfRange↔400.
            let http = match code {
                Code::NotFound => 404,
                Code::PermissionDenied | Code::Unauthenticated => 403,
                Code::AlreadyExists | Code::Aborted => 409,
                Code::FailedPrecondition => 412,
                Code::InvalidArgument | Code::OutOfRange => 400,
                _ => 400,
            };
            DaprFailure {
                status: http,
                message,
                error_code: None,
            }
        }
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
