//! Sidecar metadata and health.
//! https://docs.dapr.io/reference/api/metadata_api/
//! https://docs.dapr.io/reference/api/health_api/

use wstd::http::Method;

use crate::exports::runtime::{Guest, RuntimeError};
use crate::sidecar::{seg, DaprFailure, Sidecar};
use crate::Component;

/// Map a recoverable failure to the runtime error (only permission-denied).
fn runtime_error(f: DaprFailure) -> RuntimeError {
    RuntimeError::PermissionDenied(f.message)
}

impl Guest for Component {
    fn get_metadata() -> Result<String, RuntimeError> {
        let sidecar = Sidecar::from_env();
        let response = sidecar
            .expect_success(Method::GET, "/v1.0/metadata", &[], Vec::new())
            .map_err(runtime_error)?;
        Ok(String::from_utf8_lossy(&response.body).into_owned())
    }

    fn set_metadata_label(key: String, value: String) -> Result<(), RuntimeError> {
        let sidecar = Sidecar::from_env();
        let path = format!("/v1.0/metadata/{}", seg(&key));
        sidecar
            .expect_success(
                Method::PUT,
                &path,
                &[("content-type".to_string(), "text/plain".to_string())],
                value.into_bytes(),
            )
            .map_err(runtime_error)?;
        Ok(())
    }

    fn healthz() -> bool {
        let sidecar = Sidecar::from_env();
        sidecar
            .request(Method::GET, "/v1.0/healthz", &[], Vec::new())
            .status
            / 100
            == 2
    }

    fn outbound_healthz() -> bool {
        let sidecar = Sidecar::from_env();
        sidecar
            .request(Method::GET, "/v1.0/healthz/outbound", &[], Vec::new())
            .status
            / 100
            == 2
    }
}
