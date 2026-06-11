//! Sidecar metadata and health.
//! https://docs.dapr.io/reference/api/metadata_api/
//! https://docs.dapr.io/reference/api/health_api/

use wstd::http::Method;

use crate::exports::runtime::Guest;
use crate::sidecar::{seg, Sidecar};
use crate::types::Error;
use crate::Component;

impl Guest for Component {
    fn get_metadata() -> Result<String, Error> {
        let sidecar = Sidecar::from_env();
        let response = sidecar.expect_success(Method::GET, "/v1.0/metadata", &[], Vec::new())?;
        Ok(String::from_utf8_lossy(&response.body).into_owned())
    }

    fn set_metadata_label(key: String, value: String) -> Result<(), Error> {
        let sidecar = Sidecar::from_env();
        let path = format!("/v1.0/metadata/{}", seg(&key));
        sidecar.expect_success(
            Method::PUT,
            &path,
            &[("content-type".to_string(), "text/plain".to_string())],
            value.into_bytes(),
        )?;
        Ok(())
    }

    fn healthz() -> bool {
        let sidecar = Sidecar::from_env();
        matches!(
            sidecar.request(Method::GET, "/v1.0/healthz", &[], Vec::new()),
            Ok(response) if response.status / 100 == 2
        )
    }

    fn outbound_healthz() -> bool {
        let sidecar = Sidecar::from_env();
        matches!(
            sidecar.request(Method::GET, "/v1.0/healthz/outbound", &[], Vec::new()),
            Ok(response) if response.status / 100 == 2
        )
    }
}
