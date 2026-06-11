//! Output bindings — https://docs.dapr.io/reference/api/bindings_api/

use serde_json::json;
use wstd::http::Method;

use crate::exports::bindings::{Guest, InvokeBindingResponse};
use crate::sidecar::{seg, Sidecar};
use crate::state::value_to_json;
use crate::types::{Error, Metadata};
use crate::Component;

impl Guest for Component {
    fn invoke_binding(
        name: String,
        operation: String,
        data: Vec<u8>,
        metadata: Metadata,
    ) -> Result<InvokeBindingResponse, Error> {
        let sidecar = Sidecar::from_env();
        let path = format!("/v1.0/bindings/{}", seg(&name));

        let mut body = json!({
            "operation": operation,
            "data": value_to_json(&data),
        });
        if !metadata.is_empty() {
            body["metadata"] = serde_json::Value::Object(
                metadata
                    .iter()
                    .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                    .collect(),
            );
        }

        let response = sidecar.json(Method::POST, &path, &body)?;
        Ok(InvokeBindingResponse {
            data: response.body,
            metadata: response.headers,
        })
    }
}
