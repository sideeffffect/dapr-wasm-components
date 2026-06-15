//! Output bindings — https://docs.dapr.io/reference/api/bindings_api/

use serde_json::json;
use wstd::http::Method;

use crate::exports::bindings::{BindingsError, Guest, InvokeBindingError, InvokeBindingResponse};
use crate::sidecar::{seg, DaprFailure, Sidecar};
use crate::state::value_to_json;
use crate::types::Metadata;
use crate::Component;

/// Map a recoverable failure to the bindings setup/config error.
fn bindings_error(f: DaprFailure) -> BindingsError {
    if f.is_permission() {
        BindingsError::PermissionDenied(f.message)
    } else {
        BindingsError::BindingNotFound(f.message)
    }
}

/// Map a recoverable failure of an invoke-binding.
fn invoke_binding_error(f: DaprFailure) -> InvokeBindingError {
    if f.error_code
        .as_deref()
        .is_some_and(|c| c.contains("NOT_SUPPORTED") || c.contains("UNSUPPORTED"))
        || f.message.contains("not supported")
    {
        InvokeBindingError::OperationNotSupported(f.message)
    } else {
        InvokeBindingError::Bindings(bindings_error(f))
    }
}

impl Guest for Component {
    fn invoke_binding(
        name: String,
        operation: String,
        data: Vec<u8>,
        metadata: Metadata,
    ) -> Result<InvokeBindingResponse, InvokeBindingError> {
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

        let response = sidecar
            .json(Method::POST, &path, &body)
            .map_err(invoke_binding_error)?;
        Ok(InvokeBindingResponse {
            data: response.body,
            metadata: response.headers,
        })
    }
}
