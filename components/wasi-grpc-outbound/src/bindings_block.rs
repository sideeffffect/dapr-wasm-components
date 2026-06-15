//! Output bindings over gRPC — `InvokeBinding`.
//! Unlike the HTTP API there is no JSON envelope: the payload travels as
//! raw bytes, and response metadata is the proto map the binding component
//! returned (not transport headers).

use crate::exports::bindings::{BindingsError, Guest, InvokeBindingError, InvokeBindingResponse};
use crate::proto::runtime as pb;
use crate::sidecar::{metadata_map, metadata_pairs, DaprFailure, Sidecar};
use crate::types::Metadata;
use crate::Component;

fn bindings_error(f: DaprFailure) -> BindingsError {
    if f.is_permission() {
        BindingsError::PermissionDenied(f.message)
    } else {
        BindingsError::BindingNotFound(f.message)
    }
}

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
        let response = sidecar
            .unary(
                pb::InvokeBindingRequest {
                    name,
                    data,
                    metadata: metadata_map(&metadata),
                    operation,
                },
                |mut client, request| async move { client.invoke_binding(request).await },
            )
            .map_err(invoke_binding_error)?;
        Ok(InvokeBindingResponse {
            data: response.data,
            metadata: metadata_pairs(response.metadata),
        })
    }
}
