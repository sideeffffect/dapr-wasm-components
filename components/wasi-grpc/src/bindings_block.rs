//! Output bindings over gRPC — `InvokeBinding`.
//! Unlike the HTTP API there is no JSON envelope: the payload travels as
//! raw bytes, and response metadata is the proto map the binding component
//! returned (not transport headers).

use crate::exports::bindings::{Guest, InvokeBindingResponse};
use crate::proto::runtime as pb;
use crate::sidecar::{metadata_map, metadata_pairs, Sidecar};
use crate::types::{Error, Metadata};
use crate::Component;

impl Guest for Component {
    fn invoke_binding(
        name: String,
        operation: String,
        data: Vec<u8>,
        metadata: Metadata,
    ) -> Result<InvokeBindingResponse, Error> {
        let sidecar = Sidecar::from_env()?;
        let response = sidecar.unary(
            pb::InvokeBindingRequest {
                name,
                data,
                metadata: metadata_map(&metadata),
                operation,
            },
            |mut client, request| async move { client.invoke_binding(request).await },
        )?;
        Ok(InvokeBindingResponse {
            data: response.data,
            metadata: metadata_pairs(response.metadata),
        })
    }
}
