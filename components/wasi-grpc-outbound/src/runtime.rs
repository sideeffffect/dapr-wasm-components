//! Sidecar metadata and health over gRPC — `GetMetadata`, `SetMetadata`.
//!
//! daprd registers no gRPC health service on the API port, and the health
//! endpoints are HTTP-only; `GetMetadata` is the conventional gRPC
//! readiness ping (the gRPC API only starts serving once the runtime and
//! outbound building blocks are initialized), so both health functions use
//! it.

use serde_json::json;

use crate::exports::runtime::{Guest, RuntimeError};
use crate::proto::runtime as pb;
use crate::sidecar::{DaprFailure, Sidecar};
use crate::Component;

fn runtime_error(f: DaprFailure) -> RuntimeError {
    RuntimeError::PermissionDenied(f.message)
}

/// Render the proto metadata as a JSON document shaped like the HTTP
/// `/v1.0/metadata` response (the WIT contract returns JSON). Covers the
/// commonly used fields; HTTP-only niceties may be missing.
fn metadata_json(response: pb::GetMetadataResponse) -> serde_json::Value {
    let components: Vec<serde_json::Value> = response
        .registered_components
        .iter()
        .map(|c| {
            json!({
                "name": c.name,
                "type": c.r#type,
                "version": c.version,
                "capabilities": c.capabilities,
            })
        })
        .collect();
    let http_endpoints: Vec<serde_json::Value> = response
        .http_endpoints
        .iter()
        .map(|e| json!({ "name": e.name }))
        .collect();
    let subscriptions: Vec<serde_json::Value> = response
        .subscriptions
        .iter()
        .map(|s| {
            json!({
                "pubsubname": s.pubsub_name,
                "topic": s.topic,
                "deadLetterTopic": s.dead_letter_topic,
                "type": s.r#type,
            })
        })
        .collect();
    let mut document = json!({
        "id": response.id,
        "runtimeVersion": response.runtime_version,
        "enabledFeatures": response.enabled_features,
        "components": components,
        "httpEndpoints": http_endpoints,
        "subscriptions": subscriptions,
        "extended": response.extended_metadata,
    });
    if let Some(app) = &response.app_connection_properties {
        document["appConnectionProperties"] = json!({
            "port": app.port,
            "protocol": app.protocol,
            "channelAddress": app.channel_address,
            "maxConcurrency": app.max_concurrency,
        });
    }
    if let Some(actor_runtime) = &response.actor_runtime {
        document["actorRuntime"] = json!({
            "runtimeStatus": actor_runtime.runtime_status,
            "hostReady": actor_runtime.host_ready,
            "placement": actor_runtime.placement,
        });
    }
    if let Some(scheduler) = &response.scheduler {
        document["scheduler"] = json!({
            "connectedAddresses": scheduler.connected_addresses,
        });
    }
    document
}

fn get_metadata_pb() -> Result<pb::GetMetadataResponse, DaprFailure> {
    let sidecar = Sidecar::from_env();
    sidecar.unary(
        pb::GetMetadataRequest {},
        |mut client, request| async move { client.get_metadata(request).await },
    )
}

impl Guest for Component {
    fn get_metadata() -> Result<String, RuntimeError> {
        let response = get_metadata_pb().map_err(runtime_error)?;
        // A metadata document we built ourselves must serialize; a failure is
        // a programming error, not a recoverable one.
        Ok(serde_json::to_string(&metadata_json(response))
            .unwrap_or_else(|e| panic!("failed to serialize metadata: {e}")))
    }

    fn set_metadata_label(key: String, value: String) -> Result<(), RuntimeError> {
        let sidecar = Sidecar::from_env();
        sidecar
            .unary(
                pb::SetMetadataRequest { key, value },
                |mut client, request| async move { client.set_metadata(request).await },
            )
            .map_err(runtime_error)?;
        Ok(())
    }

    fn healthz() -> bool {
        get_metadata_pb().is_ok()
    }

    fn outbound_healthz() -> bool {
        get_metadata_pb().is_ok()
    }
}
