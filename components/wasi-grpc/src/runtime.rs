//! Sidecar metadata and health over gRPC — `GetMetadata`, `SetMetadata`.
//!
//! daprd registers no gRPC health service on the API port, and the health
//! endpoints are HTTP-only; `GetMetadata` is the conventional gRPC
//! readiness ping (the gRPC API only starts serving once the runtime and
//! outbound building blocks are initialized), so both health functions use
//! it.

use serde_json::json;

use crate::exports::runtime::Guest;
use crate::proto::runtime as pb;
use crate::sidecar::Sidecar;
use crate::types::Error;
use crate::Component;

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

fn get_metadata_pb() -> Result<pb::GetMetadataResponse, Error> {
    let sidecar = Sidecar::from_env()?;
    sidecar.unary(
        pb::GetMetadataRequest {},
        |mut client, request| async move { client.get_metadata(request).await },
    )
}

impl Guest for Component {
    fn get_metadata() -> Result<String, Error> {
        let response = get_metadata_pb()?;
        serde_json::to_string(&metadata_json(response))
            .map_err(|e| Error::Internal(format!("failed to serialize metadata: {e}")))
    }

    fn set_metadata_label(key: String, value: String) -> Result<(), Error> {
        let sidecar = Sidecar::from_env()?;
        sidecar.unary(
            pb::SetMetadataRequest { key, value },
            |mut client, request| async move { client.set_metadata(request).await },
        )?;
        Ok(())
    }

    fn healthz() -> bool {
        get_metadata_pb().is_ok()
    }

    fn outbound_healthz() -> bool {
        get_metadata_pb().is_ok()
    }
}
