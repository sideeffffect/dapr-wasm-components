//! Pub/sub (publishing side) over gRPC — `PublishEvent`, `BulkPublishEvent`.
//! Event payloads are raw bytes with an explicit content type — no JSON
//! envelope like the HTTP API, so non-JSON events roundtrip byte-exact.
//! The vendored v1.18 protos mark `BulkPublishEventAlpha1` deprecated in
//! favor of the stable `BulkPublishEvent`, so that is what we call.

use crate::exports::pubsub::{
    BulkPublishEntry, BulkPublishFailedEntry, Guest, PublishBulkError, PubsubError,
};
use crate::proto::runtime as pb;
use crate::sidecar::{metadata_map, DaprFailure, Sidecar};
use crate::types::Metadata;
use crate::Component;

fn pubsub_error(f: DaprFailure) -> PubsubError {
    if f.is_permission() {
        PubsubError::PermissionDenied(f.message)
    } else {
        PubsubError::ComponentNotFound(f.message)
    }
}

fn publish_bulk_error(f: DaprFailure) -> PublishBulkError {
    if f.error_code
        .as_deref()
        .is_some_and(|c| c.contains("NOT_SUPPORTED") || c.contains("UNSUPPORTED"))
    {
        PublishBulkError::NotSupported
    } else {
        PublishBulkError::Pubsub(pubsub_error(f))
    }
}

fn entry_pb(entry: BulkPublishEntry) -> pb::BulkPublishRequestEntry {
    pb::BulkPublishRequestEntry {
        entry_id: entry.entry_id,
        event: entry.event,
        content_type: entry.content_type,
        metadata: metadata_map(&entry.metadata),
    }
}

impl Guest for Component {
    fn publish(
        pubsub_name: String,
        topic: String,
        data: Vec<u8>,
        data_content_type: Option<String>,
        metadata: Metadata,
    ) -> Result<(), PubsubError> {
        let sidecar = Sidecar::from_env();
        sidecar
            .unary(
                pb::PublishEventRequest {
                    pubsub_name,
                    topic,
                    data,
                    // Empty string lets daprd apply its default content type.
                    data_content_type: data_content_type.unwrap_or_default(),
                    metadata: metadata_map(&metadata),
                },
                |mut client, request| async move { client.publish_event(request).await },
            )
            .map_err(pubsub_error)?;
        Ok(())
    }

    fn publish_bulk(
        pubsub_name: String,
        topic: String,
        entries: Vec<BulkPublishEntry>,
        metadata: Metadata,
    ) -> Result<Vec<BulkPublishFailedEntry>, PublishBulkError> {
        let sidecar = Sidecar::from_env();
        let response = sidecar
            .unary(
                pb::BulkPublishRequest {
                    pubsub_name,
                    topic,
                    entries: entries.into_iter().map(entry_pb).collect(),
                    metadata: metadata_map(&metadata),
                },
                |mut client, request| async move { client.bulk_publish_event(request).await },
            )
            .map_err(publish_bulk_error)?;
        Ok(response
            .failed_entries
            .into_iter()
            .map(|entry| BulkPublishFailedEntry {
                entry_id: entry.entry_id,
                error: entry.error,
            })
            .collect())
    }
}
