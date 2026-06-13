//! Pub/sub publishing — https://docs.dapr.io/reference/api/pubsub_api/

use serde::Deserialize;
use serde_json::json;
use wstd::http::Method;

use crate::exports::pubsub::{BulkPublishEntry, BulkPublishFailedEntry, Guest};
use crate::sidecar::{push_metadata_query, seg, with_query, Sidecar};
use crate::state::value_to_json;
use crate::types::{Error, Metadata};
use crate::Component;

impl Guest for Component {
    fn publish(
        pubsub_name: String,
        topic: String,
        data: Vec<u8>,
        data_content_type: Option<String>,
        metadata: Metadata,
    ) -> Result<(), Error> {
        let sidecar = Sidecar::from_env();
        let mut query = Vec::new();
        push_metadata_query(&mut query, &metadata);
        let path = with_query(
            format!("/v1.0/publish/{}/{}", seg(&pubsub_name), seg(&topic)),
            query,
        );

        // Omit the header when absent so the sidecar applies its default.
        let headers = match data_content_type {
            Some(content_type) => vec![("content-type".to_string(), content_type)],
            None => Vec::new(),
        };
        sidecar.expect_success(Method::POST, &path, &headers, data)?;
        Ok(())
    }

    fn publish_bulk(
        pubsub_name: String,
        topic: String,
        entries: Vec<BulkPublishEntry>,
        metadata: Metadata,
    ) -> Result<Vec<BulkPublishFailedEntry>, Error> {
        let sidecar = Sidecar::from_env();
        let mut query = Vec::new();
        push_metadata_query(&mut query, &metadata);
        let path = with_query(
            format!("/v1.0/publish/bulk/{}/{}", seg(&pubsub_name), seg(&topic)),
            query,
        );

        let body: Vec<serde_json::Value> = entries
            .iter()
            .map(|entry| {
                let mut object = json!({
                    "entryId": entry.entry_id,
                    "event": value_to_json(&entry.event),
                    "contentType": entry.content_type,
                });
                if !entry.metadata.is_empty() {
                    object["metadata"] = serde_json::Value::Object(
                        entry
                            .metadata
                            .iter()
                            .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                            .collect(),
                    );
                }
                object
            })
            .collect();

        let result = sidecar.request(
            Method::POST,
            &path,
            &[("content-type".to_string(), "application/json".to_string())],
            serde_json::to_vec(&body)
                .map_err(|e| Error::InvalidArgument(format!("failed to serialize entries: {e}")))?,
        )?;

        // 2xx: everything published. On partial failure the sidecar returns
        // an error status with a failedEntries array.
        if result.status / 100 == 2 {
            return Ok(Vec::new());
        }

        #[derive(Deserialize)]
        struct BulkFailureJson {
            #[serde(default, rename = "failedEntries")]
            failed_entries: Vec<FailedEntryJson>,
        }
        #[derive(Deserialize)]
        struct FailedEntryJson {
            #[serde(rename = "entryId")]
            entry_id: String,
            #[serde(default)]
            error: String,
        }
        if let Ok(parsed) = serde_json::from_slice::<BulkFailureJson>(&result.body) {
            if !parsed.failed_entries.is_empty() {
                return Ok(parsed
                    .failed_entries
                    .into_iter()
                    .map(|entry| BulkPublishFailedEntry {
                        entry_id: entry.entry_id,
                        error: entry.error,
                    })
                    .collect());
            }
        }
        Err(crate::sidecar::status_to_error(result.status, &result.body))
    }
}
