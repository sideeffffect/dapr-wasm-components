//! State management — https://docs.dapr.io/reference/api/state_api/

use serde::Deserialize;
use serde_json::json;
use wstd::http::Method;

use crate::exports::state::{
    BulkStateItem, Concurrency, Consistency, GetStateResponse, Guest, QueryResponse, StateItem,
    StateOptions, TransactionOperation, TransactionRequest,
};
use crate::sidecar::{push_metadata_query, seg, with_query, Sidecar};
use crate::types::{Error, Metadata};
use crate::Component;

fn concurrency_str(value: Concurrency) -> Option<&'static str> {
    match value {
        Concurrency::Unspecified => None,
        Concurrency::FirstWrite => Some("first-write"),
        Concurrency::LastWrite => Some("last-write"),
    }
}

fn consistency_str(value: Consistency) -> Option<&'static str> {
    match value {
        Consistency::Unspecified => None,
        Consistency::Eventual => Some("eventual"),
        Consistency::Strong => Some("strong"),
    }
}

/// The HTTP state API carries values as JSON. Bytes that parse as JSON are
/// embedded as-is; anything else is embedded as a JSON string (UTF-8 lossy).
pub fn value_to_json(value: &[u8]) -> serde_json::Value {
    serde_json::from_slice(value)
        .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(value).into_owned()))
}

fn item_to_json(item: &StateItem) -> serde_json::Value {
    let mut object = json!({
        "key": item.key,
        "value": value_to_json(&item.value),
    });
    if let Some(etag) = &item.etag {
        object["etag"] = json!(etag);
    }
    if !item.metadata.is_empty() {
        object["metadata"] = metadata_object(&item.metadata);
    }
    if let Some(options) = &item.options {
        object["options"] = options_to_json(options);
    }
    object
}

fn options_to_json(options: &StateOptions) -> serde_json::Value {
    let mut object = json!({});
    if let Some(concurrency) = concurrency_str(options.concurrency) {
        object["concurrency"] = json!(concurrency);
    }
    if let Some(consistency) = consistency_str(options.consistency) {
        object["consistency"] = json!(consistency);
    }
    object
}

fn metadata_object(metadata: &Metadata) -> serde_json::Value {
    serde_json::Value::Object(
        metadata
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
            .collect(),
    )
}

/// JSON values come back as documents; strings are returned unquoted so a
/// stored text value roundtrips byte-for-byte.
fn json_to_bytes(value: &serde_json::Value) -> Vec<u8> {
    match value {
        serde_json::Value::String(text) => text.clone().into_bytes(),
        other => serde_json::to_vec(other).unwrap_or_default(),
    }
}

/// Bulk-get items carry the value under `value`; query results use `data`.
#[derive(Deserialize)]
struct BulkItemJson {
    key: String,
    #[serde(default, alias = "value")]
    data: Option<serde_json::Value>,
    #[serde(default)]
    etag: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

impl From<BulkItemJson> for BulkStateItem {
    fn from(item: BulkItemJson) -> Self {
        BulkStateItem {
            key: item.key,
            data: item.data.as_ref().map(json_to_bytes).unwrap_or_default(),
            etag: item.etag,
            error: item.error,
        }
    }
}

impl Guest for Component {
    fn get(
        store_name: String,
        key: String,
        consistency: Option<Consistency>,
        metadata: Metadata,
    ) -> Result<Option<GetStateResponse>, Error> {
        let sidecar = Sidecar::from_env();
        let mut query = Vec::new();
        if let Some(consistency) = consistency.and_then(consistency_str) {
            query.push(format!("consistency={consistency}"));
        }
        push_metadata_query(&mut query, &metadata);
        let path = with_query(
            format!("/v1.0/state/{}/{}", seg(&store_name), seg(&key)),
            query,
        );

        let response = sidecar.expect_success(Method::GET, &path, &[], Vec::new())?;
        if response.status == 204 {
            return Ok(None);
        }
        let etag = response
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("etag"))
            .map(|(_, value)| value.trim_matches('"').to_string());
        Ok(Some(GetStateResponse {
            data: response.body,
            etag,
        }))
    }

    fn get_bulk(
        store_name: String,
        keys: Vec<String>,
        parallelism: Option<u32>,
        metadata: Metadata,
    ) -> Result<Vec<BulkStateItem>, Error> {
        let sidecar = Sidecar::from_env();
        let mut query = Vec::new();
        push_metadata_query(&mut query, &metadata);
        let path = with_query(format!("/v1.0/state/{}/bulk", seg(&store_name)), query);

        let mut body = json!({ "keys": keys });
        if let Some(parallelism) = parallelism {
            body["parallelism"] = json!(parallelism);
        }
        let response = sidecar.json(Method::POST, &path, &body)?;
        let items: Vec<BulkItemJson> = serde_json::from_slice(&response.body)
            .map_err(|e| Error::Internal(format!("unexpected bulk-get response: {e}")))?;
        Ok(items.into_iter().map(Into::into).collect())
    }

    fn save(store_name: String, items: Vec<StateItem>, metadata: Metadata) -> Result<(), Error> {
        let sidecar = Sidecar::from_env();
        let mut query = Vec::new();
        push_metadata_query(&mut query, &metadata);
        let path = with_query(format!("/v1.0/state/{}", seg(&store_name)), query);

        let body: Vec<serde_json::Value> = items.iter().map(item_to_json).collect();
        sidecar.json(Method::POST, &path, &body)?;
        Ok(())
    }

    fn delete(
        store_name: String,
        key: String,
        etag: Option<String>,
        options: Option<StateOptions>,
        metadata: Metadata,
    ) -> Result<(), Error> {
        let sidecar = Sidecar::from_env();
        let mut query = Vec::new();
        if let Some(options) = &options {
            if let Some(concurrency) = concurrency_str(options.concurrency) {
                query.push(format!("concurrency={concurrency}"));
            }
            if let Some(consistency) = consistency_str(options.consistency) {
                query.push(format!("consistency={consistency}"));
            }
        }
        push_metadata_query(&mut query, &metadata);
        let path = with_query(
            format!("/v1.0/state/{}/{}", seg(&store_name), seg(&key)),
            query,
        );

        let headers = match etag {
            Some(etag) => vec![("if-match".to_string(), etag)],
            None => Vec::new(),
        };
        sidecar.expect_success(Method::DELETE, &path, &headers, Vec::new())?;
        Ok(())
    }

    fn execute_transaction(
        store_name: String,
        operations: Vec<TransactionRequest>,
        metadata: Metadata,
    ) -> Result<(), Error> {
        let sidecar = Sidecar::from_env();
        let path = format!("/v1.0/state/{}/transaction", seg(&store_name));

        let operations: Vec<serde_json::Value> = operations
            .iter()
            .map(|op| {
                json!({
                    "operation": match op.operation {
                        TransactionOperation::Upsert => "upsert",
                        TransactionOperation::Delete => "delete",
                    },
                    "request": item_to_json(&op.item),
                })
            })
            .collect();
        let mut body = json!({ "operations": operations });
        if !metadata.is_empty() {
            body["metadata"] = metadata_object(&metadata);
        }
        sidecar.json(Method::POST, &path, &body)?;
        Ok(())
    }

    fn query(
        store_name: String,
        query: String,
        metadata: Metadata,
    ) -> Result<QueryResponse, Error> {
        let sidecar = Sidecar::from_env();
        let mut query_params = Vec::new();
        push_metadata_query(&mut query_params, &metadata);
        let path = with_query(
            format!("/v1.0-alpha1/state/{}/query", seg(&store_name)),
            query_params,
        );

        let response = sidecar.expect_success(
            Method::POST,
            &path,
            &[("content-type".to_string(), "application/json".to_string())],
            query.into_bytes(),
        )?;

        #[derive(Deserialize)]
        struct QueryJson {
            #[serde(default)]
            results: Vec<BulkItemJson>,
            #[serde(default)]
            token: Option<String>,
        }
        let parsed: QueryJson = serde_json::from_slice(&response.body)
            .map_err(|e| Error::Internal(format!("unexpected query response: {e}")))?;
        Ok(QueryResponse {
            items: parsed.results.into_iter().map(Into::into).collect(),
            token: parsed.token,
        })
    }
}
