//! State management — https://docs.dapr.io/reference/api/state_api/

use serde::Deserialize;
use serde_json::json;
use wstd::http::Method;

use crate::exports::state::{
    BulkStateItem, Concurrency, Consistency, GetError, GetStateResponse, Guest, QueryError,
    QueryResponse, QueryStateItem, StateError, StateItem, StateOptions, TransactionError,
    TransactionOperation, TransactionRequest, WriteError,
};
use crate::sidecar::{push_metadata_query, seg, with_query, DaprFailure, Sidecar};
use crate::types::Metadata;
use crate::Component;

/// Map a recoverable failure to the common state setup/config error.
fn state_error(failure: DaprFailure) -> StateError {
    if failure.is_permission() {
        StateError::PermissionDenied(failure.message)
    } else {
        StateError::StoreNotFound(failure.message)
    }
}

/// Map a recoverable failure of a `get` to the common state setup/config
/// error wrapped in `get-error`. (The `key-not-found` case is produced from
/// the 204 branch in `get`, not from a `DaprFailure`.)
fn get_error(failure: DaprFailure) -> GetError {
    GetError::State(state_error(failure))
}

/// Map a recoverable failure of a conditional write.
fn write_error(failure: DaprFailure) -> WriteError {
    match failure.status {
        409 | 412 => WriteError::EtagMismatch(None),
        _ => WriteError::State(state_error(failure)),
    }
}

/// Map a recoverable failure of a transaction.
fn transaction_error(failure: DaprFailure) -> TransactionError {
    if matches!(failure.status, 409 | 412) {
        return TransactionError::EtagMismatch(None);
    }
    if failure
        .error_code
        .as_deref()
        .is_some_and(|c| c.contains("NOT_SUPPORTED") || c.contains("NOT_TRANSACTIONAL"))
    {
        return TransactionError::NotTransactional;
    }
    TransactionError::State(state_error(failure))
}

/// Map a recoverable failure of a query.
fn query_error(failure: DaprFailure) -> QueryError {
    if failure.status == 400 {
        return QueryError::InvalidQuery(failure.message);
    }
    QueryError::State(state_error(failure))
}

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

/// Build the `request` object of a transaction operation. `value` is
/// omitted for deletes (the field is `none`).
fn transaction_request_to_json(op: &TransactionRequest) -> serde_json::Value {
    let mut object = json!({ "key": op.key });
    if let Some(value) = &op.value {
        object["value"] = value_to_json(value);
    }
    if let Some(etag) = &op.etag {
        object["etag"] = json!(etag);
    }
    if !op.metadata.is_empty() {
        object["metadata"] = metadata_object(&op.metadata);
    }
    if let Some(options) = &op.options {
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
/// One deserializer accepts either wire field; the two WIT records below
/// keep the API's `value`/`data` naming apart.
#[derive(Deserialize)]
struct StateItemJson {
    key: String,
    #[serde(default, alias = "value")]
    data: Option<serde_json::Value>,
    #[serde(default)]
    etag: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

impl StateItemJson {
    fn bytes(&self) -> Vec<u8> {
        self.data.as_ref().map(json_to_bytes).unwrap_or_default()
    }
}

impl From<StateItemJson> for BulkStateItem {
    fn from(item: StateItemJson) -> Self {
        let value = item.bytes();
        BulkStateItem {
            key: item.key,
            value,
            etag: item.etag,
            error: item.error,
        }
    }
}

impl From<StateItemJson> for QueryStateItem {
    fn from(item: StateItemJson) -> Self {
        let data = item.bytes();
        QueryStateItem {
            key: item.key,
            data,
            etag: item.etag,
        }
    }
}

impl Guest for Component {
    fn get(
        store_name: String,
        key: String,
        consistency: Option<Consistency>,
        metadata: Metadata,
    ) -> Result<GetStateResponse, GetError> {
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

        let response = sidecar
            .expect_success(Method::GET, &path, &[], Vec::new())
            .map_err(get_error)?;
        // 204 = no value stored under the key.
        if response.status == 204 {
            return Err(GetError::KeyNotFound);
        }
        let etag = response
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("etag"))
            .map(|(_, value)| value.trim_matches('"').to_string());
        Ok(GetStateResponse {
            data: response.body,
            etag,
        })
    }

    fn get_bulk(
        store_name: String,
        keys: Vec<String>,
        parallelism: Option<u32>,
        metadata: Metadata,
    ) -> Result<Vec<BulkStateItem>, StateError> {
        let sidecar = Sidecar::from_env();
        let mut query = Vec::new();
        push_metadata_query(&mut query, &metadata);
        let path = with_query(format!("/v1.0/state/{}/bulk", seg(&store_name)), query);

        let mut body = json!({ "keys": keys });
        if let Some(parallelism) = parallelism {
            body["parallelism"] = json!(parallelism);
        }
        let response = sidecar
            .json(Method::POST, &path, &body)
            .map_err(state_error)?;
        let items: Vec<StateItemJson> = serde_json::from_slice(&response.body)
            .unwrap_or_else(|e| panic!("unexpected bulk-get response: {e}"));
        Ok(items.into_iter().map(Into::into).collect())
    }

    fn save(
        store_name: String,
        items: Vec<StateItem>,
        metadata: Metadata,
    ) -> Result<(), WriteError> {
        let sidecar = Sidecar::from_env();
        let mut query = Vec::new();
        push_metadata_query(&mut query, &metadata);
        let path = with_query(format!("/v1.0/state/{}", seg(&store_name)), query);

        let body: Vec<serde_json::Value> = items.iter().map(item_to_json).collect();
        sidecar
            .json(Method::POST, &path, &body)
            .map_err(write_error)?;
        Ok(())
    }

    fn delete(
        store_name: String,
        key: String,
        etag: Option<String>,
        options: Option<StateOptions>,
        metadata: Metadata,
    ) -> Result<(), WriteError> {
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
        sidecar
            .expect_success(Method::DELETE, &path, &headers, Vec::new())
            .map_err(write_error)?;
        Ok(())
    }

    fn execute_transaction(
        store_name: String,
        operations: Vec<TransactionRequest>,
        metadata: Metadata,
    ) -> Result<(), TransactionError> {
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
                    "request": transaction_request_to_json(op),
                })
            })
            .collect();
        let mut body = json!({ "operations": operations });
        if !metadata.is_empty() {
            body["metadata"] = metadata_object(&metadata);
        }
        sidecar
            .json(Method::POST, &path, &body)
            .map_err(transaction_error)?;
        Ok(())
    }

    fn query(
        store_name: String,
        query: String,
        metadata: Metadata,
    ) -> Result<QueryResponse, QueryError> {
        let sidecar = Sidecar::from_env();
        let mut query_params = Vec::new();
        push_metadata_query(&mut query_params, &metadata);
        let path = with_query(
            format!("/v1.0-alpha1/state/{}/query", seg(&store_name)),
            query_params,
        );

        let response = sidecar
            .expect_success(
                Method::POST,
                &path,
                &[("content-type".to_string(), "application/json".to_string())],
                query.into_bytes(),
            )
            .map_err(query_error)?;

        #[derive(Deserialize)]
        struct QueryJson {
            #[serde(default)]
            results: Vec<StateItemJson>,
            #[serde(default)]
            token: Option<String>,
        }
        let parsed: QueryJson = serde_json::from_slice(&response.body)
            .unwrap_or_else(|e| panic!("unexpected query response: {e}"));
        Ok(QueryResponse {
            items: parsed.results.into_iter().map(Into::into).collect(),
            token: parsed.token,
        })
    }
}
