//! State management over gRPC — `GetState`, `GetBulkState`, `SaveState`,
//! `DeleteState`, `ExecuteStateTransaction`, `QueryStateAlpha1`.
//! Unlike the HTTP API there is no JSON envelope: values are raw bytes and
//! roundtrip byte-exact.

use crate::exports::state::{
    BulkStateItem, Concurrency, Consistency, GetError, GetStateResponse, Guest, QueryError,
    QueryResponse, QueryStateItem, StateError, StateItem, StateOptions, TransactionError,
    TransactionOperation, TransactionRequest, WriteError,
};
use crate::proto::common::{self, state_options};
use crate::proto::runtime as pb;
use crate::sidecar::{metadata_map, opt_string, DaprFailure, Sidecar};
use crate::types::Metadata;
use crate::Component;

fn state_error(f: DaprFailure) -> StateError {
    if f.is_permission() {
        StateError::PermissionDenied(f.message)
    } else {
        StateError::StoreNotFound(f.message)
    }
}

/// Map a recoverable failure of `get` through the state setup/config error.
/// The gRPC API has no analogue of HTTP 204, so absence is never detected
/// here and the `key-not-found` case is never produced.
fn get_error(f: DaprFailure) -> GetError {
    GetError::State(state_error(f))
}

fn write_error(f: DaprFailure) -> WriteError {
    match f.status {
        409 | 412 => WriteError::EtagMismatch(None),
        _ => WriteError::State(state_error(f)),
    }
}

fn transaction_error(f: DaprFailure) -> TransactionError {
    if matches!(f.status, 409 | 412) {
        return TransactionError::EtagMismatch(None);
    }
    if f.error_code
        .as_deref()
        .is_some_and(|c| c.contains("NOT_SUPPORTED") || c.contains("NOT_TRANSACTIONAL"))
    {
        return TransactionError::NotTransactional;
    }
    TransactionError::State(state_error(f))
}

fn query_error(f: DaprFailure) -> QueryError {
    if f.status == 400 {
        return QueryError::InvalidQuery(f.message);
    }
    QueryError::State(state_error(f))
}

fn concurrency_pb(value: Concurrency) -> state_options::StateConcurrency {
    match value {
        Concurrency::Unspecified => state_options::StateConcurrency::ConcurrencyUnspecified,
        Concurrency::FirstWrite => state_options::StateConcurrency::ConcurrencyFirstWrite,
        Concurrency::LastWrite => state_options::StateConcurrency::ConcurrencyLastWrite,
    }
}

fn consistency_pb(value: Consistency) -> state_options::StateConsistency {
    match value {
        Consistency::Unspecified => state_options::StateConsistency::ConsistencyUnspecified,
        Consistency::Eventual => state_options::StateConsistency::ConsistencyEventual,
        Consistency::Strong => state_options::StateConsistency::ConsistencyStrong,
    }
}

fn options_pb(options: &StateOptions) -> common::StateOptions {
    common::StateOptions {
        concurrency: concurrency_pb(options.concurrency) as i32,
        consistency: consistency_pb(options.consistency) as i32,
    }
}

/// The gRPC `SaveState`/`ExecuteStateTransaction` carry metadata per item,
/// not per request — fold the request-level metadata into each item
/// (item-level entries win).
fn item_pb(item: &StateItem, request_metadata: &Metadata) -> common::StateItem {
    let mut metadata = metadata_map(request_metadata);
    metadata.extend(item.metadata.iter().cloned());
    common::StateItem {
        key: item.key.clone(),
        value: item.value.clone(),
        etag: item.etag.clone().map(|value| common::Etag { value }),
        metadata,
        options: item.options.as_ref().map(options_pb),
    }
}

// Both the proto `BulkStateItem` and `QueryStateItem` carry the bytes under
// `data`; the WIT records name them `value` (bulk) and `data` (query) to
// mirror the Dapr HTTP API's field names, which differ between the two.
fn bulk_item(item: pb::BulkStateItem) -> BulkStateItem {
    BulkStateItem {
        key: item.key,
        value: item.data,
        etag: opt_string(item.etag),
        error: opt_string(item.error),
    }
}

/// Build the proto `StateItem` for one transaction operation. The WIT
/// flattens the operation fields and makes `value` optional (absent for
/// deletes); the proto value field is non-optional, so a delete sends empty
/// bytes — the sidecar ignores the value for delete operations.
fn transaction_item_pb(op: &TransactionRequest) -> common::StateItem {
    common::StateItem {
        key: op.key.clone(),
        value: op.value.clone().unwrap_or_default(),
        etag: op.etag.clone().map(|value| common::Etag { value }),
        metadata: metadata_map(&op.metadata),
        options: op.options.as_ref().map(options_pb),
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
        let response = sidecar
            .unary(
                pb::GetStateRequest {
                    store_name,
                    key,
                    consistency: consistency.map(consistency_pb).unwrap_or_default() as i32,
                    metadata: metadata_map(&metadata),
                },
                |mut client, request| async move { client.get_state(request).await },
            )
            .map_err(get_error)?;
        // gRPC GetState cannot distinguish a missing key from a stored empty
        // value — there is no HTTP-204 analogue — so the `key-not-found` case
        // is never produced here; an absent key surfaces as empty data/etag.
        Ok(GetStateResponse {
            data: response.data,
            etag: opt_string(response.etag),
        })
    }

    fn get_bulk(
        store_name: String,
        keys: Vec<String>,
        parallelism: Option<u32>,
        metadata: Metadata,
    ) -> Result<Vec<BulkStateItem>, StateError> {
        let sidecar = Sidecar::from_env();
        let response = sidecar
            .unary(
                pb::GetBulkStateRequest {
                    store_name,
                    keys,
                    parallelism: parallelism.map(|p| p as i32).unwrap_or_default(),
                    metadata: metadata_map(&metadata),
                },
                |mut client, request| async move { client.get_bulk_state(request).await },
            )
            .map_err(state_error)?;
        Ok(response.items.into_iter().map(bulk_item).collect())
    }

    fn save(
        store_name: String,
        items: Vec<StateItem>,
        metadata: Metadata,
    ) -> Result<(), WriteError> {
        let sidecar = Sidecar::from_env();
        sidecar
            .unary(
                pb::SaveStateRequest {
                    store_name,
                    states: items.iter().map(|item| item_pb(item, &metadata)).collect(),
                },
                |mut client, request| async move { client.save_state(request).await },
            )
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
        sidecar
            .unary(
                pb::DeleteStateRequest {
                    store_name,
                    key,
                    etag: etag.map(|value| common::Etag { value }),
                    options: options.as_ref().map(options_pb),
                    metadata: metadata_map(&metadata),
                },
                |mut client, request| async move { client.delete_state(request).await },
            )
            .map_err(write_error)?;
        Ok(())
    }

    fn execute_transaction(
        store_name: String,
        operations: Vec<TransactionRequest>,
        metadata: Metadata,
    ) -> Result<(), TransactionError> {
        let sidecar = Sidecar::from_env();
        sidecar
            .unary(
                pb::ExecuteStateTransactionRequest {
                    store_name,
                    operations: operations
                        .iter()
                        .map(|op| pb::TransactionalStateOperation {
                            operation_type: match op.operation {
                                TransactionOperation::Upsert => "upsert".to_string(),
                                TransactionOperation::Delete => "delete".to_string(),
                            },
                            request: Some(transaction_item_pb(op)),
                        })
                        .collect(),
                    metadata: metadata_map(&metadata),
                },
                |mut client, request| async move {
                    client.execute_state_transaction(request).await
                },
            )
            .map_err(transaction_error)?;
        Ok(())
    }

    fn query(
        store_name: String,
        query: String,
        metadata: Metadata,
    ) -> Result<QueryResponse, QueryError> {
        let sidecar = Sidecar::from_env();
        let response = sidecar
            .unary(
                pb::QueryStateRequest {
                    store_name,
                    query,
                    metadata: metadata_map(&metadata),
                },
                |mut client, request| async move { client.query_state_alpha1(request).await },
            )
            .map_err(query_error)?;
        Ok(QueryResponse {
            items: response
                .results
                .into_iter()
                .map(|item| QueryStateItem {
                    key: item.key,
                    data: item.data,
                    etag: opt_string(item.etag),
                })
                .collect(),
            token: opt_string(response.token),
        })
    }
}
