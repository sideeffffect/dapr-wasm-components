//! State management over gRPC — `GetState`, `GetBulkState`, `SaveState`,
//! `DeleteState`, `ExecuteStateTransaction`, `QueryStateAlpha1`.
//! Unlike the HTTP API there is no JSON envelope: values are raw bytes and
//! roundtrip byte-exact.

use crate::exports::state::{
    BulkStateItem, Concurrency, Consistency, GetStateResponse, Guest, QueryResponse, StateItem,
    StateOptions, TransactionOperation, TransactionRequest,
};
use crate::proto::common::{self, state_options};
use crate::proto::runtime as pb;
use crate::sidecar::{metadata_map, opt_string, Sidecar};
use crate::types::{Error, Metadata};
use crate::Component;

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

fn bulk_item(item: pb::BulkStateItem) -> BulkStateItem {
    BulkStateItem {
        key: item.key,
        data: item.data,
        etag: opt_string(item.etag),
        error: opt_string(item.error),
    }
}

impl Guest for Component {
    fn get(
        store_name: String,
        key: String,
        consistency: Option<Consistency>,
        metadata: Metadata,
    ) -> Result<Option<GetStateResponse>, Error> {
        let sidecar = Sidecar::from_env()?;
        let response = sidecar.unary(
            pb::GetStateRequest {
                store_name,
                key,
                consistency: consistency.map(consistency_pb).unwrap_or_default() as i32,
                metadata: metadata_map(&metadata),
            },
            |mut client, request| async move { client.get_state(request).await },
        )?;
        // gRPC GetState reports a missing key as an empty response, not an
        // error: no data and no etag means "not found".
        if response.data.is_empty() && response.etag.is_empty() {
            return Ok(None);
        }
        Ok(Some(GetStateResponse {
            data: response.data,
            etag: opt_string(response.etag),
        }))
    }

    fn get_bulk(
        store_name: String,
        keys: Vec<String>,
        parallelism: Option<u32>,
        metadata: Metadata,
    ) -> Result<Vec<BulkStateItem>, Error> {
        let sidecar = Sidecar::from_env()?;
        let response = sidecar.unary(
            pb::GetBulkStateRequest {
                store_name,
                keys,
                parallelism: parallelism.map(|p| p as i32).unwrap_or_default(),
                metadata: metadata_map(&metadata),
            },
            |mut client, request| async move { client.get_bulk_state(request).await },
        )?;
        Ok(response.items.into_iter().map(bulk_item).collect())
    }

    fn save(store_name: String, items: Vec<StateItem>, metadata: Metadata) -> Result<(), Error> {
        let sidecar = Sidecar::from_env()?;
        sidecar.unary(
            pb::SaveStateRequest {
                store_name,
                states: items.iter().map(|item| item_pb(item, &metadata)).collect(),
            },
            |mut client, request| async move { client.save_state(request).await },
        )?;
        Ok(())
    }

    fn delete(
        store_name: String,
        key: String,
        etag: Option<String>,
        options: Option<StateOptions>,
        metadata: Metadata,
    ) -> Result<(), Error> {
        let sidecar = Sidecar::from_env()?;
        sidecar.unary(
            pb::DeleteStateRequest {
                store_name,
                key,
                etag: etag.map(|value| common::Etag { value }),
                options: options.as_ref().map(options_pb),
                metadata: metadata_map(&metadata),
            },
            |mut client, request| async move { client.delete_state(request).await },
        )?;
        Ok(())
    }

    fn execute_transaction(
        store_name: String,
        operations: Vec<TransactionRequest>,
        metadata: Metadata,
    ) -> Result<(), Error> {
        let sidecar = Sidecar::from_env()?;
        sidecar.unary(
            pb::ExecuteStateTransactionRequest {
                store_name,
                operations: operations
                    .iter()
                    .map(|op| pb::TransactionalStateOperation {
                        operation_type: match op.operation {
                            TransactionOperation::Upsert => "upsert".to_string(),
                            TransactionOperation::Delete => "delete".to_string(),
                        },
                        request: Some(item_pb(&op.item, &Vec::new())),
                    })
                    .collect(),
                metadata: metadata_map(&metadata),
            },
            |mut client, request| async move { client.execute_state_transaction(request).await },
        )?;
        Ok(())
    }

    fn query(
        store_name: String,
        query: String,
        metadata: Metadata,
    ) -> Result<QueryResponse, Error> {
        let sidecar = Sidecar::from_env()?;
        let response = sidecar.unary(
            pb::QueryStateRequest {
                store_name,
                query,
                metadata: metadata_map(&metadata),
            },
            |mut client, request| async move { client.query_state_alpha1(request).await },
        )?;
        Ok(QueryResponse {
            items: response
                .results
                .into_iter()
                .map(|item| BulkStateItem {
                    key: item.key,
                    data: item.data,
                    etag: opt_string(item.etag),
                    error: opt_string(item.error),
                })
                .collect(),
            token: opt_string(response.token),
        })
    }
}
