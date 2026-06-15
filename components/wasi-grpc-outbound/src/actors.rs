//! Actors (client side) over gRPC — `InvokeActor`, `GetActorState`,
//! `ExecuteActorStateTransaction`, `RegisterActorReminder`,
//! `GetActorReminder`, `UnregisterActorReminder`, `RegisterActorTimer`,
//! `UnregisterActorTimer`.
//!
//! daprd quirk: the gRPC reminder/timer registration wraps `data` with
//! `json.Marshal([]byte)`, i.e. base64 — a reminder registered through
//! this provider reaches its callback base64-wrapped on daprd <= 1.18
//! (the HTTP API keeps the JSON verbatim).

use std::collections::HashMap;

use serde_json::json;

use crate::anyjson::unpack_json;
use crate::exports::actors::{
    ActorStateOperation, ActorStateOperationType, ActorsError, GetReminderError, GetStateError,
    Guest, InvokeActorError, Reminder, Timer,
};
use crate::proto::runtime as pb;
use crate::sidecar::{DaprFailure, Sidecar};
use crate::Component;

fn actors_error(f: DaprFailure) -> ActorsError {
    ActorsError::PermissionDenied(f.message)
}

fn invoke_actor_error(f: DaprFailure) -> InvokeActorError {
    InvokeActorError::Actors(actors_error(f))
}

/// Map a recoverable failure of `get-state` through the actors error. The
/// gRPC API cannot signal absence distinctly (no HTTP-204 analogue), so the
/// `key-not-found` case is never produced here.
fn get_state_error(f: DaprFailure) -> GetStateError {
    GetStateError::Actors(actors_error(f))
}

/// Map a recoverable failure of `get-reminder` through the actors error.
/// (The `reminder-not-found` case is produced from the 404 branch.)
fn get_reminder_error(f: DaprFailure) -> GetReminderError {
    GetReminderError::Actors(actors_error(f))
}

fn operation_pb(op: &ActorStateOperation) -> pb::TransactionalActorStateOperation {
    let (operation_type, value) = match op.operation {
        // daprd treats this `Any` opaquely — it unwraps `Any.value` as the
        // raw bytes to store; "type.googleapis.com/bytes" is the SDK
        // convention for that.
        ActorStateOperationType::Upsert => (
            "upsert".to_string(),
            Some(prost_types::Any {
                type_url: "type.googleapis.com/bytes".to_string(),
                // An upsert without a value is a programming error.
                value: op
                    .value
                    .clone()
                    .unwrap_or_else(|| panic!("upsert of {:?} requires a value", op.key)),
            }),
        ),
        ActorStateOperationType::Delete => ("delete".to_string(), None),
    };
    pb::TransactionalActorStateOperation {
        operation_type,
        key: op.key.clone(),
        value,
        metadata: HashMap::new(),
    }
}

impl Guest for Component {
    fn invoke(
        actor_type: String,
        actor_id: String,
        method: String,
        body: Vec<u8>,
        content_type: Option<String>,
    ) -> Result<Vec<u8>, InvokeActorError> {
        let sidecar = Sidecar::from_env();
        // gRPC carries the content type in the request metadata map; daprd
        // reads the key case-sensitively, lowercase. Default like the
        // wasi-http provider does.
        let mut metadata = HashMap::new();
        metadata.insert(
            "content-type".to_string(),
            content_type.unwrap_or_else(|| "application/json".to_string()),
        );
        let response = sidecar
            .unary(
                pb::InvokeActorRequest {
                    actor_type,
                    actor_id,
                    method,
                    data: body,
                    metadata,
                },
                |mut client, request| async move { client.invoke_actor(request).await },
            )
            .map_err(invoke_actor_error)?;
        Ok(response.data)
    }

    fn get_state(
        actor_type: String,
        actor_id: String,
        key: String,
    ) -> Result<Vec<u8>, GetStateError> {
        let sidecar = Sidecar::from_env();
        let response = sidecar
            .unary(
                pb::GetActorStateRequest {
                    actor_type,
                    actor_id,
                    key,
                },
                |mut client, request| async move { client.get_actor_state(request).await },
            )
            .map_err(get_state_error)?;
        // gRPC GetActorState cannot distinguish a missing key from an empty
        // stored value (the HTTP API signals "not found" via 204), so the
        // `key-not-found` case is never produced here; an absent key surfaces
        // as empty data.
        Ok(response.data)
    }

    fn execute_state_transaction(
        actor_type: String,
        actor_id: String,
        operations: Vec<ActorStateOperation>,
    ) -> Result<(), ActorsError> {
        let sidecar = Sidecar::from_env();
        sidecar
            .unary(
                pb::ExecuteActorStateTransactionRequest {
                    actor_type,
                    actor_id,
                    operations: operations.iter().map(operation_pb).collect(),
                },
                |mut client, request| async move {
                    client.execute_actor_state_transaction(request).await
                },
            )
            .map_err(actors_error)?;
        Ok(())
    }

    fn register_reminder(
        actor_type: String,
        actor_id: String,
        name: String,
        reminder: Reminder,
    ) -> Result<(), ActorsError> {
        let sidecar = Sidecar::from_env();
        sidecar
            .unary(
                pb::RegisterActorReminderRequest {
                    actor_type,
                    actor_id,
                    name,
                    due_time: reminder.due_time.unwrap_or_default(),
                    period: reminder.period.unwrap_or_default(),
                    data: reminder.data.map(String::into_bytes).unwrap_or_default(),
                    ttl: reminder.ttl.unwrap_or_default(),
                    overwrite: None,
                    failure_policy: None,
                },
                |mut client, request| async move { client.register_actor_reminder(request).await },
            )
            .map_err(actors_error)?;
        Ok(())
    }

    fn get_reminder(
        actor_type: String,
        actor_id: String,
        name: String,
    ) -> Result<String, GetReminderError> {
        let sidecar = Sidecar::from_env();
        let response = match sidecar.unary(
            pb::GetActorReminderRequest {
                actor_type,
                actor_id,
                name,
            },
            |mut client, request| async move { client.get_actor_reminder(request).await },
        ) {
            Ok(response) => response,
            // A missing reminder maps to the not-found error case.
            Err(f) if f.status == 404 => return Err(GetReminderError::ReminderNotFound),
            Err(f) => return Err(get_reminder_error(f)),
        };
        // Shape the proto response like the HTTP API's reminder document.
        let mut object = json!({});
        if !response.actor_type.is_empty() {
            object["actorType"] = json!(response.actor_type);
        }
        if !response.actor_id.is_empty() {
            object["actorID"] = json!(response.actor_id);
        }
        if let Some(due_time) = response.due_time {
            object["dueTime"] = json!(due_time);
        }
        if let Some(period) = response.period {
            object["period"] = json!(period);
        }
        if let Some(ttl) = response.ttl {
            object["ttl"] = json!(ttl);
        }
        if let Some(data) = &response.data {
            // Embed the payload like the HTTP API does: valid JSON as-is,
            // anything else as a JSON string.
            let text = unpack_json(data);
            object["data"] = serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text));
        }
        Ok(object.to_string())
    }

    fn unregister_reminder(
        actor_type: String,
        actor_id: String,
        name: String,
    ) -> Result<(), ActorsError> {
        let sidecar = Sidecar::from_env();
        sidecar
            .unary(
                pb::UnregisterActorReminderRequest {
                    actor_type,
                    actor_id,
                    name,
                },
                |mut client, request| async move {
                    client.unregister_actor_reminder(request).await
                },
            )
            .map_err(actors_error)?;
        Ok(())
    }

    fn register_timer(
        actor_type: String,
        actor_id: String,
        name: String,
        timer: Timer,
    ) -> Result<(), ActorsError> {
        let sidecar = Sidecar::from_env();
        sidecar
            .unary(
                pb::RegisterActorTimerRequest {
                    actor_type,
                    actor_id,
                    name,
                    due_time: timer.due_time.unwrap_or_default(),
                    period: timer.period.unwrap_or_default(),
                    callback: timer.callback.unwrap_or_default(),
                    data: timer.data.map(String::into_bytes).unwrap_or_default(),
                    ttl: timer.ttl.unwrap_or_default(),
                },
                |mut client, request| async move { client.register_actor_timer(request).await },
            )
            .map_err(actors_error)?;
        Ok(())
    }

    fn unregister_timer(
        actor_type: String,
        actor_id: String,
        name: String,
    ) -> Result<(), ActorsError> {
        let sidecar = Sidecar::from_env();
        sidecar
            .unary(
                pb::UnregisterActorTimerRequest {
                    actor_type,
                    actor_id,
                    name,
                },
                |mut client, request| async move { client.unregister_actor_timer(request).await },
            )
            .map_err(actors_error)?;
        Ok(())
    }
}
