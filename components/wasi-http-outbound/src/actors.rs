//! Actors (client-side) — https://docs.dapr.io/reference/api/actors_api/

use serde_json::json;
use wstd::http::Method;

use crate::exports::actors::{
    ActorStateOperation, ActorStateOperationType, ActorsError, Guest, InvokeActorError, Reminder,
    Timer,
};
use crate::sidecar::{seg, DaprFailure, Sidecar};
use crate::state::value_to_json;
use crate::Component;

/// Map a recoverable failure to the actors error (only permission-denied).
fn actors_error(f: DaprFailure) -> ActorsError {
    ActorsError::PermissionDenied(f.message)
}

/// Map a recoverable failure of an actor invoke. The actor method's error
/// payload is carried as bytes; the failure message captures the HTTP body.
fn invoke_actor_error(f: DaprFailure) -> InvokeActorError {
    InvokeActorError::ActorError(f.message.into_bytes())
}

fn schedule_json(
    due_time: &Option<String>,
    period: &Option<String>,
    ttl: &Option<String>,
    data: &Option<String>,
    callback: Option<&String>,
) -> serde_json::Value {
    let mut object = json!({});
    if let Some(due_time) = due_time {
        object["dueTime"] = json!(due_time);
    }
    if let Some(period) = period {
        object["period"] = json!(period);
    }
    if let Some(ttl) = ttl {
        object["ttl"] = json!(ttl);
    }
    if let Some(data) = data {
        object["data"] =
            serde_json::from_str(data).unwrap_or_else(|e| panic!("data is not valid JSON: {e}"));
    }
    if let Some(callback) = callback {
        object["callback"] = json!(callback);
    }
    object
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
        let path = format!(
            "/v1.0/actors/{}/{}/method/{}",
            seg(&actor_type),
            seg(&actor_id),
            seg(&method)
        );
        let headers = vec![(
            "content-type".to_string(),
            content_type.unwrap_or_else(|| "application/json".to_string()),
        )];
        let response = sidecar
            .expect_success(Method::POST, &path, &headers, body)
            .map_err(invoke_actor_error)?;
        Ok(response.body)
    }

    fn get_state(
        actor_type: String,
        actor_id: String,
        key: String,
    ) -> Result<Option<Vec<u8>>, ActorsError> {
        let sidecar = Sidecar::from_env();
        let path = format!(
            "/v1.0/actors/{}/{}/state/{}",
            seg(&actor_type),
            seg(&actor_id),
            seg(&key)
        );
        let response = sidecar
            .expect_success(Method::GET, &path, &[], Vec::new())
            .map_err(actors_error)?;
        // 204 = key not found.
        if response.status == 204 {
            return Ok(None);
        }
        Ok(Some(response.body))
    }

    fn execute_state_transaction(
        actor_type: String,
        actor_id: String,
        operations: Vec<ActorStateOperation>,
    ) -> Result<(), ActorsError> {
        let sidecar = Sidecar::from_env();
        let path = format!("/v1.0/actors/{}/{}/state", seg(&actor_type), seg(&actor_id));

        let body: Vec<serde_json::Value> = operations
            .iter()
            .map(|op| match op.operation {
                ActorStateOperationType::Upsert => {
                    let value = op
                        .value
                        .as_deref()
                        .map(value_to_json)
                        .unwrap_or(serde_json::Value::Null);
                    json!({
                        "operation": "upsert",
                        "request": { "key": op.key, "value": value },
                    })
                }
                ActorStateOperationType::Delete => json!({
                    "operation": "delete",
                    "request": { "key": op.key },
                }),
            })
            .collect();
        sidecar
            .json(Method::POST, &path, &body)
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
        let path = format!(
            "/v1.0/actors/{}/{}/reminders/{}",
            seg(&actor_type),
            seg(&actor_id),
            seg(&name)
        );
        let body = schedule_json(
            &reminder.due_time,
            &reminder.period,
            &reminder.ttl,
            &reminder.data,
            None,
        );
        sidecar
            .json(Method::POST, &path, &body)
            .map_err(actors_error)?;
        Ok(())
    }

    fn get_reminder(
        actor_type: String,
        actor_id: String,
        name: String,
    ) -> Result<Option<String>, ActorsError> {
        let sidecar = Sidecar::from_env();
        let path = format!(
            "/v1.0/actors/{}/{}/reminders/{}",
            seg(&actor_type),
            seg(&actor_id),
            seg(&name)
        );
        let response = match sidecar.expect_success(Method::GET, &path, &[], Vec::new()) {
            Ok(r) => r,
            Err(f) if f.status == 404 => return Ok(None),
            Err(f) => return Err(actors_error(f)),
        };
        Ok(Some(String::from_utf8_lossy(&response.body).into_owned()))
    }

    fn unregister_reminder(
        actor_type: String,
        actor_id: String,
        name: String,
    ) -> Result<(), ActorsError> {
        let sidecar = Sidecar::from_env();
        let path = format!(
            "/v1.0/actors/{}/{}/reminders/{}",
            seg(&actor_type),
            seg(&actor_id),
            seg(&name)
        );
        sidecar
            .expect_success(Method::DELETE, &path, &[], Vec::new())
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
        let path = format!(
            "/v1.0/actors/{}/{}/timers/{}",
            seg(&actor_type),
            seg(&actor_id),
            seg(&name)
        );
        let body = schedule_json(
            &timer.due_time,
            &timer.period,
            &timer.ttl,
            &timer.data,
            timer.callback.as_ref(),
        );
        sidecar
            .json(Method::POST, &path, &body)
            .map_err(actors_error)?;
        Ok(())
    }

    fn unregister_timer(
        actor_type: String,
        actor_id: String,
        name: String,
    ) -> Result<(), ActorsError> {
        let sidecar = Sidecar::from_env();
        let path = format!(
            "/v1.0/actors/{}/{}/timers/{}",
            seg(&actor_type),
            seg(&actor_id),
            seg(&name)
        );
        sidecar
            .expect_success(Method::DELETE, &path, &[], Vec::new())
            .map_err(actors_error)?;
        Ok(())
    }
}
