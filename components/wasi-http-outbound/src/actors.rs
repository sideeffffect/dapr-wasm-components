//! Actors (client-side) — https://docs.dapr.io/reference/api/actors_api/

use serde_json::json;
use wstd::http::Method;

use crate::exports::actors::{
    ActorStateOperation, ActorStateOperationType, ActorsError, GetReminderError, GetStateError,
    Guest, InvokeActorError, Reminder, Timer,
};
use crate::sidecar::{check_status, dapr_failure, seg, DaprFailure, HttpResult, Sidecar};
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

/// Map a recoverable failure of `get-state` through the actors error.
/// (The `key-not-found` case is produced from the 204 branch.)
fn get_state_error(f: DaprFailure) -> GetStateError {
    GetStateError::Actors(actors_error(f))
}

/// Map a recoverable failure of `get-reminder` through the actors error.
/// (The `reminder-not-found` case is produced from the 404 branch.)
fn get_reminder_error(f: DaprFailure) -> GetReminderError {
    GetReminderError::Actors(actors_error(f))
}

/// Classify the answer to a timer registration. daprd reports a timer for an
/// actor type this app does not host as a generic `ERR_ACTOR_TIMER_CREATE`
/// (HTTP 500) whose message ends in `actor type not registered` — not as a
/// sidecar fault. It is the caller's mistake, and the very condition daprd
/// itself reports for reminders as a 403 `ERR_ACTOR_REMINDER_NON_HOSTED`
/// (permission-denied), so it maps to `permission-denied` with daprd's
/// message. Any other `ERR_ACTOR_TIMER_CREATE` (e.g. the timer storage
/// failing) is a sidecar fault and still traps.
fn register_timer_result(result: HttpResult) -> Result<(), ActorsError> {
    if result.status == 500 {
        let failure = dapr_failure(result.status, &result.body);
        if failure.error_code.as_deref() == Some("ERR_ACTOR_TIMER_CREATE")
            && failure.message.ends_with("actor type not registered")
        {
            return Err(ActorsError::PermissionDenied(failure.message));
        }
    }
    check_status(result).map_err(actors_error)?;
    Ok(())
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
    ) -> Result<Vec<u8>, GetStateError> {
        let sidecar = Sidecar::from_env();
        let path = format!(
            "/v1.0/actors/{}/{}/state/{}",
            seg(&actor_type),
            seg(&actor_id),
            seg(&key)
        );
        let response = sidecar
            .expect_success(Method::GET, &path, &[], Vec::new())
            .map_err(get_state_error)?;
        // 204 = key not found.
        if response.status == 204 {
            return Err(GetStateError::KeyNotFound);
        }
        Ok(response.body)
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
    ) -> Result<String, GetReminderError> {
        let sidecar = Sidecar::from_env();
        let path = format!(
            "/v1.0/actors/{}/{}/reminders/{}",
            seg(&actor_type),
            seg(&actor_id),
            seg(&name)
        );
        let response = match sidecar.expect_success(Method::GET, &path, &[], Vec::new()) {
            Ok(r) => r,
            Err(f) if f.status == 404 => return Err(GetReminderError::ReminderNotFound),
            Err(f) => return Err(get_reminder_error(f)),
        };
        Ok(String::from_utf8_lossy(&response.body).into_owned())
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
        let body = serde_json::to_vec(&body)
            .unwrap_or_else(|e| panic!("failed to serialize request: {e}"));
        let headers = [("content-type".to_string(), "application/json".to_string())];
        register_timer_result(sidecar.request(Method::POST, &path, &headers, body))
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

#[cfg(test)]
mod tests {
    use super::register_timer_result;
    use crate::exports::actors::ActorsError;
    use crate::sidecar::HttpResult;

    fn response(status: u16, body: &str) -> HttpResult {
        HttpResult {
            status,
            headers: Vec::new(),
            body: body.as_bytes().to_vec(),
        }
    }

    #[test]
    fn timer_for_an_unhosted_actor_type_is_permission_denied() {
        // daprd 1.17.7's answer, verbatim, to a timer for an actor type the
        // app does not host.
        let result = register_timer_result(response(
            500,
            r#"{"errorCode":"ERR_ACTOR_TIMER_CREATE","message":"error creating actor timer: can't create timer for actor ProbeActor||probe-1: actor type not registered"}"#,
        ));
        match result {
            Err(ActorsError::PermissionDenied(message)) => assert_eq!(
                message,
                "error creating actor timer: can't create timer for actor ProbeActor||probe-1: actor type not registered"
            ),
            other => panic!("expected permission-denied, got {other:?}"),
        }
    }

    #[test]
    fn timer_registered() {
        assert!(register_timer_result(response(204, "")).is_ok());
    }

    #[test]
    fn other_recoverable_timer_failures_are_permission_denied() {
        let result = register_timer_result(response(
            403,
            r#"{"errorCode":"ERR_ACTOR_TYPE_RESERVED","message":"reserved"}"#,
        ));
        assert!(matches!(result, Err(ActorsError::PermissionDenied(m)) if m == "reserved"));
    }
}
