//! Actors (client-side) — https://docs.dapr.io/reference/api/actors_api/

use serde_json::json;
use wstd::http::Method;

use crate::exports::actors::{
    ActorStateOperation, ActorStateOperationType, Guest, Reminder, Timer,
};
use crate::sidecar::{seg, Sidecar};
use crate::state::value_to_json;
use crate::types::Error;
use crate::Component;

fn schedule_json(
    due_time: &Option<String>,
    period: &Option<String>,
    ttl: &Option<String>,
    data: &Option<String>,
    callback: Option<&String>,
) -> Result<serde_json::Value, Error> {
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
        object["data"] = serde_json::from_str(data)
            .map_err(|e| Error::InvalidArgument(format!("data is not valid JSON: {e}")))?;
    }
    if let Some(callback) = callback {
        object["callback"] = json!(callback);
    }
    Ok(object)
}

impl Guest for Component {
    fn invoke(
        actor_type: String,
        actor_id: String,
        method: String,
        body: Vec<u8>,
        content_type: Option<String>,
    ) -> Result<Vec<u8>, Error> {
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
        let response = sidecar.expect_success(Method::POST, &path, &headers, body)?;
        Ok(response.body)
    }

    fn get_state(
        actor_type: String,
        actor_id: String,
        key: String,
    ) -> Result<Option<Vec<u8>>, Error> {
        let sidecar = Sidecar::from_env();
        let path = format!(
            "/v1.0/actors/{}/{}/state/{}",
            seg(&actor_type),
            seg(&actor_id),
            seg(&key)
        );
        let response = sidecar.expect_success(Method::GET, &path, &[], Vec::new())?;
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
    ) -> Result<(), Error> {
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
        sidecar.json(Method::POST, &path, &body)?;
        Ok(())
    }

    fn register_reminder(
        actor_type: String,
        actor_id: String,
        name: String,
        reminder: Reminder,
    ) -> Result<(), Error> {
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
        )?;
        sidecar.json(Method::POST, &path, &body)?;
        Ok(())
    }

    fn get_reminder(actor_type: String, actor_id: String, name: String) -> Result<String, Error> {
        let sidecar = Sidecar::from_env();
        let path = format!(
            "/v1.0/actors/{}/{}/reminders/{}",
            seg(&actor_type),
            seg(&actor_id),
            seg(&name)
        );
        let response = sidecar.expect_success(Method::GET, &path, &[], Vec::new())?;
        Ok(String::from_utf8_lossy(&response.body).into_owned())
    }

    fn unregister_reminder(
        actor_type: String,
        actor_id: String,
        name: String,
    ) -> Result<(), Error> {
        let sidecar = Sidecar::from_env();
        let path = format!(
            "/v1.0/actors/{}/{}/reminders/{}",
            seg(&actor_type),
            seg(&actor_id),
            seg(&name)
        );
        sidecar.expect_success(Method::DELETE, &path, &[], Vec::new())?;
        Ok(())
    }

    fn register_timer(
        actor_type: String,
        actor_id: String,
        name: String,
        timer: Timer,
    ) -> Result<(), Error> {
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
        )?;
        sidecar.json(Method::POST, &path, &body)?;
        Ok(())
    }

    fn unregister_timer(actor_type: String, actor_id: String, name: String) -> Result<(), Error> {
        let sidecar = Sidecar::from_env();
        let path = format!(
            "/v1.0/actors/{}/{}/timers/{}",
            seg(&actor_type),
            seg(&actor_id),
            seg(&name)
        );
        sidecar.expect_success(Method::DELETE, &path, &[], Vec::new())?;
        Ok(())
    }
}
