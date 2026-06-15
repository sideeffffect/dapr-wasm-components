//! Workflow management — https://docs.dapr.io/reference/api/workflow_api/
//! (HTTP workflow API is deprecated by Dapr but still served; the
//! workflow-component is "dapr" in current releases.)

use serde::Deserialize;
use std::collections::BTreeMap;
use wstd::http::Method;

use crate::exports::workflow::{
    GetError, Guest, InstanceError, StartError, WorkflowError, WorkflowInstance,
};
use crate::sidecar::{seg, DaprFailure, Sidecar};
use crate::Component;

/// Map a recoverable failure to the workflow setup/config error.
fn workflow_error(f: DaprFailure) -> WorkflowError {
    if f.is_permission() {
        WorkflowError::PermissionDenied(f.message)
    } else {
        WorkflowError::ComponentNotFound(f.message)
    }
}

/// Map a recoverable failure of a workflow start.
fn start_error(f: DaprFailure) -> StartError {
    if f.status == 409
        || f.error_code
            .as_deref()
            .is_some_and(|c| c.contains("ALREADY_EXISTS"))
    {
        StartError::InstanceAlreadyExists(f.message)
    } else {
        StartError::Workflow(workflow_error(f))
    }
}

/// Map a recoverable failure of an operation on an existing instance.
fn instance_error(f: DaprFailure) -> InstanceError {
    if f.status == 404 {
        return InstanceError::NotFound(f.message);
    }
    if matches!(f.status, 409 | 412) {
        return InstanceError::InvalidState(f.message);
    }
    InstanceError::Workflow(workflow_error(f))
}

/// Map a recoverable failure of `get`. A 404 means the instance does not
/// exist; everything else maps through the workflow setup/config error.
fn get_error(f: DaprFailure) -> GetError {
    if f.status == 404 {
        return GetError::NotFound(f.message);
    }
    GetError::Workflow(workflow_error(f))
}

impl Guest for Component {
    fn start(
        workflow_component: String,
        workflow_name: String,
        instance_id: Option<String>,
        input: Vec<u8>,
    ) -> Result<String, StartError> {
        let sidecar = Sidecar::from_env();
        let mut path = format!(
            "/v1.0/workflows/{}/{}/start",
            seg(&workflow_component),
            seg(&workflow_name)
        );
        if let Some(id) = &instance_id {
            path.push_str(&format!("?instanceID={}", urlencoding::encode(id)));
        }
        let response = sidecar
            .expect_success(
                Method::POST,
                &path,
                &[("content-type".to_string(), "application/json".to_string())],
                input,
            )
            .map_err(start_error)?;

        #[derive(Deserialize)]
        struct StartJson {
            #[serde(rename = "instanceID")]
            instance_id: String,
        }
        let parsed: StartJson = serde_json::from_slice(&response.body)
            .unwrap_or_else(|e| panic!("unexpected workflow start response: {e}"));
        Ok(parsed.instance_id)
    }

    fn get(workflow_component: String, instance_id: String) -> Result<WorkflowInstance, GetError> {
        let sidecar = Sidecar::from_env();
        let path = format!(
            "/v1.0/workflows/{}/{}",
            seg(&workflow_component),
            seg(&instance_id)
        );
        let response = sidecar
            .expect_success(Method::GET, &path, &[], Vec::new())
            .map_err(get_error)?;

        #[derive(Deserialize)]
        struct InstanceJson {
            #[serde(rename = "instanceID")]
            instance_id: String,
            #[serde(default, rename = "createdAt")]
            created_at: String,
            #[serde(default, rename = "lastUpdatedAt")]
            last_updated_at: String,
            #[serde(default, rename = "runtimeStatus")]
            runtime_status: String,
            #[serde(default)]
            properties: BTreeMap<String, serde_json::Value>,
        }
        let parsed: InstanceJson = serde_json::from_slice(&response.body)
            .unwrap_or_else(|e| panic!("unexpected workflow response: {e}"));
        Ok(WorkflowInstance {
            instance_id: parsed.instance_id,
            created_at: parsed.created_at,
            last_updated_at: parsed.last_updated_at,
            runtime_status: parsed.runtime_status,
            properties: parsed
                .properties
                .into_iter()
                .map(|(key, value)| {
                    let text = match value {
                        serde_json::Value::String(text) => text,
                        other => other.to_string(),
                    };
                    (key, text)
                })
                .collect(),
        })
    }

    fn terminate(workflow_component: String, instance_id: String) -> Result<(), InstanceError> {
        post_simple(&workflow_component, &instance_id, "terminate")
    }

    fn raise_event(
        workflow_component: String,
        instance_id: String,
        event_name: String,
        event_data: Vec<u8>,
    ) -> Result<(), InstanceError> {
        let sidecar = Sidecar::from_env();
        let path = format!(
            "/v1.0/workflows/{}/{}/raiseEvent/{}",
            seg(&workflow_component),
            seg(&instance_id),
            seg(&event_name)
        );
        sidecar
            .expect_success(
                Method::POST,
                &path,
                &[("content-type".to_string(), "application/json".to_string())],
                event_data,
            )
            .map_err(instance_error)?;
        Ok(())
    }

    fn pause(workflow_component: String, instance_id: String) -> Result<(), InstanceError> {
        post_simple(&workflow_component, &instance_id, "pause")
    }

    fn resume(workflow_component: String, instance_id: String) -> Result<(), InstanceError> {
        post_simple(&workflow_component, &instance_id, "resume")
    }

    fn purge(workflow_component: String, instance_id: String) -> Result<(), InstanceError> {
        post_simple(&workflow_component, &instance_id, "purge")
    }
}

fn post_simple(
    workflow_component: &str,
    instance_id: &str,
    action: &str,
) -> Result<(), InstanceError> {
    let sidecar = Sidecar::from_env();
    let path = format!(
        "/v1.0/workflows/{}/{}/{}",
        seg(workflow_component),
        seg(instance_id),
        action
    );
    sidecar
        .expect_success(Method::POST, &path, &[], Vec::new())
        .map_err(instance_error)?;
    Ok(())
}
