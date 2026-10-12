//! Workflow management — https://docs.dapr.io/reference/api/workflow_api/
//! (HTTP workflow API is deprecated by Dapr but still served.)

use serde::Deserialize;
use std::collections::BTreeMap;
use wstd::http::Method;

use crate::exports::workflow::{
    FailureDetails, GetError, Guest, InstanceError, RuntimeStatus, StartError, WorkflowError,
    WorkflowInstance,
};
use crate::sidecar::{seg, DaprFailure, Sidecar};
use crate::Component;

/// Dapr's only workflow engine is the built-in workflow component `dapr`;
/// the WIT functions take no component name and always target it.
const WORKFLOW_COMPONENT: &str = "dapr";

/// daprd's property keys for the values `workflow-instance` carries as typed
/// fields (dapr/dapr `pkg/api/universal/workflow.go`, `GetWorkflow`).
const PROP_INPUT: &str = "dapr.workflow.input";
const PROP_OUTPUT: &str = "dapr.workflow.output";
const PROP_FAILURE_TYPE: &str = "dapr.workflow.failure.error_type";
const PROP_FAILURE_MESSAGE: &str = "dapr.workflow.failure.error_message";
const PROP_FAILURE_STACK_TRACE: &str = "dapr.workflow.failure.stack_trace";
/// Where the raw status string goes when it maps to `runtime-status.unknown`.
const PROP_RUNTIME_STATUS: &str = "dapr.workflow.runtime_status";

/// Map daprd's status string to the WIT enum. daprd renders durabletask's
/// `OrchestrationStatus` through a fixed table (`RUNNING`, `COMPLETED`,
/// `CONTINUED_AS_NEW`, `FAILED`, `CANCELED`, `TERMINATED`, `PENDING`,
/// `SUSPENDED`) and falls back to `UNKNOWN`. The match is ASCII
/// case-insensitive; anything else is `unknown` (the caller keeps the raw
/// string in the properties).
fn runtime_status(raw: &str) -> RuntimeStatus {
    match raw.to_ascii_uppercase().as_str() {
        "RUNNING" => RuntimeStatus::Running,
        "COMPLETED" => RuntimeStatus::Completed,
        "CONTINUED_AS_NEW" => RuntimeStatus::ContinuedAsNew,
        "FAILED" => RuntimeStatus::Failed,
        "CANCELED" => RuntimeStatus::Canceled,
        "TERMINATED" => RuntimeStatus::Terminated,
        "PENDING" => RuntimeStatus::Pending,
        "SUSPENDED" => RuntimeStatus::Suspended,
        _ => RuntimeStatus::Unknown,
    }
}

/// Build the typed `workflow-instance` from daprd's flat response: the
/// input/output/failure properties move into typed fields, the rest stay in
/// `properties` (sorted by key, for determinism).
fn workflow_instance(
    instance_id: String,
    workflow_name: String,
    created_at: String,
    last_updated_at: String,
    raw_status: String,
    mut properties: BTreeMap<String, String>,
) -> WorkflowInstance {
    let status = runtime_status(&raw_status);
    if status == RuntimeStatus::Unknown {
        properties.insert(PROP_RUNTIME_STATUS.to_string(), raw_status);
    }
    let input = properties.remove(PROP_INPUT).map(String::into_bytes);
    let output = properties.remove(PROP_OUTPUT).map(String::into_bytes);
    let error_type = properties.remove(PROP_FAILURE_TYPE);
    let message = properties.remove(PROP_FAILURE_MESSAGE);
    let stack_trace = properties.remove(PROP_FAILURE_STACK_TRACE);
    // daprd sets type and message together, whenever the instance has
    // failure details; either one alone still means "failed".
    let failure = (error_type.is_some() || message.is_some()).then(|| FailureDetails {
        error_type: error_type.unwrap_or_default(),
        message: message.unwrap_or_default(),
        stack_trace,
    });
    WorkflowInstance {
        instance_id,
        workflow_name,
        created_at,
        last_updated_at,
        runtime_status: status,
        input,
        output,
        failure,
        properties: properties.into_iter().collect(),
    }
}

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
        workflow_name: String,
        instance_id: Option<String>,
        input: Vec<u8>,
    ) -> Result<String, StartError> {
        let sidecar = Sidecar::from_env();
        let mut path = format!(
            "/v1.0/workflows/{}/{}/start",
            seg(WORKFLOW_COMPONENT),
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

    fn get(instance_id: String) -> Result<WorkflowInstance, GetError> {
        let sidecar = Sidecar::from_env();
        let path = format!(
            "/v1.0/workflows/{}/{}",
            seg(WORKFLOW_COMPONENT),
            seg(&instance_id)
        );
        let response = sidecar
            .expect_success(Method::GET, &path, &[], Vec::new())
            .map_err(get_error)?;

        #[derive(Deserialize)]
        struct InstanceJson {
            #[serde(rename = "instanceID")]
            instance_id: String,
            #[serde(default, rename = "workflowName")]
            workflow_name: String,
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
        // daprd serves `properties` as a string map; a non-string value
        // (never sent today) is kept as its JSON text.
        let properties = parsed
            .properties
            .into_iter()
            .map(|(key, value)| {
                let text = match value {
                    serde_json::Value::String(text) => text,
                    other => other.to_string(),
                };
                (key, text)
            })
            .collect();
        Ok(workflow_instance(
            parsed.instance_id,
            parsed.workflow_name,
            parsed.created_at,
            parsed.last_updated_at,
            parsed.runtime_status,
            properties,
        ))
    }

    fn terminate(instance_id: String) -> Result<(), InstanceError> {
        post_simple(&instance_id, "terminate")
    }

    fn raise_event(
        instance_id: String,
        event_name: String,
        event_data: Vec<u8>,
    ) -> Result<(), InstanceError> {
        let sidecar = Sidecar::from_env();
        let path = format!(
            "/v1.0/workflows/{}/{}/raiseEvent/{}",
            seg(WORKFLOW_COMPONENT),
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

    fn pause(instance_id: String) -> Result<(), InstanceError> {
        post_simple(&instance_id, "pause")
    }

    fn resume(instance_id: String) -> Result<(), InstanceError> {
        post_simple(&instance_id, "resume")
    }

    fn purge(instance_id: String) -> Result<(), InstanceError> {
        post_simple(&instance_id, "purge")
    }
}

fn post_simple(instance_id: &str, action: &str) -> Result<(), InstanceError> {
    let sidecar = Sidecar::from_env();
    let path = format!(
        "/v1.0/workflows/{}/{}/{}",
        seg(WORKFLOW_COMPONENT),
        seg(instance_id),
        action
    );
    sidecar
        .expect_success(Method::POST, &path, &[], Vec::new())
        .map_err(instance_error)?;
    Ok(())
}
