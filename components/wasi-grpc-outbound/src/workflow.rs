//! Workflow management over gRPC — `StartWorkflowBeta1`, `GetWorkflowBeta1`,
//! `TerminateWorkflowBeta1`, `RaiseEventWorkflowBeta1`, `PauseWorkflowBeta1`,
//! `ResumeWorkflowBeta1`, `PurgeWorkflowBeta1` (the Alpha1 set is deprecated
//! in the proto). Unlike the HTTP API, input and event payloads are raw
//! proto bytes (no JSON content-type involved), and `GetWorkflow` returns
//! proto timestamps, rendered here as RFC 3339 to match the HTTP shape.

use std::collections::BTreeMap;
use std::time::{Duration, UNIX_EPOCH};

use crate::exports::workflow::{
    FailureDetails, GetError, Guest, InstanceError, RuntimeStatus, StartError, WorkflowError,
    WorkflowInstance,
};
use crate::proto::runtime as pb;
use crate::sidecar::{DaprFailure, Sidecar};
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

fn workflow_error(f: DaprFailure) -> WorkflowError {
    if f.is_permission() {
        WorkflowError::PermissionDenied(f.message)
    } else {
        WorkflowError::ComponentNotFound(f.message)
    }
}

fn start_error(f: DaprFailure) -> StartError {
    if matches!(f.status, 409)
        || f.error_code
            .as_deref()
            .is_some_and(|c| c.contains("ALREADY_EXISTS"))
    {
        StartError::InstanceAlreadyExists(f.message)
    } else {
        StartError::Workflow(workflow_error(f))
    }
}

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

/// Render a proto timestamp as RFC 3339. The WIT fields are plain strings
/// (the HTTP API serves strings), so a missing or pre-epoch timestamp
/// becomes `""` — the same default the wasi-http provider falls back to.
fn rfc3339(timestamp: Option<prost_types::Timestamp>) -> String {
    // Upper bound: humantime can only render up to year 9999; a value past
    // it would make `to_string` panic.
    const YEAR_10000: i64 = 253_402_300_800;
    match timestamp {
        Some(ts) if (0..YEAR_10000).contains(&ts.seconds) && ts.nanos >= 0 => {
            let time = UNIX_EPOCH + Duration::new(ts.seconds as u64, ts.nanos as u32);
            humantime::format_rfc3339(time).to_string()
        }
        _ => String::new(),
    }
}

impl Guest for Component {
    fn start(
        workflow_name: String,
        instance_id: Option<String>,
        input: Vec<u8>,
    ) -> Result<String, StartError> {
        let sidecar = Sidecar::from_env();
        let response = sidecar
            .unary(
                pb::StartWorkflowRequest {
                    // proto3: an empty instance id asks daprd to generate one.
                    instance_id: instance_id.unwrap_or_default(),
                    workflow_component: WORKFLOW_COMPONENT.to_string(),
                    workflow_name,
                    options: Default::default(),
                    input,
                },
                |mut client, request| async move { client.start_workflow_beta1(request).await },
            )
            .map_err(start_error)?;
        Ok(response.instance_id)
    }

    fn get(instance_id: String) -> Result<WorkflowInstance, GetError> {
        let sidecar = Sidecar::from_env();
        let response = sidecar
            .unary(
                pb::GetWorkflowRequest {
                    instance_id,
                    workflow_component: WORKFLOW_COMPONENT.to_string(),
                },
                |mut client, request| async move { client.get_workflow_beta1(request).await },
            )
            .map_err(get_error)?;
        Ok(workflow_instance(
            response.instance_id,
            response.workflow_name,
            rfc3339(response.created_at),
            rfc3339(response.last_updated_at),
            response.runtime_status,
            response.properties.into_iter().collect(),
        ))
    }

    fn terminate(instance_id: String) -> Result<(), InstanceError> {
        let sidecar = Sidecar::from_env();
        sidecar
            .unary(
                pb::TerminateWorkflowRequest {
                    instance_id,
                    workflow_component: WORKFLOW_COMPONENT.to_string(),
                },
                |mut client, request| async move { client.terminate_workflow_beta1(request).await },
            )
            .map_err(instance_error)?;
        Ok(())
    }

    fn raise_event(
        instance_id: String,
        event_name: String,
        event_data: Vec<u8>,
    ) -> Result<(), InstanceError> {
        let sidecar = Sidecar::from_env();
        sidecar
            .unary(
                pb::RaiseEventWorkflowRequest {
                    instance_id,
                    workflow_component: WORKFLOW_COMPONENT.to_string(),
                    event_name,
                    event_data,
                },
                |mut client, request| async move {
                    client.raise_event_workflow_beta1(request).await
                },
            )
            .map_err(instance_error)?;
        Ok(())
    }

    fn pause(instance_id: String) -> Result<(), InstanceError> {
        let sidecar = Sidecar::from_env();
        sidecar
            .unary(
                pb::PauseWorkflowRequest {
                    instance_id,
                    workflow_component: WORKFLOW_COMPONENT.to_string(),
                },
                |mut client, request| async move { client.pause_workflow_beta1(request).await },
            )
            .map_err(instance_error)?;
        Ok(())
    }

    fn resume(instance_id: String) -> Result<(), InstanceError> {
        let sidecar = Sidecar::from_env();
        sidecar
            .unary(
                pb::ResumeWorkflowRequest {
                    instance_id,
                    workflow_component: WORKFLOW_COMPONENT.to_string(),
                },
                |mut client, request| async move { client.resume_workflow_beta1(request).await },
            )
            .map_err(instance_error)?;
        Ok(())
    }

    fn purge(instance_id: String) -> Result<(), InstanceError> {
        let sidecar = Sidecar::from_env();
        sidecar
            .unary(
                pb::PurgeWorkflowRequest {
                    instance_id,
                    workflow_component: WORKFLOW_COMPONENT.to_string(),
                },
                |mut client, request| async move { client.purge_workflow_beta1(request).await },
            )
            .map_err(instance_error)?;
        Ok(())
    }
}
