//! Workflow management over gRPC — `StartWorkflowBeta1`, `GetWorkflowBeta1`,
//! `TerminateWorkflowBeta1`, `RaiseEventWorkflowBeta1`, `PauseWorkflowBeta1`,
//! `ResumeWorkflowBeta1`, `PurgeWorkflowBeta1` (the Alpha1 set is deprecated
//! in the proto). Unlike the HTTP API, input and event payloads are raw
//! proto bytes (no JSON content-type involved), and `GetWorkflow` returns
//! proto timestamps, rendered here as RFC 3339 to match the HTTP shape.

use std::time::{Duration, UNIX_EPOCH};

use crate::exports::workflow::{
    GetError, Guest, InstanceError, StartError, WorkflowError, WorkflowInstance,
};
use crate::proto::runtime as pb;
use crate::sidecar::{metadata_pairs, DaprFailure, Sidecar};
use crate::Component;

/// Dapr's only workflow engine is the built-in workflow component `dapr`;
/// the WIT functions take no component name and always target it.
const WORKFLOW_COMPONENT: &str = "dapr";

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
        Ok(WorkflowInstance {
            instance_id: response.instance_id,
            created_at: rfc3339(response.created_at),
            last_updated_at: rfc3339(response.last_updated_at),
            runtime_status: response.runtime_status,
            properties: metadata_pairs(response.properties),
        })
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
