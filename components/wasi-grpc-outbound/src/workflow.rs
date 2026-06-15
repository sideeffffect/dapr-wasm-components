//! Workflow management over gRPC — `StartWorkflowBeta1`, `GetWorkflowBeta1`,
//! `TerminateWorkflowBeta1`, `RaiseEventWorkflowBeta1`, `PauseWorkflowBeta1`,
//! `ResumeWorkflowBeta1`, `PurgeWorkflowBeta1` (the Alpha1 set is deprecated
//! in the proto). Unlike the HTTP API, input and event payloads are raw
//! proto bytes (no JSON content-type involved), and `GetWorkflow` returns
//! proto timestamps, rendered here as RFC 3339 to match the HTTP shape.

use std::time::{Duration, UNIX_EPOCH};

use crate::exports::workflow::{Guest, InstanceError, StartError, WorkflowError, WorkflowInstance};
use crate::proto::runtime as pb;
use crate::sidecar::{metadata_pairs, DaprFailure, Sidecar};
use crate::Component;

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
        workflow_component: String,
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
                    workflow_component,
                    workflow_name,
                    options: Default::default(),
                    input,
                },
                |mut client, request| async move { client.start_workflow_beta1(request).await },
            )
            .map_err(start_error)?;
        Ok(response.instance_id)
    }

    fn get(
        workflow_component: String,
        instance_id: String,
    ) -> Result<Option<WorkflowInstance>, WorkflowError> {
        let sidecar = Sidecar::from_env();
        let response = match sidecar.unary(
            pb::GetWorkflowRequest {
                instance_id,
                workflow_component,
            },
            |mut client, request| async move { client.get_workflow_beta1(request).await },
        ) {
            Ok(response) => response,
            // A missing instance is absence, not an error.
            Err(f) if f.status == 404 => return Ok(None),
            Err(f) => return Err(workflow_error(f)),
        };
        Ok(Some(WorkflowInstance {
            instance_id: response.instance_id,
            created_at: rfc3339(response.created_at),
            last_updated_at: rfc3339(response.last_updated_at),
            runtime_status: response.runtime_status,
            properties: metadata_pairs(response.properties),
        }))
    }

    fn terminate(workflow_component: String, instance_id: String) -> Result<(), InstanceError> {
        let sidecar = Sidecar::from_env();
        sidecar
            .unary(
                pb::TerminateWorkflowRequest {
                    instance_id,
                    workflow_component,
                },
                |mut client, request| async move { client.terminate_workflow_beta1(request).await },
            )
            .map_err(instance_error)?;
        Ok(())
    }

    fn raise_event(
        workflow_component: String,
        instance_id: String,
        event_name: String,
        event_data: Vec<u8>,
    ) -> Result<(), InstanceError> {
        let sidecar = Sidecar::from_env();
        sidecar
            .unary(
                pb::RaiseEventWorkflowRequest {
                    instance_id,
                    workflow_component,
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

    fn pause(workflow_component: String, instance_id: String) -> Result<(), InstanceError> {
        let sidecar = Sidecar::from_env();
        sidecar
            .unary(
                pb::PauseWorkflowRequest {
                    instance_id,
                    workflow_component,
                },
                |mut client, request| async move { client.pause_workflow_beta1(request).await },
            )
            .map_err(instance_error)?;
        Ok(())
    }

    fn resume(workflow_component: String, instance_id: String) -> Result<(), InstanceError> {
        let sidecar = Sidecar::from_env();
        sidecar
            .unary(
                pb::ResumeWorkflowRequest {
                    instance_id,
                    workflow_component,
                },
                |mut client, request| async move { client.resume_workflow_beta1(request).await },
            )
            .map_err(instance_error)?;
        Ok(())
    }

    fn purge(workflow_component: String, instance_id: String) -> Result<(), InstanceError> {
        let sidecar = Sidecar::from_env();
        sidecar
            .unary(
                pb::PurgeWorkflowRequest {
                    instance_id,
                    workflow_component,
                },
                |mut client, request| async move { client.purge_workflow_beta1(request).await },
            )
            .map_err(instance_error)?;
        Ok(())
    }
}
