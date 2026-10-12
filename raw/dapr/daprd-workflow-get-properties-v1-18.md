# daprd workflow GetWorkflow: response properties and status strings (v1.18 / v1.19)

> Source: https://github.com/dapr/dapr (branches release-1.18 `pkg/runtime/wfengine/client.go`, `pkg/api/http/workflow.go`; release-1.19 `pkg/api/universal/workflow.go`), https://github.com/dapr/durabletask-go v0.12.6 `api/protos/orchestration.pb.go`
> Collected: 2026-10-12
> Published: Unknown

Verbatim excerpts, release-1.18 `pkg/runtime/wfengine/client.go`:

```go
// Status values are defined at: https://github.com/dapr/durabletask-go/blob/119b361079c45e368f83b223888d56a436ac59b9/internal/protos/orchestrator_service.pb.go#L42-L64
var statusMap = map[int32]string{
	0: "RUNNING",
	1: "COMPLETED",
	2: "CONTINUED_AS_NEW",
	3: "FAILED",
	4: "CANCELED",
	5: "TERMINATED",
	6: "PENDING",
	7: "SUSPENDED",
}

```go
	res := &workflows.StateResponse{
		Workflow: &workflows.WorkflowState{
			InstanceID:    req.InstanceID,
			WorkflowName:  metadata.GetName(),
			CreatedAt:     metadata.GetCreatedAt().AsTime(),
			LastUpdatedAt: metadata.GetLastUpdatedAt().AsTime(),
			RuntimeStatus: getStatusString(int32(metadata.GetRuntimeStatus())),
			Properties:    make(map[string]string),
		},
	}

	if metadata.GetCustomStatus() != nil {
		res.Workflow.Properties["dapr.workflow.custom_status"] = metadata.GetCustomStatus().GetValue()
	}

	if metadata.Input != nil {
		res.Workflow.Properties["dapr.workflow.input"] = metadata.GetInput().GetValue()
	}

	if metadata.Output != nil {
		res.Workflow.Properties["dapr.workflow.output"] = metadata.GetOutput().GetValue()
	}

	// Status-specific fields
	if metadata.FailureDetails != nil {
		res.Workflow.Properties["dapr.workflow.failure.error_type"] = metadata.GetFailureDetails().GetErrorType()

		res.Workflow.Properties["dapr.workflow.failure.error_message"] = metadata.GetFailureDetails().GetErrorMessage()
		if trace := metadata.GetFailureDetails().GetStackTrace(); trace != nil {
			res.Workflow.Properties["dapr.workflow.failure.stack_trace"] = trace.GetValue()
		}
	}

	return res, nil
```

```go
func getStatusString(status int32) string {
	if statusStr, ok := statusMap[status]; ok {
		return statusStr
	}

	return "UNKNOWN"
}
```

release-1.18 `pkg/api/http/workflow.go` (HTTP get handler; the response is the gRPC `GetWorkflowResponse` rendered as protojson — `instanceID`, `workflowName`, `createdAt`, `lastUpdatedAt`, `runtimeStatus`, `properties`):

```go
// Route: GET "workflows/{workflowComponent}/{instanceID}"
func (a *api) onGetWorkflowHandler() http.HandlerFunc {
	return UniversalHTTPHandler(
		a.universal.GetWorkflow,
		UniversalHTTPHandlerOpts[*runtimev1pb.GetWorkflowRequest, *runtimev1pb.GetWorkflowResponse]{
			InModifier: workflowInModifier[*runtimev1pb.GetWorkflowRequest],
			OutModifier: func(out *runtimev1pb.GetWorkflowResponse) (any, error) {
				if out.GetProperties() == nil {
					return nil, messages.ErrWorkflowInstanceNotFound.WithFormat(out.GetInstanceId())
				}
				return out, nil
			},
		})
}
```

release-1.19 moved this into `pkg/api/universal/workflow.go`, with the same `statusMap` and property keys; it adds:

```go
	// A failed workflow has no successful output: durabletask-go stores the
	// failure message in the completed-event result (surfaced here as Output),
	// but the failure is already reported via dapr.workflow.failure.* below, so
	// it must not also be exposed as dapr.workflow.output.
	if metadata.Output != nil && metadata.FailureDetails == nil {
		res.Properties["dapr.workflow.output"] = metadata.GetOutput().GetValue()
	}

	// Status-specific fields
	if metadata.FailureDetails != nil {
		res.Properties["dapr.workflow.failure.error_type"] = metadata.GetFailureDetails().GetErrorType()
		res.Properties["dapr.workflow.failure.error_message"] = metadata.GetFailureDetails().GetErrorMessage()
		if trace := metadata.GetFailureDetails().GetStackTrace(); trace != nil {
			res.Properties["dapr.workflow.failure.stack_trace"] = trace.GetValue()
		}
```

durabletask-go v0.12.6 `api/protos/orchestration.pb.go` (the enum daprd maps from — note `STALLED = 8`, which is not in `statusMap` and therefore renders as `UNKNOWN`):

```go
	OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING          OrchestrationStatus = 0
	OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED        OrchestrationStatus = 1
	OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW OrchestrationStatus = 2
	OrchestrationStatus_ORCHESTRATION_STATUS_FAILED           OrchestrationStatus = 3
	OrchestrationStatus_ORCHESTRATION_STATUS_CANCELED         OrchestrationStatus = 4
	OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED       OrchestrationStatus = 5
	OrchestrationStatus_ORCHESTRATION_STATUS_PENDING          OrchestrationStatus = 6
	OrchestrationStatus_ORCHESTRATION_STATUS_SUSPENDED        OrchestrationStatus = 7
	OrchestrationStatus_ORCHESTRATION_STATUS_STALLED          OrchestrationStatus = 8
```
