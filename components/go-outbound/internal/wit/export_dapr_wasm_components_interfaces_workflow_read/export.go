// Workflow reads, served by the embedded runtime's API — GetWorkflowBeta1
// (the Alpha1 set is deprecated) against the built-in "dapr" workflow
// component. Timestamps are rendered as RFC 3339 to match the HTTP shape.
// Mirrors wasi-grpc-outbound's workflow.rs (the workflow-read half).
package export_dapr_wasm_components_interfaces_workflow_read

import (
	"context"
	"sort"
	"strings"

	daprgrpc "github.com/dapr/dapr/pkg/api/grpc"
	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	witTypes "go.bytecodealliance.org/pkg/wit/types"
	"google.golang.org/protobuf/types/known/timestamppb"

	wfblk "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/blocks/workflow"
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	wr "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_workflow_read"
)

// daprd's property keys for the values workflow-instance carries as typed
// fields (dapr/dapr pkg/api/universal/workflow.go, GetWorkflow).
const (
	propInput             = "dapr.workflow.input"
	propOutput            = "dapr.workflow.output"
	propFailureType       = "dapr.workflow.failure.error_type"
	propFailureMessage    = "dapr.workflow.failure.error_message"
	propFailureStackTrace = "dapr.workflow.failure.stack_trace"
	// Where the raw status string goes when it maps to runtime-status.unknown.
	propRuntimeStatus = "dapr.workflow.runtime_status"
)

func getError(f *call.Failure) wr.GetError {
	if f.Status == 404 {
		return wr.MakeGetErrorNotFound(f.Message)
	}
	return wr.MakeGetErrorWorkflow(wfblk.Error(f))
}

// rfc3339 renders a proto timestamp like the Rust provider (humantime's
// "smart" precision: no fraction when nanos are zero, else all 9 digits).
// The WIT fields are plain strings, so a missing, pre-epoch or post-9999
// timestamp becomes "".
func rfc3339(ts *timestamppb.Timestamp) string {
	const year10000 = 253_402_300_800
	if ts == nil || ts.GetSeconds() < 0 || ts.GetSeconds() >= year10000 || ts.GetNanos() < 0 {
		return ""
	}
	t := ts.AsTime()
	if t.Nanosecond() == 0 {
		return t.Format("2006-01-02T15:04:05Z")
	}
	return t.Format("2006-01-02T15:04:05.000000000Z")
}

// runtimeStatus maps daprd's status string to the WIT enum. daprd renders
// durabletask's OrchestrationStatus through a fixed table and falls back to
// UNKNOWN. The match is ASCII case-insensitive; anything else is unknown
// (the caller keeps the raw string in the properties).
func runtimeStatus(raw string) wr.RuntimeStatus {
	switch strings.ToUpper(raw) {
	case "RUNNING":
		return wr.RuntimeStatusRunning
	case "COMPLETED":
		return wr.RuntimeStatusCompleted
	case "CONTINUED_AS_NEW":
		return wr.RuntimeStatusContinuedAsNew
	case "FAILED":
		return wr.RuntimeStatusFailed
	case "CANCELED":
		return wr.RuntimeStatusCanceled
	case "TERMINATED":
		return wr.RuntimeStatusTerminated
	case "PENDING":
		return wr.RuntimeStatusPending
	case "SUSPENDED":
		return wr.RuntimeStatusSuspended
	}
	return wr.RuntimeStatusUnknown
}

// take removes key from properties and returns its value, if present.
func take(properties map[string]string, key string) (string, bool) {
	v, ok := properties[key]
	delete(properties, key)
	return v, ok
}

func takeBytes(properties map[string]string, key string) witTypes.Option[[]uint8] {
	if v, ok := take(properties, key); ok {
		return witTypes.Some([]uint8(v))
	}
	return witTypes.None[[]uint8]()
}

// workflowInstance builds the typed workflow-instance from daprd's flat
// response: the input/output/failure properties move into typed fields,
// the rest stay in properties (sorted by key, for determinism).
func workflowInstance(r *runtimev1pb.GetWorkflowResponse) wr.WorkflowInstance {
	properties := make(map[string]string, len(r.GetProperties())+1)
	for k, v := range r.GetProperties() {
		properties[k] = v
	}
	status := runtimeStatus(r.GetRuntimeStatus())
	if status == wr.RuntimeStatusUnknown {
		properties[propRuntimeStatus] = r.GetRuntimeStatus()
	}
	input := takeBytes(properties, propInput)
	output := takeBytes(properties, propOutput)
	errorType, hasType := take(properties, propFailureType)
	message, hasMessage := take(properties, propFailureMessage)
	stackTrace, hasStackTrace := take(properties, propFailureStackTrace)
	// daprd sets type and message together, whenever the instance has
	// failure details; either one alone still means "failed".
	failure := witTypes.None[wr.FailureDetails]()
	if hasType || hasMessage {
		st := witTypes.None[string]()
		if hasStackTrace {
			st = witTypes.Some(stackTrace)
		}
		failure = witTypes.Some(wr.FailureDetails{ErrorType: errorType, Message: message, StackTrace: st})
	}
	pairs := make([]witTypes.Tuple2[string, string], 0, len(properties))
	for k, v := range properties {
		pairs = append(pairs, witTypes.Tuple2[string, string]{F0: k, F1: v})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].F0 < pairs[j].F0 })
	return wr.WorkflowInstance{
		InstanceId:    r.GetInstanceId(),
		WorkflowName:  r.GetWorkflowName(),
		CreatedAt:     rfc3339(r.GetCreatedAt()),
		LastUpdatedAt: rfc3339(r.GetLastUpdatedAt()),
		RuntimeStatus: status,
		Input:         input,
		Output:        output,
		Failure:       failure,
		Properties:    pairs,
	}
}

func Get(instanceID string) witTypes.Result[wr.WorkflowInstance, wr.GetError] {
	r, f := call.Do(func(ctx context.Context, api daprgrpc.API) (*runtimev1pb.GetWorkflowResponse, error) {
		return api.GetWorkflowBeta1(ctx, &runtimev1pb.GetWorkflowRequest{InstanceId: instanceID, WorkflowComponent: wfblk.Component})
	})
	if f != nil {
		return witTypes.Err[wr.WorkflowInstance](getError(f))
	}
	return witTypes.Ok[wr.WorkflowInstance, wr.GetError](workflowInstance(r))
}
