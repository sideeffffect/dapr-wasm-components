// Workflow management, served by the embedded runtime's API —
// StartWorkflowBeta1, GetWorkflowBeta1, TerminateWorkflowBeta1,
// RaiseEventWorkflowBeta1, PauseWorkflowBeta1, ResumeWorkflowBeta1,
// PurgeWorkflowBeta1 (the Alpha1 set is deprecated). Input and event
// payloads are raw bytes; timestamps are rendered as RFC 3339 to match the
// HTTP shape. Mirrors wasi-grpc-outbound's workflow.rs.
package export_dapr_wasm_components_interfaces_workflow

import (
	"context"
	"strings"

	daprgrpc "github.com/dapr/dapr/pkg/api/grpc"
	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	witTypes "go.bytecodealliance.org/pkg/wit/types"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	wf "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_workflow"
)

type unitResult = witTypes.Result[witTypes.Unit, wf.InstanceError]

func workflowError(f *call.Failure) wf.WorkflowError {
	if f.IsPermission() {
		return wf.MakeWorkflowErrorPermissionDenied(f.Message)
	}
	return wf.MakeWorkflowErrorComponentNotFound(f.Message)
}

func startError(f *call.Failure) wf.StartError {
	if f.Status == 409 || strings.Contains(f.ErrorCode, "ALREADY_EXISTS") {
		return wf.MakeStartErrorInstanceAlreadyExists(f.Message)
	}
	return wf.MakeStartErrorWorkflow(workflowError(f))
}

func instanceError(f *call.Failure) wf.InstanceError {
	switch f.Status {
	case 404:
		return wf.MakeInstanceErrorNotFound(f.Message)
	case 409, 412:
		return wf.MakeInstanceErrorInvalidState(f.Message)
	}
	return wf.MakeInstanceErrorWorkflow(workflowError(f))
}

func getError(f *call.Failure) wf.GetError {
	if f.Status == 404 {
		return wf.MakeGetErrorNotFound(f.Message)
	}
	return wf.MakeGetErrorWorkflow(workflowError(f))
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

// instanceCall runs one of the instance-level RPCs that return Empty.
func instanceCall(f func(ctx context.Context, api daprgrpc.API) (any, error)) unitResult {
	if _, fail := call.Do(f); fail != nil {
		return witTypes.Err[witTypes.Unit](instanceError(fail))
	}
	return witTypes.Ok[witTypes.Unit, wf.InstanceError](witTypes.Unit{})
}

func Start(workflowComponent string, workflowName string, instanceID witTypes.Option[string], input []uint8) witTypes.Result[string, wf.StartError] {
	r, f := call.Do(func(ctx context.Context, api daprgrpc.API) (*runtimev1pb.StartWorkflowResponse, error) {
		return api.StartWorkflowBeta1(ctx, &runtimev1pb.StartWorkflowRequest{
			// proto3: an empty instance id asks the runtime to generate one.
			InstanceId:        call.OrEmpty(instanceID),
			WorkflowComponent: workflowComponent,
			WorkflowName:      workflowName,
			Input:             input,
		})
	})
	if f != nil {
		return witTypes.Err[string](startError(f))
	}
	return witTypes.Ok[string, wf.StartError](r.GetInstanceId())
}

func Get(workflowComponent string, instanceID string) witTypes.Result[wf.WorkflowInstance, wf.GetError] {
	r, f := call.Do(func(ctx context.Context, api daprgrpc.API) (*runtimev1pb.GetWorkflowResponse, error) {
		return api.GetWorkflowBeta1(ctx, &runtimev1pb.GetWorkflowRequest{InstanceId: instanceID, WorkflowComponent: workflowComponent})
	})
	if f != nil {
		return witTypes.Err[wf.WorkflowInstance](getError(f))
	}
	return witTypes.Ok[wf.WorkflowInstance, wf.GetError](wf.WorkflowInstance{
		InstanceId:    r.GetInstanceId(),
		CreatedAt:     rfc3339(r.GetCreatedAt()),
		LastUpdatedAt: rfc3339(r.GetLastUpdatedAt()),
		RuntimeStatus: r.GetRuntimeStatus(),
		Properties:    call.MetadataPairs(r.GetProperties()),
	})
}

func Terminate(workflowComponent string, instanceID string) witTypes.Result[witTypes.Unit, wf.InstanceError] {
	return instanceCall(func(ctx context.Context, api daprgrpc.API) (any, error) {
		return api.TerminateWorkflowBeta1(ctx, &runtimev1pb.TerminateWorkflowRequest{InstanceId: instanceID, WorkflowComponent: workflowComponent})
	})
}

func RaiseEvent(workflowComponent string, instanceID string, eventName string, eventData []uint8) witTypes.Result[witTypes.Unit, wf.InstanceError] {
	return instanceCall(func(ctx context.Context, api daprgrpc.API) (any, error) {
		return api.RaiseEventWorkflowBeta1(ctx, &runtimev1pb.RaiseEventWorkflowRequest{
			InstanceId: instanceID, WorkflowComponent: workflowComponent, EventName: eventName, EventData: eventData,
		})
	})
}

func Pause(workflowComponent string, instanceID string) witTypes.Result[witTypes.Unit, wf.InstanceError] {
	return instanceCall(func(ctx context.Context, api daprgrpc.API) (any, error) {
		return api.PauseWorkflowBeta1(ctx, &runtimev1pb.PauseWorkflowRequest{InstanceId: instanceID, WorkflowComponent: workflowComponent})
	})
}

func Resume(workflowComponent string, instanceID string) witTypes.Result[witTypes.Unit, wf.InstanceError] {
	return instanceCall(func(ctx context.Context, api daprgrpc.API) (any, error) {
		return api.ResumeWorkflowBeta1(ctx, &runtimev1pb.ResumeWorkflowRequest{InstanceId: instanceID, WorkflowComponent: workflowComponent})
	})
}

func Purge(workflowComponent string, instanceID string) witTypes.Result[witTypes.Unit, wf.InstanceError] {
	return instanceCall(func(ctx context.Context, api daprgrpc.API) (any, error) {
		return api.PurgeWorkflowBeta1(ctx, &runtimev1pb.PurgeWorkflowRequest{InstanceId: instanceID, WorkflowComponent: workflowComponent})
	})
}
