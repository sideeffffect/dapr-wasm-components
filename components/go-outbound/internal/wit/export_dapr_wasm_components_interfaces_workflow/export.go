// Workflow management, served by the embedded runtime's API —
// StartWorkflowBeta1, TerminateWorkflowBeta1, RaiseEventWorkflowBeta1,
// PauseWorkflowBeta1, ResumeWorkflowBeta1, PurgeWorkflowBeta1 (the Alpha1
// set is deprecated), always against the built-in "dapr" workflow
// component. Input and event payloads are raw bytes. The reads live in
// workflow-read. Mirrors wasi-grpc-outbound's workflow.rs (the workflow
// half).
package export_dapr_wasm_components_interfaces_workflow

import (
	"context"
	"strings"

	daprgrpc "github.com/dapr/dapr/pkg/api/grpc"
	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	witTypes "go.bytecodealliance.org/pkg/wit/types"

	wfblk "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/blocks/workflow"
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	wf "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_workflow"
)

type unitResult = witTypes.Result[witTypes.Unit, wf.InstanceError]

func startError(f *call.Failure) wf.StartError {
	if f.Status == 409 || strings.Contains(f.ErrorCode, "ALREADY_EXISTS") {
		return wf.MakeStartErrorInstanceAlreadyExists(f.Message)
	}
	return wf.MakeStartErrorWorkflow(wfblk.Error(f))
}

func instanceError(f *call.Failure) wf.InstanceError {
	switch f.Status {
	case 404:
		return wf.MakeInstanceErrorNotFound(f.Message)
	case 409, 412:
		return wf.MakeInstanceErrorInvalidState(f.Message)
	}
	return wf.MakeInstanceErrorWorkflow(wfblk.Error(f))
}

// instanceCall runs one of the instance-level RPCs that return Empty.
func instanceCall(f func(ctx context.Context, api daprgrpc.API) (any, error)) unitResult {
	if _, fail := call.Do(f); fail != nil {
		return witTypes.Err[witTypes.Unit](instanceError(fail))
	}
	return witTypes.Ok[witTypes.Unit, wf.InstanceError](witTypes.Unit{})
}

func Start(workflowName string, instanceID witTypes.Option[string], input []uint8) witTypes.Result[string, wf.StartError] {
	r, f := call.Do(func(ctx context.Context, api daprgrpc.API) (*runtimev1pb.StartWorkflowResponse, error) {
		return api.StartWorkflowBeta1(ctx, &runtimev1pb.StartWorkflowRequest{
			// proto3: an empty instance id asks the runtime to generate one.
			InstanceId:        call.OrEmpty(instanceID),
			WorkflowComponent: wfblk.Component,
			WorkflowName:      workflowName,
			Input:             input,
		})
	})
	if f != nil {
		return witTypes.Err[string](startError(f))
	}
	return witTypes.Ok[string, wf.StartError](r.GetInstanceId())
}

func Terminate(instanceID string) witTypes.Result[witTypes.Unit, wf.InstanceError] {
	return instanceCall(func(ctx context.Context, api daprgrpc.API) (any, error) {
		return api.TerminateWorkflowBeta1(ctx, &runtimev1pb.TerminateWorkflowRequest{InstanceId: instanceID, WorkflowComponent: wfblk.Component})
	})
}

func RaiseEvent(instanceID string, eventName string, eventData []uint8) witTypes.Result[witTypes.Unit, wf.InstanceError] {
	return instanceCall(func(ctx context.Context, api daprgrpc.API) (any, error) {
		return api.RaiseEventWorkflowBeta1(ctx, &runtimev1pb.RaiseEventWorkflowRequest{
			InstanceId: instanceID, WorkflowComponent: wfblk.Component, EventName: eventName, EventData: eventData,
		})
	})
}

func Pause(instanceID string) witTypes.Result[witTypes.Unit, wf.InstanceError] {
	return instanceCall(func(ctx context.Context, api daprgrpc.API) (any, error) {
		return api.PauseWorkflowBeta1(ctx, &runtimev1pb.PauseWorkflowRequest{InstanceId: instanceID, WorkflowComponent: wfblk.Component})
	})
}

func Resume(instanceID string) witTypes.Result[witTypes.Unit, wf.InstanceError] {
	return instanceCall(func(ctx context.Context, api daprgrpc.API) (any, error) {
		return api.ResumeWorkflowBeta1(ctx, &runtimev1pb.ResumeWorkflowRequest{InstanceId: instanceID, WorkflowComponent: wfblk.Component})
	})
}

func Purge(instanceID string) witTypes.Result[witTypes.Unit, wf.InstanceError] {
	return instanceCall(func(ctx context.Context, api daprgrpc.API) (any, error) {
		return api.PurgeWorkflowBeta1(ctx, &runtimev1pb.PurgeWorkflowRequest{InstanceId: instanceID, WorkflowComponent: wfblk.Component})
	})
}
