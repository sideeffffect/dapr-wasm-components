package export_dapr_wasm_components_interfaces_workflow

import (
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_workflow"
	witTypes "go.bytecodealliance.org/pkg/wit/types"
)

func Start(_workflowComponent string, _workflowName string, _instanceId witTypes.Option[string], _input []uint8) witTypes.Result[string, dapr_wasm_components_interfaces_workflow.StartError] {
	panic("not implemented")
}

func Get(_workflowComponent string, _instanceId string) witTypes.Result[dapr_wasm_components_interfaces_workflow.WorkflowInstance, dapr_wasm_components_interfaces_workflow.GetError] {
	panic("not implemented")
}

func Terminate(_workflowComponent string, _instanceId string) witTypes.Result[witTypes.Unit, dapr_wasm_components_interfaces_workflow.InstanceError] {
	panic("not implemented")
}

func RaiseEvent(_workflowComponent string, _instanceId string, _eventName string, _eventData []uint8) witTypes.Result[witTypes.Unit, dapr_wasm_components_interfaces_workflow.InstanceError] {
	panic("not implemented")
}

func Pause(_workflowComponent string, _instanceId string) witTypes.Result[witTypes.Unit, dapr_wasm_components_interfaces_workflow.InstanceError] {
	panic("not implemented")
}

func Resume(_workflowComponent string, _instanceId string) witTypes.Result[witTypes.Unit, dapr_wasm_components_interfaces_workflow.InstanceError] {
	panic("not implemented")
}

func Purge(_workflowComponent string, _instanceId string) witTypes.Result[witTypes.Unit, dapr_wasm_components_interfaces_workflow.InstanceError] {
	panic("not implemented")
}
