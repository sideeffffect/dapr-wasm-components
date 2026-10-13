// Package workflow holds the logic both halves of the workflow block share
// (workflow-read and workflow): the target component and the base error
// mapping.
package workflow

import (
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	wr "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_workflow_read"
)

// Component is Dapr's only workflow engine, the built-in workflow component
// "dapr"; the WIT functions take no component name and always target it.
const Component = "dapr"

// Error maps a recoverable failure to the shared workflow-error.
func Error(f *call.Failure) wr.WorkflowError {
	if f.IsPermission() {
		return wr.MakeWorkflowErrorPermissionDenied(f.Message)
	}
	return wr.MakeWorkflowErrorComponentNotFound(f.Message)
}
