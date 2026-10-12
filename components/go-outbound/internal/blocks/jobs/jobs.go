// Package jobs holds the logic both halves of the jobs block share
// (jobs-read and jobs): the base error mapping.
package jobs

import (
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	jr "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_jobs_read"
)

// Error maps a recoverable failure to the shared jobs-error.
func Error(f *call.Failure) jr.JobsError {
	return jr.MakeJobsErrorPermissionDenied(f.Message)
}
