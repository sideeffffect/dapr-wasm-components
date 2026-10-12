package export_dapr_wasm_components_interfaces_jobs

import (
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_jobs"
	witTypes "go.bytecodealliance.org/pkg/wit/types"
)

func Schedule(_name string, _job dapr_wasm_components_interfaces_jobs.Job, _overwrite bool) witTypes.Result[witTypes.Unit, dapr_wasm_components_interfaces_jobs.ScheduleError] {
	panic("not implemented")
}

func Get(_name string) witTypes.Result[dapr_wasm_components_interfaces_jobs.Job, dapr_wasm_components_interfaces_jobs.GetJobError] {
	panic("not implemented")
}

func Delete(_name string) witTypes.Result[witTypes.Unit, dapr_wasm_components_interfaces_jobs.JobsError] {
	panic("not implemented")
}
