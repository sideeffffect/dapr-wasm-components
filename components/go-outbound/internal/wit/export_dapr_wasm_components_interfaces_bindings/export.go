package export_dapr_wasm_components_interfaces_bindings

import (
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_bindings"
	witTypes "go.bytecodealliance.org/pkg/wit/types"
)

func InvokeBinding(_name string, _operation string, _data []uint8, _metadata []witTypes.Tuple2[string, string]) witTypes.Result[dapr_wasm_components_interfaces_bindings.InvokeBindingResponse, dapr_wasm_components_interfaces_bindings.InvokeBindingError] {
	panic("not implemented")
}
