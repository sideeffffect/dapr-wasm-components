package export_dapr_wasm_components_interfaces_invocation

import (
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_invocation"
	witTypes "go.bytecodealliance.org/pkg/wit/types"
)

func Invoke(_appId string, _methodPath string, _verb dapr_wasm_components_interfaces_invocation.HttpVerb, _headers []witTypes.Tuple2[string, string], _query witTypes.Option[string], _body []uint8) witTypes.Result[dapr_wasm_components_interfaces_invocation.HttpResponse, dapr_wasm_components_interfaces_invocation.InvokeError] {
	panic("not implemented")
}
