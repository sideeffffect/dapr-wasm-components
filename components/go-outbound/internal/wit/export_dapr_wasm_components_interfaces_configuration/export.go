package export_dapr_wasm_components_interfaces_configuration

import (
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_configuration"
	witTypes "go.bytecodealliance.org/pkg/wit/types"
)

func Get(_storeName string, _keys []string, _metadata []witTypes.Tuple2[string, string]) witTypes.Result[[]witTypes.Tuple2[string, dapr_wasm_components_interfaces_configuration.ConfigurationItem], dapr_wasm_components_interfaces_configuration.ConfigurationError] {
	panic("not implemented")
}

func Subscribe(_storeName string, _keys []string, _metadata []witTypes.Tuple2[string, string]) witTypes.Result[string, dapr_wasm_components_interfaces_configuration.ConfigurationError] {
	panic("not implemented")
}

func Unsubscribe(_storeName string, _id string) witTypes.Result[witTypes.Unit, dapr_wasm_components_interfaces_configuration.UnsubscribeError] {
	panic("not implemented")
}
