package export_dapr_wasm_components_interfaces_secrets

import (
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_secrets"
	witTypes "go.bytecodealliance.org/pkg/wit/types"
)

func GetSecret(_storeName string, _key string, _metadata []witTypes.Tuple2[string, string]) witTypes.Result[[]witTypes.Tuple2[string, string], dapr_wasm_components_interfaces_secrets.GetSecretError] {
	panic("not implemented")
}

func GetBulkSecret(_storeName string, _metadata []witTypes.Tuple2[string, string]) witTypes.Result[[]witTypes.Tuple2[string, []witTypes.Tuple2[string, string]], dapr_wasm_components_interfaces_secrets.SecretsError] {
	panic("not implemented")
}
