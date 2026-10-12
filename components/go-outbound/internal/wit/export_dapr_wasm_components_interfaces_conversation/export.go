package export_dapr_wasm_components_interfaces_conversation

import (
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_conversation"
	witTypes "go.bytecodealliance.org/pkg/wit/types"
)

func Converse(_componentName string, _inputs []dapr_wasm_components_interfaces_conversation.ConversationInput, _options witTypes.Option[dapr_wasm_components_interfaces_conversation.ConversationOptions]) witTypes.Result[dapr_wasm_components_interfaces_conversation.ConversationResponse, dapr_wasm_components_interfaces_conversation.ConverseError] {
	panic("not implemented")
}
