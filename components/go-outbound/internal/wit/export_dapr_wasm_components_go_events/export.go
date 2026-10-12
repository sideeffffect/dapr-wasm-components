package export_dapr_wasm_components_go_events

import (
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_go_events"
)

func Attach() {
	panic("not implemented")
}

func NextEvent() (uint64, dapr_wasm_components_go_events.Event) {
	panic("not implemented")
}

func CompleteEvent(_id uint64, _outcome dapr_wasm_components_go_events.EventResult) {
	panic("not implemented")
}
