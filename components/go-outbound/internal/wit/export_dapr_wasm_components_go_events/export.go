// The private events interface pumped by go-inbound (see internal/events).
package export_dapr_wasm_components_go_events

import (
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/appchannel"
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/daprrt"
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/events"
	ev "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_go_events"
)

// Attach enables the app channel and starts the runtime with it.
func Attach() {
	events.Attach()
	if !daprrt.StartAttached(&appchannel.Handler{}) {
		panic("attach: the Dapr runtime was already started without an app channel " +
			"(a building block was called before go-inbound attached)")
	}
}

func NextEvent() (uint64, ev.Event) {
	return events.Next()
}

func CompleteEvent(id uint64, outcome ev.EventResult) {
	events.Complete(id, outcome)
}
