package export_dapr_wasm_components_interfaces_actors

import (
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_actors"
	witTypes "go.bytecodealliance.org/pkg/wit/types"
)

func Invoke(_actorType string, _actorId string, _method string, _body []uint8, _contentType witTypes.Option[string]) witTypes.Result[[]uint8, dapr_wasm_components_interfaces_actors.InvokeActorError] {
	panic("not implemented")
}

func GetState(_actorType string, _actorId string, _key string) witTypes.Result[[]uint8, dapr_wasm_components_interfaces_actors.GetStateError] {
	panic("not implemented")
}

func ExecuteStateTransaction(_actorType string, _actorId string, _operations []dapr_wasm_components_interfaces_actors.ActorStateOperation) witTypes.Result[witTypes.Unit, dapr_wasm_components_interfaces_actors.ActorsError] {
	panic("not implemented")
}

func RegisterReminder(_actorType string, _actorId string, _name string, _reminder dapr_wasm_components_interfaces_actors.Reminder) witTypes.Result[witTypes.Unit, dapr_wasm_components_interfaces_actors.ActorsError] {
	panic("not implemented")
}

func GetReminder(_actorType string, _actorId string, _name string) witTypes.Result[string, dapr_wasm_components_interfaces_actors.GetReminderError] {
	panic("not implemented")
}

func UnregisterReminder(_actorType string, _actorId string, _name string) witTypes.Result[witTypes.Unit, dapr_wasm_components_interfaces_actors.ActorsError] {
	panic("not implemented")
}

func RegisterTimer(_actorType string, _actorId string, _name string, _timer dapr_wasm_components_interfaces_actors.Timer) witTypes.Result[witTypes.Unit, dapr_wasm_components_interfaces_actors.ActorsError] {
	panic("not implemented")
}

func UnregisterTimer(_actorType string, _actorId string, _name string) witTypes.Result[witTypes.Unit, dapr_wasm_components_interfaces_actors.ActorsError] {
	panic("not implemented")
}
