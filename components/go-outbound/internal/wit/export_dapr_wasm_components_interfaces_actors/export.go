// Actors (client side), served by the embedded runtime's API —
// InvokeActor, GetActorState, ExecuteActorStateTransaction,
// RegisterActorReminder, GetActorReminder, UnregisterActorReminder,
// RegisterActorTimer, UnregisterActorTimer. Mirrors wasi-grpc-outbound's
// actors.rs, including its quirk: the runtime's gRPC reminder/timer
// registration wraps data with json.Marshal([]byte), i.e. base64, so a
// reminder registered here reaches its callback base64-wrapped.
package export_dapr_wasm_components_interfaces_actors

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	daprgrpc "github.com/dapr/dapr/pkg/api/grpc"
	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	witTypes "go.bytecodealliance.org/pkg/wit/types"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/anyjson"
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	ac "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_actors"
)

type unitResult = witTypes.Result[witTypes.Unit, ac.ActorsError]

func actorsError(f *call.Failure) ac.ActorsError {
	return ac.MakeActorsErrorPermissionDenied(f.Message)
}

// unitCall runs one of the RPCs that return Empty and fail with the plain
// actors error.
func unitCall(f func(ctx context.Context, api daprgrpc.API) (any, error)) unitResult {
	if _, fail := call.Do(f); fail != nil {
		return witTypes.Err[witTypes.Unit](actorsError(fail))
	}
	return witTypes.Ok[witTypes.Unit, ac.ActorsError](witTypes.Unit{})
}

func operationPb(op ac.ActorStateOperation) *runtimev1pb.TransactionalActorStateOperation {
	if op.Operation == ac.ActorStateOperationTypeDelete {
		return &runtimev1pb.TransactionalActorStateOperation{OperationType: "delete", Key: op.Key}
	}
	// An upsert without a value is a programming error.
	if op.Value.IsNone() {
		panic(fmt.Sprintf("upsert of %q requires a value", op.Key))
	}
	return &runtimev1pb.TransactionalActorStateOperation{
		OperationType: "upsert",
		Key:           op.Key,
		// The runtime treats this Any opaquely and stores Any.value as the
		// raw bytes; "type.googleapis.com/bytes" is the SDK convention.
		Value: &anypb.Any{TypeUrl: "type.googleapis.com/bytes", Value: op.Value.Some()},
	}
}

func optBytes(o witTypes.Option[string]) []byte {
	if o.IsSome() {
		return []byte(o.Some())
	}
	return nil
}

func Invoke(actorType string, actorID string, method string, body []uint8, contentType witTypes.Option[string]) witTypes.Result[[]uint8, ac.InvokeActorError] {
	// The content type travels in the request metadata map, under the
	// lowercase key the runtime reads. Default like the wasi-http provider.
	ct := "application/json"
	if contentType.IsSome() {
		ct = contentType.Some()
	}
	r, f := call.Do(func(ctx context.Context, api daprgrpc.API) (*runtimev1pb.InvokeActorResponse, error) {
		return api.InvokeActor(ctx, &runtimev1pb.InvokeActorRequest{
			ActorType: actorType, ActorId: actorID, Method: method, Data: body,
			Metadata: map[string]string{"content-type": ct},
		})
	})
	if f != nil {
		return witTypes.Err[[]uint8](ac.MakeInvokeActorErrorActors(actorsError(f)))
	}
	return witTypes.Ok[[]uint8, ac.InvokeActorError](r.GetData())
}

func GetState(actorType string, actorID string, key string) witTypes.Result[[]uint8, ac.GetStateError] {
	r, f := call.Do(func(ctx context.Context, api daprgrpc.API) (*runtimev1pb.GetActorStateResponse, error) {
		return api.GetActorState(ctx, &runtimev1pb.GetActorStateRequest{ActorType: actorType, ActorId: actorID, Key: key})
	})
	if f != nil {
		return witTypes.Err[[]uint8](ac.MakeGetStateErrorActors(actorsError(f)))
	}
	// The API cannot tell a missing key from an empty stored value (HTTP
	// signals absence with 204), so key-not-found is never produced: an
	// absent key comes back as empty data.
	return witTypes.Ok[[]uint8, ac.GetStateError](r.GetData())
}

func ExecuteStateTransaction(actorType string, actorID string, operations []ac.ActorStateOperation) witTypes.Result[witTypes.Unit, ac.ActorsError] {
	ops := make([]*runtimev1pb.TransactionalActorStateOperation, 0, len(operations))
	for _, op := range operations {
		ops = append(ops, operationPb(op))
	}
	return unitCall(func(ctx context.Context, api daprgrpc.API) (any, error) {
		return api.ExecuteActorStateTransaction(ctx, &runtimev1pb.ExecuteActorStateTransactionRequest{
			ActorType: actorType, ActorId: actorID, Operations: ops,
		})
	})
}

func RegisterReminder(actorType string, actorID string, name string, reminder ac.Reminder) witTypes.Result[witTypes.Unit, ac.ActorsError] {
	return unitCall(func(ctx context.Context, api daprgrpc.API) (any, error) {
		return api.RegisterActorReminder(ctx, &runtimev1pb.RegisterActorReminderRequest{
			ActorType: actorType, ActorId: actorID, Name: name,
			DueTime: call.OrEmpty(reminder.DueTime),
			Period:  call.OrEmpty(reminder.Period),
			Data:    optBytes(reminder.Data),
			Ttl:     call.OrEmpty(reminder.Ttl),
		})
	})
}

func GetReminder(actorType string, actorID string, name string) witTypes.Result[string, ac.GetReminderError] {
	r, f := call.Do(func(ctx context.Context, api daprgrpc.API) (*runtimev1pb.GetActorReminderResponse, error) {
		return api.GetActorReminder(ctx, &runtimev1pb.GetActorReminderRequest{ActorType: actorType, ActorId: actorID, Name: name})
	})
	if f != nil {
		if f.Status == 404 {
			return witTypes.Err[string](ac.MakeGetReminderErrorReminderNotFound())
		}
		return witTypes.Err[string](ac.MakeGetReminderErrorActors(actorsError(f)))
	}
	// Shape the proto response like the HTTP API's reminder document.
	doc := map[string]any{}
	if r.GetActorType() != "" {
		doc["actorType"] = r.GetActorType()
	}
	if r.GetActorId() != "" {
		doc["actorID"] = r.GetActorId()
	}
	if r.DueTime != nil {
		doc["dueTime"] = *r.DueTime
	}
	if r.Period != nil {
		doc["period"] = *r.Period
	}
	if r.Ttl != nil {
		doc["ttl"] = *r.Ttl
	}
	if r.GetData() != nil {
		// Embed the payload like the HTTP API: valid JSON as-is, anything
		// else as a JSON string.
		text := anyjson.UnpackJSON(r.GetData())
		if json.Valid([]byte(text)) {
			doc["data"] = json.RawMessage(text)
		} else {
			doc["data"] = text
		}
	}
	// No HTML escaping, so "<", ">" and "&" stay verbatim as in serde_json.
	var out strings.Builder
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(doc); err != nil {
		panic(fmt.Sprintf("encode reminder: %v", err)) // unreachable: strings and valid raw JSON only
	}
	return witTypes.Ok[string, ac.GetReminderError](strings.TrimSuffix(out.String(), "\n"))
}

func UnregisterReminder(actorType string, actorID string, name string) witTypes.Result[witTypes.Unit, ac.ActorsError] {
	return unitCall(func(ctx context.Context, api daprgrpc.API) (any, error) {
		return api.UnregisterActorReminder(ctx, &runtimev1pb.UnregisterActorReminderRequest{ActorType: actorType, ActorId: actorID, Name: name})
	})
}

func RegisterTimer(actorType string, actorID string, name string, timer ac.Timer) witTypes.Result[witTypes.Unit, ac.ActorsError] {
	return unitCall(func(ctx context.Context, api daprgrpc.API) (any, error) {
		return api.RegisterActorTimer(ctx, &runtimev1pb.RegisterActorTimerRequest{
			ActorType: actorType, ActorId: actorID, Name: name,
			DueTime:  call.OrEmpty(timer.DueTime),
			Period:   call.OrEmpty(timer.Period),
			Callback: call.OrEmpty(timer.Callback),
			Data:     optBytes(timer.Data),
			Ttl:      call.OrEmpty(timer.Ttl),
		})
	})
}

func UnregisterTimer(actorType string, actorID string, name string) witTypes.Result[witTypes.Unit, ac.ActorsError] {
	return unitCall(func(ctx context.Context, api daprgrpc.API) (any, error) {
		return api.UnregisterActorTimer(ctx, &runtimev1pb.UnregisterActorTimerRequest{ActorType: actorType, ActorId: actorID, Name: name})
	})
}
