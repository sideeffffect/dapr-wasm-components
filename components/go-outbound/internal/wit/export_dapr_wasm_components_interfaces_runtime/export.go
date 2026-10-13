// Sidecar metadata and health, served by the embedded runtime — GetMetadata,
// SetMetadata. Both health functions use GetMetadata: the in-process API is
// only handed out once the runtime and its outbound building blocks are
// initialized, which is exactly what healthz/outbound-healthz report.
package export_dapr_wasm_components_interfaces_runtime

import (
	"context"
	"encoding/json"
	"fmt"

	daprgrpc "github.com/dapr/dapr/pkg/api/grpc"
	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	witTypes "go.bytecodealliance.org/pkg/wit/types"

	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	rt "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_runtime"
)

type json_ = map[string]any

// metadataJSON renders the proto metadata as a JSON document shaped like the
// HTTP /v1.0/metadata response (the WIT contract returns JSON).
func metadataJSON(r *runtimev1pb.GetMetadataResponse) json_ {
	components := []json_{}
	for _, c := range r.GetRegisteredComponents() {
		components = append(components, json_{
			"name": c.GetName(), "type": c.GetType(), "version": c.GetVersion(), "capabilities": c.GetCapabilities(),
		})
	}
	endpoints := []json_{}
	for _, e := range r.GetHttpEndpoints() {
		endpoints = append(endpoints, json_{"name": e.GetName()})
	}
	subscriptions := []json_{}
	for _, s := range r.GetSubscriptions() {
		subscriptions = append(subscriptions, json_{
			"pubsubname": s.GetPubsubName(), "topic": s.GetTopic(), "deadLetterTopic": s.GetDeadLetterTopic(), "type": s.GetType().String(),
		})
	}
	doc := json_{
		"id":              r.GetId(),
		"runtimeVersion":  r.GetRuntimeVersion(),
		"enabledFeatures": r.GetEnabledFeatures(),
		"components":      components,
		"httpEndpoints":   endpoints,
		"subscriptions":   subscriptions,
		"extended":        r.GetExtendedMetadata(),
	}
	if a := r.GetAppConnectionProperties(); a != nil {
		doc["appConnectionProperties"] = json_{
			"port": a.GetPort(), "protocol": a.GetProtocol(), "channelAddress": a.GetChannelAddress(), "maxConcurrency": a.GetMaxConcurrency(),
		}
	}
	if a := r.GetActorRuntime(); a != nil {
		doc["actorRuntime"] = json_{
			"runtimeStatus": a.GetRuntimeStatus().String(), "hostReady": a.GetHostReady(), "placement": a.GetPlacement(),
		}
	}
	if s := r.GetScheduler(); s != nil {
		doc["scheduler"] = json_{"connectedAddresses": s.GetConnectedAddresses()}
	}
	return doc
}

func getMetadata() (*runtimev1pb.GetMetadataResponse, *call.Failure) {
	return call.Do(func(ctx context.Context, api daprgrpc.API) (*runtimev1pb.GetMetadataResponse, error) {
		return api.GetMetadata(ctx, &runtimev1pb.GetMetadataRequest{})
	})
}

func GetMetadata() witTypes.Result[string, rt.RuntimeError] {
	r, f := getMetadata()
	if f != nil {
		return witTypes.Err[string](rt.MakeRuntimeErrorPermissionDenied(f.Message))
	}
	b, err := json.Marshal(metadataJSON(r))
	if err != nil {
		panic(fmt.Sprintf("failed to serialize metadata: %v", err))
	}
	return witTypes.Ok[string, rt.RuntimeError](string(b))
}

func SetMetadataLabel(key string, value string) witTypes.Result[witTypes.Unit, rt.RuntimeError] {
	_, f := call.Do(func(ctx context.Context, api daprgrpc.API) (any, error) {
		return api.SetMetadata(ctx, &runtimev1pb.SetMetadataRequest{Key: key, Value: value})
	})
	if f != nil {
		return witTypes.Err[witTypes.Unit](rt.MakeRuntimeErrorPermissionDenied(f.Message))
	}
	return witTypes.Ok[witTypes.Unit, rt.RuntimeError](witTypes.Unit{})
}

func Healthz() bool {
	_, f := getMetadata()
	return f == nil
}

func OutboundHealthz() bool {
	_, f := getMetadata()
	return f == nil
}
