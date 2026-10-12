// State reads, served by the embedded runtime's API — GetState,
// GetBulkState, QueryStateAlpha1. Values are raw bytes and roundtrip
// byte-exact (no JSON envelope). Mirrors wasi-grpc-outbound's state.rs
// (the state-read half).
package export_dapr_wasm_components_interfaces_state_read

import (
	"context"

	daprgrpc "github.com/dapr/dapr/pkg/api/grpc"
	commonv1pb "github.com/dapr/dapr/pkg/proto/common/v1"
	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	witTypes "go.bytecodealliance.org/pkg/wit/types"

	stblk "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/blocks/state"
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	sr "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_state_read"
)

type metadata = []witTypes.Tuple2[string, string]

func queryError(f *call.Failure) sr.QueryError {
	if f.Status == 400 {
		return sr.MakeQueryErrorInvalidQuery(f.Message)
	}
	return sr.MakeQueryErrorState(stblk.Error(f))
}

func Get(storeName string, key string, consistency witTypes.Option[sr.Consistency], md metadata) witTypes.Result[sr.GetStateResponse, sr.GetError] {
	c := commonv1pb.StateOptions_CONSISTENCY_UNSPECIFIED
	if consistency.IsSome() {
		c = stblk.ConsistencyPb(consistency.Some())
	}
	r, f := call.Do(func(ctx context.Context, api daprgrpc.API) (*runtimev1pb.GetStateResponse, error) {
		return api.GetState(ctx, &runtimev1pb.GetStateRequest{
			StoreName: storeName, Key: key, Consistency: c, Metadata: call.MetadataMap(md),
		})
	})
	if f != nil {
		return witTypes.Err[sr.GetStateResponse](sr.MakeGetErrorState(stblk.Error(f)))
	}
	// Like the gRPC provider, the API cannot tell a missing key from a stored
	// empty value, so key-not-found is never produced: an absent key comes
	// back as empty data and no etag.
	return witTypes.Ok[sr.GetStateResponse, sr.GetError](sr.GetStateResponse{
		Data: r.GetData(), Etag: call.OptString(r.GetEtag()),
	})
}

func GetBulk(storeName string, keys []string, parallelism witTypes.Option[uint32], md metadata) witTypes.Result[[]sr.BulkStateItem, sr.StateError] {
	var p int32
	if parallelism.IsSome() {
		p = int32(parallelism.Some())
	}
	r, f := call.Do(func(ctx context.Context, api daprgrpc.API) (*runtimev1pb.GetBulkStateResponse, error) {
		return api.GetBulkState(ctx, &runtimev1pb.GetBulkStateRequest{
			StoreName: storeName, Keys: keys, Parallelism: p, Metadata: call.MetadataMap(md),
		})
	})
	if f != nil {
		return witTypes.Err[[]sr.BulkStateItem](stblk.Error(f))
	}
	items := make([]sr.BulkStateItem, 0, len(r.GetItems()))
	for _, it := range r.GetItems() {
		items = append(items, sr.BulkStateItem{
			Key: it.GetKey(), Value: it.GetData(), Etag: call.OptString(it.GetEtag()), Error: call.OptString(it.GetError()),
		})
	}
	return witTypes.Ok[[]sr.BulkStateItem, sr.StateError](items)
}

func Query(storeName string, query string, md metadata) witTypes.Result[sr.QueryResponse, sr.QueryError] {
	r, f := call.Do(func(ctx context.Context, api daprgrpc.API) (*runtimev1pb.QueryStateResponse, error) {
		return api.QueryStateAlpha1(ctx, &runtimev1pb.QueryStateRequest{StoreName: storeName, Query: query, Metadata: call.MetadataMap(md)})
	})
	if f != nil {
		return witTypes.Err[sr.QueryResponse](queryError(f))
	}
	items := make([]sr.QueryStateItem, 0, len(r.GetResults()))
	for _, it := range r.GetResults() {
		items = append(items, sr.QueryStateItem{Key: it.GetKey(), Data: it.GetData(), Etag: call.OptString(it.GetEtag())})
	}
	return witTypes.Ok[sr.QueryResponse, sr.QueryError](sr.QueryResponse{Items: items, Token: call.OptString(r.GetToken())})
}
