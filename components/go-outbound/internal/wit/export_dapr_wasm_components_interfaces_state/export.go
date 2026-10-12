// State management, served by the embedded runtime's API — GetState,
// GetBulkState, SaveState, DeleteState, ExecuteStateTransaction,
// QueryStateAlpha1. Values are raw bytes and roundtrip byte-exact (no JSON
// envelope). Mirrors wasi-grpc-outbound's state.rs.
package export_dapr_wasm_components_interfaces_state

import (
	"context"
	"strings"

	daprgrpc "github.com/dapr/dapr/pkg/api/grpc"
	commonv1pb "github.com/dapr/dapr/pkg/proto/common/v1"
	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	witTypes "go.bytecodealliance.org/pkg/wit/types"

	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	st "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_state"
)

type metadata = []witTypes.Tuple2[string, string]

func stateError(f *call.Failure) st.StateError {
	if f.IsPermission() {
		return st.MakeStateErrorPermissionDenied(f.Message)
	}
	return st.MakeStateErrorStoreNotFound(f.Message)
}

func writeError(f *call.Failure) st.WriteError {
	if f.Status == 409 || f.Status == 412 {
		return st.MakeWriteErrorEtagMismatch(witTypes.None[string]())
	}
	return st.MakeWriteErrorState(stateError(f))
}

func transactionError(f *call.Failure) st.TransactionError {
	if f.Status == 409 || f.Status == 412 {
		return st.MakeTransactionErrorEtagMismatch(witTypes.None[string]())
	}
	if strings.Contains(f.ErrorCode, "NOT_SUPPORTED") || strings.Contains(f.ErrorCode, "NOT_TRANSACTIONAL") {
		return st.MakeTransactionErrorNotTransactional()
	}
	return st.MakeTransactionErrorState(stateError(f))
}

func queryError(f *call.Failure) st.QueryError {
	if f.Status == 400 {
		return st.MakeQueryErrorInvalidQuery(f.Message)
	}
	return st.MakeQueryErrorState(stateError(f))
}

func concurrencyPb(c st.Concurrency) commonv1pb.StateOptions_StateConcurrency {
	switch c {
	case st.ConcurrencyFirstWrite:
		return commonv1pb.StateOptions_CONCURRENCY_FIRST_WRITE
	case st.ConcurrencyLastWrite:
		return commonv1pb.StateOptions_CONCURRENCY_LAST_WRITE
	}
	return commonv1pb.StateOptions_CONCURRENCY_UNSPECIFIED
}

func consistencyPb(c st.Consistency) commonv1pb.StateOptions_StateConsistency {
	switch c {
	case st.ConsistencyEventual:
		return commonv1pb.StateOptions_CONSISTENCY_EVENTUAL
	case st.ConsistencyStrong:
		return commonv1pb.StateOptions_CONSISTENCY_STRONG
	}
	return commonv1pb.StateOptions_CONSISTENCY_UNSPECIFIED
}

func optionsPb(o witTypes.Option[st.StateOptions]) *commonv1pb.StateOptions {
	if o.IsNone() {
		return nil
	}
	v := o.Some()
	return &commonv1pb.StateOptions{Concurrency: concurrencyPb(v.Concurrency), Consistency: consistencyPb(v.Consistency)}
}

func etagPb(e witTypes.Option[string]) *commonv1pb.Etag {
	if e.IsNone() {
		return nil
	}
	return &commonv1pb.Etag{Value: e.Some()}
}

// itemPb folds the request-level metadata into each item (item-level entries
// win): the API carries metadata per item, not per request.
func itemPb(item st.StateItem, requestMetadata metadata) *commonv1pb.StateItem {
	md := call.MetadataMap(requestMetadata)
	if md == nil && len(item.Metadata) > 0 {
		md = map[string]string{}
	}
	for _, kv := range item.Metadata {
		md[kv.F0] = kv.F1
	}
	return &commonv1pb.StateItem{
		Key:      item.Key,
		Value:    item.Value,
		Etag:     etagPb(item.Etag),
		Metadata: md,
		Options:  optionsPb(item.Options),
	}
}

func Get(storeName string, key string, consistency witTypes.Option[st.Consistency], md metadata) witTypes.Result[st.GetStateResponse, st.GetError] {
	c := commonv1pb.StateOptions_CONSISTENCY_UNSPECIFIED
	if consistency.IsSome() {
		c = consistencyPb(consistency.Some())
	}
	r, f := call.Do(func(ctx context.Context, api daprgrpc.API) (*runtimev1pb.GetStateResponse, error) {
		return api.GetState(ctx, &runtimev1pb.GetStateRequest{
			StoreName: storeName, Key: key, Consistency: c, Metadata: call.MetadataMap(md),
		})
	})
	if f != nil {
		return witTypes.Err[st.GetStateResponse](st.MakeGetErrorState(stateError(f)))
	}
	// Like the gRPC provider, the API cannot tell a missing key from a stored
	// empty value, so key-not-found is never produced: an absent key comes
	// back as empty data and no etag.
	return witTypes.Ok[st.GetStateResponse, st.GetError](st.GetStateResponse{
		Data: r.GetData(), Etag: call.OptString(r.GetEtag()),
	})
}

func GetBulk(storeName string, keys []string, parallelism witTypes.Option[uint32], md metadata) witTypes.Result[[]st.BulkStateItem, st.StateError] {
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
		return witTypes.Err[[]st.BulkStateItem](stateError(f))
	}
	items := make([]st.BulkStateItem, 0, len(r.GetItems()))
	for _, it := range r.GetItems() {
		items = append(items, st.BulkStateItem{
			Key: it.GetKey(), Value: it.GetData(), Etag: call.OptString(it.GetEtag()), Error: call.OptString(it.GetError()),
		})
	}
	return witTypes.Ok[[]st.BulkStateItem, st.StateError](items)
}

func Save(storeName string, items []st.StateItem, md metadata) witTypes.Result[witTypes.Unit, st.WriteError] {
	states := make([]*commonv1pb.StateItem, 0, len(items))
	for _, it := range items {
		states = append(states, itemPb(it, md))
	}
	_, f := call.Do(func(ctx context.Context, api daprgrpc.API) (any, error) {
		return api.SaveState(ctx, &runtimev1pb.SaveStateRequest{StoreName: storeName, States: states})
	})
	if f != nil {
		return witTypes.Err[witTypes.Unit](writeError(f))
	}
	return witTypes.Ok[witTypes.Unit, st.WriteError](witTypes.Unit{})
}

func Delete(storeName string, key string, etag witTypes.Option[string], options witTypes.Option[st.StateOptions], md metadata) witTypes.Result[witTypes.Unit, st.WriteError] {
	_, f := call.Do(func(ctx context.Context, api daprgrpc.API) (any, error) {
		return api.DeleteState(ctx, &runtimev1pb.DeleteStateRequest{
			StoreName: storeName, Key: key, Etag: etagPb(etag), Options: optionsPb(options), Metadata: call.MetadataMap(md),
		})
	})
	if f != nil {
		return witTypes.Err[witTypes.Unit](writeError(f))
	}
	return witTypes.Ok[witTypes.Unit, st.WriteError](witTypes.Unit{})
}

func ExecuteTransaction(storeName string, operations []st.TransactionRequest, md metadata) witTypes.Result[witTypes.Unit, st.TransactionError] {
	ops := make([]*runtimev1pb.TransactionalStateOperation, 0, len(operations))
	for _, op := range operations {
		kind := "upsert"
		if op.Operation == st.TransactionOperationDelete {
			kind = "delete"
		}
		var value []byte
		if op.Value.IsSome() {
			value = op.Value.Some()
		}
		ops = append(ops, &runtimev1pb.TransactionalStateOperation{
			OperationType: kind,
			Request: &commonv1pb.StateItem{
				Key: op.Key, Value: value, Etag: etagPb(op.Etag), Metadata: call.MetadataMap(op.Metadata), Options: optionsPb(op.Options),
			},
		})
	}
	_, f := call.Do(func(ctx context.Context, api daprgrpc.API) (any, error) {
		return api.ExecuteStateTransaction(ctx, &runtimev1pb.ExecuteStateTransactionRequest{
			StoreName: storeName, Operations: ops, Metadata: call.MetadataMap(md),
		})
	})
	if f != nil {
		return witTypes.Err[witTypes.Unit](transactionError(f))
	}
	return witTypes.Ok[witTypes.Unit, st.TransactionError](witTypes.Unit{})
}

func Query(storeName string, query string, md metadata) witTypes.Result[st.QueryResponse, st.QueryError] {
	r, f := call.Do(func(ctx context.Context, api daprgrpc.API) (*runtimev1pb.QueryStateResponse, error) {
		return api.QueryStateAlpha1(ctx, &runtimev1pb.QueryStateRequest{StoreName: storeName, Query: query, Metadata: call.MetadataMap(md)})
	})
	if f != nil {
		return witTypes.Err[st.QueryResponse](queryError(f))
	}
	items := make([]st.QueryStateItem, 0, len(r.GetResults()))
	for _, it := range r.GetResults() {
		items = append(items, st.QueryStateItem{Key: it.GetKey(), Data: it.GetData(), Etag: call.OptString(it.GetEtag())})
	}
	return witTypes.Ok[st.QueryResponse, st.QueryError](st.QueryResponse{Items: items, Token: call.OptString(r.GetToken())})
}
