// State writes, served by the embedded runtime's API — SaveState,
// DeleteState, ExecuteStateTransaction. Values are raw bytes and roundtrip
// byte-exact (no JSON envelope). The reads live in state-read. Mirrors
// wasi-grpc-outbound's state.rs (the state half).
package export_dapr_wasm_components_interfaces_state

import (
	"context"
	"strings"

	daprgrpc "github.com/dapr/dapr/pkg/api/grpc"
	commonv1pb "github.com/dapr/dapr/pkg/proto/common/v1"
	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	witTypes "go.bytecodealliance.org/pkg/wit/types"

	stblk "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/blocks/state"
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	st "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_state"
	sr "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_state_read"
)

type metadata = []witTypes.Tuple2[string, string]

func writeError(f *call.Failure) st.WriteError {
	if f.Status == 409 || f.Status == 412 {
		return st.MakeWriteErrorEtagMismatch(witTypes.None[string]())
	}
	return st.MakeWriteErrorState(stblk.Error(f))
}

func transactionError(f *call.Failure) st.TransactionError {
	if f.Status == 409 || f.Status == 412 {
		return st.MakeTransactionErrorEtagMismatch(witTypes.None[string]())
	}
	if strings.Contains(f.ErrorCode, "NOT_SUPPORTED") || strings.Contains(f.ErrorCode, "NOT_TRANSACTIONAL") {
		return st.MakeTransactionErrorNotTransactional()
	}
	return st.MakeTransactionErrorState(stblk.Error(f))
}

func concurrencyPb(c sr.Concurrency) commonv1pb.StateOptions_StateConcurrency {
	switch c {
	case sr.ConcurrencyFirstWrite:
		return commonv1pb.StateOptions_CONCURRENCY_FIRST_WRITE
	case sr.ConcurrencyLastWrite:
		return commonv1pb.StateOptions_CONCURRENCY_LAST_WRITE
	}
	return commonv1pb.StateOptions_CONCURRENCY_UNSPECIFIED
}

func optionsPb(o witTypes.Option[sr.StateOptions]) *commonv1pb.StateOptions {
	if o.IsNone() {
		return nil
	}
	v := o.Some()
	return &commonv1pb.StateOptions{Concurrency: concurrencyPb(v.Concurrency), Consistency: stblk.ConsistencyPb(v.Consistency)}
}

func etagPb(e witTypes.Option[string]) *commonv1pb.Etag {
	if e.IsNone() {
		return nil
	}
	return &commonv1pb.Etag{Value: e.Some()}
}

// itemPb folds the request-level metadata into each item (item-level entries
// win): the API carries metadata per item, not per request.
func itemPb(item sr.StateItem, requestMetadata metadata) *commonv1pb.StateItem {
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

func Save(storeName string, items []sr.StateItem, md metadata) witTypes.Result[witTypes.Unit, st.WriteError] {
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

func Delete(storeName string, key string, etag witTypes.Option[string], options witTypes.Option[sr.StateOptions], md metadata) witTypes.Result[witTypes.Unit, st.WriteError] {
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
