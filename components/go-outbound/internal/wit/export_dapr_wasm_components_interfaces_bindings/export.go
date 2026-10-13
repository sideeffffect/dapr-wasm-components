// Output bindings, served by the embedded runtime's API — InvokeBinding.
// No JSON envelope: the payload travels as raw bytes, and response metadata
// is the map the binding component returned. Mirrors wasi-grpc-outbound's
// bindings_block.rs.
package export_dapr_wasm_components_interfaces_bindings

import (
	"context"
	"strings"

	daprgrpc "github.com/dapr/dapr/pkg/api/grpc"
	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	witTypes "go.bytecodealliance.org/pkg/wit/types"

	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	bd "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_bindings"
)

func bindingsError(f *call.Failure) bd.BindingsError {
	if f.IsPermission() {
		return bd.MakeBindingsErrorPermissionDenied(f.Message)
	}
	return bd.MakeBindingsErrorBindingNotFound(f.Message)
}

func invokeBindingError(f *call.Failure) bd.InvokeBindingError {
	if strings.Contains(f.ErrorCode, "NOT_SUPPORTED") || strings.Contains(f.ErrorCode, "UNSUPPORTED") ||
		strings.Contains(f.Message, "not supported") {
		return bd.MakeInvokeBindingErrorOperationNotSupported(f.Message)
	}
	return bd.MakeInvokeBindingErrorBindings(bindingsError(f))
}

func InvokeBinding(name string, operation string, data []uint8, md []witTypes.Tuple2[string, string]) witTypes.Result[bd.InvokeBindingResponse, bd.InvokeBindingError] {
	r, f := call.Do(func(ctx context.Context, api daprgrpc.API) (*runtimev1pb.InvokeBindingResponse, error) {
		return api.InvokeBinding(ctx, &runtimev1pb.InvokeBindingRequest{
			Name: name, Data: data, Metadata: call.MetadataMap(md), Operation: operation,
		})
	})
	if f != nil {
		return witTypes.Err[bd.InvokeBindingResponse](invokeBindingError(f))
	}
	return witTypes.Ok[bd.InvokeBindingResponse, bd.InvokeBindingError](bd.InvokeBindingResponse{
		Data: r.GetData(), Metadata: call.MetadataPairs(r.GetMetadata()),
	})
}
