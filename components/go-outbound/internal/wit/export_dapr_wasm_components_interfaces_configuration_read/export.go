// Configuration reads, served by the embedded runtime's API — the stable
// GetConfiguration (not the Alpha1 variant). Mirrors wasi-grpc-outbound's
// configuration.rs (the configuration-read half).
package export_dapr_wasm_components_interfaces_configuration_read

import (
	"context"

	daprgrpc "github.com/dapr/dapr/pkg/api/grpc"
	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	witTypes "go.bytecodealliance.org/pkg/wit/types"

	cfblk "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/blocks/configuration"
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	cr "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_configuration_read"
)

func Get(storeName string, keys []string, md []witTypes.Tuple2[string, string]) witTypes.Result[cfblk.Items, cr.ConfigurationError] {
	r, f := call.Do(func(ctx context.Context, api daprgrpc.API) (*runtimev1pb.GetConfigurationResponse, error) {
		return api.GetConfiguration(ctx, &runtimev1pb.GetConfigurationRequest{
			StoreName: storeName, Keys: keys, Metadata: call.MetadataMap(md),
		})
	})
	if f != nil {
		return witTypes.Err[cfblk.Items](cfblk.Error(f))
	}
	return witTypes.Ok[cfblk.Items, cr.ConfigurationError](cfblk.ItemsWit(r.GetItems()))
}
