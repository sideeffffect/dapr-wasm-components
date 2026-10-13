// Bulk secret listing, served by the embedded runtime's API — GetBulkSecret.
// Single-secret reads live in secrets-read. Mirrors wasi-grpc-outbound's
// secrets.rs (the secrets half).
package export_dapr_wasm_components_interfaces_secrets

import (
	"context"
	"sort"

	daprgrpc "github.com/dapr/dapr/pkg/api/grpc"
	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	witTypes "go.bytecodealliance.org/pkg/wit/types"

	scblk "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/blocks/secrets"
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	sc "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_secrets"
)

type secrets = []witTypes.Tuple2[string, sc.Secret]

func GetBulkSecret(storeName string, md []witTypes.Tuple2[string, string]) witTypes.Result[secrets, sc.SecretsError] {
	r, f := call.Do(func(ctx context.Context, api daprgrpc.API) (*runtimev1pb.GetBulkSecretResponse, error) {
		return api.GetBulkSecret(ctx, &runtimev1pb.GetBulkSecretRequest{StoreName: storeName, Metadata: call.MetadataMap(md)})
	})
	if f != nil {
		return witTypes.Err[secrets](scblk.Error(f))
	}
	// Proto maps have no order: sort by secret name for determinism
	// (MetadataPairs sorts each secret's entries).
	out := make(secrets, 0, len(r.GetData()))
	for name, s := range r.GetData() {
		out = append(out, witTypes.Tuple2[string, sc.Secret]{F0: name, F1: call.MetadataPairs(s.GetSecrets())})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].F0 < out[j].F0 })
	return witTypes.Ok[secrets, sc.SecretsError](out)
}
