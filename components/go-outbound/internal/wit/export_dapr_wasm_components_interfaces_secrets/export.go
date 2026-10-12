// Secrets, served by the embedded runtime's API — GetSecret, GetBulkSecret.
// Mirrors wasi-grpc-outbound's secrets.rs.
//
// A missing secret: the runtime reports a store's "no such secret" as a
// generic ERR_SECRET_GET (Internal). Most stores put "not found" in the
// store error appended after "... from secret store <store>: ", so GetSecret
// maps that to secret-not-found; any other Internal is a store backend
// failure and still panics. A store that returns no data at all is also
// secret-not-found.
package export_dapr_wasm_components_interfaces_secrets

import (
	"context"
	"sort"
	"strings"

	daprgrpc "github.com/dapr/dapr/pkg/api/grpc"
	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	witTypes "go.bytecodealliance.org/pkg/wit/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	sc "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_secrets"
)

type metadata = []witTypes.Tuple2[string, string]

func secretsError(f *call.Failure) sc.SecretsError {
	if f.IsPermission() {
		return sc.MakeSecretsErrorPermissionDenied(f.Message)
	}
	return sc.MakeSecretsErrorStoreNotFound(f.Message)
}

// getSecretError checks the raw error before call.Classify, which would
// panic on the Internal code a store "not found" arrives with.
func getSecretError(err error, storeName string) sc.GetSecretError {
	if s, ok := status.FromError(err); ok && s.Code() == codes.Internal && storeErrorIsNotFound(s.Message(), storeName) {
		return sc.MakeGetSecretErrorSecretNotFound()
	}
	return sc.MakeGetSecretErrorSecrets(secretsError(call.Classify(err)))
}

// storeErrorIsNotFound reports whether the store error appended to an
// ERR_SECRET_GET message says "not found" (case-insensitive).
func storeErrorIsNotFound(message string, storeName string) bool {
	marker := "from secret store " + storeName + ": "
	at := strings.Index(message, marker)
	return at >= 0 && strings.Contains(strings.ToLower(message[at+len(marker):]), "not found")
}

func GetSecret(storeName string, key string, md metadata) witTypes.Result[sc.Secret, sc.GetSecretError] {
	r, err := call.API().GetSecret(call.Context(), &runtimev1pb.GetSecretRequest{
		StoreName: storeName, Key: key, Metadata: call.MetadataMap(md),
	})
	if err != nil {
		return witTypes.Err[sc.Secret](getSecretError(err, storeName))
	}
	if len(r.GetData()) == 0 {
		return witTypes.Err[sc.Secret](sc.MakeGetSecretErrorSecretNotFound())
	}
	return witTypes.Ok[sc.Secret, sc.GetSecretError](call.MetadataPairs(r.GetData()))
}

func GetBulkSecret(storeName string, md metadata) witTypes.Result[[]witTypes.Tuple2[string, sc.Secret], sc.SecretsError] {
	r, f := call.Do(func(ctx context.Context, api daprgrpc.API) (*runtimev1pb.GetBulkSecretResponse, error) {
		return api.GetBulkSecret(ctx, &runtimev1pb.GetBulkSecretRequest{StoreName: storeName, Metadata: call.MetadataMap(md)})
	})
	if f != nil {
		return witTypes.Err[[]witTypes.Tuple2[string, sc.Secret]](secretsError(f))
	}
	// Proto maps have no order: sort by secret name for determinism
	// (MetadataPairs sorts each secret's entries).
	secrets := make([]witTypes.Tuple2[string, sc.Secret], 0, len(r.GetData()))
	for name, s := range r.GetData() {
		secrets = append(secrets, witTypes.Tuple2[string, sc.Secret]{F0: name, F1: call.MetadataPairs(s.GetSecrets())})
	}
	sort.Slice(secrets, func(i, j int) bool { return secrets[i].F0 < secrets[j].F0 })
	return witTypes.Ok[[]witTypes.Tuple2[string, sc.Secret], sc.SecretsError](secrets)
}
