// Secret reads, served by the embedded runtime's API — GetSecret. Mirrors
// wasi-grpc-outbound's secrets.rs (the secrets-read half).
//
// A missing secret: the runtime reports a store's "no such secret" as a
// generic ERR_SECRET_GET (Internal). Most stores put "not found" in the
// store error appended after "... from secret store <store>: ", so GetSecret
// maps that to secret-not-found; any other Internal is a store backend
// failure and still panics. A store that returns no data at all is also
// secret-not-found.
package export_dapr_wasm_components_interfaces_secrets_read

import (
	"strings"

	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	witTypes "go.bytecodealliance.org/pkg/wit/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	scblk "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/blocks/secrets"
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	sr "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_secrets_read"
)

// getSecretError checks the raw error before call.Classify, which would
// panic on the Internal code a store "not found" arrives with.
func getSecretError(err error, storeName string) sr.GetSecretError {
	if s, ok := status.FromError(err); ok && s.Code() == codes.Internal && storeErrorIsNotFound(s.Message(), storeName) {
		return sr.MakeGetSecretErrorSecretNotFound()
	}
	return sr.MakeGetSecretErrorSecrets(scblk.Error(call.Classify(err)))
}

// storeErrorIsNotFound reports whether the store error appended to an
// ERR_SECRET_GET message says "not found" (case-insensitive).
func storeErrorIsNotFound(message string, storeName string) bool {
	marker := "from secret store " + storeName + ": "
	at := strings.Index(message, marker)
	return at >= 0 && strings.Contains(strings.ToLower(message[at+len(marker):]), "not found")
}

func GetSecret(storeName string, key string, md []witTypes.Tuple2[string, string]) witTypes.Result[sr.Secret, sr.GetSecretError] {
	r, err := call.API().GetSecret(call.Context(), &runtimev1pb.GetSecretRequest{
		StoreName: storeName, Key: key, Metadata: call.MetadataMap(md),
	})
	if err != nil {
		return witTypes.Err[sr.Secret](getSecretError(err, storeName))
	}
	if len(r.GetData()) == 0 {
		return witTypes.Err[sr.Secret](sr.MakeGetSecretErrorSecretNotFound())
	}
	return witTypes.Ok[sr.Secret, sr.GetSecretError](call.MetadataPairs(r.GetData()))
}
