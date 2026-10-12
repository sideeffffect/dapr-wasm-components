// Package secrets holds the logic both halves of the secrets block share
// (secrets-read and secrets): the base error mapping.
package secrets

import (
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	sr "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_secrets_read"
)

// Error maps a recoverable failure to the shared secrets-error.
func Error(f *call.Failure) sr.SecretsError {
	if f.IsPermission() {
		return sr.MakeSecretsErrorPermissionDenied(f.Message)
	}
	return sr.MakeSecretsErrorStoreNotFound(f.Message)
}
