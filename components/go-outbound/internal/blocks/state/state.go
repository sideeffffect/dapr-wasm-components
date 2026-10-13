// Package state holds the logic both halves of the state block share
// (state-read and state): the base error mapping and the consistency
// conversion.
package state

import (
	commonv1pb "github.com/dapr/dapr/pkg/proto/common/v1"

	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	sr "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_state_read"
)

// Error maps a recoverable failure to the shared state-error.
func Error(f *call.Failure) sr.StateError {
	if f.IsPermission() {
		return sr.MakeStateErrorPermissionDenied(f.Message)
	}
	return sr.MakeStateErrorStoreNotFound(f.Message)
}

// ConsistencyPb converts the WIT consistency to the proto enum.
func ConsistencyPb(c sr.Consistency) commonv1pb.StateOptions_StateConsistency {
	switch c {
	case sr.ConsistencyEventual:
		return commonv1pb.StateOptions_CONSISTENCY_EVENTUAL
	case sr.ConsistencyStrong:
		return commonv1pb.StateOptions_CONSISTENCY_STRONG
	}
	return commonv1pb.StateOptions_CONSISTENCY_UNSPECIFIED
}
