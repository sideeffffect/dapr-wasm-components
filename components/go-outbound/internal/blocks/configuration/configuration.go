// Package configuration holds the logic both halves of the configuration
// block share (configuration-read and configuration): the base error
// mapping and the item conversion.
package configuration

import (
	"sort"

	commonv1pb "github.com/dapr/dapr/pkg/proto/common/v1"
	witTypes "go.bytecodealliance.org/pkg/wit/types"

	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	cr "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_configuration_read"
)

// Items is the WIT shape of a configuration item map.
type Items = []witTypes.Tuple2[string, cr.ConfigurationItem]

// Error maps a recoverable failure to the shared configuration-error.
func Error(f *call.Failure) cr.ConfigurationError {
	if f.IsPermission() {
		return cr.MakeConfigurationErrorPermissionDenied(f.Message)
	}
	return cr.MakeConfigurationErrorStoreNotFound(f.Message)
}

// ItemsWit converts the proto map, sorted by key for determinism (proto
// maps have no order).
func ItemsWit(m map[string]*commonv1pb.ConfigurationItem) Items {
	out := make(Items, 0, len(m))
	for k, v := range m {
		out = append(out, witTypes.Tuple2[string, cr.ConfigurationItem]{F0: k, F1: cr.ConfigurationItem{
			Value: v.GetValue(), Version: v.GetVersion(), Metadata: call.MetadataPairs(v.GetMetadata()),
		}})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].F0 < out[j].F0 })
	return out
}
