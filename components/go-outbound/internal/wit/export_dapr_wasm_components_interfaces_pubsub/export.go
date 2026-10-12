// Pub/sub (publishing side), served by the embedded runtime's API —
// PublishEvent, BulkPublishEvent. Payloads are raw bytes with an explicit
// content type, so non-JSON events roundtrip byte-exact. Mirrors
// wasi-grpc-outbound's pubsub.rs.
package export_dapr_wasm_components_interfaces_pubsub

import (
	"context"
	"strings"

	daprgrpc "github.com/dapr/dapr/pkg/api/grpc"
	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	witTypes "go.bytecodealliance.org/pkg/wit/types"

	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	ps "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_pubsub"
)

func pubsubError(f *call.Failure) ps.PubsubError {
	if f.IsPermission() {
		return ps.MakePubsubErrorPermissionDenied(f.Message)
	}
	return ps.MakePubsubErrorComponentNotFound(f.Message)
}

func Publish(pubsubName string, topic string, data []uint8, dataContentType witTypes.Option[string], md []witTypes.Tuple2[string, string]) witTypes.Result[witTypes.Unit, ps.PubsubError] {
	_, f := call.Do(func(ctx context.Context, api daprgrpc.API) (any, error) {
		return api.PublishEvent(ctx, &runtimev1pb.PublishEventRequest{
			PubsubName: pubsubName, Topic: topic, Data: data,
			// "" lets the runtime apply its default content type.
			DataContentType: call.OrEmpty(dataContentType),
			Metadata:        call.MetadataMap(md),
		})
	})
	if f != nil {
		return witTypes.Err[witTypes.Unit](pubsubError(f))
	}
	return witTypes.Ok[witTypes.Unit, ps.PubsubError](witTypes.Unit{})
}

func PublishBulk(pubsubName string, topic string, entries []ps.BulkPublishEntry, md []witTypes.Tuple2[string, string]) witTypes.Result[[]ps.BulkPublishFailedEntry, ps.PublishBulkError] {
	pbEntries := make([]*runtimev1pb.BulkPublishRequestEntry, 0, len(entries))
	for _, e := range entries {
		pbEntries = append(pbEntries, &runtimev1pb.BulkPublishRequestEntry{
			EntryId: e.EntryId, Event: e.Event, ContentType: e.ContentType, Metadata: call.MetadataMap(e.Metadata),
		})
	}
	r, f := call.Do(func(ctx context.Context, api daprgrpc.API) (*runtimev1pb.BulkPublishResponse, error) {
		return api.BulkPublishEvent(ctx, &runtimev1pb.BulkPublishRequest{
			PubsubName: pubsubName, Topic: topic, Entries: pbEntries, Metadata: call.MetadataMap(md),
		})
	})
	if f != nil {
		if strings.Contains(f.ErrorCode, "NOT_SUPPORTED") || strings.Contains(f.ErrorCode, "UNSUPPORTED") {
			return witTypes.Err[[]ps.BulkPublishFailedEntry](ps.MakePublishBulkErrorNotSupported())
		}
		return witTypes.Err[[]ps.BulkPublishFailedEntry](ps.MakePublishBulkErrorPubsub(pubsubError(f)))
	}
	failed := make([]ps.BulkPublishFailedEntry, 0, len(r.GetFailedEntries()))
	for _, e := range r.GetFailedEntries() {
		failed = append(failed, ps.BulkPublishFailedEntry{EntryId: e.GetEntryId(), Error: e.GetError()})
	}
	return witTypes.Ok[[]ps.BulkPublishFailedEntry, ps.PublishBulkError](failed)
}
