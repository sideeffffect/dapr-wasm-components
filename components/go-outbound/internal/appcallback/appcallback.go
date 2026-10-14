// Package appcallback connects the embedded Dapr runtime to the app with
// plain Go calls — no app protocol on the wire, no socket, no HTTP.
//
// Dapr reaches an app through two seams, and this package fills both:
//
//   - Conn, a grpc.ClientConnInterface. In gRPC app-protocol mode Dapr calls
//     the `AppCallback` service through it (pub/sub, bulk pub/sub, input
//     bindings and the lists of subscriptions and bindings). Conn is not a
//     connection: it switches on the method name and hands the typed request
//     message to a handler. Mirrors wasi-grpc-inbound, minus the framing.
//   - Channel, a channel.AppChannel. Dapr calls it directly for service
//     invocation, jobs, actors, the actor configuration and health probes.
//
// Every handler turns its request into a typed event and waits for the
// app's answer via the go-inbound pump (package events). daprrt plugs both
// into the runtime before it starts. Configuration updates do not pass
// through here: go-outbound subscribes in-process and queues them directly.
package appcallback

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	commonv1pb "github.com/dapr/dapr/pkg/proto/common/v1"
	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	witTypes "go.bytecodealliance.org/pkg/wit/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/events"
	ev "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_go_events"
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_types"
)

type metadata = []witTypes.Tuple2[string, string]
type appError = dapr_wasm_components_interfaces_types.AppError

// deliver queues one event for the app and returns its result. Dapr only
// gets Conn and Channel when a pump is attached, so ErrNotAttached is a bug.
func deliver(e ev.Event) ev.EventResult {
	r, err := events.Deliver(e)
	if err != nil {
		panic(fmt.Sprintf("app callback without an attached app: %v", err))
	}
	return r
}

// Conn serves Dapr's `AppCallback` gRPC service in-process.
type Conn struct{}

var _ grpc.ClientConnInterface = Conn{}

// Invoke runs the unary RPC method with the request args and stores the
// answer in reply. An unknown method is UNIMPLEMENTED, like a gRPC server
// without it; Dapr then falls back where it has a fallback.
func (Conn) Invoke(_ context.Context, method string, args, reply any, _ ...grpc.CallOption) error {
	var (
		res proto.Message
		err error
	)
	switch method {
	case "/dapr.proto.runtime.v1.AppCallback/ListTopicSubscriptions":
		res = listTopicSubscriptions()
	case "/dapr.proto.runtime.v1.AppCallback/OnTopicEvent":
		res = onTopicEvent(args.(*runtimev1pb.TopicEventRequest))
	// Dapr calls the stable RPC and falls back to the deprecated Alpha1
	// one only on UNIMPLEMENTED; serve both with the same handler.
	case "/dapr.proto.runtime.v1.AppCallback/OnBulkTopicEvent",
		"/dapr.proto.runtime.v1.AppCallbackAlpha/OnBulkTopicEventAlpha1":
		res = onBulkTopicEvent(args.(*runtimev1pb.TopicEventBulkRequest))
	case "/dapr.proto.runtime.v1.AppCallback/ListInputBindings":
		res = listInputBindings()
	case "/dapr.proto.runtime.v1.AppCallback/OnBindingEvent":
		res, err = onBindingEvent(args.(*runtimev1pb.BindingEventRequest))
	default:
		return status.Errorf(codes.Unimplemented, "unimplemented method: %s", method)
	}
	if err != nil {
		return err
	}
	out := reply.(proto.Message)
	proto.Reset(out)
	proto.Merge(out, res)
	return nil
}

// NewStream is UNIMPLEMENTED: Dapr calls no streaming `AppCallback` RPC.
func (Conn) NewStream(_ context.Context, _ *grpc.StreamDesc, method string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, status.Errorf(codes.Unimplemented, "unimplemented streaming method: %s", method)
}

// appErrorStatus reports an app-error (a message only) as INTERNAL, like
// wasi-grpc-inbound.
func appErrorStatus(e appError) error {
	return status.Error(codes.Internal, e.Message)
}

// --- pub/sub ---------------------------------------------------------------

func listTopicSubscriptions() *runtimev1pb.ListTopicSubscriptionsResponse {
	res := &runtimev1pb.ListTopicSubscriptionsResponse{}
	for _, sub := range deliver(ev.MakeEventListTopicSubscriptions()).ListTopicSubscriptions() {
		pb := &runtimev1pb.TopicSubscription{
			PubsubName:      sub.PubsubName,
			Topic:           sub.Topic,
			Metadata:        metadataMap(sub.Metadata),
			DeadLetterTopic: sub.DeadLetterTopic.SomeOr(""),
		}
		if sub.Routes.IsSome() {
			routes := sub.Routes.Some()
			pb.Routes = &runtimev1pb.TopicRoutes{Default: routes.Default.SomeOr("")}
			for _, rule := range routes.Rules {
				pb.Routes.Rules = append(pb.Routes.Rules, &runtimev1pb.TopicRule{Match: rule.Match, Path: rule.Path})
			}
		}
		if sub.BulkSubscribe.IsSome() {
			config := sub.BulkSubscribe.Some()
			// 0 = unset; Dapr applies its defaults.
			pb.BulkSubscribe = &runtimev1pb.BulkSubscribeConfig{
				Enabled:            config.Enabled,
				MaxMessagesCount:   config.MaxMessagesCount.SomeOr(0),
				MaxAwaitDurationMs: config.MaxAwaitDurationMs.SomeOr(0),
			}
		}
		res.Subscriptions = append(res.Subscriptions, pb)
	}
	return res
}

func onTopicEvent(req *runtimev1pb.TopicEventRequest) *runtimev1pb.TopicEventResponse {
	status := deliver(ev.MakeEventTopic(ev.TopicEvent{
		Id:              req.GetId(),
		Source:          req.GetSource(),
		Type:            req.GetType(),
		SpecVersion:     req.GetSpecVersion(),
		DataContentType: nonEmpty(req.GetDataContentType()),
		Data:            req.GetData(),
		Topic:           req.GetTopic(),
		PubsubName:      req.GetPubsubName(),
		Path:            nonEmpty(req.GetPath()),
		Extensions:      extensions(req.GetExtensions()),
	})).Topic()
	return &runtimev1pb.TopicEventResponse{Status: statusToProto(status)}
}

func onBulkTopicEvent(req *runtimev1pb.TopicEventBulkRequest) *runtimev1pb.TopicEventBulkResponse {
	path := nonEmpty(req.GetPath())
	entries := []ev.BulkTopicEventEntry{}
	for _, entry := range req.GetEntries() {
		var event ev.BulkEvent
		if ce := entry.GetCloudEvent(); ce != nil {
			event = ev.MakeBulkEventCloudEvent(ev.TopicEvent{
				Id:              ce.GetId(),
				Source:          ce.GetSource(),
				Type:            ce.GetType(),
				SpecVersion:     ce.GetSpecVersion(),
				DataContentType: nonEmpty(ce.GetDataContentType()),
				Data:            ce.GetData(),
				Topic:           req.GetTopic(),
				PubsubName:      req.GetPubsubName(),
				Path:            path,
				Extensions:      extensions(ce.GetExtensions()),
			})
		} else {
			event = ev.MakeBulkEventRaw(entry.GetBytes())
		}
		entries = append(entries, ev.BulkTopicEventEntry{
			EntryId:     entry.GetEntryId(),
			Event:       event,
			ContentType: nonEmpty(entry.GetContentType()),
			Metadata:    sortedPairs(entry.GetMetadata()),
		})
	}
	res := &runtimev1pb.TopicEventBulkResponse{}
	for _, entry := range deliver(ev.MakeEventBulkTopic(ev.BulkTopicEvent{
		Id:         req.GetId(),
		Entries:    entries,
		Metadata:   sortedPairs(req.GetMetadata()),
		Topic:      req.GetTopic(),
		PubsubName: req.GetPubsubName(),
		Type:       req.GetType(),
		Path:       path,
	})).BulkTopic() {
		res.Statuses = append(res.Statuses, &runtimev1pb.TopicEventBulkResponseEntry{
			EntryId: entry.EntryId,
			Status:  statusToProto(entry.Status),
		})
	}
	return res
}

func statusToProto(s ev.TopicEventResponse) runtimev1pb.TopicEventResponse_TopicEventResponseStatus {
	switch s {
	case ev.TopicEventResponseRetry:
		return runtimev1pb.TopicEventResponse_RETRY
	case ev.TopicEventResponseDrop:
		return runtimev1pb.TopicEventResponse_DROP
	}
	return runtimev1pb.TopicEventResponse_SUCCESS
}

// extensions maps the CloudEvent extension attributes to WIT metadata: a
// string as itself, anything else as its JSON text.
func extensions(s *structpb.Struct) metadata {
	md := metadata{}
	for key, value := range s.GetFields() {
		text, isString := value.AsInterface().(string)
		if !isString {
			b, _ := json.Marshal(value.AsInterface())
			text = string(b)
		}
		md = append(md, witTypes.Tuple2[string, string]{F0: key, F1: text})
	}
	return sortPairs(md)
}

// --- input bindings --------------------------------------------------------

func listInputBindings() *runtimev1pb.ListInputBindingsResponse {
	return &runtimev1pb.ListInputBindingsResponse{
		Bindings: deliver(ev.MakeEventListInputBindings()).ListInputBindings(),
	}
}

// onBindingEvent delivers an input-binding event. Dapr (checked up to 1.18)
// applies `to`, `data` and `concurrency` from the reply and ignores
// `store_name`/`states`; all are mapped anyway.
func onBindingEvent(req *runtimev1pb.BindingEventRequest) (*runtimev1pb.BindingEventResponse, error) {
	res := deliver(ev.MakeEventBinding(ev.BindingEvent{
		Name:     req.GetName(),
		Data:     req.GetData(),
		Metadata: sortedPairs(req.GetMetadata()),
	})).Binding()
	if res.IsErr() {
		return nil, appErrorStatus(res.Err())
	}
	r := res.Ok()
	pb := &runtimev1pb.BindingEventResponse{
		StoreName: r.StoreName.SomeOr(""),
		To:        r.To,
		Data:      r.Data.SomeOr(nil),
	}
	if r.Concurrency == ev.BindingConcurrencyParallel {
		pb.Concurrency = runtimev1pb.BindingEventResponse_PARALLEL
	}
	for _, item := range r.States {
		state := &commonv1pb.StateItem{Key: item.Key, Value: item.Value}
		if item.Etag.IsSome() {
			state.Etag = &commonv1pb.Etag{Value: item.Etag.Some()}
		}
		pb.States = append(pb.States, state)
	}
	return pb, nil
}

// --- helpers ---------------------------------------------------------------

func nonEmpty(s string) witTypes.Option[string] {
	if s == "" {
		return witTypes.None[string]()
	}
	return witTypes.Some(s)
}

func sortedPairs(m map[string]string) metadata {
	md := make(metadata, 0, len(m))
	for key, value := range m {
		md = append(md, witTypes.Tuple2[string, string]{F0: key, F1: value})
	}
	return sortPairs(md)
}

func sortPairs(md metadata) metadata {
	sort.Slice(md, func(i, j int) bool {
		if md[i].F0 != md[j].F0 {
			return md[i].F0 < md[j].F0
		}
		return md[i].F1 < md[j].F1
	})
	return md
}

func metadataMap(md metadata) map[string]string {
	if len(md) == 0 {
		return nil
	}
	out := make(map[string]string, len(md))
	for _, kv := range md {
		out[kv.F0] = kv.F1
	}
	return out
}
