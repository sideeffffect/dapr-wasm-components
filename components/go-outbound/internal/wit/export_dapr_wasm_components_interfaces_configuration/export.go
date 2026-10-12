// Configuration, served by the embedded runtime's API — the stable
// GetConfiguration, SubscribeConfiguration and UnsubscribeConfiguration
// (not the Alpha1 variants). Mirrors wasi-grpc-outbound's configuration.rs
// for get.
//
// Unlike the network provider, subscribe is supported: SubscribeConfiguration
// is a server-streaming call, and in-process it runs in a goroutine against
// a fake server stream. Its first message carries the subscription id;
// every later one is an update, delivered to the app as a
// configuration-update event through the events queue.
package export_dapr_wasm_components_interfaces_configuration

import (
	"context"
	"errors"
	"sort"
	"sync"

	daprgrpc "github.com/dapr/dapr/pkg/api/grpc"
	commonv1pb "github.com/dapr/dapr/pkg/proto/common/v1"
	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	witTypes "go.bytecodealliance.org/pkg/wit/types"
	"google.golang.org/grpc/metadata"

	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/events"
	ev "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_go_events"
	cf "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_configuration"
)

type items = []witTypes.Tuple2[string, cf.ConfigurationItem]

func configurationError(f *call.Failure) cf.ConfigurationError {
	if f.IsPermission() {
		return cf.MakeConfigurationErrorPermissionDenied(f.Message)
	}
	return cf.MakeConfigurationErrorStoreNotFound(f.Message)
}

func unsubscribeError(f *call.Failure) cf.UnsubscribeError {
	if f.Status == 404 {
		return cf.MakeUnsubscribeErrorNoSuchSubscription(f.Message)
	}
	return cf.MakeUnsubscribeErrorConfiguration(configurationError(f))
}

// itemsWit converts the proto map, sorted by key for determinism (proto
// maps have no order).
func itemsWit(m map[string]*commonv1pb.ConfigurationItem) items {
	out := make(items, 0, len(m))
	for k, v := range m {
		out = append(out, witTypes.Tuple2[string, cf.ConfigurationItem]{F0: k, F1: cf.ConfigurationItem{
			Value: v.GetValue(), Version: v.GetVersion(), Metadata: call.MetadataPairs(v.GetMetadata()),
		}})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].F0 < out[j].F0 })
	return out
}

func Get(storeName string, keys []string, md []witTypes.Tuple2[string, string]) witTypes.Result[items, cf.ConfigurationError] {
	r, f := call.Do(func(ctx context.Context, api daprgrpc.API) (*runtimev1pb.GetConfigurationResponse, error) {
		return api.GetConfiguration(ctx, &runtimev1pb.GetConfigurationRequest{
			StoreName: storeName, Keys: keys, Metadata: call.MetadataMap(md),
		})
	})
	if f != nil {
		return witTypes.Err[items](configurationError(f))
	}
	return witTypes.Ok[items, cf.ConfigurationError](itemsWit(r.GetItems()))
}

// stream is the server side of one in-process SubscribeConfiguration call.
// The API serializes its Send calls.
type stream struct {
	ctx       context.Context
	storeName string
	first     bool
	id        chan string // receives the subscription id (first message)
}

func (s *stream) Send(m *runtimev1pb.SubscribeConfigurationResponse) error {
	if !s.first {
		s.first = true
		s.id <- m.GetId()
		return nil
	}
	if err := s.ctx.Err(); err != nil {
		return err // unsubscribed: stop delivering
	}
	_, err := events.Deliver(ev.MakeEventConfigurationUpdate(ev.ConfigurationUpdate{
		StoreName: s.storeName, Id: m.GetId(), Items: itemsWit(m.GetItems()),
	}))
	if errors.Is(err, events.ErrNotAttached) {
		return nil // no app to deliver to: drop the update
	}
	return err
}

func (*stream) SetHeader(metadata.MD) error  { return nil }
func (*stream) SendHeader(metadata.MD) error { return nil }
func (*stream) SetTrailer(metadata.MD)       {}
func (s *stream) Context() context.Context   { return s.ctx }
func (s *stream) SendMsg(m any) error {
	return s.Send(m.(*runtimev1pb.SubscribeConfigurationResponse))
}
func (*stream) RecvMsg(any) error {
	return errors.New("SubscribeConfiguration: client sends no messages")
}

// subscriptions maps a subscription id to the cancel of its stream context.
var (
	mu            sync.Mutex
	subscriptions = map[string]context.CancelFunc{}
)

func Subscribe(storeName string, keys []string, md []witTypes.Tuple2[string, string]) witTypes.Result[string, cf.ConfigurationError] {
	ctx, cancel := context.WithCancel(call.Context())
	s := &stream{ctx: ctx, storeName: storeName, id: make(chan string, 1)}
	req := &runtimev1pb.SubscribeConfigurationRequest{StoreName: storeName, Keys: keys, Metadata: call.MetadataMap(md)}
	done := make(chan error, 1)
	go func() {
		// Errors after the id was sent have no caller to report to.
		done <- call.API().SubscribeConfiguration(req, s)
		cancel()
	}()
	select {
	case id := <-s.id:
		mu.Lock()
		subscriptions[id] = cancel
		mu.Unlock()
		// Forget the subscription once its stream ends, however it ends.
		go func() {
			<-ctx.Done()
			mu.Lock()
			delete(subscriptions, id)
			mu.Unlock()
		}()
		return witTypes.Ok[string, cf.ConfigurationError](id)
	case err := <-done:
		cancel()
		if err == nil {
			panic("SubscribeConfiguration ended without sending a subscription id")
		}
		return witTypes.Err[string](configurationError(call.Classify(err)))
	}
}

func Unsubscribe(storeName string, id string) witTypes.Result[witTypes.Unit, cf.UnsubscribeError] {
	// Cancelling the stream context ends the SubscribeConfiguration call,
	// which unsubscribes from the store, even if the API call below fails.
	mu.Lock()
	cancel, ok := subscriptions[id]
	mu.Unlock()
	if ok {
		defer cancel()
	}
	r, f := call.Do(func(ctx context.Context, api daprgrpc.API) (*runtimev1pb.UnsubscribeConfigurationResponse, error) {
		return api.UnsubscribeConfiguration(ctx, &runtimev1pb.UnsubscribeConfigurationRequest{StoreName: storeName, Id: id})
	})
	if f != nil {
		return witTypes.Err[witTypes.Unit](unsubscribeError(f))
	}
	// The API reports an unknown id as ok=false, not as a NotFound error.
	if !r.GetOk() {
		return witTypes.Err[witTypes.Unit](cf.MakeUnsubscribeErrorNoSuchSubscription(r.GetMessage()))
	}
	return witTypes.Ok[witTypes.Unit, cf.UnsubscribeError](witTypes.Unit{})
}
