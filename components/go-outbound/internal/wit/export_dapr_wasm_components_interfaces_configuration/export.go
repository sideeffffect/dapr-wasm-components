// Configuration subscriptions, served by the embedded runtime's API — the
// stable SubscribeConfiguration and UnsubscribeConfiguration (not the Alpha1
// variants). The reads live in configuration-read.
//
// Unlike wasi-grpc-outbound (whose subscribe traps), subscribe is
// supported: SubscribeConfiguration is a server-streaming call, and
// in-process it runs in a goroutine against a fake server stream. Its first
// message carries the subscription id; every later one is an update,
// delivered to the app as a configuration-update event through the events
// queue. Dropping the subscription handle unsubscribes, like
// wasi-http-outbound: a failed unsubscribe is logged to stderr and ignored.
package export_dapr_wasm_components_interfaces_configuration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"

	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	witTypes "go.bytecodealliance.org/pkg/wit/types"
	"google.golang.org/grpc/metadata"

	cfblk "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/blocks/configuration"
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/events"
	ev "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_go_events"
	cf "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_configuration"
)

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
		StoreName: s.storeName, Id: m.GetId(), Items: cfblk.ItemsWit(m.GetItems()),
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

// Subscription is one active SubscribeConfiguration stream.
type Subscription struct {
	pinner    runtime.Pinner
	handle    int32
	storeName string
	id        string
	cancel    context.CancelFunc
	dropped   sync.Once
}

// Id is the subscription id the runtime assigned.
func (self *Subscription) Id() string { return self.id }

// OnDrop unsubscribes. It may run twice (generated Drop and the dtor), so
// it is idempotent. It cannot report a failure, and panicking would trap
// the app while it only cleans up, so a failed unsubscribe is logged and
// ignored.
func (self *Subscription) OnDrop() {
	self.dropped.Do(func() {
		// Cancelling the stream context ends the SubscribeConfiguration
		// call, which unsubscribes from the store, even if the API call
		// below fails.
		defer self.cancel()
		r, err := call.API().UnsubscribeConfiguration(call.Context(), &runtimev1pb.UnsubscribeConfigurationRequest{
			StoreName: self.storeName, Id: self.id,
		})
		switch {
		case err != nil:
			fmt.Fprintf(os.Stderr, "configuration: unsubscribing %q from store %q failed: %v\n", self.id, self.storeName, err)
		case !r.GetOk():
			// The API reports an unknown id as ok=false, not as an error.
			fmt.Fprintf(os.Stderr, "configuration: unsubscribing %q from store %q failed: %s\n", self.id, self.storeName, r.GetMessage())
		}
	})
}

func Subscribe(storeName string, keys []string, md []witTypes.Tuple2[string, string]) witTypes.Result[*Subscription, cf.ConfigurationError] {
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
		return witTypes.Ok[*Subscription, cf.ConfigurationError](&Subscription{storeName: storeName, id: id, cancel: cancel})
	case err := <-done:
		cancel()
		if err == nil {
			panic("SubscribeConfiguration ended without sending a subscription id")
		}
		return witTypes.Err[*Subscription](cfblk.Error(call.Classify(err)))
	}
}
