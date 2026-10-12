// Package streams runs a bidirectional-streaming gRPC handler in process,
// without a network: the runtime's streaming API methods (EncryptAlpha1,
// DecryptAlpha1) take a grpc.BidiStreamingServer, and Bidi is one whose
// client side is plain Go method calls.
//
// Requests are handed over synchronously (Push blocks until the handler
// receives), so the handler sees backpressure. Responses are buffered
// without bound, so a handler that sends while its client is still
// sending can never deadlock against it.
package streams

import (
	"context"
	"errors"
	"io"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// ErrHandlerDone is returned by Push when the handler returned nil without
// receiving the request.
var ErrHandlerDone = errors.New("stream handler returned before receiving all requests")

// Bidi is an in-process grpc.BidiStreamingServer[Req, Resp]. Req and Resp
// must be proto message structs (*Req and *Resp implement proto.Message).
type Bidi[Req, Resp any] struct {
	ctx    context.Context
	cancel context.CancelFunc

	requests  chan *Req
	closed    chan struct{} // closed by CloseSend: the handler sees io.EOF
	closeOnce sync.Once
	done      chan struct{} // closed when the handler returns
	err       error         // the handler's result, valid after done

	mu        sync.Mutex
	responses []*Resp
}

// Start runs handler on a new goroutine against a fresh stream. The stream
// context is cancelled when the handler returns (as gRPC does) or on Cancel.
func Start[Req, Resp any](ctx context.Context, handler func(grpc.BidiStreamingServer[Req, Resp]) error) *Bidi[Req, Resp] {
	ctx, cancel := context.WithCancel(ctx)
	b := &Bidi[Req, Resp]{
		ctx:      ctx,
		cancel:   cancel,
		requests: make(chan *Req),
		closed:   make(chan struct{}),
		done:     make(chan struct{}),
	}
	go func() {
		// done closes before the context is cancelled, so whoever sees the
		// cancellation can also see the handler's result.
		defer cancel()
		defer close(b.done)
		b.err = handler(b)
	}()
	return b
}

// Client side.

// Push hands one request to the handler. If the handler has already
// returned, Push returns its error (or ErrHandlerDone if it succeeded).
func (b *Bidi[Req, Resp]) Push(req *Req) error {
	select {
	case b.requests <- req:
		return nil
	case <-b.done:
		return b.result()
	case <-b.ctx.Done():
		select {
		case <-b.done:
			return b.result()
		default:
			return b.ctx.Err()
		}
	}
}

func (b *Bidi[Req, Resp]) result() error {
	if b.err != nil {
		return b.err
	}
	return ErrHandlerDone
}

// CloseSend half-closes the stream: the handler's next Recv gets io.EOF.
func (b *Bidi[Req, Resp]) CloseSend() {
	b.closeOnce.Do(func() { close(b.closed) })
}

// Wait blocks until the handler returns, then yields every response it
// sent and its error.
func (b *Bidi[Req, Resp]) Wait() ([]*Resp, error) {
	<-b.done
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.responses, b.err
}

// Cancel aborts the stream (the handler sees a cancelled context and its
// Recv fails). Safe to call any number of times.
func (b *Bidi[Req, Resp]) Cancel() {
	b.cancel()
}

// Server side: grpc.BidiStreamingServer[Req, Resp].

func (b *Bidi[Req, Resp]) Recv() (*Req, error) {
	select {
	case req := <-b.requests:
		return req, nil
	case <-b.closed:
		return nil, io.EOF
	case <-b.ctx.Done():
		return nil, b.ctx.Err()
	}
}

// Send buffers one response. The handler may reuse resp's buffers after the
// call returns (daprd's crypto does), so it is deep-copied.
func (b *Bidi[Req, Resp]) Send(resp *Resp) error {
	if err := b.ctx.Err(); err != nil {
		return err
	}
	clone := proto.Clone(any(resp).(proto.Message)).(any).(*Resp)
	b.mu.Lock()
	b.responses = append(b.responses, clone)
	b.mu.Unlock()
	return nil
}

func (b *Bidi[Req, Resp]) SendMsg(m any) error { return b.Send(m.(*Resp)) }

func (b *Bidi[Req, Resp]) RecvMsg(m any) error {
	req, err := b.Recv()
	if err != nil {
		return err
	}
	dst := m.(proto.Message)
	proto.Reset(dst)
	proto.Merge(dst, any(req).(proto.Message))
	return nil
}

func (b *Bidi[Req, Resp]) Context() context.Context     { return b.ctx }
func (b *Bidi[Req, Resp]) SetHeader(metadata.MD) error  { return nil }
func (b *Bidi[Req, Resp]) SendHeader(metadata.MD) error { return nil }
func (b *Bidi[Req, Resp]) SetTrailer(metadata.MD)       {}
