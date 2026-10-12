// Package events is the Dapr → app delivery queue behind the private
// `dapr-wasm-components:go/events` interface.
//
// Dapr's goroutines (the app channel handler, configuration subscriptions)
// call Deliver, which queues the event and waits. The go-inbound pump
// pulls it with Next (an export call, during which the Go scheduler keeps
// running every goroutine), calls the app's matching callback, and hands
// the answer back with Complete, which wakes the waiting Deliver.
package events

import (
	"errors"
	"fmt"
	"sync"
	"time"

	ev "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_go_events"
)

// ErrNotAttached is returned by Deliver when no pump is attached (the
// runtime runs outbound only, like daprd without --app-port).
var ErrNotAttached = errors.New("no app attached: compose go-outbound with go-inbound to receive deliveries")

type pending struct {
	event ev.Event
	done  chan ev.EventResult
}

var (
	mu       sync.Mutex
	attached bool
	nextID   uint64
	inflight = map[uint64]*pending{}
	queue    = make(chan uint64, 1024)
)

// Attach marks the pump as attached; from now on Deliver queues events.
func Attach() {
	mu.Lock()
	attached = true
	mu.Unlock()
}

// Attached reports whether a pump is attached.
func Attached() bool {
	mu.Lock()
	defer mu.Unlock()
	return attached
}

// Deliver queues e for the app and blocks until the pump completes it.
func Deliver(e ev.Event) (ev.EventResult, error) {
	mu.Lock()
	if !attached {
		mu.Unlock()
		return ev.EventResult{}, ErrNotAttached
	}
	nextID++
	id := nextID
	p := &pending{event: e, done: make(chan ev.EventResult, 1)}
	inflight[id] = p
	mu.Unlock()
	queue <- id
	return <-p.done, nil
}

// Next blocks until an event is queued and returns it with its id. While
// it waits, the Go scheduler runs Dapr's goroutines. The periodic wake-up
// keeps a timer pending, so the Go runtime never mistakes an idle wait for
// a deadlock ("all goroutines are asleep").
func Next() (uint64, ev.Event) {
	for {
		select {
		case id := <-queue:
			mu.Lock()
			p := inflight[id]
			mu.Unlock()
			return id, p.event
		case <-time.After(time.Second):
		}
	}
}

// Complete hands the app's result for event id back to its Deliver. An
// unknown id, or a result case that does not match the event's case, is a
// bug in the pump: it panics (the component traps).
func Complete(id uint64, r ev.EventResult) {
	mu.Lock()
	p, ok := inflight[id]
	delete(inflight, id)
	mu.Unlock()
	if !ok {
		panic(fmt.Sprintf("complete-event: unknown event id %d", id))
	}
	if p.event.Tag() != r.Tag() {
		panic(fmt.Sprintf("complete-event: result case %d does not match event case %d", r.Tag(), p.event.Tag()))
	}
	p.done <- r
}
