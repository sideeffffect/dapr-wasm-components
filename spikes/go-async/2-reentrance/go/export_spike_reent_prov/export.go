package export_spike_reent_prov

import (
	"sync"
	"time"

	mc "wit_component/wasi_clocks_monotonic_clock"
)

const tick = uint64(2 * time.Millisecond)

type pending struct {
	depth uint32
	done  chan uint32
}

var (
	mu       sync.Mutex
	nextID   uint32
	inflight = map[uint32]*pending{}
	queue    = make(chan uint32, 64)
)

// block waits for ch while keeping a waitable of this task pending, so the
// Go callback loop never sees "idle with nothing pending" and exits early.
func block[T any](ch <-chan T) T {
	for {
		select {
		case v := <-ch:
			return v
		default:
		}
		mc.WaitFor(tick)
	}
}

func Run() {
	for {
		mc.WaitFor(uint64(5 * time.Millisecond))
	}
}

func NextEvent() (uint32, uint32) {
	id := block(queue)
	mu.Lock()
	p := inflight[id]
	mu.Unlock()
	return id, p.depth
}

func CompleteEvent(id uint32, outcome uint32) {
	mu.Lock()
	p := inflight[id]
	delete(inflight, id)
	mu.Unlock()
	p.done <- outcome
}

func deliver(depth uint32) chan uint32 {
	mu.Lock()
	nextID++
	id := nextID
	p := &pending{depth: depth, done: make(chan uint32, 1)}
	inflight[id] = p
	mu.Unlock()
	queue <- id
	return p.done
}

func Invoke(depth uint32) uint32 { return block(deliver(depth)) }

func InvokeSync(depth uint32) uint32 { return <-deliver(depth) }
