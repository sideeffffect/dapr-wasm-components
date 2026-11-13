package events

import (
	"runtime"
	"time"
	_ "unsafe" // go:linkname

	mc "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/wasi_clocks_0_3_0_monotonic_clock"
)

// How go-outbound runs on the WASI 0.3 event loop (componentize-go async):
//
//   - Each async export (`run`, `next-event`) is a task. The host resumes a
//     task only when one of its waitables (here: a clock wait) has an event.
//     On each resume the Go scheduler runs every runnable goroutine until all
//     are idle, then the task returns to the host's event loop.
//   - The patched Go runtime's idle hook does not wake the host for Go
//     timers, so a goroutine in time.Sleep (wasisock's I/O backoff, Dapr's
//     tickers) runs only on some resume. Run's clock wait provides those
//     resumes, aimed at the next Go timer.
//   - The glue ends a task that is idle with no pending waitable, even before
//     it returned its result. So an export goroutine must never wait on a
//     plain Go channel: Wait keeps a clock wait of the task pending.
//   - An export goroutine must also never be woken by another task's resume
//     (it would return its result in the wrong task). Wait only proceeds
//     after its own clock wait, so it resumes in its own task.

const (
	// minTick and maxTick bound Run's wait: at most maxTick late for a Go
	// timer that was set after Run computed its wait, and no busy spinning.
	minTick = 200 * time.Microsecond
	maxTick = 10 * time.Millisecond
	// waitMin/waitMax: Wait's polling interval, backing off while idle.
	waitMin = 500 * time.Microsecond
	waitMax = 10 * time.Millisecond
)

// wasiNextTimer and wasiNanotime are added to the runtime by the overlay
// (overlay/gen-overlay.py): the earliest pending Go timer (runtime nanotime,
// math.MaxInt64 if none) and the runtime's monotonic clock.
//
//go:linkname wasiNextTimer runtime.wasiNextTimer
func wasiNextTimer() int64

//go:linkname wasiNanotime runtime.wasiNanotime
func wasiNanotime() int64

// Run drives the runtime: it never returns. It is the body of the async
// `run` export, which the pump starts once and never waits for. Its task
// stays alive, and each of its clock waits ends at the next Go timer (within
// [minTick, maxTick]), so Dapr's goroutines run whenever the host turns its
// event loop — not only while a caller is inside go-outbound.
func Run() {
	for {
		// Let the goroutines made runnable by this resume run first, so the
		// timers they set count for the next wait.
		runtime.Gosched()
		d := time.Duration(wasiNextTimer() - wasiNanotime())
		d = min(max(d, minTick), maxTick)
		mc.WaitFor(uint64(d))
	}
}

// Wait returns the next value from ch. It is for export goroutines of async
// exports (see above): between non-blocking receives it waits on the
// monotonic clock, backing off from waitMin to waitMax.
func Wait[T any](ch <-chan T) T {
	d := waitMin
	for {
		select {
		case v := <-ch:
			return v
		default:
		}
		mc.WaitFor(uint64(d))
		d = min(2*d, waitMax)
	}
}
