package export_spike_bg_bg

import (
	"sync/atomic"
	"time"

	mc "wit_component/wasi_clocks_monotonic_clock"
)

var counter atomic.Uint64
var started atomic.Bool

// Run keeps one subtask (a clock wait) pending forever, so the Go task never
// exits and the host resumes it every tick; on each resume the Go scheduler
// runs every runnable goroutine (incl. those whose time.Sleep expired).
func Run() {
	if started.CompareAndSwap(false, true) {
		go func() {
			for {
				time.Sleep(10 * time.Millisecond)
				counter.Add(1)
			}
		}()
	}
	for {
		mc.WaitFor(uint64(5 * time.Millisecond))
	}
}

func Count() uint64 { return counter.Load() }
