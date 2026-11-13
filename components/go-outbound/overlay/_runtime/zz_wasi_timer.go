// Added to package runtime by the go-outbound overlay (gen-overlay.py).

package runtime

import _ "unsafe" // go:linkname

// wasiNextTimer returns when the earliest pending Go timer fires (nanotime
// clock; maxWhen if there is none). go-outbound's events.Run aims its WASI
// clock wait at it, because the idle hook (runtime.wasiOnIdle) does not wake
// the host for Go timers.
//
//go:linkname wasiNextTimer
func wasiNextTimer() int64 {
	return timeSleepUntil()
}

// wasiNanotime returns the runtime's monotonic clock, the clock of
// wasiNextTimer.
//
//go:linkname wasiNanotime
func wasiNanotime() int64 {
	return nanotime()
}
