// Call0..Call8: one per arity of the go-outbound exports.

package events

// CallN runs an async export's body f(args...) in a goroutine of its own and
// waits for its result with Wait. The generated glue calls it (rewritten by
// scripts/fix-bindings.py) so that the export goroutine — the one that lowers
// the result and calls task.return — only ever waits on a clock wait of its
// own task, never directly on Dapr (see drive.go). The body may block on
// anything: channels, network, Go timers.

// Call0 runs f() off the export goroutine (see above).
func Call0[R any](f func() R) R {
	done := make(chan R, 1)
	go func() { done <- f() }()
	return Wait(done)
}

// Call1 runs f(a1) off the export goroutine (see above).
func Call1[A1 any, R any](f func(A1) R, a1 A1) R {
	done := make(chan R, 1)
	go func() { done <- f(a1) }()
	return Wait(done)
}

// Call2 runs f(a1, a2) off the export goroutine (see above).
func Call2[A1 any, A2 any, R any](f func(A1, A2) R, a1 A1, a2 A2) R {
	done := make(chan R, 1)
	go func() { done <- f(a1, a2) }()
	return Wait(done)
}

// Call3 runs f(a1, a2, a3) off the export goroutine (see above).
func Call3[A1 any, A2 any, A3 any, R any](f func(A1, A2, A3) R, a1 A1, a2 A2, a3 A3) R {
	done := make(chan R, 1)
	go func() { done <- f(a1, a2, a3) }()
	return Wait(done)
}

// Call4 runs f(a1, a2, a3, a4) off the export goroutine (see above).
func Call4[A1 any, A2 any, A3 any, A4 any, R any](f func(A1, A2, A3, A4) R, a1 A1, a2 A2, a3 A3, a4 A4) R {
	done := make(chan R, 1)
	go func() { done <- f(a1, a2, a3, a4) }()
	return Wait(done)
}

// Call5 runs f(a1, a2, a3, a4, a5) off the export goroutine (see above).
func Call5[A1 any, A2 any, A3 any, A4 any, A5 any, R any](f func(A1, A2, A3, A4, A5) R, a1 A1, a2 A2, a3 A3, a4 A4, a5 A5) R {
	done := make(chan R, 1)
	go func() { done <- f(a1, a2, a3, a4, a5) }()
	return Wait(done)
}

// Call6 runs f(a1, a2, a3, a4, a5, a6) off the export goroutine (see above).
func Call6[A1 any, A2 any, A3 any, A4 any, A5 any, A6 any, R any](f func(A1, A2, A3, A4, A5, A6) R, a1 A1, a2 A2, a3 A3, a4 A4, a5 A5, a6 A6) R {
	done := make(chan R, 1)
	go func() { done <- f(a1, a2, a3, a4, a5, a6) }()
	return Wait(done)
}

// Call7 runs f(a1, a2, a3, a4, a5, a6, a7) off the export goroutine (see above).
func Call7[A1 any, A2 any, A3 any, A4 any, A5 any, A6 any, A7 any, R any](f func(A1, A2, A3, A4, A5, A6, A7) R, a1 A1, a2 A2, a3 A3, a4 A4, a5 A5, a6 A6, a7 A7) R {
	done := make(chan R, 1)
	go func() { done <- f(a1, a2, a3, a4, a5, a6, a7) }()
	return Wait(done)
}

// Call8 runs f(a1, a2, a3, a4, a5, a6, a7, a8) off the export goroutine (see above).
func Call8[A1 any, A2 any, A3 any, A4 any, A5 any, A6 any, A7 any, A8 any, R any](f func(A1, A2, A3, A4, A5, A6, A7, A8) R, a1 A1, a2 A2, a3 A3, a4 A4, a5 A5, a6 A6, a7 A7, a8 A8) R {
	done := make(chan R, 1)
	go func() { done <- f(a1, a2, a3, a4, a5, a6, a7, a8) }()
	return Wait(done)
}
