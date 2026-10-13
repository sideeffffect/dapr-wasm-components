// Package daprerr recognizes the runtime errors that carry a recoverable
// condition under a tier-3 gRPC code, so the exports can map them to a WIT
// error instead of panicking in call.Classify. It depends only on the gRPC
// status types, so it is unit-tested natively.
package daprerr

import (
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TimerActorTypeNotHosted reports whether err is the runtime's answer to a
// timer for an actor type this app does not host, and returns its message.
// The runtime reports it as a generic ERR_ACTOR_TIMER_CREATE (code Internal,
// HTTP 500) whose message ends in "actor type not registered" — the caller's
// mistake, not a runtime fault, and the very condition the runtime itself
// reports for reminders as ERR_ACTOR_REMINDER_NON_HOSTED (PermissionDenied).
// Mirrors wasi-grpc-outbound's register_timer_error.
func TimerActorTypeNotHosted(err error) (string, bool) {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Internal || !strings.HasSuffix(st.Message(), "actor type not registered") {
		return "", false
	}
	return st.Message(), true
}
