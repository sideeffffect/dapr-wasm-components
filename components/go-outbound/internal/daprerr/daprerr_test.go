package daprerr

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// daprd's ERR_ACTOR_TIMER_CREATE for an actor type the app does not host
// (message checked against daprd 1.17.7's answer).
const notHosted = "error creating actor timer: can't create timer for actor ProbeActor||probe-1: actor type not registered"

func TestTimerActorTypeNotHosted(t *testing.T) {
	message, ok := TimerActorTypeNotHosted(status.Error(codes.Internal, notHosted))
	if !ok || message != notHosted {
		t.Fatalf("got (%q, %v), want (%q, true)", message, ok, notHosted)
	}
}

func TestTimerOtherFailures(t *testing.T) {
	for _, err := range []error{
		status.Error(codes.Internal, "error creating actor timer: timer storage unavailable"),
		status.Error(codes.PermissionDenied, notHosted),
		errors.New(notHosted),
	} {
		if _, ok := TimerActorTypeNotHosted(err); ok {
			t.Errorf("%v: classified as an unhosted actor type", err)
		}
	}
}
