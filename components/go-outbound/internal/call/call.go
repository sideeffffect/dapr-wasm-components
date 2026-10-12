// Package call is the shared plumbing of the building-block exports: it runs
// one call against the embedded runtime's in-process API and classifies its
// gRPC status error — the Go counterpart of wasi-grpc-outbound's sidecar.rs.
//
// Error model: unrecoverable failures (an internal runtime error, a timeout,
// an unimplemented call, ...) panic — they are in no WIT return type and
// surface to the app as a trap. Recoverable failures become a *Failure that
// each block maps to its own WIT error type.
package call

import (
	"context"
	"fmt"
	"sort"
	"strings"

	daprgrpc "github.com/dapr/dapr/pkg/api/grpc"
	witTypes "go.bytecodealliance.org/pkg/wit/types"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/daprrt"
)

// Failure is a recoverable error. Status is the equivalent HTTP status so
// the per-block mapping matches the other providers; Message keeps the gRPC
// code and message; ErrorCode is the Dapr error code (ErrorInfo reason)
// when the runtime attached one.
type Failure struct {
	Status    int
	Code      codes.Code
	Message   string
	ErrorCode string
}

// IsPermission reports whether the failure is a permission/authorization
// problem.
func (f *Failure) IsPermission() bool {
	return f.Status == 401 || f.Status == 403 ||
		strings.Contains(f.ErrorCode, "PERMISSION") || strings.Contains(f.ErrorCode, "FORBIDDEN")
}

// API returns the embedded runtime's in-process API (starting it if needed).
func API() daprgrpc.API { return daprrt.API() }

// Do runs f against the in-process API and classifies its error.
func Do[T any](f func(ctx context.Context, api daprgrpc.API) (T, error)) (T, *Failure) {
	res, err := f(context.Background(), API())
	if err != nil {
		var zero T
		return zero, Classify(err)
	}
	return res, nil
}

// Classify turns an API error into a recoverable *Failure, or panics for
// the unrecoverable codes (see the package doc).
func Classify(err error) *Failure {
	st, ok := status.FromError(err)
	if !ok {
		panic(fmt.Sprintf("Dapr runtime error: %v", err))
	}
	message := fmt.Sprintf("%s: %s", st.Code(), st.Message())
	var httpStatus int
	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.Internal,
		codes.DataLoss, codes.ResourceExhausted, codes.Unknown, codes.Unimplemented:
		panic("Dapr runtime error: " + message)
	case codes.NotFound:
		httpStatus = 404
	case codes.PermissionDenied, codes.Unauthenticated:
		httpStatus = 403
	case codes.AlreadyExists, codes.Aborted:
		httpStatus = 409
	case codes.FailedPrecondition:
		httpStatus = 412
	default:
		httpStatus = 400
	}
	f := &Failure{Status: httpStatus, Code: st.Code(), Message: message}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			f.ErrorCode = info.GetReason()
		}
	}
	return f
}

// MetadataMap converts WIT metadata pairs to a proto map.
func MetadataMap(md []witTypes.Tuple2[string, string]) map[string]string {
	if len(md) == 0 {
		return nil
	}
	m := make(map[string]string, len(md))
	for _, kv := range md {
		m[kv.F0] = kv.F1
	}
	return m
}

// MetadataPairs converts a proto map to WIT metadata pairs, sorted for
// determinism (proto maps have no order).
func MetadataPairs(m map[string]string) []witTypes.Tuple2[string, string] {
	out := make([]witTypes.Tuple2[string, string], 0, len(m))
	for k, v := range m {
		out = append(out, witTypes.Tuple2[string, string]{F0: k, F1: v})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].F0 != out[j].F0 {
			return out[i].F0 < out[j].F0
		}
		return out[i].F1 < out[j].F1
	})
	return out
}

// OptString maps proto3's "" (unset) to a WIT none.
func OptString(s string) witTypes.Option[string] {
	if s == "" {
		return witTypes.None[string]()
	}
	return witTypes.Some(s)
}

// OrEmpty returns the option's value, or "" for none.
func OrEmpty(o witTypes.Option[string]) string {
	if o.IsSome() {
		return o.Some()
	}
	return ""
}

// OptPtr returns a pointer to the option's value, or nil for none.
func OptPtr[T any](o witTypes.Option[T]) *T {
	if o.IsSome() {
		v := o.Some()
		return &v
	}
	return nil
}
