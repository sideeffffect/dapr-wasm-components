// Service invocation, served by the embedded runtime's API — InvokeService.
// Mirrors wasi-grpc-outbound's invocation.rs.
//
// The WIT contract has HTTP semantics; they map onto the call like this:
// verb and query string go into HTTPExtension, the content-type header
// becomes InvokeRequest.ContentType, the other request headers travel as
// incoming gRPC metadata on the context, and the target app's response
// headers are captured from grpc.SetHeader by a fake ServerTransportStream.
//
// As over the network, a non-2xx app response surfaces as an API error
// (an invoke-error, or a panic per call.Classify), not as an http-response.
// Unlike the network provider, the status of a successful HTTP app response
// is exact: the runtime reports it in the dapr-http-status header.
package export_dapr_wasm_components_interfaces_invocation

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"

	daprgrpc "github.com/dapr/dapr/pkg/api/grpc"
	commonv1pb "github.com/dapr/dapr/pkg/proto/common/v1"
	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	witTypes "go.bytecodealliance.org/pkg/wit/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/daprrt"
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/events"
	iv "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_invocation"
)

const httpStatusHeader = "dapr-http-status"

func invokeError(f *call.Failure) iv.InvokeError {
	if f.IsPermission() {
		return iv.MakeInvokeErrorPermissionDenied(f.Message)
	}
	return iv.MakeInvokeErrorAppUnreachable(f.Message)
}

func verbPb(v iv.HttpVerb) commonv1pb.HTTPExtension_Verb {
	switch v {
	case iv.HttpVerbGet:
		return commonv1pb.HTTPExtension_GET
	case iv.HttpVerbPost:
		return commonv1pb.HTTPExtension_POST
	case iv.HttpVerbPut:
		return commonv1pb.HTTPExtension_PUT
	case iv.HttpVerbDelete:
		return commonv1pb.HTTPExtension_DELETE
	case iv.HttpVerbPatch:
		return commonv1pb.HTTPExtension_PATCH
	case iv.HttpVerbHead:
		return commonv1pb.HTTPExtension_HEAD
	case iv.HttpVerbOptions:
		return commonv1pb.HTTPExtension_OPTIONS
	}
	panic("unknown http-verb " + strconv.Itoa(int(v)))
}

// isTransportHeader reports headers of the gRPC transport itself, not of
// the app's response (content-type is replaced by InvokeResponse.ContentType).
func isTransportHeader(name string) bool {
	return name == "content-type" || strings.HasPrefix(name, "grpc-")
}

// headerCapture is the ServerTransportStream the API's grpc.SetHeader
// writes the app's response headers to (there is no real gRPC server).
type headerCapture struct {
	mu     sync.Mutex
	header metadata.MD
}

func (*headerCapture) Method() string {
	return runtimev1pb.Dapr_InvokeService_FullMethodName
}

func (h *headerCapture) SetHeader(md metadata.MD) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.header = metadata.Join(h.header, md)
	return nil
}

func (h *headerCapture) SendHeader(md metadata.MD) error { return h.SetHeader(md) }

func (*headerCapture) SetTrailer(metadata.MD) error { return nil }

func Invoke(appID string, methodPath string, verb iv.HttpVerb, headers []witTypes.Tuple2[string, string], query witTypes.Option[string], body []uint8) witTypes.Result[iv.HttpResponse, iv.InvokeError] {
	// The app is one instance and only runs inside a callback, so an
	// invocation of its own app id could only be delivered back into the
	// busy instance: it would deadlock. Fail fast instead.
	if events.Attached() && daprrt.IsSelf(appID) {
		return witTypes.Err[iv.HttpResponse](iv.MakeInvokeErrorAppUnreachable(
			"self-invocation is not supported by the go provider family: the app is a single " +
				"instance, busy in the callback that makes this call"))
	}

	// content-type rides in a dedicated proto field; everything else is
	// passed through as incoming metadata (gRPC keys are lowercase).
	var contentType string
	incoming := metadata.MD{}
	for _, h := range headers {
		if strings.EqualFold(h.F0, "content-type") {
			contentType = h.F1
		} else {
			incoming.Append(strings.ToLower(h.F0), h.F1)
		}
	}
	// Like a real gRPC client, always send a user-agent: Dapr expects
	// non-empty incoming metadata (see call.WithIncoming).
	if len(incoming.Get("user-agent")) == 0 {
		incoming.Set("user-agent", call.UserAgent)
	}
	req := &runtimev1pb.InvokeServiceRequest{
		Id: appID,
		Message: &commonv1pb.InvokeRequest{
			Method: strings.TrimLeft(methodPath, "/"),
			// An unset type URL makes Dapr treat the value as raw bytes.
			Data:          &anypb.Any{Value: body},
			ContentType:   contentType,
			HttpExtension: &commonv1pb.HTTPExtension{Verb: verbPb(verb), Querystring: call.OrEmpty(query)},
		},
	}
	capture := &headerCapture{}
	r, f := call.Do(func(ctx context.Context, api daprgrpc.API) (*commonv1pb.InvokeResponse, error) {
		ctx = call.WithIncoming(ctx, incoming)
		ctx = grpc.NewContextWithServerTransportStream(ctx, capture)
		return api.InvokeService(ctx, req)
	})
	if f != nil {
		return witTypes.Err[iv.HttpResponse](invokeError(f))
	}

	httpStatus := uint16(200)
	names := make([]string, 0, len(capture.header))
	for name := range capture.header {
		names = append(names, name)
	}
	sort.Strings(names) // metadata.MD is a map: sort for determinism
	var respHeaders []witTypes.Tuple2[string, string]
	for _, name := range names {
		values := capture.header[name]
		if name == httpStatusHeader && len(values) > 0 {
			if s, err := strconv.ParseUint(values[0], 10, 16); err == nil {
				httpStatus = uint16(s)
			}
		}
		if isTransportHeader(name) {
			continue
		}
		for _, v := range values {
			respHeaders = append(respHeaders, witTypes.Tuple2[string, string]{F0: name, F1: v})
		}
	}
	if ct := r.GetContentType(); ct != "" {
		respHeaders = append(respHeaders, witTypes.Tuple2[string, string]{F0: "content-type", F1: ct})
	}
	return witTypes.Ok[iv.HttpResponse, iv.InvokeError](iv.HttpResponse{
		Status: httpStatus, Headers: respHeaders, Body: r.GetData().GetValue(),
	})
}
