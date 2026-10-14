package appcallback

import (
	"context"
	"encoding/json"
	"net/http"
	"path"
	"strings"
	"sync/atomic"

	"github.com/dapr/dapr/pkg/apphealth"
	"github.com/dapr/dapr/pkg/channel"
	"github.com/dapr/dapr/pkg/config"
	"github.com/dapr/dapr/pkg/messages"
	invokev1 "github.com/dapr/dapr/pkg/messaging/v1"
	commonv1pb "github.com/dapr/dapr/pkg/proto/common/v1"
	witTypes "go.bytecodealliance.org/pkg/wit/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/anyjson"
	ev "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_go_events"
	inv "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_invocation"
)

// Channel is Dapr's app channel, served in-process.
type Channel struct {
	appHealth atomic.Pointer[apphealth.AppHealth]
}

var _ channel.AppChannel = (*Channel)(nil)

// GetAppConfig asks the app for its actor configuration.
func (c *Channel) GetAppConfig(context.Context, string) (*config.ApplicationConfig, error) {
	a := deliver(ev.MakeEventActorConfig()).ActorConfig()
	cfg := &config.ApplicationConfig{
		Entities:                a.Entities,
		ActorIdleTimeout:        a.ActorIdleTimeout.SomeOr(""),
		DrainOngoingCallTimeout: a.DrainOngoingCallTimeout.SomeOr(""),
		Reentrancy:              config.ReentrancyConfig{Enabled: a.ReentrancyEnabled.SomeOr(false)},
		// actor-scan-interval has no field: Dapr no longer reads it.
		RemindersStoragePartitions: int(a.RemindersStoragePartitions.SomeOr(0)),
	}
	if cfg.Entities == nil {
		cfg.Entities = []string{}
	}
	if a.DrainRebalancedActors.IsSome() {
		v := a.DrainRebalancedActors.Some()
		cfg.DrainRebalancedActors = &v
	}
	if a.ReentrancyMaxStackDepth.IsSome() {
		v := int(a.ReentrancyMaxStackDepth.Some())
		cfg.Reentrancy.MaxStackDepth = &v
	}
	return cfg, nil
}

// HealthProbe asks the app whether it is healthy.
func (c *Channel) HealthProbe(context.Context) (*apphealth.Status, error) {
	res := deliver(ev.MakeEventHealthCheck()).HealthCheck()
	if res.IsErr() {
		reason := "unhealthy: " + res.Err().Message
		return apphealth.NewStatus(false, &reason), nil
	}
	return apphealth.NewStatus(true, nil), nil
}

// SetAppHealth gives the channel Dapr's app-health tracker: while the app
// is unhealthy, invocations and jobs fail fast, like on Dapr's own channels.
func (c *Channel) SetAppHealth(ah *apphealth.AppHealth) {
	c.appHealth.Store(ah)
}

func (c *Channel) unhealthy() bool {
	ah := c.appHealth.Load()
	return ah != nil && !ah.GetStatus().IsHealthy
}

// TriggerJob delivers a due job. Job data is a google.protobuf.Any; the app
// gets it as JSON, the way go-outbound's jobs-read returns it.
func (c *Channel) TriggerJob(_ context.Context, name string, data *anypb.Any) (*invokev1.InvokeMethodResponse, error) {
	if c.unhealthy() {
		return nil, status.Error(codes.Internal, messages.ErrAppUnhealthy)
	}
	res := deliver(ev.MakeEventJob(ev.JobEvent{
		Name:        name,
		Data:        []byte(anyjson.UnpackJSON(data)),
		ContentType: witTypes.Some("application/json"),
	})).Job()
	code := codes.OK
	if res.IsErr() {
		code = codes.Internal
	}
	return invokev1.NewInvokeMethodResponse(int32(code), "", nil), nil
}

// InvokeMethod delivers a service invocation, or an actor callback: Dapr's
// actor runtime calls the channel with `actors/...` methods.
func (c *Channel) InvokeMethod(ctx context.Context, req *invokev1.InvokeMethodRequest, _ string) (*invokev1.InvokeMethodResponse, error) {
	if c.unhealthy() {
		return nil, status.Error(codes.Internal, messages.ErrAppUnhealthy)
	}
	data, err := req.RawDataFull()
	if err != nil {
		return nil, err
	}
	msg := req.Message()
	if req.Actor() != nil {
		if rest, ok := strings.CutPrefix(msg.GetMethod(), "actors/"); ok {
			if res, ok := invokeActor(msg.GetHttpExtension().GetVerb(), rest, data); ok {
				return res, nil
			}
		}
	}
	return invoke(ctx, req, data), nil
}

// --- service invocation ----------------------------------------------------

func invoke(ctx context.Context, req *invokev1.InvokeMethodRequest, data []byte) *invokev1.InvokeMethodResponse {
	msg := req.Message()
	// Like Dapr's HTTP channel: resolve any path traversal in the method.
	method := strings.TrimPrefix(path.Clean("/"+msg.GetMethod()), "/")
	headers := metadata{}
	invokev1.InternalMetadataToHTTPHeader(ctx, req.Metadata(), func(key, value string) {
		headers = append(headers, witTypes.Tuple2[string, string]{F0: strings.ToLower(key), F1: value})
	})
	contentType := req.ContentType()
	if contentType != "" {
		headers = append(headers, witTypes.Tuple2[string, string]{F0: "content-type", F1: contentType})
	}
	query := nonEmpty(req.EncodeHTTPQueryString())
	res := deliver(ev.MakeEventInvoke(ev.InvokeRequest{
		Method:      method,
		Verb:        verb(msg.GetHttpExtension().GetVerb()),
		Headers:     sortPairs(headers),
		Query:       query,
		ContentType: contentType,
		Data:        data,
	})).Invoke()
	header := http.Header{}
	for _, h := range res.Headers {
		header.Add(h.F0, h.F1)
	}
	return invokev1.NewInvokeMethodResponse(int32(res.Status), "", nil).
		WithHTTPHeaders(header).
		WithRawDataBytes(res.Body).
		WithContentType(header.Get("content-type"))
}

func verb(v commonv1pb.HTTPExtension_Verb) inv.HttpVerb {
	switch v {
	case commonv1pb.HTTPExtension_GET:
		return inv.HttpVerbGet
	case commonv1pb.HTTPExtension_PUT:
		return inv.HttpVerbPut
	case commonv1pb.HTTPExtension_DELETE:
		return inv.HttpVerbDelete
	case commonv1pb.HTTPExtension_PATCH:
		return inv.HttpVerbPatch
	case commonv1pb.HTTPExtension_HEAD:
		return inv.HttpVerbHead
	case commonv1pb.HTTPExtension_OPTIONS:
		return inv.HttpVerbOptions
	}
	// NONE/POST, and CONNECT/TRACE which have no WIT verb.
	return inv.HttpVerbPost
}

// --- actors ----------------------------------------------------------------

// invokeActor handles the method of an actor callback, which Dapr's actor
// runtime builds the same for every app channel:
//
//	DELETE actors/<type>/<id>                     actor-deactivate
//	PUT    actors/<type>/<id>/method/timer/<name>  actor-timer
//	PUT    actors/<type>/<id>/method/remind/<name> actor-reminder
//	PUT    actors/<type>/<id>/method/<method>      actor-invoke
//
// A timer or reminder carries Dapr's JSON {dueTime, period, data, callback}.
// It reports false when rest has none of these shapes.
func invokeActor(v commonv1pb.HTTPExtension_Verb, rest string, body []byte) (*invokev1.InvokeMethodResponse, bool) {
	segments := strings.SplitN(rest, "/", 4)
	if len(segments) < 2 || segments[0] == "" || segments[1] == "" {
		return nil, false
	}
	actorType, actorID := segments[0], segments[1]

	if v == commonv1pb.HTTPExtension_DELETE && len(segments) == 2 {
		res := deliver(ev.MakeEventActorDeactivate(ev.ActorRef{ActorType: actorType, ActorId: actorID})).ActorDeactivate()
		return unitResponse(res), true
	}
	if len(segments) < 4 || segments[2] != "method" || segments[3] == "" {
		return nil, false
	}
	method := segments[3]
	if name, ok := strings.CutPrefix(method, "timer/"); ok && name != "" {
		f := parseTimerFields(body)
		res := deliver(ev.MakeEventActorTimer(ev.ActorTimerEvent{
			ActorType: actorType, ActorId: actorID, Name: name,
			DueTime: f.dueTime, Period: f.period, Data: f.data, Callback: f.callback,
		})).ActorTimer()
		return unitResponse(res), true
	}
	if name, ok := strings.CutPrefix(method, "remind/"); ok && name != "" {
		f := parseTimerFields(body)
		res := deliver(ev.MakeEventActorReminder(ev.ActorReminderEvent{
			ActorType: actorType, ActorId: actorID, Name: name,
			DueTime: f.dueTime, Period: f.period, Data: f.data,
		})).ActorReminder()
		return unitResponse(res), true
	}
	res := deliver(ev.MakeEventActorInvoke(ev.ActorInvoke{
		ActorType: actorType, ActorId: actorID, Method: method, Data: body,
	})).ActorInvoke()
	if res.IsErr() {
		return errorResponse(res.Err()), true
	}
	return invokev1.NewInvokeMethodResponse(http.StatusOK, "", nil).
		WithRawDataBytes(res.Ok()).
		WithContentType("application/json"), true
}

// timerFields is the shared body of a timer/reminder fire.
type timerFields struct {
	dueTime, period, callback witTypes.Option[string]
	data                      []byte
}

func parseTimerFields(body []byte) timerFields {
	var raw map[string]any
	_ = json.Unmarshal(body, &raw)
	f := timerFields{dueTime: optStr(raw, "dueTime"), period: optStr(raw, "period"), callback: optStr(raw, "callback")}
	switch d := raw["data"].(type) {
	case nil:
	case string:
		f.data = []byte(d)
	default:
		f.data, _ = json.Marshal(d)
	}
	return f
}

func optStr(object map[string]any, key string) witTypes.Option[string] {
	if s, ok := object[key].(string); ok {
		return witTypes.Some(s)
	}
	return witTypes.None[string]()
}

func unitResponse(res witTypes.Result[witTypes.Unit, appError]) *invokev1.InvokeMethodResponse {
	if res.IsErr() {
		return errorResponse(res.Err())
	}
	return invokev1.NewInvokeMethodResponse(http.StatusOK, "", nil)
}

// errorResponse reports an app-error (a message only) to the actor runtime
// as a 500, like wasi-http-inbound.
func errorResponse(e appError) *invokev1.InvokeMethodResponse {
	b, _ := json.Marshal(map[string]any{"errorCode": "ERR_APP", "message": e.Message})
	return invokev1.NewInvokeMethodResponse(http.StatusInternalServerError, "", nil).
		WithRawDataBytes(b).
		WithContentType("application/json")
}
