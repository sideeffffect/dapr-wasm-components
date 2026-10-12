// Package appchannel serves Dapr's HTTP app channel inside go-outbound.
//
// The embedded runtime talks to "the app" with Dapr's documented HTTP app
// protocol, over in-memory pipes (see daprrt). This handler translates each
// app-channel request into a typed event and waits for the app's answer via
// the go-inbound pump (package events) — the Go counterpart of
// wasi-http-inbound, whose routing it mirrors (first match wins):
//
//	GET /healthz                          health-check
//	GET /dapr/subscribe                   list-topic-subscriptions
//	GET /dapr/config                      actor-config
//	OPTIONS /<input-binding>              binding verification (200)
//	POST <pubsub route>[/<rule path>]     topic
//	POST <bulk pubsub route>[/<rule>]     bulk-topic
//	POST /job/<name>                      job
//	PUT /actors/<t>/<id>/method/timer/<n> actor-timer
//	PUT /actors/<t>/<id>/method/remind/<n> actor-reminder
//	PUT /actors/<t>/<id>/method/<m>       actor-invoke
//	DELETE /actors/<t>/<id>               actor-deactivate
//	POST /<input-binding>                 binding
//	anything else                         invoke
//
// Configuration updates do not pass through here: go-outbound subscribes
// in-process and queues them directly.
package appchannel

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"

	witTypes "go.bytecodealliance.org/pkg/wit/types"

	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/events"
	ev "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_go_events"
	inv "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_invocation"
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_types"
)

type metadata = []witTypes.Tuple2[string, string]
type appError = dapr_wasm_components_interfaces_types.AppError

// PubsubRoute is the app-channel path Dapr posts every pub/sub delivery to.
// It is the `route` of every programmatic subscription (topic and pub/sub
// name travel inside the CloudEvent); routing-rule paths are advertised
// under it, so they never collide with service-invocation paths and the
// matched rule path is recovered from the URL.
const PubsubRoute = "/__dapr_wasm_components/pubsub"

// PubsubBulkRoute is the route of bulk-subscribed topics: Dapr posts a bulk
// envelope there, not a single CloudEvent.
const PubsubBulkRoute = "/__dapr_wasm_components/pubsub-bulk"

// Handler is the app-channel http.Handler.
type Handler struct {
	bindingsOnce sync.Once
	bindings     map[string]bool
}

// deliver queues one event for the app and returns its result. The app
// channel only exists when a pump is attached, so ErrNotAttached is a bug.
func deliver(e ev.Event) ev.EventResult {
	r, err := events.Deliver(e)
	if err != nil {
		panic(fmt.Sprintf("app channel without an attached app: %v", err))
	}
	return r
}

// inputBindings asks the app once for the input bindings it consumes: the
// list is static, and Dapr consults it on every binding delivery.
func (h *Handler) inputBindings() map[string]bool {
	h.bindingsOnce.Do(func() {
		h.bindings = map[string]bool{}
		for _, b := range deliver(ev.MakeEventListInputBindings()).ListInputBindings() {
			h.bindings[b] = true
		}
	})
	return h.bindings
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	path := r.URL.Path
	method := r.Method

	switch {
	case method == http.MethodGet && path == "/healthz":
		res := deliver(ev.MakeEventHealthCheck()).HealthCheck()
		if res.IsErr() {
			text(w, http.StatusInternalServerError, "unhealthy: "+res.Err().Message)
			return
		}
		text(w, http.StatusOK, "ok")
		return
	case method == http.MethodGet && path == "/dapr/subscribe":
		writeJSON(w, http.StatusOK, subscribeResponse(deliver(ev.MakeEventListTopicSubscriptions()).ListTopicSubscriptions()))
		return
	case method == http.MethodGet && path == "/dapr/config":
		writeJSON(w, http.StatusOK, actorConfigResponse(deliver(ev.MakeEventActorConfig()).ActorConfig()))
		return
	case method == http.MethodOptions && h.inputBindings()[strings.TrimPrefix(path, "/")]:
		// Input-binding verification at startup: 2xx = the app consumes it.
		text(w, http.StatusOK, "")
		return
	}

	if method == http.MethodPost {
		// The bulk prefix first: it shares the single-event route's prefix.
		if rest, ok := strings.CutPrefix(path, PubsubBulkRoute); ok && (rest == "" || rest[0] == '/') {
			deliverBulkTopicEvent(w, body, rulePath(rest))
			return
		}
		if rest, ok := strings.CutPrefix(path, PubsubRoute); ok && (rest == "" || rest[0] == '/') {
			deliverTopicEvent(w, body, rulePath(rest))
			return
		}
		if name, ok := strings.CutPrefix(path, "/job/"); ok {
			deliverJob(w, decode(name), r.Header.Get("content-type"), body)
			return
		}
	}

	if rest, ok := strings.CutPrefix(path, "/actors/"); ok && routeActor(w, method, rest, body) {
		return
	}

	if method == http.MethodPost {
		if name := strings.TrimPrefix(path, "/"); h.inputBindings()[name] {
			deliverBinding(w, name, r.Header, body)
			return
		}
	}

	deliverInvocation(w, r, body)
}

func rulePath(rest string) witTypes.Option[string] {
	if rest == "" {
		return witTypes.None[string]()
	}
	return witTypes.Some(decode(rest))
}

// --- pub/sub ---------------------------------------------------------------

func subscribeResponse(subs []ev.TopicSubscription) []map[string]any {
	entries := []map[string]any{}
	for _, sub := range subs {
		// Bulk-subscribed topics get their own base route (bulk envelopes,
		// not single CloudEvents); routing-rule paths go under the base.
		base := PubsubRoute
		if sub.BulkSubscribe.IsSome() && sub.BulkSubscribe.Some().Enabled {
			base = PubsubBulkRoute
		}
		entry := map[string]any{"pubsubname": sub.PubsubName, "topic": sub.Topic}
		if sub.Routes.IsNone() {
			entry["route"] = base
		} else {
			routes := sub.Routes.Some()
			rules := []map[string]any{}
			for _, rule := range routes.Rules {
				rules = append(rules, map[string]any{"match": rule.Match, "path": routedPath(base, rule.Path)})
			}
			value := map[string]any{"rules": rules}
			if routes.Default.IsSome() {
				value["default"] = routedPath(base, routes.Default.Some())
			}
			entry["routes"] = value
		}
		if sub.BulkSubscribe.IsSome() {
			config := sub.BulkSubscribe.Some()
			bulk := map[string]any{"enabled": config.Enabled}
			if config.MaxMessagesCount.IsSome() {
				bulk["maxMessagesCount"] = config.MaxMessagesCount.Some()
			}
			if config.MaxAwaitDurationMs.IsSome() {
				bulk["maxAwaitDurationMs"] = config.MaxAwaitDurationMs.Some()
			}
			entry["bulkSubscribe"] = bulk
		}
		if sub.DeadLetterTopic.IsSome() {
			entry["deadLetterTopic"] = sub.DeadLetterTopic.Some()
		}
		if len(sub.Metadata) > 0 {
			entry["metadata"] = metadataObject(sub.Metadata)
		}
		entries = append(entries, entry)
	}
	return entries
}

// routedPath is base plus the rule path, with exactly one '/' between them.
func routedPath(base, path string) string {
	return base + "/" + strings.TrimLeft(path, "/")
}

func statusName(s ev.TopicEventResponse) string {
	switch s {
	case ev.TopicEventResponseRetry:
		return "RETRY"
	case ev.TopicEventResponseDrop:
		return "DROP"
	}
	return "SUCCESS"
}

// deliverTopicEvent delivers one CloudEvent; a matched rule path (from the
// URL) overrides any path in the envelope.
func deliverTopicEvent(w http.ResponseWriter, body []byte, rule witTypes.Option[string]) {
	var object map[string]any
	if err := json.Unmarshal(body, &object); err != nil || object == nil {
		// A malformed envelope is unexpected; ask Dapr to drop it.
		fmt.Fprintf(os.Stderr, "dropping unparseable CloudEvent: %v\n", err)
		writeJSON(w, http.StatusOK, map[string]any{"status": "DROP"})
		return
	}
	event, err := cloudEventFromObject(object)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dropping unparseable CloudEvent: %v\n", err)
		writeJSON(w, http.StatusOK, map[string]any{"status": "DROP"})
		return
	}
	if rule.IsSome() {
		event.Path = rule
	}
	status := deliver(ev.MakeEventTopic(event)).Topic()
	writeJSON(w, http.StatusOK, map[string]any{"status": statusName(status)})
}

// deliverBulkTopicEvent delivers a bulk envelope and replies with Dapr's
// `{"statuses": [{entryId, status}]}`. daprd builds the envelope in
// pkg/runtime/pubsub/bulksubscribe_events.go: a CloudEvent entry carries the
// parsed CloudEvent object in `event`, a raw entry its bytes as base64.
func deliverBulkTopicEvent(w http.ResponseWriter, body []byte, rule witTypes.Option[string]) {
	bulk, err := parseBulkEvent(body)
	if err != nil {
		// Unanswerable per entry: a 404 makes Dapr drop the whole batch.
		fmt.Fprintf(os.Stderr, "dropping unparseable bulk envelope: %v\n", err)
		text(w, http.StatusNotFound, "unparseable bulk envelope")
		return
	}
	if rule.IsSome() {
		bulk.Path = rule
		for i, entry := range bulk.Entries {
			if entry.Event.Tag() == ev.BulkEventCloudEvent {
				e := entry.Event.CloudEvent()
				e.Path = rule
				bulk.Entries[i].Event = ev.MakeBulkEventCloudEvent(e)
			}
		}
	}
	statuses := []map[string]any{}
	for _, entry := range deliver(ev.MakeEventBulkTopic(bulk)).BulkTopic() {
		statuses = append(statuses, map[string]any{"entryId": entry.EntryId, "status": statusName(entry.Status)})
	}
	writeJSON(w, http.StatusOK, map[string]any{"statuses": statuses})
}

func parseBulkEvent(body []byte) (ev.BulkTopicEvent, error) {
	var object map[string]any
	if err := json.Unmarshal(body, &object); err != nil {
		return ev.BulkTopicEvent{}, fmt.Errorf("invalid bulk envelope JSON: %w", err)
	}
	if object == nil {
		return ev.BulkTopicEvent{}, fmt.Errorf("bulk envelope is not a JSON object")
	}
	topic, pubsubName := str(object, "topic"), str(object, "pubsubname")
	entries := []ev.BulkTopicEventEntry{}
	rawEntries, _ := object["entries"].([]any)
	for _, raw := range rawEntries {
		entry, _ := raw.(map[string]any)
		entryID := str(entry, "entryId")
		var event ev.BulkEvent
		switch e := entry["event"].(type) {
		case map[string]any:
			ce, err := cloudEventFromObject(e)
			if err != nil {
				return ev.BulkTopicEvent{}, err
			}
			// Fill the routing fields from the envelope if the CloudEvent
			// lacks them.
			if ce.Topic == "" {
				ce.Topic = topic
			}
			if ce.PubsubName == "" {
				ce.PubsubName = pubsubName
			}
			event = ev.MakeBulkEventCloudEvent(ce)
		case string:
			data, err := base64.StdEncoding.DecodeString(e)
			if err != nil {
				return ev.BulkTopicEvent{}, fmt.Errorf("bulk entry %s: %w", entryID, err)
			}
			event = ev.MakeBulkEventRaw(data)
		default:
			return ev.BulkTopicEvent{}, fmt.Errorf("bulk entry %s: unexpected event shape %v", entryID, e)
		}
		entries = append(entries, ev.BulkTopicEventEntry{
			EntryId:     entryID,
			Event:       event,
			ContentType: optStr(entry, "contentType"),
			Metadata:    stringMap(entry["metadata"]),
		})
	}
	return ev.BulkTopicEvent{
		Id:         str(object, "id"),
		Entries:    entries,
		Metadata:   stringMap(object["metadata"]),
		Topic:      topic,
		PubsubName: pubsubName,
		Type:       str(object, "type"),
		Path:       optStr(object, "path"),
	}, nil
}

// standardAttributes are the CloudEvent attributes that are not extensions.
var standardAttributes = map[string]bool{
	"id": true, "source": true, "type": true, "specversion": true, "datacontenttype": true,
	"subject": true, "time": true, "topic": true, "pubsubname": true, "data": true,
	"data_base64": true, "path": true, "traceid": true, "traceparent": true, "tracestate": true,
}

// cloudEventFromObject builds a topic-event from a CloudEvent JSON object.
// JSON `data` is re-serialized (a string as its text); `data_base64` is
// decoded.
func cloudEventFromObject(object map[string]any) (ev.TopicEvent, error) {
	var data []byte
	contentType := optStr(object, "datacontenttype")
	if b64, ok := object["data_base64"].(string); ok {
		decoded, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			return ev.TopicEvent{}, fmt.Errorf("invalid data_base64: %w", err)
		}
		data = decoded
	} else if d, ok := object["data"]; ok {
		if s, isString := d.(string); isString {
			data = []byte(s)
		} else {
			data, _ = json.Marshal(d)
		}
		if contentType.IsNone() {
			contentType = witTypes.Some("application/json")
		}
	}
	extensions := metadata{}
	for key, value := range object {
		if !standardAttributes[key] {
			extensions = append(extensions, witTypes.Tuple2[string, string]{F0: key, F1: textOf(value)})
		}
	}
	specVersion := str(object, "specversion")
	if specVersion == "" {
		specVersion = "1.0"
	}
	return ev.TopicEvent{
		Id:              str(object, "id"),
		Source:          str(object, "source"),
		Type:            str(object, "type"),
		SpecVersion:     specVersion,
		DataContentType: contentType,
		Data:            data,
		Topic:           str(object, "topic"),
		PubsubName:      str(object, "pubsubname"),
		Path:            optStr(object, "path"),
		Extensions:      sortPairs(extensions),
	}, nil
}

// --- input bindings --------------------------------------------------------

func deliverBinding(w http.ResponseWriter, name string, headers http.Header, body []byte) {
	// Component-set metadata arrives as `metadata.<key>` headers.
	md := metadata{}
	for key, values := range headers {
		if stripped, ok := strings.CutPrefix(strings.ToLower(key), "metadata."); ok && len(values) > 0 {
			md = append(md, witTypes.Tuple2[string, string]{F0: stripped, F1: values[0]})
		}
	}
	res := deliver(ev.MakeEventBinding(ev.BindingEvent{Name: name, Data: body, Metadata: sortPairs(md)})).Binding()
	if res.IsErr() {
		errorResponse(w, res.Err())
		return
	}
	writeJSON(w, http.StatusOK, bindingResponse(res.Ok()))
}

// bindingResponse renders the input-binding response JSON (state to persist,
// output bindings to forward `data` to). Field names follow the bindings API
// reference. daprd (checked up to 1.18) does not act on it over the HTTP app
// channel — kept for parity with wasi-http-inbound.
func bindingResponse(r ev.BindingEventResponse) map[string]any {
	object := map[string]any{}
	if r.StoreName.IsSome() {
		object["storeName"] = r.StoreName.Some()
	}
	if len(r.States) > 0 {
		states := []map[string]any{}
		for _, item := range r.States {
			states = append(states, map[string]any{"key": item.Key, "value": valueToJSON(item.Value)})
		}
		object["state"] = states
	}
	if len(r.To) > 0 {
		object["to"] = r.To
		concurrency := "sequential"
		if r.Concurrency == ev.BindingConcurrencyParallel {
			concurrency = "parallel"
		}
		object["concurrency"] = concurrency
	}
	if r.Data.IsSome() {
		object["data"] = valueToJSON(r.Data.Some())
	}
	return object
}

// --- jobs ------------------------------------------------------------------

func deliverJob(w http.ResponseWriter, name, contentType string, body []byte) {
	ct := witTypes.None[string]()
	if contentType != "" {
		ct = witTypes.Some(contentType)
	}
	res := deliver(ev.MakeEventJob(ev.JobEvent{Name: name, Data: body, ContentType: ct})).Job()
	if res.IsErr() {
		errorResponse(w, res.Err())
		return
	}
	text(w, http.StatusOK, "")
}

// --- actors ----------------------------------------------------------------

// routeActor handles /actors/<rest>; false when rest is no known actor
// app-channel shape (the caller falls through).
func routeActor(w http.ResponseWriter, method, rest string, body []byte) bool {
	var segments []string
	for _, s := range strings.Split(rest, "/") {
		if s != "" {
			segments = append(segments, decode(s))
		}
	}
	if len(segments) < 2 {
		return false
	}
	actorType, actorID := segments[0], segments[1]

	if method == http.MethodDelete && len(segments) == 2 {
		res := deliver(ev.MakeEventActorDeactivate(ev.ActorRef{ActorType: actorType, ActorId: actorID})).ActorDeactivate()
		unitResponse(w, res)
		return true
	}
	if len(segments) < 4 || segments[2] != "method" {
		return false
	}
	switch {
	case segments[3] == "timer" && len(segments) >= 5:
		f := parseTimerFields(body)
		res := deliver(ev.MakeEventActorTimer(ev.ActorTimerEvent{
			ActorType: actorType, ActorId: actorID, Name: segments[4],
			DueTime: f.dueTime, Period: f.period, Data: f.data, Callback: f.callback,
		})).ActorTimer()
		unitResponse(w, res)
	case segments[3] == "remind" && len(segments) >= 5:
		f := parseTimerFields(body)
		res := deliver(ev.MakeEventActorReminder(ev.ActorReminderEvent{
			ActorType: actorType, ActorId: actorID, Name: segments[4],
			DueTime: f.dueTime, Period: f.period, Data: f.data,
		})).ActorReminder()
		unitResponse(w, res)
	default:
		res := deliver(ev.MakeEventActorInvoke(ev.ActorInvoke{
			ActorType: actorType, ActorId: actorID, Method: strings.Join(segments[3:], "/"), Data: body,
		})).ActorInvoke()
		if res.IsErr() {
			errorResponse(w, res.Err())
			return true
		}
		writeBytes(w, http.StatusOK, "application/json", res.Ok())
	}
	return true
}

// timerFields is the shared body of a timer/reminder fire:
// {dueTime, period, data, callback}.
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

func actorConfigResponse(c ev.ActorConfig) map[string]any {
	object := map[string]any{"entities": c.Entities}
	if c.Entities == nil {
		object["entities"] = []string{}
	}
	if c.ActorIdleTimeout.IsSome() {
		object["actorIdleTimeout"] = c.ActorIdleTimeout.Some()
	}
	if c.ActorScanInterval.IsSome() {
		object["actorScanInterval"] = c.ActorScanInterval.Some()
	}
	if c.DrainOngoingCallTimeout.IsSome() {
		object["drainOngoingCallTimeout"] = c.DrainOngoingCallTimeout.Some()
	}
	if c.DrainRebalancedActors.IsSome() {
		object["drainRebalancedActors"] = c.DrainRebalancedActors.Some()
	}
	if c.RemindersStoragePartitions.IsSome() {
		object["remindersStoragePartitions"] = c.RemindersStoragePartitions.Some()
	}
	if c.ReentrancyEnabled.IsSome() || c.ReentrancyMaxStackDepth.IsSome() {
		reentrancy := map[string]any{}
		if c.ReentrancyEnabled.IsSome() {
			reentrancy["enabled"] = c.ReentrancyEnabled.Some()
		}
		if c.ReentrancyMaxStackDepth.IsSome() {
			reentrancy["maxStackDepth"] = c.ReentrancyMaxStackDepth.Some()
		}
		object["reentrancy"] = reentrancy
	}
	return object
}

// --- service invocation ----------------------------------------------------

func deliverInvocation(w http.ResponseWriter, r *http.Request, body []byte) {
	headers := metadata{}
	for key, values := range r.Header {
		for _, v := range values {
			headers = append(headers, witTypes.Tuple2[string, string]{F0: strings.ToLower(key), F1: v})
		}
	}
	query := witTypes.None[string]()
	if r.URL.RawQuery != "" {
		query = witTypes.Some(r.URL.RawQuery)
	}
	res := deliver(ev.MakeEventInvoke(ev.InvokeRequest{
		Method:      strings.TrimPrefix(r.URL.Path, "/"),
		Verb:        verb(r.Method),
		Headers:     sortPairs(headers),
		Query:       query,
		ContentType: r.Header.Get("content-type"),
		Data:        body,
	})).Invoke()
	for _, h := range res.Headers {
		w.Header().Add(h.F0, h.F1)
	}
	w.WriteHeader(int(res.Status))
	_, _ = w.Write(res.Body)
}

func verb(method string) inv.HttpVerb {
	switch method {
	case http.MethodGet:
		return inv.HttpVerbGet
	case http.MethodPut:
		return inv.HttpVerbPut
	case http.MethodDelete:
		return inv.HttpVerbDelete
	case http.MethodPatch:
		return inv.HttpVerbPatch
	case http.MethodHead:
		return inv.HttpVerbHead
	case http.MethodOptions:
		return inv.HttpVerbOptions
	}
	// POST, and CONNECT/TRACE which have no WIT verb.
	return inv.HttpVerbPost
}

// --- helpers ---------------------------------------------------------------

func str(object map[string]any, key string) string {
	s, _ := object[key].(string)
	return s
}

func optStr(object map[string]any, key string) witTypes.Option[string] {
	if s, ok := object[key].(string); ok {
		return witTypes.Some(s)
	}
	return witTypes.None[string]()
}

// textOf renders a JSON value as text: a string as itself, anything else as
// JSON.
func textOf(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	b, _ := json.Marshal(v)
	return string(b)
}

// stringMap turns a JSON object into WIT metadata (non-strings as JSON).
func stringMap(v any) metadata {
	object, _ := v.(map[string]any)
	out := metadata{}
	for key, value := range object {
		out = append(out, witTypes.Tuple2[string, string]{F0: key, F1: textOf(value)})
	}
	return sortPairs(out)
}

func sortPairs(md metadata) metadata {
	for i := 1; i < len(md); i++ {
		for j := i; j > 0 && (md[j].F0 < md[j-1].F0 || (md[j].F0 == md[j-1].F0 && md[j].F1 < md[j-1].F1)); j-- {
			md[j], md[j-1] = md[j-1], md[j]
		}
	}
	return md
}

func metadataObject(md metadata) map[string]string {
	out := make(map[string]string, len(md))
	for _, kv := range md {
		out[kv.F0] = kv.F1
	}
	return out
}

// valueToJSON renders bytes as a JSON value: valid JSON as-is, anything else
// as a (UTF-8 lossy) JSON string — the wasi-http providers' convention.
func valueToJSON(b []byte) any {
	var v any
	if json.Unmarshal(b, &v) == nil {
		return json.RawMessage(b)
	}
	return strings.ToValidUTF8(string(b), "�")
}

func decode(segment string) string {
	if s, err := url.PathUnescape(segment); err == nil {
		return s
	}
	return segment
}

func text(w http.ResponseWriter, status int, message string) {
	writeBytes(w, status, "text/plain", []byte(message))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("failed to serialize app-channel response: %v", err))
	}
	writeBytes(w, status, "application/json", b)
}

func writeBytes(w http.ResponseWriter, status int, contentType string, body []byte) {
	w.Header().Set("content-type", contentType)
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func unitResponse(w http.ResponseWriter, res witTypes.Result[witTypes.Unit, appError]) {
	if res.IsErr() {
		errorResponse(w, res.Err())
		return
	}
	text(w, http.StatusOK, "")
}

// errorResponse reports an app-error (a message only) as a 500, like
// wasi-http-inbound.
func errorResponse(w http.ResponseWriter, e appError) {
	writeJSON(w, http.StatusInternalServerError, map[string]any{"errorCode": "ERR_APP", "message": e.Message})
}
