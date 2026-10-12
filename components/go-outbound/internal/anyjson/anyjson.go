// Package anyjson converts between JSON text and google.protobuf.Any — the
// Go counterpart of wasi-grpc-outbound's anyjson.rs.
//
// Several Dapr API messages carry JSON-ish payloads as Any (job data, actor
// reminder data, conversation parameters). The WIT contract keeps them as
// JSON strings; they are packed as google.protobuf.Value — the canonical
// proto encoding of a JSON document — and unpacked tolerantly.
package anyjson

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const typePrefix = "type.googleapis.com/"

// parse decodes one JSON document. json.Number keeps integers exact until
// the caller decides how to encode them.
func parse(text string) (any, error) {
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if dec.More() {
		return nil, fmt.Errorf("trailing data after JSON value")
	}
	return v, nil
}

// toValue converts a decoded JSON document to a proto Value. Numbers become
// doubles (proto Value has no integer kind), like the Rust provider.
func toValue(v any) *structpb.Value {
	switch x := v.(type) {
	case nil:
		return structpb.NewNullValue()
	case bool:
		return structpb.NewBoolValue(x)
	case json.Number:
		f, err := x.Float64()
		if err != nil {
			f = math.NaN()
		}
		return structpb.NewNumberValue(f)
	case float64:
		return structpb.NewNumberValue(x)
	case string:
		return structpb.NewStringValue(x)
	case []any:
		values := make([]*structpb.Value, 0, len(x))
		for _, item := range x {
			values = append(values, toValue(item))
		}
		return structpb.NewListValue(&structpb.ListValue{Values: values})
	case map[string]any:
		return structpb.NewStructValue(toStruct(x))
	}
	panic(fmt.Sprintf("unexpected JSON value %T", v))
}

func toStruct(m map[string]any) *structpb.Struct {
	fields := make(map[string]*structpb.Value, len(m))
	for k, item := range m {
		fields[k] = toValue(item)
	}
	return &structpb.Struct{Fields: fields}
}

// fromValue converts a proto Value back to a JSON-encodable Go value. A
// non-finite number has no JSON form and becomes null.
func fromValue(v *structpb.Value) any {
	switch k := v.GetKind().(type) {
	case *structpb.Value_BoolValue:
		return k.BoolValue
	case *structpb.Value_NumberValue:
		if math.IsNaN(k.NumberValue) || math.IsInf(k.NumberValue, 0) {
			return nil
		}
		return k.NumberValue
	case *structpb.Value_StringValue:
		return k.StringValue
	case *structpb.Value_ListValue:
		out := make([]any, 0, len(k.ListValue.GetValues()))
		for _, item := range k.ListValue.GetValues() {
			out = append(out, fromValue(item))
		}
		return out
	case *structpb.Value_StructValue:
		out := make(map[string]any, len(k.StructValue.GetFields()))
		for key, item := range k.StructValue.GetFields() {
			out[key] = fromValue(item)
		}
		return out
	}
	return nil // unset kind or NullValue
}

// encode renders compact JSON without Go's HTML escaping, so text payloads
// keep "<", ">" and "&" verbatim (as serde_json does). Object keys come out
// sorted.
func encode(v any) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		panic(fmt.Sprintf("encode JSON: %v", err)) // unreachable: fromValue yields only encodable values
	}
	return strings.TrimSuffix(buf.String(), "\n")
}

func pack(m proto.Message) *anypb.Any {
	b, err := proto.Marshal(m)
	if err != nil {
		panic(fmt.Sprintf("marshal %T: %v", m, err))
	}
	return &anypb.Any{TypeUrl: typePrefix + string(m.ProtoReflect().Descriptor().FullName()), Value: b}
}

// JSONToStruct parses a JSON object (text) into a google.protobuf.Struct —
// for conversation tool parameters and the response-format schema.
func JSONToStruct(text string) (*structpb.Struct, error) {
	v, err := parse(text)
	if err != nil {
		return nil, fmt.Errorf("expected a JSON object: %w", err)
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected a JSON object")
	}
	return toStruct(m), nil
}

// PackJSON packs a JSON document (text) as Any(google.protobuf.Value).
func PackJSON(text string) (*anypb.Any, error) {
	v, err := parse(text)
	if err != nil {
		return nil, fmt.Errorf("payload is not valid JSON: %w", err)
	}
	return pack(toValue(v)), nil
}

// UnpackJSON turns an Any back into JSON text: google.protobuf.Value
// payloads are decoded properly, BytesValue/StringValue wrappers (daprd
// packs e.g. actor reminder data as BytesValue) are unwrapped to their
// payload, anything else is returned as its raw bytes (lossy UTF-8).
func UnpackJSON(a *anypb.Any) string {
	url := a.GetTypeUrl()
	if strings.HasSuffix(url, "google.protobuf.Value") {
		var v structpb.Value
		if proto.Unmarshal(a.GetValue(), &v) == nil {
			return encode(fromValue(&v))
		}
	}
	if strings.HasSuffix(url, "google.protobuf.BytesValue") {
		var w wrapperspb.BytesValue
		if proto.Unmarshal(a.GetValue(), &w) == nil {
			return lossy(w.GetValue())
		}
	}
	if strings.HasSuffix(url, "google.protobuf.StringValue") {
		var w wrapperspb.StringValue
		if proto.Unmarshal(a.GetValue(), &w) == nil {
			return w.GetValue()
		}
	}
	return lossy(a.GetValue())
}

func lossy(b []byte) string {
	return strings.ToValidUTF8(string(b), string(utf8.RuneError))
}

// PackProtojsonWrapper packs a protojson-style wrapper object —
// {"@type": "type.googleapis.com/google.protobuf.Int64Value", "value": "100"}
// — as the corresponding Any. This is the encoding the Dapr docs use for
// typed conversation parameters. ok is false when object carries no string
// @type (the caller falls back to PackJSON); err is set for @types this
// provider cannot encode. Decode object with json.Decoder.UseNumber so
// 64-bit integers stay exact.
func PackProtojsonWrapper(object map[string]any) (a *anypb.Any, ok bool, err error) {
	typeURL, isString := object["@type"].(string)
	if !isString {
		return nil, false, nil
	}
	value := object["value"]
	suffix := typeURL[strings.LastIndex(typeURL, "/")+1:]
	invalid := fmt.Errorf("@type %s: invalid value %v", suffix, value)

	// protojson allows numbers (64-bit ints especially) as JSON strings.
	text := func() string {
		switch x := value.(type) {
		case string:
			return x
		case json.Number:
			return x.String()
		case float64:
			return strconv.FormatFloat(x, 'g', -1, 64)
		}
		return fmt.Sprint(value)
	}()

	var m proto.Message
	switch suffix {
	case "google.protobuf.DoubleValue":
		f, perr := strconv.ParseFloat(text, 64)
		if perr != nil {
			return nil, true, invalid
		}
		m = wrapperspb.Double(f)
	case "google.protobuf.FloatValue":
		f, perr := strconv.ParseFloat(text, 32)
		if perr != nil {
			return nil, true, invalid
		}
		m = wrapperspb.Float(float32(f))
	case "google.protobuf.Int64Value":
		i, perr := strconv.ParseInt(text, 10, 64)
		if perr != nil {
			return nil, true, invalid
		}
		m = wrapperspb.Int64(i)
	case "google.protobuf.UInt64Value":
		u, perr := strconv.ParseUint(text, 10, 64)
		if perr != nil {
			return nil, true, invalid
		}
		m = wrapperspb.UInt64(u)
	case "google.protobuf.Int32Value":
		i, perr := strconv.ParseInt(text, 10, 32)
		if perr != nil {
			return nil, true, invalid
		}
		m = wrapperspb.Int32(int32(i))
	case "google.protobuf.UInt32Value":
		u, perr := strconv.ParseUint(text, 10, 32)
		if perr != nil {
			return nil, true, invalid
		}
		m = wrapperspb.UInt32(uint32(u))
	case "google.protobuf.BoolValue":
		b, isBool := value.(bool)
		if !isBool {
			return nil, true, fmt.Errorf("@type %s: invalid value", suffix)
		}
		m = wrapperspb.Bool(b)
	case "google.protobuf.StringValue":
		s, isStr := value.(string)
		if !isStr {
			return nil, true, fmt.Errorf("@type %s: invalid value", suffix)
		}
		m = wrapperspb.String(s)
	case "google.protobuf.Value":
		m = toValue(value)
	default:
		return nil, true, fmt.Errorf("@type %s is not supported here", suffix)
	}
	b, merr := proto.Marshal(m)
	if merr != nil {
		return nil, true, merr
	}
	// Keep the caller's type URL verbatim, like the Rust provider.
	return &anypb.Any{TypeUrl: typeURL, Value: b}, true, nil
}
