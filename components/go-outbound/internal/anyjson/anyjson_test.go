package anyjson

import (
	"encoding/json"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestPackUnpackRoundtrip(t *testing.T) {
	for in, want := range map[string]string{
		`{"b":[1,true,null],"a":"x<y&z"}`: `{"a":"x<y&z","b":[1,true,null]}`,
		` 1.5 `:                           `1.5`,
		`"s"`:                             `"s"`,
		`null`:                            `null`,
	} {
		a, err := PackJSON(in)
		if err != nil {
			t.Fatalf("PackJSON(%q): %v", in, err)
		}
		if a.GetTypeUrl() != "type.googleapis.com/google.protobuf.Value" {
			t.Fatalf("type url %q", a.GetTypeUrl())
		}
		if got := UnpackJSON(a); got != want {
			t.Errorf("roundtrip %q: got %q, want %q", in, got, want)
		}
	}
}

func TestPackJSONRejectsInvalid(t *testing.T) {
	for _, in := range []string{`{`, `1 2`, ``} {
		if _, err := PackJSON(in); err == nil {
			t.Errorf("PackJSON(%q): expected error", in)
		}
	}
}

func TestUnpackWrappersAndRaw(t *testing.T) {
	bytesAny, _ := anypb.New(wrapperspb.Bytes([]byte(`{"k":1}`)))
	if got := UnpackJSON(bytesAny); got != `{"k":1}` {
		t.Errorf("BytesValue: %q", got)
	}
	strAny, _ := anypb.New(wrapperspb.String("hi"))
	if got := UnpackJSON(strAny); got != "hi" {
		t.Errorf("StringValue: %q", got)
	}
	raw := &anypb.Any{TypeUrl: "type.googleapis.com/bytes", Value: []byte("raw\xff")}
	if got := UnpackJSON(raw); got != "raw�" {
		t.Errorf("raw: %q", got)
	}
}

func TestJSONToStruct(t *testing.T) {
	s, err := JSONToStruct(`{"type":"object"}`)
	if err != nil || s.GetFields()["type"].GetStringValue() != "object" {
		t.Fatalf("JSONToStruct: %v %v", s, err)
	}
	if _, err := JSONToStruct(`[1]`); err == nil {
		t.Error("JSONToStruct([1]): expected error")
	}
}

func TestPackProtojsonWrapper(t *testing.T) {
	decode := func(text string) map[string]any {
		dec := json.NewDecoder(strings.NewReader(text))
		dec.UseNumber()
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	a, ok, err := PackProtojsonWrapper(decode(`{"@type":"type.googleapis.com/google.protobuf.Int64Value","value":"9007199254740993"}`))
	if !ok || err != nil {
		t.Fatalf("Int64Value: ok=%v err=%v", ok, err)
	}
	var i wrapperspb.Int64Value
	if err := proto.Unmarshal(a.GetValue(), &i); err != nil || i.GetValue() != 9007199254740993 {
		t.Errorf("Int64Value: %v %v", i.GetValue(), err)
	}
	a, _, err = PackProtojsonWrapper(decode(`{"@type":"type.googleapis.com/google.protobuf.Value","value":{"x":2}}`))
	var v structpb.Value
	if err != nil || proto.Unmarshal(a.GetValue(), &v) != nil || v.GetStructValue().GetFields()["x"].GetNumberValue() != 2 {
		t.Errorf("Value: %v %v", &v, err)
	}
	if _, ok, _ := PackProtojsonWrapper(decode(`{"value":1}`)); ok {
		t.Error("no @type: expected ok=false")
	}
	if _, ok, err := PackProtojsonWrapper(decode(`{"@type":"x/foo.Bar"}`)); !ok || err == nil {
		t.Error("unsupported @type: expected error")
	}
	if _, _, err := PackProtojsonWrapper(decode(`{"@type":"x/google.protobuf.Int32Value","value":"abc"}`)); err == nil {
		t.Error("invalid Int32Value: expected error")
	}
}
