package oai

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestContentJSON(t *testing.T) {
	tests := []struct {
		in       string
		wantText string
		isParts  bool
		isNull   bool
		out      string
	}{
		{`"hello"`, "hello", false, false, `"hello"`},
		{`[{"type":"text","text":"a"},{"type":"text","text":"b"}]`, "ab", true, false,
			`[{"type":"text","text":"a"},{"type":"text","text":"b"}]`},
		{`[{"type":"image_url","image_url":{"url":"https://x/y.png","detail":"low"}}]`, "", true, false,
			`[{"type":"image_url","image_url":{"url":"https://x/y.png","detail":"low"}}]`},
		{`[]`, "", true, false, `[]`},
		{`null`, "", false, true, `null`},
	}
	for _, tt := range tests {
		var c Content
		if err := json.Unmarshal([]byte(tt.in), &c); err != nil {
			t.Fatalf("Unmarshal(%s): %v", tt.in, err)
		}
		if c.String() != tt.wantText || (c.Parts != nil) != tt.isParts || c.Null != tt.isNull {
			t.Errorf("Unmarshal(%s) = %+v", tt.in, c)
		}
		out, err := json.Marshal(c)
		if err != nil || string(out) != tt.out {
			t.Errorf("Marshal round trip of %s = %s, %v; want %s", tt.in, out, err, tt.out)
		}
	}
	var c Content
	if err := json.Unmarshal([]byte(`42`), &c); err == nil {
		t.Error("numeric content must be rejected")
	}
}

func TestStringOrList(t *testing.T) {
	for in, want := range map[string]StringOrList{
		`"x"`:       {"x"},
		`["x","y"]`: {"x", "y"},
		`null`:      nil,
	} {
		var s StringOrList
		if err := json.Unmarshal([]byte(in), &s); err != nil || !reflect.DeepEqual(s, want) {
			t.Errorf("Unmarshal(%s) = %v, %v; want %v", in, s, err, want)
		}
	}
}

func TestOutputLimit(t *testing.T) {
	a, b := 10, 20
	r := ChatCompletionRequest{MaxTokens: &a}
	if *r.OutputLimit() != 10 {
		t.Error("max_tokens not used")
	}
	r.MaxCompletionTokens = &b
	if *r.OutputLimit() != 20 {
		t.Error("max_completion_tokens must take precedence")
	}
}

func TestRequestKeepsUnknownFields(t *testing.T) {
	in := `{"model":"m","messages":[],"seed":7,"response_format":{"type":"json_object"},"temperature":0.5}`
	var r ChatCompletionRequest
	if err := json.Unmarshal([]byte(in), &r); err != nil {
		t.Fatal(err)
	}
	if r.Model != "m" || r.Temperature == nil || *r.Temperature != 0.5 {
		t.Errorf("known fields = %+v", r)
	}
	if len(r.Extra) != 2 || string(r.Extra["seed"]) != "7" || string(r.Extra["response_format"]) != `{"type":"json_object"}` {
		t.Errorf("Extra = %v", r.Extra)
	}

	// Extra is written back, and a known field wins over an Extra entry with its name.
	r.Model = "upstream"
	r.Extra["model"] = json.RawMessage(`"smuggled"`)
	out, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var back map[string]json.RawMessage
	_ = json.Unmarshal(out, &back)
	if string(back["model"]) != `"upstream"` || string(back["seed"]) != "7" {
		t.Errorf("marshaled = %s", out)
	}

	var plain ChatCompletionRequest
	_ = json.Unmarshal([]byte(`{"model":"m","messages":[]}`), &plain)
	if plain.Extra != nil {
		t.Errorf("Extra = %v, want nil when there are no unknown fields", plain.Extra)
	}
}

func TestKnownRequestFieldsMatchStruct(t *testing.T) {
	for _, name := range []string{"model", "messages", "tools", "tool_choice", "parallel_tool_calls", "stream_options"} {
		found := false
		for _, k := range knownRequestFields {
			found = found || k == name
		}
		if !found {
			t.Errorf("knownRequestFields is missing %q", name)
		}
	}
	for _, k := range knownRequestFields {
		if k == "-" || k == "Extra" {
			t.Errorf("knownRequestFields contains %q", k)
		}
	}
}

func TestToolChoice(t *testing.T) {
	tests := []struct {
		in   string
		want ToolChoice
	}{
		{`"auto"`, ToolChoice{Mode: ToolChoiceAuto}},
		{`"none"`, ToolChoice{Mode: ToolChoiceNone}},
		{`"required"`, ToolChoice{Mode: ToolChoiceRequired}},
		{`{"type":"function","function":{"name":"get_weather"}}`, ToolChoice{Mode: ToolChoiceFunction, Function: "get_weather"}},
	}
	for _, tt := range tests {
		var tc ToolChoice
		if err := json.Unmarshal([]byte(tt.in), &tc); err != nil || tc != tt.want {
			t.Errorf("Unmarshal(%s) = %+v, %v", tt.in, tc, err)
		}
		out, _ := json.Marshal(tc)
		if !jsonEqual(t, string(out), tt.in) {
			t.Errorf("Marshal(%+v) = %s, want %s", tc, out, tt.in)
		}
	}
	for _, bad := range []string{`"sometimes"`, `{"type":"function","function":{}}`, `{"type":"tool"}`} {
		var tc ToolChoice
		if err := json.Unmarshal([]byte(bad), &tc); err == nil {
			t.Errorf("Unmarshal(%s) should fail", bad)
		}
	}
}

func TestToolMessagesRoundTrip(t *testing.T) {
	in := `{"model":"m","messages":[
		{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"f","arguments":"{\"x\":1}"}}]},
		{"role":"tool","tool_call_id":"call_1","content":"42"}
	],"tools":[{"type":"function","function":{"name":"f","parameters":{"type":"object"}}}]}`
	var r ChatCompletionRequest
	if err := json.Unmarshal([]byte(in), &r); err != nil {
		t.Fatal(err)
	}
	a := r.Messages[0]
	if !a.Content.Null || len(a.ToolCalls) != 1 || a.ToolCalls[0].Function.Arguments != `{"x":1}` {
		t.Errorf("assistant = %+v", a)
	}
	if r.Messages[1].ToolCallID != "call_1" || r.Messages[1].Content.Text != "42" {
		t.Errorf("tool = %+v", r.Messages[1])
	}
	if r.Tools[0].Function.Name != "f" || string(r.Tools[0].Function.Parameters) != `{"type":"object"}` {
		t.Errorf("tools = %+v", r.Tools)
	}
	out, _ := json.Marshal(r)
	if !strings.Contains(string(out), `"content":null`) {
		t.Errorf("null content not preserved: %s", out)
	}
}

func jsonEqual(t *testing.T, a, b string) bool {
	t.Helper()
	var x, y any
	if err := json.Unmarshal([]byte(a), &x); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(b), &y); err != nil {
		t.Fatal(err)
	}
	return reflect.DeepEqual(x, y)
}
