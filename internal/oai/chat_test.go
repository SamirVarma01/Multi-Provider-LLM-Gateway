package oai

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestContentJSON(t *testing.T) {
	tests := []struct {
		in       string
		wantText string
		isParts  bool
		out      string
	}{
		{`"hello"`, "hello", false, `"hello"`},
		{`[{"type":"text","text":"a"},{"type":"text","text":"b"}]`, "ab", true,
			`[{"type":"text","text":"a"},{"type":"text","text":"b"}]`},
		{`[]`, "", true, `[]`},
		{`null`, "", false, `""`},
	}
	for _, tt := range tests {
		var c Content
		if err := json.Unmarshal([]byte(tt.in), &c); err != nil {
			t.Fatalf("Unmarshal(%s): %v", tt.in, err)
		}
		if c.String() != tt.wantText || (c.Parts != nil) != tt.isParts {
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
