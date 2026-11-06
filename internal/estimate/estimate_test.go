package estimate

import (
	"encoding/json"
	"testing"

	"github.com/SamirVarma01/llm-gateway/internal/provider"
)

func TestText(t *testing.T) {
	e := Estimator{}
	for s, want := range map[string]int{
		"":         0,
		"a":        1,
		"abcd":     1,
		"abcde":    2,
		"日本語":      3, // 9 bytes
		"hello wo": 2,
	} {
		if got := e.Text(s); got != want {
			t.Errorf("Text(%q) = %d, want %d", s, got, want)
		}
	}
}

func TestInput(t *testing.T) {
	e := Estimator{ImageTokens: 100}
	req := &provider.ChatRequest{
		Messages: []provider.Message{
			{Role: provider.RoleSystem, Content: []provider.ContentPart{{Type: provider.PartText, Text: "12345678"}}}, // 4 + 2
			{Role: provider.RoleUser, Content: []provider.ContentPart{
				{Type: provider.PartText, Text: "1234"},                           // 1
				{Type: provider.PartImage, ImageURL: "data:image/png;base64,..."}, // 100, not the URL length
			}}, // 4 + 101
			{Role: provider.RoleAssistant, ToolCalls: []provider.ToolCall{{Name: "abcd", Arguments: "{}"}}}, // 4 + 1 + 1
		},
		Tools: []provider.Tool{{Name: "abcd", Description: "abcdefgh", Parameters: json.RawMessage(`{"a":1}`)}}, // 1 + 2 + 2
	}
	if got, want := e.Input(req), 3+6+105+6+5; got != want {
		t.Errorf("Input = %d, want %d", got, want)
	}
}

func TestResolve(t *testing.T) {
	e := Estimator{}
	req := &provider.ChatRequest{Messages: []provider.Message{
		{Role: provider.RoleUser, Content: []provider.ContentPart{{Type: provider.PartText, Text: "12345678"}}},
	}}
	inputEstimate := 3 + 4 + 2

	tests := []struct {
		name string
		resp provider.Response
		want provider.Usage
	}{
		{
			name: "fully reported",
			resp: provider.Response{Content: "ignored", Reported: provider.Reported{
				PromptTokens: 50, CompletionTokens: 7, HasPrompt: true, HasCompletion: true}},
			want: provider.Usage{PromptTokens: 50, CompletionTokens: 7},
		},
		{
			name: "prompt reported, stream cut before output count (Anthropic)",
			resp: provider.Response{Content: "12345678", Reported: provider.Reported{PromptTokens: 50, HasPrompt: true}},
			want: provider.Usage{PromptTokens: 50, CompletionTokens: 2, Estimated: true},
		},
		{
			name: "nothing reported",
			resp: provider.Response{Content: "1234", ToolCalls: []provider.ToolCall{{Name: "f", Arguments: "{}"}}},
			want: provider.Usage{PromptTokens: inputEstimate, CompletionTokens: 1 + 1 + 1, Estimated: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := e.Resolve(req, tt.resp); got != tt.want {
				t.Errorf("Resolve = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestReserve(t *testing.T) {
	req := &provider.ChatRequest{Messages: []provider.Message{
		{Role: provider.RoleUser, Content: []provider.ContentPart{{Type: provider.PartText, Text: "12345678"}}},
	}}
	input := Estimator{}.Input(req) // 3 + 4 + 2 = 9
	if got := (Estimator{SafetyFactor: 1.25}).Reserve(req); got != 12 {
		t.Errorf("Reserve = %d, want ceil(9 * 1.25) = 12", got)
	}
	if got := (Estimator{}).Reserve(req); got != input {
		t.Errorf("Reserve with no factor = %d, want Input = %d", got, input)
	}
}
