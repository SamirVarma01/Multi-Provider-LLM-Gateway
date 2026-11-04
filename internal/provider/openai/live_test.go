//go:build live

// Live tests against a real OpenAI-format API. They cost money and are never
// part of the default build; run them with:
//
//	OPENAI_API_KEY=... OPENAI_LIVE_MODEL=<model> go test -tags live -run Live -count=1 ./internal/provider/openai
//
// OPENAI_BASE_URL defaults to https://api.openai.com/v1. The output limit is
// sent as max_completion_tokens unless OPENAI_LIVE_MAX_TOKENS_FIELD=max_tokens.
package openai

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/SamirVarma01/llm-gateway/internal/provider"
)

func liveProvider(t *testing.T) (*Provider, provider.Target) {
	t.Helper()
	key, model := os.Getenv("OPENAI_API_KEY"), os.Getenv("OPENAI_LIVE_MODEL")
	if key == "" || model == "" {
		t.Skip("OPENAI_API_KEY and OPENAI_LIVE_MODEL must both be set")
	}
	base := os.Getenv("OPENAI_BASE_URL")
	if base == "" {
		base = "https://api.openai.com/v1"
	}
	var opts []Option
	if os.Getenv("OPENAI_LIVE_MAX_TOKENS_FIELD") != "max_tokens" {
		opts = append(opts, UseMaxCompletionTokens())
	}
	return New("openai-live", base, key, &http.Client{}, opts...), provider.Target{Provider: "openai-live", UpstreamModel: model}
}

func liveCollect(t *testing.T, p *Provider, tgt provider.Target, req *provider.ChatRequest) provider.Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	s, err := p.Stream(ctx, tgt, req)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer s.Close()
	resp, err := provider.Collect(s)
	if err != nil {
		t.Fatalf("Collect: %v (partial %+v)", err, resp)
	}
	if !resp.Reported.HasPrompt || !resp.Reported.HasCompletion || resp.Reported.PromptTokens == 0 {
		t.Errorf("usage not reported: %+v", resp.Reported)
	}
	t.Logf("finish=%s usage=%+v content=%q tool_calls=%+v", resp.FinishReason, resp.Reported, resp.Content, resp.ToolCalls)
	return resp
}

func text(s string) []provider.ContentPart {
	return []provider.ContentPart{{Type: provider.PartText, Text: s}}
}

// Room for reasoning models to think before answering.
var liveMaxTokens = 2048

func TestLiveText(t *testing.T) {
	p, tgt := liveProvider(t)
	resp := liveCollect(t, p, tgt, &provider.ChatRequest{
		Messages: []provider.Message{
			{Role: provider.RoleSystem, Content: text("Answer in one short sentence.")},
			{Role: provider.RoleUser, Content: text("What is the capital of France?")},
		},
		MaxTokens: &liveMaxTokens,
	})
	if resp.FinishReason != provider.FinishStop || resp.Content == "" {
		t.Errorf("finish %q, content %q", resp.FinishReason, resp.Content)
	}
}

func TestLiveToolRoundTrip(t *testing.T) {
	p, tgt := liveProvider(t)
	req := &provider.ChatRequest{
		Messages: []provider.Message{
			{Role: provider.RoleUser, Content: text("Use the get_weather tool to check the weather in Paris.")},
		},
		Tools: []provider.Tool{{
			Name:        "get_weather",
			Description: "Get the current weather for a city.",
			Parameters:  json.RawMessage(`{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}`),
		}},
		ToolChoice: &provider.ToolChoice{Mode: provider.ToolChoiceRequired},
		MaxTokens:  &liveMaxTokens,
	}
	first := liveCollect(t, p, tgt, req)
	if len(first.ToolCalls) == 0 {
		t.Fatalf("no tool call: %+v", first)
	}
	call := first.ToolCalls[0]
	var args map[string]any
	if call.ID == "" || call.Name != "get_weather" || json.Unmarshal([]byte(call.Arguments), &args) != nil {
		t.Fatalf("tool call = %+v", call)
	}
	req.ToolChoice = &provider.ToolChoice{Mode: provider.ToolChoiceAuto}
	req.Messages = append(req.Messages,
		provider.Message{Role: provider.RoleAssistant, ToolCalls: first.ToolCalls},
		provider.Message{Role: provider.RoleTool, ToolCallID: call.ID, Content: text("18C and sunny")},
	)
	second := liveCollect(t, p, tgt, req)
	if second.FinishReason != provider.FinishStop || second.Content == "" {
		t.Errorf("finish %q, content %q", second.FinishReason, second.Content)
	}
}
