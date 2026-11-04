//go:build live

// Live tests against the real Messages API. They cost money and are never
// part of the default build; run them with:
//
//	ANTHROPIC_API_KEY=... go test -tags live -run Live -count=1 ./internal/provider/anthropic
//
// ANTHROPIC_LIVE_MODEL (default claude-opus-5-5) and ANTHROPIC_BASE_URL
// (default https://api.anthropic.com/v1) are optional.
package anthropic

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
	key := os.Getenv("ANTHROPIC_API_KEY")
	if key == "" {
		t.Skip("ANTHROPIC_API_KEY is not set")
	}
	base := os.Getenv("ANTHROPIC_BASE_URL")
	if base == "" {
		base = "https://api.anthropic.com/v1"
	}
	model := os.Getenv("ANTHROPIC_LIVE_MODEL")
	if model == "" {
		model = "claude-opus-5-5"
	}
	return New("anthropic-live", base, key, &http.Client{}), provider.Target{Provider: "anthropic-live", UpstreamModel: model}
}

func liveCollect(t *testing.T, p *Provider, tgt provider.Target, req *provider.ChatRequest) provider.Response {
	t.Helper()
	if err := p.Supports(req); err != nil {
		t.Fatalf("Supports: %v", err)
	}
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
		t.Errorf("usage not fully reported: %+v", resp.Reported)
	}
	t.Logf("finish=%s usage=%+v content=%q tool_calls=%+v", resp.FinishReason, resp.Reported, resp.Content, resp.ToolCalls)
	return resp
}

// Room for adaptive thinking, which current models always run, before the answer.
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

var weatherTool = provider.Tool{
	Name:        "get_weather",
	Description: "Get the current weather for a city.",
	Parameters:  json.RawMessage(`{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}`),
}

// TestLiveToolRoundTrip makes a tool call, then sends the tool result back
// in OpenAI shape. The history carries no thinking blocks (OpenAI-format
// clients cannot return them), which the API must accept.
func TestLiveToolRoundTrip(t *testing.T) {
	p, tgt := liveProvider(t)
	req := &provider.ChatRequest{
		Messages: []provider.Message{
			{Role: provider.RoleUser, Content: text("Use the get_weather tool to check the weather in Paris.")},
		},
		Tools:      []provider.Tool{weatherTool},
		ToolChoice: &provider.ToolChoice{Mode: provider.ToolChoiceAuto},
		MaxTokens:  &liveMaxTokens,
	}
	first := liveCollect(t, p, tgt, req)
	if first.FinishReason != provider.FinishToolCalls || len(first.ToolCalls) == 0 {
		t.Fatalf("no tool call: %+v", first)
	}
	call := first.ToolCalls[0]
	var args map[string]any
	if call.ID == "" || call.Name != "get_weather" || json.Unmarshal([]byte(call.Arguments), &args) != nil {
		t.Fatalf("tool call = %+v", call)
	}

	req.Messages = append(req.Messages,
		provider.Message{Role: provider.RoleAssistant, Content: text(first.Content), ToolCalls: first.ToolCalls},
		provider.Message{Role: provider.RoleTool, ToolCallID: call.ID, Content: text("18C and sunny")},
	)
	second := liveCollect(t, p, tgt, req)
	if second.FinishReason != provider.FinishStop || second.Content == "" {
		t.Errorf("finish %q, content %q", second.FinishReason, second.Content)
	}
}
