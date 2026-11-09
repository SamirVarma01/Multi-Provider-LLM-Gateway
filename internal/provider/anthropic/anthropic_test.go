package anthropic

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SamirVarma01/llm-gateway/internal/mock"
	"github.com/SamirVarma01/llm-gateway/internal/obs"
	"github.com/SamirVarma01/llm-gateway/internal/provider"
	"github.com/SamirVarma01/llm-gateway/internal/testutil"
)

func TestMain(m *testing.M) { testutil.Main(m) }

var target = provider.Target{Provider: "claude-a", UpstreamModel: "claude-test", MaxOutput: 4096}

func text(s string) []provider.ContentPart {
	return []provider.ContentPart{{Type: provider.PartText, Text: s}}
}

func request() *provider.ChatRequest {
	limit := 64
	return &provider.ChatRequest{
		Model: "alias",
		Messages: []provider.Message{
			{Role: provider.RoleSystem, Content: text("be brief")},
			{Role: provider.RoleUser, Content: text("say something")},
		},
		MaxTokens: &limit,
	}
}

func newClient(t *testing.T) *http.Client {
	t.Helper()
	tr := &http.Transport{}
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr}
}

// setup starts a mock with b and returns an adapter for provider claude-a.
func setup(t *testing.T, b mock.Behavior, wrap func(http.Handler) http.Handler) (*mock.Server, *Provider) {
	t.Helper()
	m := mock.New(mock.Options{Defaults: b, Seed: 11})
	h := m.Handler()
	if wrap != nil {
		h = wrap(h)
	}
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return m, New("claude-a", ts.URL+"/p/claude-a/v1", "sk-ant-test", newClient(t))
}

func ctxWithIDs(reqID string, attempt int) context.Context {
	return provider.WithAttempt(obs.WithRequestID(context.Background(), reqID), attempt)
}

func TestStreamAgainstMock(t *testing.T) {
	var hdr http.Header
	var body map[string]any
	m, p := setup(t, mock.Behavior{OutputTokens: 9}, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hdr = r.Header.Clone()
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &body)
			r.Body = io.NopCloser(strings.NewReader(string(raw)))
			next.ServeHTTP(w, r)
		})
	})

	s, err := p.Stream(ctxWithIDs("req-1", 3), target, request())
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer s.Close()
	resp, err := provider.Collect(s)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}

	if hdr.Get("anthropic-version") != APIVersion || hdr.Get("x-api-key") != "sk-ant-test" ||
		hdr.Get(obs.RequestIDHeader) != "req-1" || hdr.Get(provider.AttemptHeader) != "3" {
		t.Errorf("headers = %v", hdr)
	}
	if body["model"] != "claude-test" || body["system"] != "be brief" || body["max_tokens"] != float64(64) || body["stream"] != true {
		t.Errorf("body = %v", body)
	}

	rec := m.Records("req-1")[0]
	// The final output count must come from message_delta, not message_start's provisional one.
	want := provider.Reported{PromptTokens: rec.PromptTokens, CompletionTokens: 9, HasPrompt: true, HasCompletion: true}
	if resp.Reported != want || rec.CompletionTokens != 9 {
		t.Errorf("usage = %+v, mock recorded %+v", resp.Reported, rec)
	}
	if len(strings.Fields(resp.Content)) != 9 || resp.FinishReason != provider.FinishStop {
		t.Errorf("response = %+v", resp)
	}
}

func TestStatusErrors(t *testing.T) {
	tests := []struct {
		status    int
		kind      provider.ErrKind
		retryable bool
		errType   string
	}{
		{429, provider.KindRateLimited, true, "rate_limit_error"},
		{529, provider.KindServer, true, "overloaded_error"},
		{500, provider.KindServer, true, "api_error"},
		{401, provider.KindConfig, true, "authentication_error"},
		{404, provider.KindConfig, true, "not_found_error"},
		{400, provider.KindBadRequest, false, "invalid_request_error"},
	}
	for _, tt := range tests {
		t.Run(tt.errType, func(t *testing.T) {
			_, p := setup(t, mock.Behavior{OutputTokens: 1, ErrorRate: 1, ErrorStatus: tt.status}, nil)
			_, err := p.Stream(context.Background(), target, request())
			var ue *provider.UpstreamError
			if !errors.As(err, &ue) {
				t.Fatalf("err = %v", err)
			}
			if ue.Status != tt.status || ue.Kind != tt.kind || ue.Retryable != tt.retryable ||
				!strings.HasPrefix(ue.Msg, tt.errType+": ") {
				t.Errorf("err = %+v", ue)
			}
		})
	}
}

func TestMidStreamFailures(t *testing.T) {
	for _, mode := range []string{mock.FailAbort, mock.FailErrorEvent} {
		t.Run(mode, func(t *testing.T) {
			_, p := setup(t, mock.Behavior{OutputTokens: 10, FailAfterTokens: 3, FailMode: mode}, nil)
			s, err := p.Stream(context.Background(), target, request())
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			resp, err := provider.Collect(s)
			var ue *provider.UpstreamError
			if !errors.As(err, &ue) || !ue.Retryable {
				t.Fatalf("err = %v", err)
			}
			if mode == mock.FailErrorEvent && !strings.Contains(ue.Msg, "overloaded_error") {
				t.Errorf("error event message = %q", ue.Msg)
			}
			if n := len(strings.Fields(resp.Content)); n != 3 {
				t.Errorf("partial content has %d tokens, want 3", n)
			}
			// Input was reported up front; the output count never arrived.
			if !resp.Reported.HasPrompt || resp.Reported.HasCompletion {
				t.Errorf("usage = %+v", resp.Reported)
			}
		})
	}
}

func TestOmittedUsage(t *testing.T) {
	_, p := setup(t, mock.Behavior{OutputTokens: 2, OmitUsage: true}, nil)
	s, err := p.Stream(context.Background(), target, request())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if resp, err := provider.Collect(s); err != nil || resp.Reported != (provider.Reported{}) {
		t.Errorf("resp = %+v, err = %v", resp, err)
	}
}

func TestCallerCancel(t *testing.T) {
	m, p := setup(t, mock.Behavior{OutputTokens: 1000, TokensPerSec: 200}, nil)
	ctx, cancel := context.WithCancel(ctxWithIDs("cancel", 1))
	s, err := p.Stream(ctx, target, request())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for range 3 {
		if _, err := s.Recv(); err != nil {
			t.Fatal(err)
		}
	}
	cancel()
	var recvErr error
	for recvErr == nil {
		_, recvErr = s.Recv()
	}
	if !errors.Is(recvErr, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", recvErr)
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(m.Records("cancel")) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if recs := m.Records("cancel"); len(recs) != 1 || recs[0].Outcome != mock.OutcomeClientCancelled {
		t.Errorf("mock did not see the cancellation: %+v", recs)
	}
}

// A Messages API stream with a thinking block, text, and two tool_use blocks
// (the second takes no input), in the shape the API sends them.
const toolStream = `event: message_start
data: {"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"claude-test","content":[],"stop_reason":null,"usage":{"input_tokens":20,"cache_creation_input_tokens":5,"cache_read_input_tokens":100,"output_tokens":1}}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":""}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig"}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: ping
data: {"type":"ping"}

event: content_block_start
data: {"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}

event: content_block_delta
data: {"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"Checking."}}

event: content_block_stop
data: {"type":"content_block_stop","index":1}

event: content_block_start
data: {"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"toolu_a","name":"get_weather","input":{}}}

event: content_block_delta
data: {"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":""}}

event: content_block_delta
data: {"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{\"city\": \"Par"}}

event: content_block_delta
data: {"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"is\"}"}}

event: content_block_stop
data: {"type":"content_block_stop","index":2}

event: content_block_start
data: {"type":"content_block_start","index":3,"content_block":{"type":"tool_use","id":"toolu_b","name":"get_time","input":{}}}

event: content_block_stop
data: {"type":"content_block_stop","index":3}

event: some_future_event
data: {"type":"some_future_event"}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"output_tokens":57}}

event: message_stop
data: {"type":"message_stop"}

`

func replay(t *testing.T, body string) *Provider {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(ts.Close)
	return New("replay", ts.URL+"/v1", "", newClient(t))
}

func TestStreamTranslation(t *testing.T) {
	s, err := replay(t, toolStream).Stream(context.Background(), target, request())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var chunks []provider.Chunk
	for {
		c, err := s.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		chunks = append(chunks, *c)
	}
	want := []provider.Chunk{
		{Role: "assistant"},
		{Content: "Checking."},
		{ToolCalls: []provider.ToolCallDelta{{Index: 0, ID: "toolu_a", Name: "get_weather"}}},
		{ToolCalls: []provider.ToolCallDelta{{Index: 0, ArgumentsFragment: `{"city": "Par`}}},
		{ToolCalls: []provider.ToolCallDelta{{Index: 0, ArgumentsFragment: `is"}`}}},
		{ToolCalls: []provider.ToolCallDelta{{Index: 1, ID: "toolu_b", Name: "get_time"}}},
		{ToolCalls: []provider.ToolCallDelta{{Index: 1, ArgumentsFragment: `{}`}}},
		{FinishReason: provider.FinishToolCalls},
	}
	if !reflect.DeepEqual(chunks, want) {
		t.Errorf("chunks =\n%+v\nwant\n%+v", chunks, want)
	}
	// Input is uncached + cache writes + cache reads.
	if got := s.Usage(); got != (provider.Reported{PromptTokens: 125, CompletionTokens: 57, HasPrompt: true, HasCompletion: true}) {
		t.Errorf("usage = %+v", got)
	}
}

func TestStopReasons(t *testing.T) {
	for in, want := range map[string]string{
		"end_turn": "stop", "stop_sequence": "stop", "pause_turn": "stop", "max_tokens": "length",
		"model_context_window_exceeded": "length", "tool_use": "tool_calls", "refusal": "content_filter",
	} {
		if got := stopReason(in); got != want {
			t.Errorf("stopReason(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRequestMapping(t *testing.T) {
	strict, parallel := true, false
	limit := 300
	req := &provider.ChatRequest{
		Messages: []provider.Message{
			{Role: provider.RoleSystem, Content: text("rule one")},
			{Role: provider.RoleUser, Content: []provider.ContentPart{
				{Type: provider.PartText, Text: "compare these"},
				{Type: provider.PartImage, ImageURL: "data:image/png;base64,iVBORw0KGgo="},
				{Type: provider.PartImage, ImageURL: "https://example.com/cat.jpg"},
				{Type: provider.PartText, Text: ""},
			}},
			{Role: provider.RoleAssistant, Content: text("Looking."), ToolCalls: []provider.ToolCall{
				{ID: "call_1", Name: "lookup", Arguments: `{"q":"cat"}`},
				{ID: "call_2", Name: "now", Arguments: ""},
			}},
			{Role: provider.RoleTool, ToolCallID: "call_1", Content: text("a cat")},
			{Role: provider.RoleTool, ToolCallID: "call_2", Content: text("noon")},
			{Role: provider.RoleUser, Content: text("thanks")},
			{Role: provider.RoleSystem, Content: text("rule two")},
		},
		Tools: []provider.Tool{
			{Name: "lookup", Description: "search", Parameters: json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}}}`), Strict: &strict},
			{Name: "now"},
		},
		ToolChoice:        &provider.ToolChoice{Mode: provider.ToolChoiceRequired},
		ParallelToolCalls: &parallel,
		MaxTokens:         &limit,
		Stop:              []string{"END"},
		User:              "user-42",
	}
	w, err := toWire(target, req)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := json.Marshal(w)
	want := `{
		"model": "claude-test",
		"max_tokens": 300,
		"system": "rule one\n\nrule two",
		"messages": [
			{"role": "user", "content": [
				{"type": "text", "text": "compare these"},
				{"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "iVBORw0KGgo="}},
				{"type": "image", "source": {"type": "url", "url": "https://example.com/cat.jpg"}}
			]},
			{"role": "assistant", "content": [
				{"type": "text", "text": "Looking."},
				{"type": "tool_use", "id": "call_1", "name": "lookup", "input": {"q": "cat"}},
				{"type": "tool_use", "id": "call_2", "name": "now", "input": {}}
			]},
			{"role": "user", "content": [
				{"type": "tool_result", "tool_use_id": "call_1", "content": "a cat"},
				{"type": "tool_result", "tool_use_id": "call_2", "content": "noon"},
				{"type": "text", "text": "thanks"}
			]}
		],
		"stop_sequences": ["END"],
		"tools": [
			{"name": "lookup", "description": "search", "input_schema": {"type": "object", "properties": {"q": {"type": "string"}}}, "strict": true},
			{"name": "now", "input_schema": {"type": "object", "properties": {}}}
		],
		"tool_choice": {"type": "any", "disable_parallel_tool_use": true},
		"metadata": {"user_id": "user-42"},
		"stream": true
	}`
	var g, e any
	_ = json.Unmarshal(got, &g)
	if err := json.Unmarshal([]byte(want), &e); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(g, e) {
		t.Errorf("wire request =\n%s", got)
	}
}

func TestToolChoiceMapping(t *testing.T) {
	limit := 1
	for mode, want := range map[string]string{
		provider.ToolChoiceAuto:     `{"type":"auto"}`,
		provider.ToolChoiceNone:     `{"type":"none"}`,
		provider.ToolChoiceRequired: `{"type":"any"}`,
		provider.ToolChoiceFunction: `{"type":"tool","name":"f"}`,
	} {
		w, err := toWire(target, &provider.ChatRequest{
			MaxTokens: &limit, Tools: []provider.Tool{{Name: "f"}},
			ToolChoice: &provider.ToolChoice{Mode: mode, Name: "f"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if got, _ := json.Marshal(w.ToolChoice); string(got) != want {
			t.Errorf("%s -> %s, want %s", mode, got, want)
		}
	}
}

func TestSupports(t *testing.T) {
	one, half := 1.0, 0.5
	tests := []struct {
		name    string
		mutate  func(*provider.ChatRequest)
		wantErr string
	}{
		{"plain", func(*provider.ChatRequest) {}, ""},
		{"no output cap yet", func(r *provider.ChatRequest) { r.MaxTokens = nil }, ""},
		{"default sampling values", func(r *provider.ChatRequest) { r.Temperature, r.TopP = &one, &one }, ""},
		{"temperature", func(r *provider.ChatRequest) { r.Temperature = &half }, "temperature"},
		{"top_p", func(r *provider.ChatRequest) { r.TopP = &half }, "top_p"},
		{"unmodeled fields", func(r *provider.ChatRequest) {
			r.Extra = map[string]json.RawMessage{"seed": json.RawMessage("1"), "logit_bias": json.RawMessage("{}")}
		}, "unsupported parameters: logit_bias, seed"},
		{"bad tool arguments", func(r *provider.ChatRequest) {
			r.Messages = append(r.Messages, provider.Message{Role: provider.RoleAssistant,
				ToolCalls: []provider.ToolCall{{ID: "c", Name: "f", Arguments: "not json"}}})
		}, "JSON object"},
		{"bad image url", func(r *provider.ChatRequest) {
			r.Messages[1].Content = append(r.Messages[1].Content, provider.ContentPart{Type: provider.PartImage, ImageURL: "ftp://x"})
		}, "image_url"},
		{"non-base64 data uri", func(r *provider.ChatRequest) {
			r.Messages[1].Content = append(r.Messages[1].Content, provider.ContentPart{Type: provider.PartImage, ImageURL: "data:image/png,raw"})
		}, "base64"},
	}
	p := New("a", "http://unused/v1", "", http.DefaultClient)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := request()
			tt.mutate(req)
			err := p.Supports(req)
			switch {
			case tt.wantErr == "" && err != nil:
				t.Errorf("Supports = %v, want nil", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Errorf("Supports = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultSamplingIsNotSent(t *testing.T) {
	one := 1.0
	req := request()
	req.Temperature, req.TopP = &one, &one
	// The mock rejects temperature/top_p as the real API does at this version.
	_, p := setup(t, mock.Behavior{OutputTokens: 1}, nil)
	s, err := p.Stream(context.Background(), target, req)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	s.Close()
}
