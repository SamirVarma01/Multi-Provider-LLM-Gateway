package openai

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SamirVarma01/llm-gateway/internal/mock"
	"github.com/SamirVarma01/llm-gateway/internal/oai"
	"github.com/SamirVarma01/llm-gateway/internal/obs"
	"github.com/SamirVarma01/llm-gateway/internal/provider"
	"github.com/SamirVarma01/llm-gateway/internal/testutil"
)

func TestMain(m *testing.M) { testutil.Main(m) }

var target = provider.Target{Provider: "mock-a", UpstreamModel: "m-small", MaxOutput: 4096}

func request() *provider.ChatRequest {
	limit := 64
	return &provider.ChatRequest{
		Model:     "alias",
		Messages:  []provider.Message{{Role: provider.RoleUser, Content: []provider.ContentPart{{Type: "text", Text: "say something"}}}},
		MaxTokens: &limit,
	}
}

// setup starts a mock with b for provider mock-a and returns an adapter for it.
// The wrap hook lets a test observe or replace upstream requests.
func setup(t *testing.T, b mock.Behavior, wrap func(http.Handler) http.Handler) (*mock.Server, *Provider) {
	t.Helper()
	m := mock.New(mock.Options{Defaults: b, Seed: 7})
	var h http.Handler = m.Handler()
	if wrap != nil {
		h = wrap(h)
	}
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	tr := &http.Transport{}
	t.Cleanup(tr.CloseIdleConnections)
	return m, New("mock-a", ts.URL+"/p/mock-a/v1", "sk-test", &http.Client{Transport: tr})
}

func ctxWithIDs(reqID string, attempt int) context.Context {
	return provider.WithAttempt(obs.WithRequestID(context.Background(), reqID), attempt)
}

func TestStreamHappyPath(t *testing.T) {
	var upstream *http.Request
	var upstreamBody oai.ChatCompletionRequest
	m, p := setup(t, mock.Behavior{OutputTokens: 9}, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			upstream = r.Clone(r.Context())
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &upstreamBody)
			r.Body = io.NopCloser(strings.NewReader(string(raw)))
			next.ServeHTTP(w, r)
		})
	})

	s, err := p.Stream(ctxWithIDs("req-1", 2), target, request())
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer s.Close()
	resp, err := provider.Collect(s)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}

	if upstream.URL.Path != "/p/mock-a/v1/chat/completions" ||
		upstream.Header.Get("Authorization") != "Bearer sk-test" ||
		upstream.Header.Get(obs.RequestIDHeader) != "req-1" ||
		upstream.Header.Get(provider.AttemptHeader) != "2" {
		t.Errorf("upstream request = %s %v", upstream.URL.Path, upstream.Header)
	}
	if upstreamBody.Model != "m-small" || !upstreamBody.Stream || upstreamBody.StreamOptions == nil ||
		!upstreamBody.StreamOptions.IncludeUsage || upstreamBody.MaxTokens == nil || *upstreamBody.MaxTokens != 64 {
		t.Errorf("upstream body = %+v", upstreamBody)
	}

	rec := m.Records("req-1")[0]
	if rec.Attempt != 2 || rec.Provider != "mock-a" {
		t.Errorf("record = %+v", rec)
	}
	want := provider.Reported{PromptTokens: rec.PromptTokens, CompletionTokens: rec.CompletionTokens,
		HasPrompt: true, HasCompletion: true}
	if resp.Reported != want {
		t.Errorf("usage = %+v, mock recorded %+v", resp.Reported, rec)
	}
	if got := len(strings.Fields(resp.Content)); got != 9 || resp.FinishReason != "stop" {
		t.Errorf("content %q (%d words) finish %q", resp.Content, got, resp.FinishReason)
	}
}

func TestStatusErrors(t *testing.T) {
	tests := []struct {
		status    int
		kind      provider.ErrKind
		retryable bool
	}{
		{http.StatusBadRequest, provider.KindBadRequest, false},
		{http.StatusUnauthorized, provider.KindConfig, true},
		{http.StatusNotFound, provider.KindConfig, true},
		{http.StatusTooManyRequests, provider.KindRateLimited, true},
		{http.StatusInternalServerError, provider.KindServer, true},
		{http.StatusServiceUnavailable, provider.KindServer, true},
	}
	for _, tt := range tests {
		t.Run(http.StatusText(tt.status), func(t *testing.T) {
			_, p := setup(t, mock.Behavior{OutputTokens: 3, ErrorRate: 1, ErrorStatus: tt.status}, nil)
			_, err := p.Stream(context.Background(), target, request())
			var ue *provider.UpstreamError
			if !errors.As(err, &ue) {
				t.Fatalf("err = %v, want *UpstreamError", err)
			}
			if ue.Status != tt.status || ue.Kind != tt.kind || ue.Retryable != tt.retryable ||
				!strings.Contains(ue.Msg, "injected error") {
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
				t.Fatalf("err = %v, want retryable *UpstreamError", err)
			}
			if got := len(strings.Fields(resp.Content)); got != 3 {
				t.Errorf("partial content has %d tokens, want 3", got)
			}
			if resp.Reported.HasPrompt || resp.Reported.HasCompletion {
				t.Error("usage must not be reported for an interrupted stream")
			}
			// The error is sticky.
			if _, err2 := s.Recv(); err2 != err {
				t.Errorf("second Recv = %v, want %v", err2, err)
			}
		})
	}
}

func TestOmittedUsage(t *testing.T) {
	_, p := setup(t, mock.Behavior{OutputTokens: 4, OmitUsage: true}, nil)
	s, err := p.Stream(context.Background(), target, request())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	resp, err := provider.Collect(s)
	if err != nil || resp.Reported != (provider.Reported{}) || resp.FinishReason != "stop" {
		t.Errorf("resp = %+v, err = %v", resp, err)
	}
}

func TestCallerCancelIsNotAnUpstreamError(t *testing.T) {
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

func TestDeadlineIsTimeout(t *testing.T) {
	_, p := setup(t, mock.Behavior{Hang: true}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := p.Stream(ctx, target, request())
	var ue *provider.UpstreamError
	if !errors.As(err, &ue) || ue.Kind != provider.KindTimeout || !ue.Retryable {
		t.Errorf("err = %v, want retryable timeout", err)
	}
}

func TestConnectionRefused(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	p := New("dead", "http://"+addr+"/v1", "", &http.Client{})
	_, err = p.Stream(context.Background(), target, request())
	var ue *provider.UpstreamError
	if !errors.As(err, &ue) || ue.Kind != provider.KindTransport || !ue.Retryable {
		t.Errorf("err = %v, want retryable transport error", err)
	}
}

func TestMultiPartContentIsSentAsParts(t *testing.T) {
	req := request()
	req.Messages[0].Content = []provider.ContentPart{{Type: "text", Text: "a"}, {Type: "text", Text: "b"}}
	w := toWire(target, req)
	b, _ := json.Marshal(w.Messages[0])
	if !strings.Contains(string(b), `"content":[{"type":"text","text":"a"},{"type":"text","text":"b"}]`) {
		t.Errorf("message = %s", b)
	}
}

// replay serves a fixed SSE body for every request.
func replay(t *testing.T, body string) *Provider {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(ts.Close)
	tr := &http.Transport{}
	t.Cleanup(tr.CloseIdleConnections)
	return New("replay", ts.URL+"/v1", "", &http.Client{Transport: tr})
}

// An OpenAI stream with text followed by two parallel tool calls, in the
// shape the Chat Completions API sends them.
const toolCallStream = `data: {"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"role":"assistant","content":null},"finish_reason":null}]}

data: {"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"content":"Let me check."},"finish_reason":null}]}

data: {"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_a","type":"function","function":{"name":"get_weather","arguments":""}}]},"finish_reason":null}]}

data: {"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"city\":"}}]},"finish_reason":null}]}

data: {"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"id":"call_b","type":"function","function":{"name":"get_time","arguments":"{}"}}]},"finish_reason":null}]}

data: {"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"Paris\"}"}}]},"finish_reason":null}]}

data: {"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}

data: {"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[],"usage":{"prompt_tokens":31,"completion_tokens":24,"total_tokens":55}}

data: [DONE]

`

func TestToolCallStreamTranslation(t *testing.T) {
	p := replay(t, toolCallStream)
	s, err := p.Stream(context.Background(), target, request())
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
		{Content: "Let me check."},
		{ToolCalls: []provider.ToolCallDelta{{Index: 0, ID: "call_a", Name: "get_weather"}}},
		{ToolCalls: []provider.ToolCallDelta{{Index: 0, ArgumentsFragment: `{"city":`}}},
		{ToolCalls: []provider.ToolCallDelta{{Index: 1, ID: "call_b", Name: "get_time", ArgumentsFragment: `{}`}}},
		{ToolCalls: []provider.ToolCallDelta{{Index: 0, ArgumentsFragment: `"Paris"}`}}},
		{FinishReason: provider.FinishToolCalls},
	}
	if !reflect.DeepEqual(chunks, want) {
		t.Errorf("chunks =\n%+v\nwant\n%+v", chunks, want)
	}
	if got := s.Usage(); got != (provider.Reported{PromptTokens: 31, CompletionTokens: 24, HasPrompt: true, HasCompletion: true}) {
		t.Errorf("usage = %+v", got)
	}
}

func TestEOFAfterFinishWithoutDoneIsClean(t *testing.T) {
	p := replay(t, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\n")
	s, err := p.Stream(context.Background(), target, request())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if resp, err := provider.Collect(s); err != nil || resp.Content != "hi" {
		t.Errorf("Collect = %+v, %v", resp, err)
	}
}

func TestRequestMapping(t *testing.T) {
	strict := true
	parallel := false
	req := request()
	req.Messages = []provider.Message{
		{Role: provider.RoleUser, Content: []provider.ContentPart{
			{Type: provider.PartText, Text: "what is this?"},
			{Type: provider.PartImage, ImageURL: "data:image/png;base64,AAAA", ImageDetail: "low"},
		}},
		{Role: provider.RoleAssistant, ToolCalls: []provider.ToolCall{{ID: "call_1", Name: "lookup", Arguments: `{"q":"x"}`}}},
		{Role: provider.RoleTool, ToolCallID: "call_1", Content: []provider.ContentPart{{Type: provider.PartText, Text: "a cat"}}},
	}
	req.Tools = []provider.Tool{{Name: "lookup", Description: "search", Parameters: json.RawMessage(`{"type":"object"}`), Strict: &strict}}
	req.ToolChoice = &provider.ToolChoice{Mode: provider.ToolChoiceFunction, Name: "lookup"}
	req.ParallelToolCalls = &parallel
	req.Extra = map[string]json.RawMessage{"seed": json.RawMessage("7")}

	b, err := json.Marshal(toWire(target, req))
	if err != nil {
		t.Fatal(err)
	}
	want := `{
		"model": "m-small",
		"messages": [
			{"role": "user", "content": [
				{"type": "text", "text": "what is this?"},
				{"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA", "detail": "low"}}
			]},
			{"role": "assistant", "content": null,
			 "tool_calls": [{"id": "call_1", "type": "function", "function": {"name": "lookup", "arguments": "{\"q\":\"x\"}"}}]},
			{"role": "tool", "tool_call_id": "call_1", "content": "a cat"}
		],
		"max_tokens": 64,
		"stream": true,
		"stream_options": {"include_usage": true},
		"tools": [{"type": "function", "function": {"name": "lookup", "description": "search", "parameters": {"type": "object"}, "strict": true}}],
		"tool_choice": {"type": "function", "function": {"name": "lookup"}},
		"parallel_tool_calls": false,
		"seed": 7
	}`
	var got, exp any
	_ = json.Unmarshal(b, &got)
	if err := json.Unmarshal([]byte(want), &exp); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, exp) {
		t.Errorf("wire request =\n%s", b)
	}
}

func TestMaxCompletionTokensOption(t *testing.T) {
	var body map[string]any
	capture := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, _ := io.ReadAll(r.Body)
			body = nil
			_ = json.Unmarshal(raw, &body)
			r.Body = io.NopCloser(strings.NewReader(string(raw)))
			next.ServeHTTP(w, r)
		})
	}
	_, p := setup(t, mock.Behavior{OutputTokens: 100}, capture)
	for _, opts := range [][]Option{nil, {UseMaxCompletionTokens()}} {
		pp := New(p.name, p.baseURL, "", p.client, opts...)
		s, err := pp.Stream(context.Background(), target, request())
		if err != nil {
			t.Fatal(err)
		}
		resp, _ := provider.Collect(s)
		s.Close()
		field, other := "max_tokens", "max_completion_tokens"
		if opts != nil {
			field, other = other, field
		}
		if body[field] != float64(64) || body[other] != nil {
			t.Errorf("options %d: body = %v", len(opts), body)
		}
		// The mock honors either field.
		if resp.Reported.CompletionTokens != 64 {
			t.Errorf("options %d: completion tokens = %d", len(opts), resp.Reported.CompletionTokens)
		}
	}
}
