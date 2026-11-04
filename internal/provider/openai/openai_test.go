package openai

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
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
	if !resp.UsageReported || resp.Usage.PromptTokens != rec.PromptTokens ||
		resp.Usage.CompletionTokens != rec.CompletionTokens || resp.Usage.Estimated {
		t.Errorf("usage = %+v (reported %v), mock recorded %+v", resp.Usage, resp.UsageReported, rec)
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
		{http.StatusUnauthorized, provider.KindAuth, true},
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
			if resp.UsageReported {
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
	if err != nil || resp.UsageReported || resp.FinishReason != "stop" {
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
