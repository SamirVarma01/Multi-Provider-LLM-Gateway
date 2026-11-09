package mock

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SamirVarma01/llm-gateway/internal/oai"
	"github.com/SamirVarma01/llm-gateway/internal/sse"
	"github.com/SamirVarma01/llm-gateway/internal/testutil"
)

func TestMain(m *testing.M) { testutil.Main(m) }

func newServer(t *testing.T, defaults Behavior) (*Server, *httptest.Server) {
	t.Helper()
	s := New(Options{Defaults: defaults, Seed: 42, AllowHeaderOverrides: true})
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return s, ts
}

func chatBody(stream, includeUsage bool, maxTokens *int) []byte {
	req := oai.ChatCompletionRequest{
		Model:     "m-small",
		Messages:  []oai.Message{{Role: "user", Content: oai.TextContent("hello there mock")}},
		MaxTokens: maxTokens,
		Stream:    stream,
	}
	if includeUsage {
		req.StreamOptions = &oai.StreamOptions{IncludeUsage: true}
	}
	b, _ := json.Marshal(req)
	return b
}

func post(t *testing.T, ctx context.Context, url string, body []byte, hdr map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// readStream collects content and the final usage from an OpenAI SSE stream.
type streamResult struct {
	content      string
	finishReason string
	usage        *oai.Usage
	sawDone      bool
	errEvent     *oai.ErrorDetail
	err          error
}

func readStream(r io.Reader) streamResult {
	var res streamResult
	rd := sse.NewReader(r)
	for {
		ev, err := rd.Next()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				res.err = err
			}
			return res
		}
		if string(ev.Data) == oai.DoneData {
			res.sawDone = true
			continue
		}
		var c oai.ChatCompletionChunk
		if err := json.Unmarshal(ev.Data, &c); err != nil {
			res.err = err
			return res
		}
		if c.Error != nil {
			res.errEvent = c.Error
		}
		if c.Usage != nil {
			res.usage = c.Usage
		}
		for _, ch := range c.Choices {
			if ch.Delta.Content != nil {
				res.content += *ch.Delta.Content
			}
			if ch.FinishReason != nil {
				res.finishReason = *ch.FinishReason
			}
		}
	}
}

func onlyRecord(t *testing.T, s *Server, reqID string) Record {
	t.Helper()
	recs := s.Records(reqID)
	if len(recs) != 1 {
		t.Fatalf("records for %s = %d, want 1: %+v", reqID, len(recs), recs)
	}
	return recs[0]
}

// waitRecord polls for a record written when the handler exits (interrupted calls).
func waitRecord(t *testing.T, s *Server, reqID string) Record {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if recs := s.Records(reqID); len(recs) > 0 {
			return recs[0]
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no record for %s", reqID)
	return Record{}
}

func TestNonStreamingIsDeterministicAndRecorded(t *testing.T) {
	s, ts := newServer(t, Behavior{OutputTokens: 12})
	ctx := context.Background()
	url := ts.URL + "/p/mock-a/v1/chat/completions"

	get := func(reqID string) oai.ChatCompletionResponse {
		resp := post(t, ctx, url, chatBody(false, false, nil), map[string]string{RequestIDHeader: reqID})
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d", resp.StatusCode)
		}
		var out oai.ChatCompletionResponse
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	a1, a2, b := get("req-a"), get("req-a"), get("req-b")

	if *a1.Choices[0].Message.Content != *a2.Choices[0].Message.Content {
		t.Error("same request ID produced different output")
	}
	if *a1.Choices[0].Message.Content == *b.Choices[0].Message.Content {
		t.Error("different request IDs produced identical output")
	}
	if got := len(strings.Fields(*a1.Choices[0].Message.Content)); got != 12 {
		t.Errorf("content has %d words, want 12", got)
	}
	// "hello there mock" = 3 words; 3 per message + 3 priming.
	wantUsage := oai.Usage{PromptTokens: 9, CompletionTokens: 12, TotalTokens: 21}
	if a1.Usage == nil || *a1.Usage != wantUsage || a1.Choices[0].FinishReason != "stop" {
		t.Errorf("usage = %+v finish = %q", a1.Usage, a1.Choices[0].FinishReason)
	}

	recs := s.Records("req-a")
	if len(recs) != 2 {
		t.Fatalf("records = %d, want 2", len(recs))
	}
	r := recs[0]
	if r.Provider != "mock-a" || r.Model != "m-small" || r.Stream || !r.Billable ||
		r.Outcome != OutcomeComplete || r.PromptTokens != 9 || r.CompletionTokens != 12 {
		t.Errorf("record = %+v", r)
	}
}

func TestStreamingMatchesNonStreaming(t *testing.T) {
	s, ts := newServer(t, Behavior{OutputTokens: 7})
	ctx := context.Background()
	url := ts.URL + "/p/mock-a/v1/chat/completions"
	hdr := map[string]string{RequestIDHeader: "req-1", AttemptHeader: "2"}

	resp := post(t, ctx, url, chatBody(true, true, nil), hdr)
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q", ct)
	}
	got := readStream(resp.Body)
	resp.Body.Close()
	if got.err != nil || !got.sawDone || got.finishReason != "stop" {
		t.Fatalf("stream = %+v", got)
	}
	if got.usage == nil || got.usage.CompletionTokens != 7 || got.usage.PromptTokens != 9 {
		t.Errorf("usage = %+v", got.usage)
	}

	resp = post(t, ctx, url, chatBody(false, false, nil), hdr)
	var ns oai.ChatCompletionResponse
	_ = json.NewDecoder(resp.Body).Decode(&ns)
	resp.Body.Close()
	if *ns.Choices[0].Message.Content != got.content {
		t.Errorf("stream content %q != non-stream %q for the same attempt", got.content, *ns.Choices[0].Message.Content)
	}

	r := s.Records("req-1")[0]
	if r.Attempt != 2 || !r.Stream || r.CompletionTokens != 7 || !r.Billable {
		t.Errorf("record = %+v", r)
	}
}

func TestStreamWithoutUsageOption(t *testing.T) {
	_, ts := newServer(t, Behavior{OutputTokens: 3})
	resp := post(t, context.Background(), ts.URL+"/v1/chat/completions", chatBody(true, false, nil), nil)
	defer resp.Body.Close()
	if got := readStream(resp.Body); got.usage != nil || !got.sawDone {
		t.Errorf("usage chunk sent without stream_options.include_usage: %+v", got)
	}
}

func TestMaxTokensCapsOutput(t *testing.T) {
	s, ts := newServer(t, Behavior{OutputTokens: 50})
	limit := 5
	resp := post(t, context.Background(), ts.URL+"/v1/chat/completions", chatBody(true, true, &limit),
		map[string]string{RequestIDHeader: "capped"})
	got := readStream(resp.Body)
	resp.Body.Close()
	if got.finishReason != "length" || got.usage.CompletionTokens != 5 {
		t.Errorf("finish = %q usage = %+v, want length / 5", got.finishReason, got.usage)
	}
	if r := onlyRecord(t, s, "capped"); r.Provider != DefaultProvider || r.CompletionTokens != 5 {
		t.Errorf("record = %+v", r)
	}
}

func TestInjectedError(t *testing.T) {
	s, ts := newServer(t, Behavior{OutputTokens: 5, ErrorRate: 1, ErrorStatus: 503})
	resp := post(t, context.Background(), ts.URL+"/v1/chat/completions", chatBody(true, true, nil),
		map[string]string{RequestIDHeader: "err"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	var body oai.ErrorBody
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || body.Error.Message == "" {
		t.Errorf("error body = %+v, %v", body, err)
	}
	r := onlyRecord(t, s, "err")
	if r.Outcome != OutcomeInjectedError || r.Billable || r.Status != 503 {
		t.Errorf("record = %+v", r)
	}
}

func TestMidStreamAbort(t *testing.T) {
	s, ts := newServer(t, Behavior{OutputTokens: 10, FailAfterTokens: 4})
	resp := post(t, context.Background(), ts.URL+"/v1/chat/completions", chatBody(true, true, nil),
		map[string]string{RequestIDHeader: "abort"})
	got := readStream(resp.Body)
	resp.Body.Close()
	if got.err == nil || got.sawDone {
		t.Errorf("expected a broken stream, got %+v", got)
	}
	if n := len(strings.Fields(got.content)); n != 4 {
		t.Errorf("received %d tokens before abort, want 4", n)
	}
	r := waitRecord(t, s, "abort")
	if r.Outcome != OutcomeMidStreamFailure || r.CompletionTokens != 4 || !r.Billable {
		t.Errorf("record = %+v", r)
	}
}

func TestMidStreamErrorEvent(t *testing.T) {
	s, ts := newServer(t, Behavior{OutputTokens: 10, FailAfterTokens: 2, FailMode: FailErrorEvent})
	resp := post(t, context.Background(), ts.URL+"/v1/chat/completions", chatBody(true, true, nil),
		map[string]string{RequestIDHeader: "evt"})
	got := readStream(resp.Body)
	resp.Body.Close()
	if got.errEvent == nil || got.sawDone || got.err != nil {
		t.Errorf("expected a clean stream ending in an error event, got %+v", got)
	}
	if r := waitRecord(t, s, "evt"); r.Outcome != OutcomeMidStreamFailure || r.CompletionTokens != 2 {
		t.Errorf("record = %+v", r)
	}
}

func TestHangUntilClientCancels(t *testing.T) {
	s, ts := newServer(t, Behavior{OutputTokens: 5, Hang: true})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/v1/chat/completions",
		bytes.NewReader(chatBody(true, true, nil)))
	req.Header.Set(RequestIDHeader, "hang")
	if resp, err := http.DefaultClient.Do(req); err == nil {
		resp.Body.Close()
		t.Fatal("hanging provider returned a response")
	}
	r := waitRecord(t, s, "hang")
	if r.Outcome != OutcomeClientCancelled || r.Billable {
		t.Errorf("record = %+v", r)
	}
}

func TestClientCancelMidStreamIsBillableForFlushedTokens(t *testing.T) {
	s, ts := newServer(t, Behavior{OutputTokens: 1000, TokensPerSec: 200})
	ctx, cancel := context.WithCancel(context.Background())
	resp := post(t, ctx, ts.URL+"/v1/chat/completions", chatBody(true, true, nil),
		map[string]string{RequestIDHeader: "cancel"})
	rd := sse.NewReader(resp.Body)
	for range 4 { // role chunk + 3 tokens
		if _, err := rd.Next(); err != nil {
			t.Fatal(err)
		}
	}
	cancel()
	resp.Body.Close()
	r := waitRecord(t, s, "cancel")
	if r.Outcome != OutcomeClientCancelled || !r.Billable || r.CompletionTokens < 3 || r.CompletionTokens >= 1000 {
		t.Errorf("record = %+v", r)
	}
}

func TestPacing(t *testing.T) {
	_, ts := newServer(t, Behavior{OutputTokens: 11, TTFTMs: 50, TokensPerSec: 100})
	start := time.Now()
	resp := post(t, context.Background(), ts.URL+"/v1/chat/completions", chatBody(true, false, nil), nil)
	rd := sse.NewReader(resp.Body)
	if _, err := rd.Next(); err != nil {
		t.Fatal(err)
	}
	ttft := time.Since(start)
	got := readStream(resp.Body)
	total := time.Since(start)
	resp.Body.Close()
	if !got.sawDone {
		t.Fatalf("stream = %+v", got)
	}
	// Lower bounds only: 50ms TTFT, then tokens 1..10 at 10ms intervals.
	if ttft < 50*time.Millisecond || total < 150*time.Millisecond {
		t.Errorf("ttft = %v total = %v, want >= 50ms and >= 150ms", ttft, total)
	}
}

// TestNonStreamPacing checks that a non-streaming response ends when the
// stream's last token would have gone out, not one token later, so the two
// modes can be compared in benchmarks.
func TestNonStreamPacing(t *testing.T) {
	_, ts := newServer(t, Behavior{OutputTokens: 3, TokensPerSec: 10})
	start := time.Now()
	resp := post(t, context.Background(), ts.URL+"/v1/chat/completions", chatBody(false, false, nil), nil)
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	// Tokens at 0, 100 and 200ms; one token later would be 300ms.
	if d := time.Since(start); d < 200*time.Millisecond || d >= 280*time.Millisecond {
		t.Errorf("non-streaming response took %v, want 200ms to 280ms", d)
	}
}

func TestBehaviorEndpointsAndHeaderOverrides(t *testing.T) {
	s, ts := newServer(t, Behavior{OutputTokens: 5})

	put := func(body string) int {
		req, _ := http.NewRequest(http.MethodPut, ts.URL+"/_mock/providers/mock-b", strings.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if code := put(`{"output_tokens": 3, "ttft_ms": 1}`); code != http.StatusOK {
		t.Fatalf("PUT status = %d", code)
	}
	if b := s.Behavior("mock-b"); b.OutputTokens != 3 || b.TTFTMs != 1 {
		t.Errorf("behavior = %+v", b)
	}
	if code := put(`{"error_rate": 2}`); code != http.StatusBadRequest {
		t.Errorf("invalid behavior: status = %d, want 400", code)
	}
	if code := put(`{"bogus": 1}`); code != http.StatusBadRequest {
		t.Errorf("unknown field: status = %d, want 400", code)
	}

	resp := post(t, context.Background(), ts.URL+"/p/mock-b/v1/chat/completions", chatBody(false, false, nil),
		map[string]string{RequestIDHeader: "hdr", "X-Mock-Output-Tokens": "2"})
	resp.Body.Close()
	if r := onlyRecord(t, s, "hdr"); r.CompletionTokens != 2 {
		t.Errorf("header override ignored: %+v", r)
	}
	resp = post(t, context.Background(), ts.URL+"/v1/chat/completions", chatBody(false, false, nil),
		map[string]string{"X-Mock-Error-Rate": "nope"})
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("bad override header: status = %d, want 400", resp.StatusCode)
	}
}

func TestUsageEndpoints(t *testing.T) {
	s, ts := newServer(t, Behavior{OutputTokens: 1})
	for _, id := range []string{"u1", "u2"} {
		resp := post(t, context.Background(), ts.URL+"/v1/chat/completions", chatBody(false, false, nil),
			map[string]string{RequestIDHeader: id})
		resp.Body.Close()
	}
	getRecords := func(query string) []Record {
		resp, err := http.Get(ts.URL + "/_mock/usage" + query)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var recs []Record
		if err := json.NewDecoder(resp.Body).Decode(&recs); err != nil {
			t.Fatal(err)
		}
		return recs
	}
	if recs := getRecords(""); len(recs) != 2 {
		t.Errorf("all records = %d, want 2", len(recs))
	}
	if recs := getRecords("?request_id=u2"); len(recs) != 1 || recs[0].RequestID != "u2" {
		t.Errorf("filtered records = %+v", recs)
	}
	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/_mock/usage", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if n := len(s.AllRecords()); n != 0 {
		t.Errorf("records after reset = %d", n)
	}
}

func TestRestoreKeepsRecordsAcrossRestarts(t *testing.T) {
	var log bytes.Buffer
	first := New(Options{Defaults: Behavior{OutputTokens: 3}, Seed: 1, UsageLog: &log})
	ts := httptest.NewServer(first.Handler())
	defer ts.Close()
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/p/a/v1/chat/completions",
		strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set(RequestIDHeader, "req-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	want := first.Records("req-1")
	if len(want) != 1 || !want[0].Billable {
		t.Fatalf("records = %+v", want)
	}

	// A crash can leave a partial last line.
	saved := log.String() + `{"request_id":"req-2","att`
	var after bytes.Buffer
	second := New(Options{Defaults: Behavior{OutputTokens: 3}, Seed: 1, UsageLog: &after})
	restored, skipped, err := second.Restore(strings.NewReader(saved))
	if err != nil || restored != 1 || skipped != 1 {
		t.Fatalf("Restore = %d, %d, %v", restored, skipped, err)
	}
	got := second.Records("req-1")
	if len(got) != 1 || got[0].Billable != want[0].Billable || got[0].CompletionTokens != want[0].CompletionTokens ||
		got[0].PromptTokens != want[0].PromptTokens || got[0].Provider != "a" {
		t.Errorf("restored = %+v, want %+v", got, want)
	}
	if after.Len() != 0 {
		t.Errorf("Restore rewrote %d bytes to the log", after.Len())
	}
}
