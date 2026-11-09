package bench

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/SamirVarma01/llm-gateway/internal/mock"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func TestSummarize(t *testing.T) {
	var ds []time.Duration
	for i := 100; i >= 1; i-- {
		ds = append(ds, time.Duration(i)*time.Millisecond)
	}
	got := Summarize(ds)
	want := Dist{N: 100, Mean: 50.5, P50: 50, P95: 95, P99: 99, Max: 100}
	if got != want {
		t.Errorf("Summarize = %+v, want %+v", got, want)
	}
	if (Summarize(nil) != Dist{}) {
		t.Error("Summarize(nil) is not empty")
	}
}

func TestGenerateOpenLoop(t *testing.T) {
	var measured atomic.Bool
	run := Generate(context.Background(), Load{
		Rate: 200, Warmup: 50 * time.Millisecond, Duration: 200 * time.Millisecond,
		OnMeasure: func() { measured.Store(true) },
	}, func(context.Context, int) Result { return Result{OK: true} })
	// Sends every 5ms; the ones scheduled in [50ms, 250ms) are measured.
	if len(run.Results) != 40 {
		t.Fatalf("measured %d requests, want 40", len(run.Results))
	}
	if run.Elapsed != 200*time.Millisecond {
		t.Errorf("Elapsed = %v, want 200ms", run.Elapsed)
	}
	first := run.Results[0].Intended
	for _, r := range run.Results {
		if r.Intended.Before(first) {
			first = r.Intended
		}
	}
	for _, r := range run.Results {
		if off := r.Intended.Sub(first) % (5 * time.Millisecond); off != 0 {
			t.Fatalf("intended times are not on the 5ms schedule: off by %v", off)
		}
	}
	if !measured.Load() {
		t.Error("OnMeasure was not called")
	}
}

func TestGenerateStopLetsRequestsFinish(t *testing.T) {
	stop := make(chan struct{})
	time.AfterFunc(100*time.Millisecond, func() { close(stop) })
	start := time.Now()
	run := Generate(context.Background(), Load{Rate: 100, Duration: time.Hour, Stop: stop},
		func(ctx context.Context, _ int) Result {
			select {
			case <-ctx.Done():
				return Result{Err: "cancelled"}
			case <-time.After(50 * time.Millisecond):
				return Result{OK: true}
			}
		})
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Generate took %v after Stop", d)
	}
	if len(run.Results) < 5 {
		t.Fatalf("only %d results", len(run.Results))
	}
	for _, r := range run.Results {
		if !r.OK {
			t.Fatalf("a request in flight at Stop did not finish: %+v", r)
		}
	}
	if run.Elapsed > time.Second {
		t.Errorf("Elapsed = %v, want about 100ms", run.Elapsed)
	}
}

func TestGenerateClosedLoop(t *testing.T) {
	run := Generate(context.Background(), Load{Workers: 3, Duration: 100 * time.Millisecond},
		func(context.Context, int) Result {
			time.Sleep(10 * time.Millisecond)
			return Result{OK: true}
		})
	if run.InFlightMax != 3 {
		t.Errorf("InFlightMax = %d, want 3", run.InFlightMax)
	}
	if n := len(run.Results); n < 15 || n > 33 {
		t.Errorf("%d results, want about 30", n)
	}
	if run.Elapsed != 100*time.Millisecond {
		t.Errorf("Elapsed = %v, want the 100ms the load ran", run.Elapsed)
	}
}

func mockServer(t *testing.T, b mock.Behavior) (*httptest.Server, *http.Client) {
	t.Helper()
	ts := httptest.NewServer(mock.New(mock.Options{Defaults: b, Seed: 1}).Handler())
	client := NewHTTPClient()
	t.Cleanup(func() {
		client.CloseIdleConnections()
		ts.Close()
	})
	return ts, client
}

func chatBody(t *testing.T, stream, usage bool) []byte {
	t.Helper()
	req := ChatRequest{Model: "m", Messages: []Message{{Role: "user", Content: "hello there"}}, MaxTokens: 50, Stream: stream}
	if usage {
		req.StreamOptions = &StreamOptions{IncludeUsage: true}
	}
	b, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestChat(t *testing.T) {
	ts, client := mockServer(t, mock.Behavior{OutputTokens: 5, TTFTMs: 5})
	url := ts.URL + "/v1/chat/completions"

	r := Chat(context.Background(), client, url, "", chatBody(t, true, true), true)
	if !r.OK || r.Chunks != 5 || r.Usage == nil || r.Usage.CompletionTokens != 5 || r.FirstToken.IsZero() {
		t.Errorf("stream: %+v", r)
	}
	if r.FirstToken.Before(r.Sent) || r.End.Before(r.FirstToken) {
		t.Errorf("stream times out of order: sent %v, first token %v, end %v", r.Sent, r.FirstToken, r.End)
	}

	r = Chat(context.Background(), client, url, "", chatBody(t, true, false), true)
	if !r.OK || r.Usage != nil {
		t.Errorf("stream without usage: %+v", r)
	}

	r = Chat(context.Background(), client, url, "", chatBody(t, false, false), false)
	if !r.OK || r.Usage == nil || r.Usage.CompletionTokens != 5 || !r.FirstToken.IsZero() {
		t.Errorf("non-stream: %+v", r)
	}
}

func TestChatFailures(t *testing.T) {
	ts, client := mockServer(t, mock.Behavior{OutputTokens: 5, ErrorRate: 1, ErrorStatus: 503})
	r := Chat(context.Background(), client, ts.URL+"/v1/chat/completions", "", chatBody(t, true, true), true)
	if r.OK || r.Status != 503 || !strings.HasPrefix(r.Err, "status 503: ") {
		t.Errorf("error status: %+v", r)
	}

	ts, client = mockServer(t, mock.Behavior{OutputTokens: 5, FailAfterTokens: 2})
	r = Chat(context.Background(), client, ts.URL+"/v1/chat/completions", "", chatBody(t, true, true), true)
	if r.OK || r.Chunks != 2 || r.Err == "" {
		t.Errorf("stream cut off: %+v", r)
	}

	ts, client = mockServer(t, mock.Behavior{OutputTokens: 5, FailAfterTokens: 2, FailMode: mock.FailErrorEvent})
	r = Chat(context.Background(), client, ts.URL+"/v1/chat/completions", "", chatBody(t, true, true), true)
	if r.OK || !strings.HasPrefix(r.Err, "stream error: ") {
		t.Errorf("error event: %+v", r)
	}

	r = Chat(context.Background(), client, "http://127.0.0.1:1/v1/chat/completions", "", chatBody(t, false, false), false)
	if r.OK || r.Status != 0 || strings.Contains(r.Err, "127.0.0.1:1/v1") {
		t.Errorf("connection error should be reported without the URL: %+v", r)
	}
}

func TestScrape(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `# HELP x help
# TYPE x gauge
gateway_breaker_state{provider="a",upstream_model="m"} 2
gateway_overhead_seconds_sum{stream="true"} 1.5e-3
plain 7
`)
	}))
	defer ts.Close()
	client := NewHTTPClient()
	defer client.CloseIdleConnections()
	m, err := Scrape(context.Background(), client, ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{
		`gateway_breaker_state{provider="a",upstream_model="m"}`: 2,
		`gateway_overhead_seconds_sum{stream="true"}`:            1.5e-3,
		"plain": 7,
	}
	for k, v := range want {
		if m[k] != v {
			t.Errorf("%s = %v, want %v", k, m[k], v)
		}
	}
}
