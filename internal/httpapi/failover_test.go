package httpapi

import (
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/SamirVarma01/llm-gateway/internal/mock"
	"github.com/SamirVarma01/llm-gateway/internal/oai"
	"github.com/SamirVarma01/llm-gateway/internal/obs"
	"github.com/SamirVarma01/llm-gateway/internal/router"
)

// waitRecords waits until the mock has n records for a request; records of
// cancelled attempts are written asynchronously.
func waitRecords(t *testing.T, m *mock.Server, reqID string, n int) []mock.Record {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		recs := m.Records(reqID)
		if len(recs) >= n || time.Now().After(deadline) {
			if len(recs) != n {
				t.Fatalf("mock has %d records for %s, want %d: %+v", len(recs), reqID, n, recs)
			}
			return recs
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func setBehavior(t *testing.T, e *testEnv, provider string, b mock.Behavior) {
	t.Helper()
	if err := e.mock.SetBehavior(provider, b); err != nil {
		t.Fatal(err)
	}
}

func routeHeaders(resp *http.Response) (string, string) {
	return resp.Header.Get(headerProvider), resp.Header.Get(headerAttempts)
}

// readBody consumes a successful response, streamed or not, and returns
// its content.
func readBody(t *testing.T, resp *http.Response, stream bool) string {
	t.Helper()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d: %s", resp.StatusCode, body)
	}
	if stream {
		cs := readClientStream(resp.Body)
		if cs.err != nil || cs.errEvent != nil || !cs.sawDone {
			t.Fatalf("stream: err %v, error event %+v, [DONE] %v", cs.err, cs.errEvent, cs.sawDone)
		}
		return cs.content
	}
	out := decode[oai.ChatCompletionResponse](t, resp)
	return *out.Choices[0].Message.Content
}

func TestFailoverBeforeCommit(t *testing.T) {
	failures := []struct {
		name string
		b    mock.Behavior
	}{
		{"503", mock.Behavior{ErrorRate: 1, ErrorStatus: 503}},
		{"429", mock.Behavior{ErrorRate: 1, ErrorStatus: 429}},
		{"401", mock.Behavior{ErrorRate: 1, ErrorStatus: 401}},
		{"hang past the TTFT timeout", mock.Behavior{Hang: true}},
	}
	for _, stream := range []bool{false, true} {
		for _, f := range failures {
			t.Run(f.name+"/stream="+strconv.FormatBool(stream), func(t *testing.T) {
				e := newEnv(t, mock.Behavior{OutputTokens: 4}, nil)
				setBehavior(t, e, "mock-a", f.b)
				req := chatReq("pair", nil)
				req.Stream = stream
				resp := e.chat(t, req)
				reqID := resp.Header.Get(obs.RequestIDHeader)
				if content := readBody(t, resp, stream); content == "" {
					t.Error("empty response")
				}
				if p, n := routeHeaders(resp); p != "mock-b" || n != "2" {
					t.Errorf("served by %q after %q attempts, want mock-b after 2", p, n)
				}

				// The client is charged for the attempt that served it, exactly
				// as the mock recorded it; the failed attempt cost nothing.
				recs := waitRecords(t, e.mock, reqID, 2)
				failed, served := recs[0], recs[1]
				if failed.Provider != "mock-a" || failed.Attempt != 1 || failed.Billable {
					t.Errorf("first attempt = %+v", failed)
				}
				if served.Provider != "mock-b" || served.Attempt != 2 || !served.Billable {
					t.Errorf("second attempt = %+v", served)
				}
				o := e.outcome(t)
				if o.Provider != "mock-b" || o.Attempts != 2 || o.Result != resultCompleted || o.Usage.Estimated ||
					o.Usage.PromptTokens != served.PromptTokens || o.Usage.CompletionTokens != served.CompletionTokens {
					t.Errorf("outcome = %+v, served record %+v", o, served)
				}
			})
		}
	}
}

func TestNoFailoverOnUpstreamBadRequest(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 4}, nil)
	setBehavior(t, e, "mock-a", mock.Behavior{ErrorRate: 1, ErrorStatus: 400})
	resp := e.chat(t, chatReq("pair", nil))
	reqID := resp.Header.Get(obs.RequestIDHeader)
	wantError(t, resp, http.StatusBadRequest, "upstream_rejected")
	if p, n := routeHeaders(resp); p != "mock-a" || n != "1" {
		t.Errorf("headers: provider %q attempts %q", p, n)
	}
	if recs := waitRecords(t, e.mock, reqID, 1); recs[0].Provider != "mock-a" {
		t.Errorf("records = %+v", recs)
	}
}

// A non-streaming response is committed only when it is written, so an
// attempt that fails after producing tokens still fails over (design §6.4).
// The provider may bill those tokens; the client is charged only for the
// attempt that served it (design §12).
func TestNonStreamFailsOverAfterPartialOutput(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 4}, nil)
	setBehavior(t, e, "mock-a", mock.Behavior{OutputTokens: 10, FailAfterTokens: 3})
	resp := e.chat(t, chatReq("pair", nil))
	reqID := resp.Header.Get(obs.RequestIDHeader)
	readBody(t, resp, false)
	if p, n := routeHeaders(resp); p != "mock-b" || n != "2" {
		t.Errorf("served by %q after %q attempts", p, n)
	}
	recs := waitRecords(t, e.mock, reqID, 2)
	if !recs[0].Billable || recs[0].CompletionTokens != 3 {
		t.Errorf("abandoned attempt = %+v, want billable with 3 tokens", recs[0])
	}
	if o := e.outcome(t); o.Usage.CompletionTokens != recs[1].CompletionTokens || o.Usage.PromptTokens != recs[1].PromptTokens {
		t.Errorf("charged %+v, want only the serving attempt %+v", o.Usage, recs[1])
	}
}

func TestStreamDoesNotFailOverAfterCommit(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 4}, nil)
	setBehavior(t, e, "mock-a", mock.Behavior{OutputTokens: 10, FailAfterTokens: 3})
	resp := e.chat(t, streamReq("pair", false))
	reqID := resp.Header.Get(obs.RequestIDHeader)
	cs := readClientStream(resp.Body)
	if cs.errEvent == nil || cs.errEvent.Code != "stream_interrupted" || cs.sawDone {
		t.Errorf("error event %+v, [DONE] %v", cs.errEvent, cs.sawDone)
	}
	if p, n := routeHeaders(resp); p != "mock-a" || n != "1" {
		t.Errorf("headers: provider %q attempts %q", p, n)
	}
	if recs := waitRecords(t, e.mock, reqID, 1); recs[0].Provider != "mock-a" {
		t.Errorf("records = %+v: no failover after the first byte", recs)
	}
}

func TestStreamIdleTimeout(t *testing.T) {
	e := newEnvTweaked(t, mock.Behavior{OutputTokens: 5, TokensPerSec: 2}, envTweak{
		router: func(o *router.Options) { o.Retry.IdleTimeout = 200 * time.Millisecond },
	})
	resp := e.chat(t, streamReq("pair", false))
	cs := readClientStream(resp.Body)
	if cs.content == "" || cs.errEvent == nil || cs.errEvent.Code != "upstream_timeout" || cs.sawDone {
		t.Errorf("content %q, error event %+v, [DONE] %v", cs.content, cs.errEvent, cs.sawDone)
	}
	if o := e.outcome(t); !o.Committed || !o.Billable || o.Result != resultUpstreamError || o.Attempts != 1 {
		t.Errorf("outcome = %+v", o)
	}
}

func TestBreakerSkipsFailingTarget(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 2}, nil)
	setBehavior(t, e, "mock-a", mock.Behavior{ErrorRate: 1, ErrorStatus: 503})
	for i := range 5 {
		resp := e.chat(t, chatReq("pair", nil))
		readBody(t, resp, false)
		if _, n := routeHeaders(resp); n != "2" {
			t.Fatalf("request %d: %s attempts, want 2", i+1, n)
		}
	}
	// Five failures in a row opened mock-a's breaker: it is skipped, and
	// skipping is not an attempt.
	resp := e.chat(t, chatReq("pair", nil))
	reqID := resp.Header.Get(obs.RequestIDHeader)
	readBody(t, resp, false)
	if p, n := routeHeaders(resp); p != "mock-b" || n != "1" {
		t.Errorf("served by %q after %q attempts, want mock-b after 1", p, n)
	}
	if recs := waitRecords(t, e.mock, reqID, 1); recs[0].Provider != "mock-b" {
		t.Errorf("records = %+v", recs)
	}
}

func TestNoHealthyTarget(t *testing.T) {
	e := newEnv(t, mock.Behavior{ErrorRate: 1, ErrorStatus: 503}, nil)
	// "chat" has one target. The first request makes 3 attempts, the second
	// trips the breaker on its 5th failure overall and stops there.
	wantError(t, e.chat(t, chatReq("chat", nil)), http.StatusBadGateway, "upstream_error")
	resp := e.chat(t, chatReq("chat", nil))
	wantError(t, resp, http.StatusBadGateway, "upstream_error")
	if _, n := routeHeaders(resp); n != "2" {
		t.Errorf("second request: %s attempts, want 2", n)
	}

	resp = e.chat(t, chatReq("chat", nil))
	reqID := resp.Header.Get(obs.RequestIDHeader)
	if ra, err := strconv.Atoi(resp.Header.Get("Retry-After")); err != nil || ra < 1 || ra > 5 {
		t.Errorf("Retry-After = %q, want 1-5s (the breaker cooldown)", resp.Header.Get("Retry-After"))
	}
	wantError(t, resp, http.StatusServiceUnavailable, "no_healthy_upstream")
	if p, n := routeHeaders(resp); p != "" || n != "0" {
		t.Errorf("headers: provider %q attempts %q", p, n)
	}
	if recs := e.mock.Records(reqID); len(recs) != 0 {
		t.Errorf("the open breaker let %d calls through", len(recs))
	}
}

func TestFailoverAcrossFormats(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 3}, nil)
	setBehavior(t, e, "claude-a", mock.Behavior{ErrorRate: 1, ErrorStatus: 529}) // Anthropic "overloaded"
	resp := e.chat(t, streamReq("mixed", true))
	readBody(t, resp, true)
	if p, n := routeHeaders(resp); p != "mock-a" || n != "2" {
		t.Errorf("served by %q after %q attempts, want mock-a after 2", p, n)
	}
}

func TestFastestPolicyUsesMeasuredTTFT(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 2}, nil)
	fastest := http.Header{headerRoutePolicy: {"fastest"}}

	// Give both targets a TTFT sample: mock-b by failing over to it, then
	// mock-a (made slow) by priority.
	setBehavior(t, e, "mock-a", mock.Behavior{ErrorRate: 1, ErrorStatus: 503})
	readBody(t, e.chat(t, streamReq("pair", false)), true)
	setBehavior(t, e, "mock-a", mock.Behavior{OutputTokens: 2, TTFTMs: 150})
	resp := e.chat(t, streamReq("pair", false))
	readBody(t, resp, true) // a TTFT sample is recorded when the attempt ends
	if p, _ := routeHeaders(resp); p != "mock-a" {
		t.Fatalf("priority request served by %s", p)
	}

	resp = e.chat(t, streamReq("pair", false), fastest)
	readBody(t, resp, true)
	if p, _ := routeHeaders(resp); p != "mock-b" {
		t.Errorf("fastest served by %s, want mock-b (no added TTFT)", p)
	}
	if p, _ := routeHeaders(e.chat(t, streamReq("pair", false))); p != "mock-a" {
		t.Errorf("the alias default (priority) served by %s, want mock-a", p)
	}
}
