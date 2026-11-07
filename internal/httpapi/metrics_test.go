package httpapi

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/SamirVarma01/llm-gateway/internal/ledger"
	"github.com/SamirVarma01/llm-gateway/internal/mock"
	"github.com/SamirVarma01/llm-gateway/internal/ratelimit"
	"github.com/SamirVarma01/llm-gateway/internal/router"
)

// series returns the samples of a metric whose labels include want.
func (e *testEnv) series(t *testing.T, name string, want map[string]string) []*dto.Metric {
	t.Helper()
	families, err := e.srv.opts.Metrics.Gatherer().Gather()
	if err != nil {
		t.Fatal(err)
	}
	var out []*dto.Metric
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
	metric:
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			for k, v := range want {
				if labels[k] != v {
					continue metric
				}
			}
			out = append(out, m)
		}
	}
	return out
}

// value sums the counters or gauges of a metric whose labels include want.
func (e *testEnv) value(t *testing.T, name string, want map[string]string) float64 {
	t.Helper()
	total := 0.0
	for _, m := range e.series(t, name, want) {
		total += m.GetCounter().GetValue() + m.GetGauge().GetValue()
	}
	return total
}

func (e *testEnv) histogram(t *testing.T, name string, want map[string]string) (count uint64, sum float64) {
	t.Helper()
	for _, m := range e.series(t, name, want) {
		count += m.GetHistogram().GetSampleCount()
		sum += m.GetHistogram().GetSampleSum()
	}
	return count, sum
}

func (e *testEnv) wantValue(t *testing.T, name string, labels map[string]string, want float64) {
	t.Helper()
	if got := e.value(t, name, labels); got != want {
		t.Errorf("%s%v = %v, want %v", name, labels, got, want)
	}
}

type labels = map[string]string

func TestRequestMetrics(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 5}, nil)
	resp := e.chat(t, chatReq("chat", nil))
	readBody(t, resp, false)
	o := e.outcome(t)

	e.wantValue(t, "gateway_requests_total",
		labels{"model": "chat", "provider": "mock-a", "status": "200", "result": "completed"}, 1)
	e.wantValue(t, "gateway_tokens_total", labels{"model": "chat", "provider": "mock-a", "type": "prompt"}, float64(o.Usage.PromptTokens))
	e.wantValue(t, "gateway_tokens_total", labels{"type": "completion"}, 5)
	cost := float64(ledger.Cost(o.Usage.PromptTokens, 5, smallPrice)) / 1e9
	e.wantValue(t, "gateway_cost_usd_total", labels{"model": "chat", "provider": "mock-a"}, cost)
	e.wantValue(t, "gateway_requests_in_flight", nil, 0)
	e.wantValue(t, "gateway_reservations_total", labels{"result": "reserved"}, 1)
	e.wantValue(t, "gateway_reservation_closes_total", labels{"result": "settled", "reason": "completed"}, 1)
	e.wantValue(t, "gateway_reservations_open", nil, 0)
	if n, _ := e.histogram(t, "gateway_ttft_seconds", labels{"model": "chat", "provider": "mock-a"}); n != 1 {
		t.Errorf("ttft samples = %d", n)
	}
	if n, _ := e.histogram(t, "gateway_overhead_seconds", labels{"stream": "false"}); n != 1 {
		t.Errorf("overhead samples = %d", n)
	}
}

func TestInterruptedStreamMetrics(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 10, FailAfterTokens: 3}, nil)
	readClientStream(e.chat(t, streamReq("chat", false)).Body)
	e.outcome(t)
	// The client saw a 200, so only result shows this failed.
	e.wantValue(t, "gateway_requests_total", labels{"status": "200", "result": "upstream_error"}, 1)
	e.wantValue(t, "gateway_reservation_closes_total", labels{"result": "settled", "reason": "interrupted"}, 1)
	if n, _ := e.histogram(t, "gateway_overhead_seconds", labels{"stream": "true"}); n != 1 {
		t.Errorf("overhead samples = %d", n)
	}
}

func TestFailoverMetrics(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 2}, nil)
	setBehavior(t, e, "mock-a", mock.Behavior{ErrorRate: 1, ErrorStatus: 503})
	readBody(t, e.chat(t, chatReq("pair", nil)), false)
	e.outcome(t)
	e.wantValue(t, "gateway_requests_total", labels{"model": "pair", "provider": "mock-b", "status": "200"}, 1)
	e.wantValue(t, "gateway_failed_attempts_total", labels{"model": "pair"}, 1)

	// Nothing commits: every attempt failed, and nothing is charged.
	setBehavior(t, e, "mock-b", mock.Behavior{ErrorRate: 1, ErrorStatus: 503})
	resp := e.chat(t, chatReq("pair", nil))
	_, _ = io.Copy(io.Discard, resp.Body)
	e.outcome(t)
	e.wantValue(t, "gateway_requests_total", labels{"model": "pair", "status": "502", "result": "upstream_error"}, 1)
	e.wantValue(t, "gateway_failed_attempts_total", labels{"model": "pair"}, 1+3)
	e.wantValue(t, "gateway_reservation_closes_total", labels{"result": "released", "reason": "failed"}, 1)
	if n, _ := e.histogram(t, "gateway_ttft_seconds", labels{"provider": "mock-a"}); n != 0 {
		t.Errorf("ttft samples for a target that never produced output: %d", n)
	}
}

// Overhead is the gateway's own time: a slow provider must not raise it.
func TestOverheadExcludesUpstreamTime(t *testing.T) {
	const ttft = 300 * time.Millisecond
	for _, stream := range []bool{false, true} {
		e := newEnv(t, mock.Behavior{OutputTokens: 5, TokensPerSec: 50, TTFTMs: int(ttft.Milliseconds())}, nil)
		req := chatReq("chat", nil)
		req.Stream = stream
		start := time.Now()
		readBody(t, e.chat(t, req), stream)
		if el := time.Since(start); el < ttft {
			t.Fatalf("request took %v, want at least the %v TTFT", el, ttft)
		}
		o := e.outcome(t)
		if o.Overhead <= 0 || o.Overhead > ttft/3 {
			t.Errorf("stream=%v: overhead = %v, want a small part of a request over %v", stream, o.Overhead, ttft)
		}
	}
}

// Waiting out a dead target and backing off are upstream time too.
func TestOverheadExcludesFailover(t *testing.T) {
	e := newEnvTweaked(t, mock.Behavior{OutputTokens: 2}, envTweak{router: func(o *router.Options) {
		o.Retry.TTFTTimeout = 200 * time.Millisecond
	}})
	setBehavior(t, e, "mock-a", mock.Behavior{Hang: true})
	readBody(t, e.chat(t, chatReq("pair", nil)), false)
	if o := e.outcome(t); o.Overhead <= 0 || o.Overhead > 100*time.Millisecond {
		t.Errorf("overhead = %v, want it to exclude the 200ms spent on the dead target", o.Overhead)
	}
}

func TestRejectionMetrics(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 2}, nil)
	policy := http.Header{headerRoutePolicy: {"random"}}

	// A client chooses these strings, so they must not become label values.
	for i := range 3 {
		resp := e.chat(t, chatReq("no-such-model-"+strings.Repeat("x", i), nil))
		wantError(t, resp, http.StatusNotFound, "model_not_found")
	}
	wantError(t, e.chat(t, `{"model":`), http.StatusBadRequest, "invalid_json")
	wantError(t, e.chat(t, chatReq("chat", nil), policy), http.StatusBadRequest, "invalid_route_policy")

	e.wantValue(t, "gateway_requests_total", labels{"model": "unknown", "provider": "none", "status": "404", "result": "rejected"}, 3)
	e.wantValue(t, "gateway_requests_total", labels{"model": "unknown", "status": "400"}, 1)
	e.wantValue(t, "gateway_requests_total", labels{"model": "chat", "status": "400"}, 1)
	if n := len(e.series(t, "gateway_requests_total", labels{"status": "404"})); n != 1 {
		t.Errorf("%d series for unknown models, want 1", n)
	}

	e.limiter.decision = &ratelimit.Decision{RetryAfter: time.Second}
	wantError(t, e.chat(t, chatReq("chat", nil)), http.StatusTooManyRequests, "rate_limit_exceeded")
	e.limiter.decision = &ratelimit.Decision{TooLarge: true}
	wantError(t, e.chat(t, chatReq("chat", nil)), http.StatusTooManyRequests, "tokens_exceed_limit")
	e.wantValue(t, "gateway_ratelimit_rejections_total", labels{"code": "rate_limit_exceeded"}, 1)
	e.wantValue(t, "gateway_ratelimit_rejections_total", labels{"code": "tokens_exceed_limit"}, 1)
	e.wantValue(t, "gateway_requests_total", labels{"model": "chat", "status": "429", "result": "rejected"}, 2)

	e.limiter.decision = nil
	e.ledger.reserveErr = ledger.ErrInsufficientBudget
	wantError(t, e.chat(t, chatReq("chat", nil)), http.StatusPaymentRequired, "insufficient_budget")
	e.ledger.reserveErr = errors.New("connection refused")
	wantError(t, e.chat(t, chatReq("chat", nil)), http.StatusServiceUnavailable, "ledger_unavailable")
	e.wantValue(t, "gateway_reservations_total", labels{"result": "insufficient_budget"}, 1)
	e.wantValue(t, "gateway_reservations_total", labels{"result": "error"}, 1)
	e.wantValue(t, "gateway_reservations_open", nil, 0)

	e.ledger.reserveErr = nil
	e.limiter.err = errors.New("redis is down")
	readBody(t, e.chat(t, chatReq("chat", nil)), false)
	e.outcome(t)
	e.wantValue(t, "gateway_ratelimit_errors_total", nil, 1)
}

func TestClosesThatFailAreCounted(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 2}, func(o *Options) { o.RequestTimeout = 200 * time.Millisecond; o.LedgerTimeout = 50 * time.Millisecond })
	e.ledger.settleErrs = -1 // never succeeds, so the close runs out of time
	readBody(t, e.chat(t, chatReq("chat", nil)), false)
	e.outcome(t)
	e.wantValue(t, "gateway_reservation_closes_total", labels{"result": "failed"}, 1)
	e.wantValue(t, "gateway_reservations_open", nil, 0)
}

func TestMetricsCarryTheKeyID(t *testing.T) {
	var id string
	e := newEnv(t, mock.Behavior{OutputTokens: 2}, func(o *Options) { id = o.Auth.(staticAuth).key.ID.String() })
	readBody(t, e.chat(t, chatReq("chat", nil)), false)
	e.outcome(t)
	e.wantValue(t, "gateway_requests_total", labels{"key": id}, 1)
	e.wantValue(t, "gateway_tokens_total", labels{"key": id, "type": "completion"}, 2)
}
