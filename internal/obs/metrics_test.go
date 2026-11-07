package obs

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestMetricsExposition(t *testing.T) {
	m := NewMetrics()
	m.Request("k1", "chat", "mock-a", 200, "completed")
	m.Overhead(2*time.Millisecond, true)
	m.Overhead(-time.Millisecond, true) // a clock step must not record a negative time
	m.Usage("k1", "chat", "mock-a", 7, -1, 0.5)
	out := scrape(t, m)
	for _, want := range []string{
		`gateway_requests_total{key="k1",model="chat",provider="mock-a",result="completed",status="200"} 1`,
		`gateway_overhead_seconds_count{stream="true"} 2`,
		`gateway_overhead_seconds_bucket{stream="true",le="0.0025"} 2`,
		`gateway_tokens_total{key="k1",model="chat",provider="mock-a",type="prompt"} 7`,
		`gateway_tokens_total{key="k1",model="chat",provider="mock-a",type="completion"} 0`,
		`gateway_cost_usd_total{key="k1",model="chat",provider="mock-a"} 0.5`,
		`go_goroutines`,
		`process_cpu_seconds_total`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("exposition lacks %s", want)
		}
	}
}

func TestBreakerMetrics(t *testing.T) {
	m := NewMetrics()
	m.BreakerChanged("p", "m", "closed", "closed") // initial report
	out := scrape(t, m)
	if !strings.Contains(out, `gateway_breaker_state{provider="p",upstream_model="m"} 0`) {
		t.Errorf("no initial state:\n%s", out)
	}
	if strings.Contains(out, `gateway_breaker_transitions_total{`) {
		t.Error("an initial report is not a transition")
	}

	m.BreakerChanged("p", "m", "closed", "open")
	if !strings.Contains(scrape(t, m), `gateway_breaker_state{provider="p",upstream_model="m"} 2`) {
		t.Error("open should be 2")
	}
	m.BreakerChanged("p", "m", "open", "half_open")
	out = scrape(t, m)
	if !strings.Contains(out, `gateway_breaker_state{provider="p",upstream_model="m"} 1`) {
		t.Error("half open should be 1")
	}
	if !strings.Contains(out, `gateway_breaker_transitions_total{provider="p",to="open",upstream_model="m"} 1`) {
		t.Errorf("transition not counted:\n%s", out)
	}
}

func TestRequestStartedTracksInFlight(t *testing.T) {
	m := NewMetrics()
	done := m.RequestStarted()
	if !strings.Contains(scrape(t, m), "gateway_requests_in_flight 1") {
		t.Error("in flight should be 1")
	}
	done()
	if !strings.Contains(scrape(t, m), "gateway_requests_in_flight 0") {
		t.Error("in flight should be 0")
	}
}
