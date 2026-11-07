package obs

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
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

// A rare event must be visible to rate and increase the first time it
// happens, which needs its series to exist at 0 beforehand.
func TestRareEventsStartAtZero(t *testing.T) {
	out := scrape(t, NewMetrics())
	for _, want := range []string{
		`gateway_reconciler_orphans_total{action="released_unknown_usage"} 0`,
		`gateway_reconciler_orphans_total{action="failed"} 0`,
		`gateway_reservation_closes_total{reason="completed",result="failed"} 0`,
		`gateway_reservations_total{result="error"} 0`,
		`gateway_ratelimit_rejections_total{code="rate_limit_exceeded"} 0`,
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

// TestDashboardQueriesRealMetrics keeps the provisioned dashboard in step with
// the code: a renamed metric would otherwise leave a panel quietly empty.
func TestDashboardQueriesRealMetrics(t *testing.T) {
	m := NewMetrics()
	m.Request("k", "m", "p", 200, "completed")
	m.Overhead(time.Millisecond, false)
	m.TTFT(time.Millisecond, "m", "p")
	m.Usage("k", "m", "p", 1, 1, 1)
	m.FailedAttempts("m", 1)
	m.BreakerChanged("p", "m", "closed", "open")
	m.RateLimited("rate_limit_exceeded")
	m.RateLimitError()
	m.Reservation("reserved")
	m.ReservationClosed("settled", "completed")
	m.ReconcilePass("ok")
	m.ReconcileOrphans("settled", 1)
	families, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatal(err)
	}
	exists := map[string]bool{}
	for _, f := range families {
		exists[f.GetName()] = true
	}

	raw, err := os.ReadFile("../../deploy/grafana/dashboards/gateway.json")
	if err != nil {
		t.Fatal(err)
	}
	var dash struct {
		Panels []struct {
			Title   string `json:"title"`
			Targets []struct {
				Expr string `json:"expr"`
			} `json:"targets"`
		} `json:"panels"`
	}
	if err := json.Unmarshal(raw, &dash); err != nil {
		t.Fatal(err)
	}
	name := regexp.MustCompile(`gateway_[a-z_]+`)
	used := map[string]bool{}
	for _, p := range dash.Panels {
		for _, target := range p.Targets {
			found := name.FindAllString(target.Expr, -1)
			if len(found) == 0 {
				t.Errorf("panel %q queries no gateway metric: %s", p.Title, target.Expr)
			}
			for _, n := range found {
				n = strings.TrimSuffix(n, "_bucket")
				used[n] = true
				if !exists[n] {
					t.Errorf("panel %q queries %s, which does not exist", p.Title, n)
				}
			}
		}
	}
	for n := range exists {
		if strings.HasPrefix(n, "gateway_") && !used[n] {
			t.Logf("not on the dashboard: %s", n)
		}
	}
}
