package obs

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metric label values.
const (
	// NoProvider labels a request that never reached a provider.
	NoProvider = "none"
	// UnknownModel labels a request whose model did not resolve. Clients
	// choose that string, so it must not become a label value.
	UnknownModel = "unknown"
	// ResultRejected labels a request refused before any upstream call.
	ResultRejected = "rejected"
)

// overheadBuckets resolve the gateway's own time, which is far below request
// latency: 100µs to 1s.
var overheadBuckets = []float64{.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1}

// ttftBuckets span fast mocks to slow models: 10ms to 30s.
var ttftBuckets = []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}

// Metrics holds the gateway's Prometheus metrics (design §4). Every label
// value is bounded by the route table, the set of API keys, or a constant;
// none comes from a client.
type Metrics struct {
	reg *prometheus.Registry

	requests       *prometheus.CounterVec
	inFlight       prometheus.Gauge
	overhead       *prometheus.HistogramVec
	ttft           *prometheus.HistogramVec
	tokens         *prometheus.CounterVec
	cost           *prometheus.CounterVec
	failedAttempts *prometheus.CounterVec

	breakerState   *prometheus.GaugeVec
	breakerChanges *prometheus.CounterVec

	rateLimited     *prometheus.CounterVec
	rateLimitErrors prometheus.Counter

	reservations     *prometheus.CounterVec
	reservationsOpen prometheus.Gauge
	closes           *prometheus.CounterVec

	reconcilePasses  *prometheus.CounterVec
	reconcileOrphans *prometheus.CounterVec
}

// NewMetrics returns Metrics on a registry of their own, with the standard
// Go and process collectors.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		v := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
		reg.MustRegister(v)
		return v
	}
	histogram := func(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
		v := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets}, labels)
		reg.MustRegister(v)
		return v
	}
	gauge := func(name, help string) prometheus.Gauge {
		g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
		reg.MustRegister(g)
		return g
	}

	m := &Metrics{reg: reg}
	m.requests = counter("gateway_requests_total",
		"Chat requests. status is the HTTP status, which stays 200 for a stream that failed partway; result tells those apart.",
		"key", "model", "provider", "status", "result")
	m.inFlight = gauge("gateway_requests_in_flight", "Chat requests being served.")
	m.overhead = histogram("gateway_overhead_seconds",
		"Gateway time per request that reached a provider: total latency minus time spent waiting on upstreams.",
		overheadBuckets, "stream")
	m.ttft = histogram("gateway_ttft_seconds",
		"Time from the upstream call to its first output, for the attempt that served the request.",
		ttftBuckets, "model", "provider")
	m.tokens = counter("gateway_tokens_total", "Tokens accounted for billable requests, by type (prompt, completion).",
		"key", "model", "provider", "type")
	m.cost = counter("gateway_cost_usd_total", "Cost of billable requests in USD. The ledger is exact; this is for dashboards.",
		"key", "model", "provider")
	m.failedAttempts = counter("gateway_failed_attempts_total",
		"Upstream attempts that did not serve the request, whether or not a later one did.", "model")

	m.breakerState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gateway_breaker_state", Help: "Circuit breaker state per target: 0 closed, 1 half open, 2 open.",
	}, []string{"provider", "upstream_model"})
	reg.MustRegister(m.breakerState)
	m.breakerChanges = counter("gateway_breaker_transitions_total", "Circuit breaker state changes, by the state entered.",
		"provider", "upstream_model", "to")

	m.rateLimited = counter("gateway_ratelimit_rejections_total", "Requests refused by rate limits, by error code.", "key", "code")
	m.rateLimitErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gateway_ratelimit_errors_total", Help: "Requests admitted without a limit because Redis failed.",
	})
	reg.MustRegister(m.rateLimitErrors)

	m.reservations = counter("gateway_reservations_total",
		"Budget reservations attempted, by result (reserved, insufficient_budget, error).", "result")
	m.reservationsOpen = gauge("gateway_reservations_open",
		"Reservations this process holds and has not finished closing. Orphans from a crash are not included.")
	m.closes = counter("gateway_reservation_closes_total",
		"Reservations closed by the gateway, by result (settled, released, already_closed, failed) and ledger reason.",
		"result", "reason")

	m.reconcilePasses = counter("gateway_reconciler_passes_total", "Reconciler passes, by result (ok, skipped, error).", "result")
	m.reconcileOrphans = counter("gateway_reconciler_orphans_total",
		"Orphans the reconciler handled, by action (settled, released_no_call, released_unknown_usage, already_closed, failed). released_unknown_usage is an undercharge.",
		"action")
	return m
}

// Handler serves the metrics in Prometheus format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// Gatherer exposes the registry, for tests.
func (m *Metrics) Gatherer() prometheus.Gatherer { return m.reg }

// RequestStarted tracks a chat request and returns the function that ends it.
func (m *Metrics) RequestStarted() func() {
	m.inFlight.Inc()
	return m.inFlight.Dec
}

// Request counts a finished chat request.
func (m *Metrics) Request(key, model, provider string, status int, result string) {
	m.requests.WithLabelValues(key, model, provider, strconv.Itoa(status), result).Inc()
}

// Overhead records the gateway's own time on a request that reached a provider.
func (m *Metrics) Overhead(d time.Duration, stream bool) {
	m.overhead.WithLabelValues(strconv.FormatBool(stream)).Observe(max(d, 0).Seconds())
}

// TTFT records the serving attempt's time to first output.
func (m *Metrics) TTFT(d time.Duration, model, provider string) {
	m.ttft.WithLabelValues(model, provider).Observe(d.Seconds())
}

// Usage adds a billable request's tokens and cost.
func (m *Metrics) Usage(key, model, provider string, prompt, completion int, costUSD float64) {
	m.tokens.WithLabelValues(key, model, provider, "prompt").Add(float64(max(prompt, 0)))
	m.tokens.WithLabelValues(key, model, provider, "completion").Add(float64(max(completion, 0)))
	m.cost.WithLabelValues(key, model, provider).Add(costUSD)
}

// FailedAttempts adds upstream attempts that did not serve a request.
func (m *Metrics) FailedAttempts(model string, n int) {
	if n > 0 {
		m.failedAttempts.WithLabelValues(model).Add(float64(n))
	}
}

// BreakerChanged records a breaker's state, named as BreakerState.String does.
// A target's first report, with from equal to to, only sets the gauge.
func (m *Metrics) BreakerChanged(provider, model, from, to string) {
	value := 0.0
	switch to {
	case "half_open":
		value = 1
	case "open":
		value = 2
	}
	m.breakerState.WithLabelValues(provider, model).Set(value)
	if from != to {
		m.breakerChanges.WithLabelValues(provider, model, to).Inc()
	}
}

// RateLimited counts a request refused by a rate limit.
func (m *Metrics) RateLimited(key, code string) { m.rateLimited.WithLabelValues(key, code).Inc() }

// RateLimitError counts a request admitted because Redis failed.
func (m *Metrics) RateLimitError() { m.rateLimitErrors.Inc() }

// Reservation counts a reservation attempt, and tracks a successful one as
// open until ReservationClosed.
func (m *Metrics) Reservation(result string) {
	m.reservations.WithLabelValues(result).Inc()
	if result == "reserved" {
		m.reservationsOpen.Inc()
	}
}

// ReservationClosed counts the gateway's attempt to close a reservation. A
// failed one is left to the reconciler, so this process stops tracking it.
func (m *Metrics) ReservationClosed(result, reason string) {
	m.closes.WithLabelValues(result, reason).Inc()
	m.reservationsOpen.Dec()
}

// ReconcilePass counts a reconciler pass.
func (m *Metrics) ReconcilePass(result string) { m.reconcilePasses.WithLabelValues(result).Inc() }

// ReconcileOrphans adds orphans the reconciler handled one way.
func (m *Metrics) ReconcileOrphans(action string, n int) {
	if n > 0 {
		m.reconcileOrphans.WithLabelValues(action).Add(float64(n))
	}
}
