// Command failover_test measures failover under load. It sends fixed-rate
// traffic to an alias with a primary and a backup target, makes the primary
// fail, and reports how long the gateway took to stop sending requests to
// it, what clients saw in the meantime, and how long traffic took to return
// once the primary recovered (design §6.3, §6.4).
//
// It reads breaker state from the gateway's /metrics, which Compose does not
// publish, so it runs inside the Compose network: scripts/bench.sh failover.
// It writes its results as JSON to stdout and progress to stderr; -render
// prints a results file as Markdown.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/SamirVarma01/llm-gateway/internal/mock"
	"github.com/SamirVarma01/llm-gateway/scripts/internal/bench"
)

// Results is the output of a run.
type Results struct {
	Meta      bench.Meta `json:"meta"`
	Scenarios []Scenario `json:"scenarios"`
}

// Scenario is one fault, injected and then removed. Times are milliseconds
// from the moment the fault was injected.
type Scenario struct {
	Name  string `json:"name"`
	Fault string `json:"fault"`

	BreakerOpenMs   *float64 `json:"breaker_open_ms,omitempty"`   // first poll that saw the primary's breaker open
	ClearedMs       float64  `json:"cleared_ms"`                  // the fault was removed
	BreakerClosedMs *float64 `json:"breaker_closed_ms,omitempty"` // first poll after the clear that saw the breaker closed

	States      []StateChange  `json:"states"`      // breaker states as polled
	Transitions map[string]int `json:"transitions"` // from gateway_breaker_transitions_total, by new state; polling can miss quick flips
	Phases      []Phase        `json:"phases"`
}

// StateChange is a breaker state seen by polling.
type StateChange struct {
	AtMs  float64 `json:"at_ms"`
	State string  `json:"state"`
}

// Phase summarizes the requests sent during one part of a scenario.
type Phase struct {
	Name         string         `json:"name"`
	FromMs       float64        `json:"from_ms"`
	ToMs         float64        `json:"to_ms"`
	Requests     int            `json:"requests"`
	Errors       int            `json:"errors"`
	StreamErrors int            `json:"stream_errors"`
	ErrorKinds   map[string]int `json:"error_kinds,omitempty"`
	ServedBy     map[string]int `json:"served_by"` // successful responses by X-Gateway-Provider
	Retried      int            `json:"retried"`   // successful responses that took more than one attempt
	Latency      bench.Dist     `json:"latency"`   // every response, failed or not
}

type config struct {
	gateway, admin, adminToken, metrics, mock string
	alias, primary, backup, upstreamModel     string
	healthy                                   mock.Behavior
	rate                                      float64
	before, fault, after, maxRecovery, poll   time.Duration
	scenarios                                 []string
}

// faults are the ways the primary fails.
var faults = map[string]struct {
	desc  string
	apply func(mock.Behavior) mock.Behavior
}{
	"errors": {"every request fails with HTTP 500 before any output", func(b mock.Behavior) mock.Behavior {
		b.ErrorRate, b.ErrorStatus = 1, http.StatusInternalServerError
		return b
	}},
	"hang": {"requests are accepted and never answered", func(b mock.Behavior) mock.Behavior {
		b.Hang = true
		return b
	}},
	"midstream": {"every response is cut off after 4 tokens of output", func(b mock.Behavior) mock.Behavior {
		b.FailAfterTokens, b.FailMode = 4, mock.FailAbort
		return b
	}},
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("failover_test: ")
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	var c config
	flag.StringVar(&c.gateway, "gateway", "http://gateway:8080", "gateway public URL")
	flag.StringVar(&c.admin, "admin", "http://gateway:8081", "gateway admin URL")
	flag.StringVar(&c.adminToken, "admin-token", envOr("ADMIN_TOKEN", "dev-admin-token"), "admin token")
	flag.StringVar(&c.metrics, "metrics", "http://gateway:9090/metrics", "gateway metrics URL")
	flag.StringVar(&c.mock, "mock", "http://mock:9000", "mock provider URL")
	flag.StringVar(&c.alias, "model", "failover", "gateway alias: -primary first, then -backup")
	flag.StringVar(&c.primary, "primary", "primary", "mock provider that fails")
	flag.StringVar(&c.backup, "backup", "backup", "mock provider that stays healthy")
	flag.StringVar(&c.upstreamModel, "upstream-model", "m-fo", "upstream model of both targets")
	ttft := flag.Duration("ttft", 50*time.Millisecond, "mock time to first token")
	flag.Float64Var(&c.healthy.TokensPerSec, "tps", 100, "mock tokens per second")
	flag.IntVar(&c.healthy.OutputTokens, "tokens", 16, "mock output tokens per response")
	flag.Float64Var(&c.rate, "rate", 50, "requests per second, alternating streaming and non-streaming")
	flag.DurationVar(&c.before, "before", 10*time.Second, "healthy load before the fault")
	flag.DurationVar(&c.fault, "fault", 30*time.Second, "how long the fault lasts")
	flag.DurationVar(&c.after, "after", 5*time.Second, "load kept up after the breaker closes again")
	flag.DurationVar(&c.maxRecovery, "max-recovery", 2*time.Minute, "give up waiting for the breaker to close after this long")
	flag.DurationVar(&c.poll, "poll", 50*time.Millisecond, "breaker state polling interval")
	scenarios := flag.String("scenarios", "errors,hang,midstream", "faults to inject, in order")
	render := flag.String("render", "", "print this results file as Markdown, then exit")
	flag.Parse()

	if *render != "" {
		return renderFile(*render, os.Stdout)
	}
	c.healthy.TTFTMs = int(ttft.Milliseconds())
	for _, s := range strings.Split(*scenarios, ",") {
		if _, ok := faults[s]; !ok {
			return fmt.Errorf("-scenarios: unknown fault %q", s)
		}
		c.scenarios = append(c.scenarios, s)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	client := bench.NewHTTPClient()
	setupCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if err := bench.WaitReady(setupCtx, client, c.gateway+"/readyz"); err != nil {
		return err
	}
	admin := bench.Admin{URL: c.admin, Token: c.adminToken, Client: client}
	key, err := admin.CreateKey(setupCtx, "failover-test", bench.HighLimits())
	if err != nil {
		return fmt.Errorf("create key: %w", err)
	}

	res := Results{Meta: bench.NewMeta("failover_test", map[string]string{
		"rate":        fmt.Sprint(c.rate),
		"mock_ttft":   ttft.String(),
		"mock_tps":    fmt.Sprint(c.healthy.TokensPerSec),
		"mock_tokens": fmt.Sprint(c.healthy.OutputTokens),
		"before":      c.before.String(),
		"fault":       c.fault.String(),
		"poll":        c.poll.String(),
		"primary":     c.primary,
		"backup":      c.backup,
	})}
	for _, name := range c.scenarios {
		log.Printf("scenario %s", name)
		sc, err := scenario(ctx, c, client, key.Secret, name)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		res.Scenarios = append(res.Scenarios, sc)
		time.Sleep(3 * time.Second)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(res)
}

// Phase names the report looks up.
const (
	phaseBefore    = "before the fault"
	phaseDetecting = "fault, until the breaker opened"
)

// breakerStates are the values of gateway_breaker_state.
var breakerStates = []string{"closed", "half_open", "open"}

func scenario(ctx context.Context, c config, client *http.Client, key, name string) (Scenario, error) {
	sc := Scenario{Name: name, Fault: faults[name].desc}
	for _, p := range []string{c.primary, c.backup} {
		if err := bench.SetBehavior(ctx, client, c.mock, p, c.healthy); err != nil {
			return sc, err
		}
	}
	stateKey := fmt.Sprintf(`gateway_breaker_state{provider=%q,upstream_model=%q}`, c.primary, c.upstreamModel)
	m, err := bench.Scrape(ctx, client, c.metrics)
	if err != nil {
		return sc, fmt.Errorf("metrics: %w", err)
	}
	if st, ok := m[stateKey]; !ok || st != 0 {
		return sc, fmt.Errorf("the primary's breaker must start closed; %s = %v (present %t)", stateKey, st, ok)
	}
	transitionsBefore := transitions(m, c)

	// Load runs until stopped; the time it needs depends on how fast the
	// breaker recovers.
	bodies := map[bool][]byte{}
	for _, stream := range []bool{false, true} {
		req := bench.ChatRequest{
			Model:     c.alias,
			Messages:  []bench.Message{{Role: "user", Content: "Describe a circuit breaker in two sentences."}},
			MaxTokens: c.healthy.OutputTokens * 2,
			Stream:    stream,
		}
		if stream {
			req.StreamOptions = &bench.StreamOptions{IncludeUsage: true}
		}
		bodies[stream], _ = json.Marshal(req)
	}
	send := func(ctx context.Context, seq int) bench.Result {
		stream := seq%2 == 1
		rctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		return bench.Chat(rctx, client, c.gateway+"/v1/chat/completions", key, bodies[stream], stream)
	}
	stopLoad := make(chan struct{})
	runc := make(chan bench.Run, 1)
	go func() {
		runc <- bench.Generate(ctx, bench.Load{Rate: c.rate, Duration: c.before + c.fault + c.maxRecovery + c.after, Stop: stopLoad}, send)
	}()
	poller := newPoller(client, c.metrics, stateKey)
	pollCtx, stopPoll := context.WithCancel(ctx)
	pollDone := make(chan struct{})
	go func() { defer close(pollDone); poller.run(pollCtx, c.poll) }()
	defer func() { stopPoll(); <-pollDone }()

	if err := sleep(ctx, c.before); err != nil {
		return sc, err
	}
	if err := bench.SetBehavior(ctx, client, c.mock, c.primary, faults[name].apply(c.healthy)); err != nil {
		return sc, err
	}
	t0 := time.Now()
	log.Printf("  fault injected")
	if err := sleep(ctx, c.fault); err != nil {
		return sc, err
	}
	if err := bench.SetBehavior(ctx, client, c.mock, c.primary, c.healthy); err != nil {
		return sc, err
	}
	t1 := time.Now()
	log.Printf("  fault removed; waiting for the breaker to close")
	closedAt, ok := poller.waitClosed(ctx, t1, c.maxRecovery)
	if !ok {
		log.Printf("  the breaker did not close within %s", c.maxRecovery)
	}
	if err := sleep(ctx, c.after); err != nil {
		return sc, err
	}
	close(stopLoad)
	run := <-runc
	stopPoll()
	<-pollDone

	m, err = bench.Scrape(ctx, client, c.metrics)
	if err != nil {
		return sc, fmt.Errorf("metrics: %w", err)
	}
	sc.Transitions = map[string]int{}
	for to, n := range transitions(m, c) {
		if d := n - transitionsBefore[to]; d > 0 {
			sc.Transitions[to] = d
		}
	}
	rel := func(t time.Time) float64 { return bench.Ms(t.Sub(t0)) }
	sc.ClearedMs = rel(t1)
	for _, s := range poller.changes() {
		sc.States = append(sc.States, StateChange{AtMs: rel(s.at), State: s.state})
		if s.state == "open" && sc.BreakerOpenMs == nil && s.at.After(t0) {
			v := rel(s.at)
			sc.BreakerOpenMs = &v
		}
	}
	if ok {
		v := rel(closedAt)
		sc.BreakerClosedMs = &v
	}
	analyze(&sc, run.Results, t0, t1, closedAt, ok)
	log.Printf("  %d requests; breaker open %s, closed again %s after the fix", len(run.Results),
		secs(sc.BreakerOpenMs), secsFrom(sc.BreakerClosedMs, sc.ClearedMs))
	return sc, nil
}

// analyze splits requests into phases by when they were sent.
func analyze(sc *Scenario, results []bench.Result, t0, t1, closedAt time.Time, closed bool) {
	rel := func(t time.Time) float64 { return bench.Ms(t.Sub(t0)) }
	// Phases cover every request: the first starts with the first request
	// sent, and the last ends after the last one.
	first, last := 0.0, 0.0
	for i, r := range results {
		at := rel(r.Intended)
		if i == 0 || at < first {
			first = at
		}
		last = max(last, at)
	}
	end := last + 1
	type bound struct {
		name     string
		from, to float64
	}
	cleared := rel(t1)
	bounds := []bound{{phaseBefore, first, 0}}
	if sc.BreakerOpenMs != nil {
		bounds = append(bounds,
			bound{phaseDetecting, 0, *sc.BreakerOpenMs},
			bound{"fault, breaker open", *sc.BreakerOpenMs, cleared})
	} else {
		bounds = append(bounds, bound{"fault (the breaker never opened)", 0, cleared})
	}
	if closed {
		bounds = append(bounds,
			bound{"fixed, until the breaker closed", cleared, rel(closedAt)},
			bound{"after the breaker closed", rel(closedAt), end})
	} else {
		bounds = append(bounds, bound{"fixed (the breaker never closed)", cleared, end})
	}
	for _, b := range bounds {
		ph := Phase{Name: b.name, FromMs: b.from, ToMs: b.to, ServedBy: map[string]int{}}
		var lat []time.Duration
		for _, r := range results {
			if at := rel(r.Intended); at < b.from || at >= b.to {
				continue
			}
			ph.Requests++
			lat = append(lat, r.Latency())
			if !r.OK {
				ph.Errors++
				if r.Stream {
					ph.StreamErrors++
				}
				if ph.ErrorKinds == nil {
					ph.ErrorKinds = map[string]int{}
				}
				ph.ErrorKinds[r.Err]++
				continue
			}
			ph.ServedBy[r.Provider]++
			if r.Attempts > 1 {
				ph.Retried++
			}
		}
		ph.Latency = bench.Summarize(lat)
		sc.Phases = append(sc.Phases, ph)
	}
}

func transitions(m map[string]float64, c config) map[string]int {
	out := map[string]int{}
	for _, to := range breakerStates {
		k := fmt.Sprintf(`gateway_breaker_transitions_total{provider=%q,to=%q,upstream_model=%q}`, c.primary, to, c.upstreamModel)
		out[to] = int(m[k])
	}
	return out
}

// poller records the primary's breaker state.
type poller struct {
	client      *http.Client
	url, series string

	mu      sync.Mutex
	samples []sample // state changes only
}

type sample struct {
	at    time.Time
	state string
}

func newPoller(client *http.Client, url, series string) *poller {
	return &poller{client: client, url: url, series: series}
}

func (p *poller) run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		m, err := bench.Scrape(ctx, p.client, p.url)
		at := time.Now()
		if err == nil {
			if v, ok := m[p.series]; ok && int(v) >= 0 && int(v) < len(breakerStates) {
				p.record(at, breakerStates[int(v)])
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (p *poller) record(at time.Time, state string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if n := len(p.samples); n == 0 || p.samples[n-1].state != state {
		p.samples = append(p.samples, sample{at, state})
	}
}

func (p *poller) changes() []sample {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]sample(nil), p.samples...)
}

// waitClosed waits until the breaker is closed, and reports when it closed:
// since itself if it was already closed then.
func (p *poller) waitClosed(ctx context.Context, since time.Time, limit time.Duration) (time.Time, bool) {
	deadline := time.Now().Add(limit)
	for {
		p.mu.Lock()
		var last sample
		if n := len(p.samples); n > 0 {
			last = p.samples[n-1]
		}
		p.mu.Unlock()
		if last.state == "closed" {
			if last.at.Before(since) {
				return since, true
			}
			return last.at, true
		}
		if time.Now().After(deadline) || sleep(ctx, 20*time.Millisecond) != nil {
			return time.Time{}, false
		}
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}
