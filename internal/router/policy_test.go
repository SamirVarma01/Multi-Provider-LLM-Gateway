package router

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/SamirVarma01/llm-gateway/internal/config"
	"github.com/SamirVarma01/llm-gateway/internal/provider"
)

// fakeProvider is a scripted provider for router tests.
type fakeProvider struct {
	name    string
	decline error // returned by Supports
	stream  func(ctx context.Context, n int) (provider.Stream, error)
}

func (p *fakeProvider) Name() string                         { return p.name }
func (p *fakeProvider) Supports(*provider.ChatRequest) error { return p.decline }
func (p *fakeProvider) Stream(ctx context.Context, _ provider.Target, _ *provider.ChatRequest) (provider.Stream, error) {
	if p.stream == nil {
		return nil, errors.New("fakeProvider: no stream configured")
	}
	return p.stream(ctx, provider.Attempt(ctx))
}

const policyRoutes = `
providers:
  - { name: a, format: openai, base_url: "http://x/a/v1" }
  - { name: b, format: openai, base_url: "http://x/b/v1" }
  - { name: c, format: openai, base_url: "http://x/c/v1" }
models:
  - alias: m
    default_max_output: 100
    candidates:
      - { provider: a, model: ma, input_per_mtok: 1, output_per_mtok: 10, priority: 1, max_output: 1000 }
      - { provider: b, model: mb, input_per_mtok: 5, output_per_mtok: 2, priority: 2, max_output: 50 }
      - { provider: c, model: mc, input_per_mtok: 5, output_per_mtok: 2, priority: 3, max_output: 50 }
`

func newTestRouter(t *testing.T, routesYAML string, opts Options) (*Router, map[string]*fakeProvider) {
	t.Helper()
	routes, err := config.ParseRoutes([]byte(routesYAML))
	if err != nil {
		t.Fatal(err)
	}
	fakes := map[string]*fakeProvider{}
	providers := map[string]provider.Provider{}
	for _, p := range routes.Providers {
		f := &fakeProvider{name: p.Name}
		fakes[p.Name] = f
		providers[p.Name] = f
	}
	r, err := New(routes, providers, opts)
	if err != nil {
		t.Fatal(err)
	}
	return r, fakes
}

func chatRequest(stream bool) *provider.ChatRequest {
	return &provider.ChatRequest{
		Model:    "m",
		Messages: []provider.Message{{Role: provider.RoleUser, Content: []provider.ContentPart{{Type: provider.PartText, Text: "hi"}}}},
		Stream:   stream,
	}
}

func order(p *Plan) string {
	var names []string
	for _, c := range p.Choices {
		names = append(names, c.Target.Provider)
	}
	return strings.Join(names, ",")
}

func mustPlan(t *testing.T, r *Router, q Request) *Plan {
	t.Helper()
	m, err := r.Resolve("m")
	if err != nil {
		t.Fatal(err)
	}
	p, err := r.Plan(m, q)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestPlanPriority(t *testing.T) {
	r, _ := newTestRouter(t, policyRoutes, Options{})
	req := chatRequest(true)
	limit := 200
	p := mustPlan(t, r, Request{Chat: req, MaxTokens: &limit})
	if p.Policy != config.PolicyPriority || order(p) != "a,b,c" {
		t.Errorf("policy %q order %s", p.Policy, order(p))
	}
	// Each choice carries its own output cap; the caller's request is untouched.
	if *p.Choices[0].Req.MaxTokens != 200 || *p.Choices[1].Req.MaxTokens != 50 {
		t.Errorf("caps = %d, %d; want 200, 50", *p.Choices[0].Req.MaxTokens, *p.Choices[1].Req.MaxTokens)
	}
	if req.MaxTokens != nil {
		t.Error("Plan modified the caller's request")
	}
}

func TestPlanLeavesOutUnsupportedTargets(t *testing.T) {
	r, fakes := newTestRouter(t, policyRoutes, Options{})
	fakes["a"].decline = errors.New("temperature is not supported")
	p := mustPlan(t, r, Request{Chat: chatRequest(true)})
	if order(p) != "b,c" {
		t.Errorf("order = %s, want b,c", order(p))
	}

	fakes["b"].decline = errors.New("no images")
	fakes["c"].decline = errors.New("no tools")
	m, _ := r.Resolve("m")
	_, err := r.Plan(m, Request{Chat: chatRequest(true)})
	var ue *UnsupportedError
	if !errors.As(err, &ue) || len(ue.Reasons) != 3 {
		t.Fatalf("err = %v, want UnsupportedError with 3 reasons", err)
	}
	for _, want := range []string{`model "m"`, "a: temperature", "b: no images", "c: no tools"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestPlanUnknownPolicy(t *testing.T) {
	r, _ := newTestRouter(t, policyRoutes, Options{})
	m, _ := r.Resolve("m")
	if _, err := r.Plan(m, Request{Chat: chatRequest(true), Policy: "random"}); !errors.Is(err, ErrUnknownPolicy) {
		t.Errorf("err = %v, want ErrUnknownPolicy", err)
	}
}

func TestPlanCheapest(t *testing.T) {
	r, _ := newTestRouter(t, policyRoutes, Options{})
	// a: 1/10 per MTok, b and c: 5/2. With the alias default of 100 output
	// tokens (capped at 50 for b and c):
	//   10 input tokens:   a = 10 + 1000 = 1010, b = c = 50 + 100 = 150
	//   1000 input tokens: a = 1000 + 1000 = 2000, b = c = 5000 + 100 = 5100
	// b and c tie, so priority keeps b ahead.
	if p := mustPlan(t, r, Request{Chat: chatRequest(true), Policy: "cheapest", EstInput: 10}); order(p) != "b,c,a" {
		t.Errorf("short prompt: order = %s, want b,c,a", order(p))
	}
	if p := mustPlan(t, r, Request{Chat: chatRequest(true), Policy: "cheapest", EstInput: 1000}); order(p) != "a,b,c" {
		t.Errorf("long prompt: order = %s, want a,b,c", order(p))
	}
}

func TestPlanFastest(t *testing.T) {
	clk := newFakeClock()
	r, _ := newTestRouter(t, policyRoutes, Options{Now: clk.now})
	m, _ := r.Resolve("m")
	lat := func(i int) *latency { return m.Candidates[i].health.latency }
	q := func(stream bool) Request { return Request{Chat: chatRequest(stream), Policy: "fastest"} }

	// No measurements: every score is unknown, so priority order.
	if p := mustPlan(t, r, q(true)); order(p) != "a,b,c" {
		t.Errorf("no data: order = %s", order(p))
	}

	// a: fast first token, slow decode. b: slower first token, fast decode.
	lat(0).observeTTFT(100*time.Millisecond, clk.now())
	lat(0).observeDecode(11, time.Second, clk.now()) // 10 tok/s
	lat(1).observeTTFT(300*time.Millisecond, clk.now())
	lat(1).observeDecode(1001, time.Second, clk.now()) // 1000 tok/s
	m.output.observe(40)

	// Streaming ranks by TTFT. c has no data, so it gets the best known
	// score (a's) and loses the tie on priority.
	if p := mustPlan(t, r, q(true)); order(p) != "a,c,b" {
		t.Errorf("stream: order = %s, want a,c,b", order(p))
	}
	// Non-streaming adds decode time for the expected 40 tokens:
	// a = 0.1 + 40/10 = 4.1s, b = 0.3 + 40/1000 = 0.34s.
	if p := mustPlan(t, r, q(false)); order(p) != "b,c,a" {
		t.Errorf("non-stream: order = %s, want b,c,a", order(p))
	}

	// After 30s without samples a's and b's numbers are stale too.
	clk.advance(31 * time.Second)
	if p := mustPlan(t, r, q(true)); order(p) != "a,b,c" {
		t.Errorf("stale: order = %s, want priority order", order(p))
	}
}

func TestPlanFastestCapsExpectedOutput(t *testing.T) {
	clk := newFakeClock()
	r, _ := newTestRouter(t, policyRoutes, Options{Now: clk.now})
	m, _ := r.Resolve("m")
	for i, tps := range []int{100, 10} { // a decodes faster than b
		m.Candidates[i].health.latency.observeTTFT(100*time.Millisecond, clk.now())
		m.Candidates[i].health.latency.observeDecode(tps+1, time.Second, clk.now())
	}
	m.Candidates[2].health.latency.observeTTFT(10*time.Second, clk.now())
	m.output.observe(10_000) // responses are usually long

	// a may produce its full cap of 100 tokens: 0.1 + 100/100 = 1.1s.
	// b is capped at 50: 0.1 + 50/10 = 5.1s. c has no decode rate yet, so
	// its score is its 10s TTFT alone.
	if p := mustPlan(t, r, Request{Chat: chatRequest(false), Policy: "fastest"}); order(p) != "a,b,c" {
		t.Errorf("order = %s, want a,b,c", order(p))
	}
}

func TestPlanExplores(t *testing.T) {
	clk := newFakeClock()
	rolls := []float64{0.01, 0.99} // explore; then pick the last healthy non-best target
	r, _ := newTestRouter(t, policyRoutes, Options{
		Now:         clk.now,
		ExploreRate: 0.05,
		Rand: func() float64 {
			v := rolls[0]
			rolls = rolls[1:]
			return v
		},
	})
	p := mustPlan(t, r, Request{Chat: chatRequest(true), Policy: "fastest"})
	if !p.Explored || order(p) != "c,a,b" {
		t.Errorf("explored=%v order=%s, want c,a,b", p.Explored, order(p))
	}

	// Open targets are not explored.
	m, _ := r.Resolve("m")
	for range 5 {
		tk, _ := m.Candidates[2].health.breaker.allow()
		tk.failure()
	}
	rolls = []float64{0.01, 0.99}
	if p := mustPlan(t, r, Request{Chat: chatRequest(true), Policy: "fastest"}); order(p) != "b,a,c" {
		t.Errorf("order = %s, want b,a,c (c is open)", order(p))
	}

	// Other policies never explore.
	rolls = []float64{0.01, 0.99}
	if p := mustPlan(t, r, Request{Chat: chatRequest(true)}); p.Explored || order(p) != "a,b,c" {
		t.Errorf("priority: explored=%v order=%s", p.Explored, order(p))
	}
}
