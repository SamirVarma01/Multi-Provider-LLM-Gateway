package router

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/SamirVarma01/llm-gateway/internal/config"
	"github.com/SamirVarma01/llm-gateway/internal/provider"
)

// Request is what the router needs to plan one chat request.
type Request struct {
	Chat      *provider.ChatRequest // MaxTokens is ignored: each Choice sets its own
	MaxTokens *int                  // the client's output limit; nil for the alias default
	Policy    string                // overrides the alias policy when set
	EstInput  int                   // estimated prompt tokens, for the cheapest policy
}

// Choice is a candidate prepared for one request.
type Choice struct {
	Candidate
	Req *provider.ChatRequest // the request with this target's output cap
}

// Plan is the targets to try for one request, in order.
type Plan struct {
	Model    *Model
	Policy   string
	Choices  []Choice
	Explored bool // a non-best target was put first to refresh its latency estimates

	// UpstreamWait adds up the time spent waiting on upstreams, across all
	// attempts, including backoff between them. Total latency minus this is
	// the gateway's own time. It is only touched by the request's goroutine.
	UpstreamWait time.Duration
}

// UnsupportedError means no candidate of an alias can serve a request.
type UnsupportedError struct {
	Alias   string
	Reasons []error // why each candidate declined
}

func (e *UnsupportedError) Error() string {
	msgs := make([]string, len(e.Reasons))
	for i, r := range e.Reasons {
		msgs[i] = r.Error()
	}
	return fmt.Sprintf("no target for model %q supports this request: %s", e.Alias, strings.Join(msgs, "; "))
}

// ErrUnknownPolicy means a request named a routing policy that does not exist.
var ErrUnknownPolicy = errors.New("router: unknown routing policy")

// Plan selects the candidates that can serve q and orders them by policy
// (design §6.2). Candidates whose adapter cannot honor the request
// (Provider.Supports) are left out; if none remain, the error is an
// *UnsupportedError. Breakers are not consulted here: an open target keeps
// its place and is skipped when attempts are made.
func (r *Router) Plan(m *Model, q Request) (*Plan, error) {
	policy := cmp.Or(q.Policy, m.Policy)
	if !config.ValidPolicy(policy) {
		return nil, fmt.Errorf("%w %q", ErrUnknownPolicy, policy)
	}
	p := &Plan{Model: m, Policy: policy}
	var reasons []error
	for _, c := range m.Candidates {
		req := *q.Chat
		maxOut := m.MaxOutput(q.MaxTokens, c)
		req.MaxTokens = &maxOut
		if err := c.Provider.Supports(&req); err != nil {
			reasons = append(reasons, fmt.Errorf("%s: %w", c.Target.Provider, err))
			continue
		}
		p.Choices = append(p.Choices, Choice{Candidate: c, Req: &req})
	}
	if len(p.Choices) == 0 {
		return nil, &UnsupportedError{Alias: m.Alias, Reasons: reasons}
	}

	switch policy {
	case config.PolicyCheapest:
		orderByScore(p.Choices, func(c Choice) (float64, bool) { return cost(c, q.EstInput), true })
	case config.PolicyFastest:
		now := r.opts.Now()
		orderByScore(p.Choices, func(c Choice) (float64, bool) { return expectedLatency(m, c, q.Chat.Stream, now) })
		p.Explored = r.explore(p.Choices)
	}
	return p, nil
}

// cost is the request's estimated cost on c, in micro-USD per 1M tokens
// times tokens. It only ranks candidates; money is computed exactly elsewhere.
func cost(c Choice, estInput int) float64 {
	pr := c.Target.Price
	return float64(estInput)*float64(pr.InputMicroPerM) + float64(*c.Req.MaxTokens)*float64(pr.OutputMicroPerM)
}

// expectedLatency is the fastest policy's score in seconds (design §6.2): TTFT
// for a streaming request, which is what its client waits for, and TTFT plus
// decode time for a non-streaming one. Without a decode rate the score is
// TTFT alone, a lower bound, so a target with a known slow first token is not
// treated as unmeasured. ok is false if the target has no recent TTFT.
func expectedLatency(m *Model, c Choice, stream bool, now time.Time) (float64, bool) {
	ttft, ok, tps, tpsOK := c.health.latency.estimates(now)
	if !ok {
		return 0, false
	}
	if stream || !tpsOK || tps <= 0 {
		return ttft, true
	}
	maxOut := float64(*c.Req.MaxTokens)
	expected := maxOut
	if avg, ok := m.output.get(); ok {
		expected = min(avg, maxOut)
	}
	return ttft + expected/tps, true
}

// orderByScore sorts choices by ascending score, keeping priority order on
// ties. A choice without a score gets the best known score, so an unmeasured
// or stale target competes with the best one on priority instead of being
// ranked last forever.
func orderByScore(choices []Choice, score func(Choice) (float64, bool)) {
	type scored struct {
		c  Choice
		s  float64
		ok bool
	}
	ss := make([]scored, len(choices))
	best, haveBest := 0.0, false
	for i, c := range choices {
		s, ok := score(c)
		ss[i] = scored{c, s, ok}
		if ok && (!haveBest || s < best) {
			best, haveBest = s, true
		}
	}
	for i := range ss {
		if !ss[i].ok {
			ss[i].s = best
		}
	}
	slices.SortStableFunc(ss, func(a, b scored) int { return cmp.Compare(a.s, b.s) })
	for i := range ss {
		choices[i] = ss[i].c
	}
}

// explore moves a random healthy non-best choice to the front for a share of
// requests, so the estimates of targets that are not winning stay fresh.
func (r *Router) explore(choices []Choice) bool {
	if len(choices) < 2 || r.opts.ExploreRate <= 0 || r.opts.Rand() >= r.opts.ExploreRate {
		return false
	}
	var healthy []int
	for i := 1; i < len(choices); i++ {
		if s, _ := choices[i].health.breaker.snapshot(); s == StateClosed {
			healthy = append(healthy, i)
		}
	}
	if len(healthy) == 0 {
		return false
	}
	pick := healthy[min(int(r.opts.Rand()*float64(len(healthy))), len(healthy)-1)]
	c := choices[pick]
	copy(choices[1:pick+1], choices[:pick])
	choices[0] = c
	return true
}
