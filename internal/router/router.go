// Package router resolves public model aliases to upstream targets, orders
// them by routing policy, and tracks each target's health (design §6).
package router

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"slices"
	"time"

	"github.com/SamirVarma01/llm-gateway/internal/config"
	"github.com/SamirVarma01/llm-gateway/internal/provider"
	"github.com/SamirVarma01/llm-gateway/internal/provider/anthropic"
	"github.com/SamirVarma01/llm-gateway/internal/provider/openai"
)

// ErrUnknownModel means no alias matched.
var ErrUnknownModel = errors.New("router: unknown model")

// Candidate is a target that can serve an alias, with its adapter.
type Candidate struct {
	Target   provider.Target
	Priority int
	Provider provider.Provider

	health *targetHealth // shared by every alias that routes to this target
}

// Model is a resolved public alias.
type Model struct {
	Alias            string
	Policy           string // default routing policy
	DefaultMaxOutput int
	Candidates       []Candidate // in priority order

	output *outputAverage // completion tokens per response
}

// MaxOutput returns the output cap to send upstream: the client's limit, or
// the alias default when the client set none, capped at the target's limit.
// It bounds the cost of the request, so it is always sent.
func (m *Model) MaxOutput(requested *int, c Candidate) int {
	n := m.DefaultMaxOutput
	if requested != nil {
		n = *requested
	}
	return min(n, c.Target.MaxOutput)
}

// Options configures a Router. Zero values select the defaults, except
// ExploreRate, where zero disables exploration.
type Options struct {
	Breaker     BreakerConfig
	Latency     LatencyConfig
	Retry       RetryConfig
	ExploreRate float64          // share of fastest-policy requests sent to a non-best target first
	Now         func() time.Time // clock; nil = time.Now
	Rand        func() float64   // uniform in [0, 1); nil = math/rand/v2
	Logger      *slog.Logger     // breaker state changes; nil = slog.Default()
}

// Router holds the route table and the health of every target. The route
// table is immutable after New; health state is safe for concurrent use.
type Router struct {
	opts   Options
	models map[string]*Model
	order  []*Model
}

// targetHealth is the state the gateway keeps for one (provider, upstream
// model) pair (design D3: in-process).
type targetHealth struct {
	breaker *breaker
	latency *latency
}

// BuildProviders constructs an adapter for every provider in routes. A
// provider whose api_key_env is set but missing from the environment is an
// error, so misconfiguration fails at startup rather than per request.
func BuildProviders(routes config.Routes, client *http.Client, lookupEnv func(string) (string, bool)) (map[string]provider.Provider, error) {
	out := make(map[string]provider.Provider, len(routes.Providers))
	for _, p := range routes.Providers {
		var key string
		if p.APIKeyEnv != "" {
			v, ok := lookupEnv(p.APIKeyEnv)
			if !ok || v == "" {
				return nil, fmt.Errorf("router: provider %q: env %s is not set", p.Name, p.APIKeyEnv)
			}
			key = v
		}
		switch p.Format {
		case config.FormatOpenAI:
			var opts []openai.Option
			if p.MaxTokensField == config.MaxCompletionTokensField {
				opts = append(opts, openai.UseMaxCompletionTokens())
			}
			out[p.Name] = openai.New(p.Name, p.BaseURL, key, client, opts...)
		case config.FormatAnthropic:
			out[p.Name] = anthropic.New(p.Name, p.BaseURL, key, client)
		default:
			return nil, fmt.Errorf("router: provider %q: unsupported format %q", p.Name, p.Format)
		}
	}
	return out, nil
}

// New resolves routes against the given adapters.
func New(routes config.Routes, providers map[string]provider.Provider, opts Options) (*Router, error) {
	if opts.Breaker == (BreakerConfig{}) {
		opts.Breaker = DefaultBreakerConfig()
	}
	if opts.Latency == (LatencyConfig{}) {
		opts.Latency = DefaultLatencyConfig()
	}
	if opts.Retry == (RetryConfig{}) {
		opts.Retry = DefaultRetryConfig()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Rand == nil {
		opts.Rand = rand.Float64
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	r := &Router{opts: opts, models: make(map[string]*Model, len(routes.Models))}
	health := make(map[targetKey]*targetHealth)
	for _, ms := range routes.Models {
		m := &Model{
			Alias: ms.Alias, Policy: ms.Policy, DefaultMaxOutput: ms.DefaultMaxOutput,
			output: &outputAverage{alpha: opts.Latency.Alpha},
		}
		for _, cs := range ms.Candidates {
			p, ok := providers[cs.Provider]
			if !ok {
				return nil, fmt.Errorf("router: model %q: no adapter for provider %q", ms.Alias, cs.Provider)
			}
			key := targetKey{cs.Provider, cs.Model}
			h, ok := health[key]
			if !ok {
				h = r.newHealth(key)
				health[key] = h
			}
			m.Candidates = append(m.Candidates, Candidate{
				Target: provider.Target{
					Provider:      cs.Provider,
					UpstreamModel: cs.Model,
					Price: provider.Price{
						InputMicroPerM:  config.MicroUSD(cs.InputPerMTok),
						OutputMicroPerM: config.MicroUSD(cs.OutputPerMTok),
					},
					MaxOutput: cs.MaxOutput,
				},
				Priority: cs.Priority,
				Provider: p,
				health:   h,
			})
		}
		slices.SortStableFunc(m.Candidates, func(a, b Candidate) int { return a.Priority - b.Priority })
		r.models[m.Alias] = m
		r.order = append(r.order, m)
	}
	return r, nil
}

type targetKey struct{ provider, model string }

func (r *Router) newHealth(t targetKey) *targetHealth {
	log := r.opts.Logger.With("provider", t.provider, "upstream_model", t.model)
	return &targetHealth{
		breaker: newBreaker(r.opts.Breaker, r.opts.Now, func(from, to BreakerState) {
			level := slog.LevelInfo
			if to == StateOpen {
				level = slog.LevelWarn
			}
			log.Log(context.Background(), level, "circuit breaker state change", "from", from.String(), "to", to.String())
		}),
		latency: &latency{cfg: r.opts.Latency},
	}
}

// Resolve returns the model for alias.
func (r *Router) Resolve(alias string) (*Model, error) {
	if m, ok := r.models[alias]; ok {
		return m, nil
	}
	return nil, ErrUnknownModel
}

// Models returns every model in route-table order.
func (r *Router) Models() []*Model { return r.order }
