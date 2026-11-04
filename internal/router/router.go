// Package router resolves public model aliases to ordered upstream candidates.
//
// Phase 1 orders candidates by static priority only; price- and
// latency-aware policies, health tracking, and failover arrive in Phase 3.
package router

import (
	"errors"
	"fmt"
	"net/http"
	"slices"

	"github.com/SamirVarma01/llm-gateway/internal/config"
	"github.com/SamirVarma01/llm-gateway/internal/provider"
	"github.com/SamirVarma01/llm-gateway/internal/provider/openai"
)

// ErrUnknownModel means no alias matched.
var ErrUnknownModel = errors.New("router: unknown model")

// Candidate is a target that can serve an alias, with its adapter.
type Candidate struct {
	Target   provider.Target
	Priority int
	Provider provider.Provider
}

// Model is a resolved public alias.
type Model struct {
	Alias            string
	Policy           string
	DefaultMaxOutput int
	Candidates       []Candidate // in policy order
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

// Router holds the resolved route table. It is immutable after New.
type Router struct {
	models map[string]*Model
	order  []*Model
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
			out[p.Name] = openai.New(p.Name, p.BaseURL, key, client)
		default:
			return nil, fmt.Errorf("router: provider %q: unsupported format %q", p.Name, p.Format)
		}
	}
	return out, nil
}

// New resolves routes against the given adapters.
func New(routes config.Routes, providers map[string]provider.Provider) (*Router, error) {
	r := &Router{models: make(map[string]*Model, len(routes.Models))}
	for _, ms := range routes.Models {
		m := &Model{Alias: ms.Alias, Policy: ms.Policy, DefaultMaxOutput: ms.DefaultMaxOutput}
		for _, cs := range ms.Candidates {
			p, ok := providers[cs.Provider]
			if !ok {
				return nil, fmt.Errorf("router: model %q: no adapter for provider %q", ms.Alias, cs.Provider)
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
			})
		}
		slices.SortStableFunc(m.Candidates, func(a, b Candidate) int { return a.Priority - b.Priority })
		r.models[m.Alias] = m
		r.order = append(r.order, m)
	}
	return r, nil
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
