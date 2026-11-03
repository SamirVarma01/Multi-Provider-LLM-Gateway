package config

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"

	"gopkg.in/yaml.v3"
)

// Provider formats the gateway can speak.
const (
	FormatOpenAI = "openai"
)

// Routing policies. Phase 1 implements only priority order.
const (
	PolicyPriority = "priority"
)

// DefaultMaxOutput is used when a model sets no default_max_output.
const DefaultMaxOutput = 1024

// Routes is the route table loaded from ROUTES_FILE (design §6.1).
type Routes struct {
	Providers []ProviderSpec `yaml:"providers"`
	Models    []ModelSpec    `yaml:"models"`
}

// ProviderSpec is one upstream API.
type ProviderSpec struct {
	Name      string `yaml:"name"`
	Format    string `yaml:"format"`
	BaseURL   string `yaml:"base_url"`    // up to and including /v1
	APIKeyEnv string `yaml:"api_key_env"` // optional env var holding the API key
}

// ModelSpec is one public model alias.
type ModelSpec struct {
	Alias            string          `yaml:"alias"`
	Policy           string          `yaml:"policy"`
	DefaultMaxOutput int             `yaml:"default_max_output"`
	Candidates       []CandidateSpec `yaml:"candidates"`
}

// CandidateSpec is one (provider, upstream model) that can serve an alias.
type CandidateSpec struct {
	Provider      string  `yaml:"provider"`
	Model         string  `yaml:"model"`
	InputPerMTok  float64 `yaml:"input_per_mtok"`  // USD per 1M input tokens
	OutputPerMTok float64 `yaml:"output_per_mtok"` // USD per 1M output tokens
	Priority      int     `yaml:"priority"`        // lower is preferred
	MaxOutput     int     `yaml:"max_output"`      // upstream model's output token limit
}

// LoadRoutes reads and validates a route table file.
func LoadRoutes(path string) (Routes, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Routes{}, fmt.Errorf("routes: %w", err)
	}
	return ParseRoutes(b)
}

// ParseRoutes decodes and validates a route table, filling in defaults.
// Unknown fields are rejected so typos do not silently change routing.
func ParseRoutes(b []byte) (Routes, error) {
	var r Routes
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&r); err != nil {
		return Routes{}, fmt.Errorf("routes: %w", err)
	}
	for i := range r.Models {
		m := &r.Models[i]
		if m.Policy == "" {
			m.Policy = PolicyPriority
		}
		if m.DefaultMaxOutput == 0 {
			m.DefaultMaxOutput = DefaultMaxOutput
		}
	}
	if err := r.Validate(); err != nil {
		return Routes{}, fmt.Errorf("routes: %w", err)
	}
	return r, nil
}

// Validate reports every problem in the route table.
func (r Routes) Validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	providers := make(map[string]bool)
	for i, p := range r.Providers {
		switch {
		case p.Name == "":
			add("providers[%d]: name is required", i)
		case providers[p.Name]:
			add("providers[%d]: duplicate name %q", i, p.Name)
		}
		providers[p.Name] = true
		if p.Format != FormatOpenAI {
			add("provider %q: unsupported format %q", p.Name, p.Format)
		}
		if u, err := url.Parse(p.BaseURL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			add("provider %q: base_url must be an absolute http(s) URL", p.Name)
		}
	}

	if len(r.Models) == 0 {
		add("at least one model is required")
	}
	aliases := make(map[string]bool)
	for i, m := range r.Models {
		switch {
		case m.Alias == "":
			add("models[%d]: alias is required", i)
		case aliases[m.Alias]:
			add("models[%d]: duplicate alias %q", i, m.Alias)
		}
		aliases[m.Alias] = true
		if m.Policy != PolicyPriority {
			add("model %q: unsupported policy %q", m.Alias, m.Policy)
		}
		if m.DefaultMaxOutput < 0 {
			add("model %q: default_max_output must be positive", m.Alias)
		}
		if len(m.Candidates) == 0 {
			add("model %q: at least one candidate is required", m.Alias)
		}
		for j, c := range m.Candidates {
			where := fmt.Sprintf("model %q candidates[%d]", m.Alias, j)
			if !providers[c.Provider] {
				add("%s: unknown provider %q", where, c.Provider)
			}
			if c.Model == "" {
				add("%s: model is required", where)
			}
			if !validPrice(c.InputPerMTok) || !validPrice(c.OutputPerMTok) {
				add("%s: prices must be finite and >= 0", where)
			}
			if c.MaxOutput <= 0 {
				add("%s: max_output must be positive", where)
			}
		}
	}
	return errors.Join(errs...)
}

// maxPricePerMTok keeps micro-USD prices far from int64 overflow in cost math.
const maxPricePerMTok = 1e6

func validPrice(p float64) bool {
	return !math.IsNaN(p) && !math.IsInf(p, 0) && p >= 0 && p <= maxPricePerMTok
}

// MicroUSD converts a USD-per-1M-tokens price to integer micro-USD per 1M tokens.
func MicroUSD(usdPerMTok float64) int64 {
	return int64(math.Round(usdPerMTok * 1e6))
}
