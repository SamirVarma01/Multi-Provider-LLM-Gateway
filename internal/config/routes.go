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
	FormatOpenAI    = "openai"
	FormatAnthropic = "anthropic"
)

// Output-limit field names an OpenAI-format provider may require.
const (
	MaxTokensField           = "max_tokens"
	MaxCompletionTokensField = "max_completion_tokens"
)

// Routing policies (design §6.2). An alias's policy is its default; clients
// can override it per request with X-Route-Policy.
const (
	PolicyPriority = "priority" // static priority from the route table
	PolicyCheapest = "cheapest" // lowest estimated cost of the request
	PolicyFastest  = "fastest"  // lowest expected latency from live measurements
)

// ValidPolicy reports whether p names a routing policy.
func ValidPolicy(p string) bool {
	return p == PolicyPriority || p == PolicyCheapest || p == PolicyFastest
}

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
	// MaxTokensField is the request field carrying the output limit for
	// OpenAI-format providers: max_tokens (default, accepted by most
	// compatible servers) or max_completion_tokens (required by newer
	// OpenAI models).
	MaxTokensField string `yaml:"max_tokens_field"`
	// UsageURL is where the reconciler can look up what the provider billed
	// for a request (design §9). Only the mock has such an API; real
	// providers leave this empty, and their orphaned requests are released.
	UsageURL string `yaml:"usage_url"`
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
		switch p.Format {
		case FormatOpenAI:
			if f := p.MaxTokensField; f != "" && f != MaxTokensField && f != MaxCompletionTokensField {
				add("provider %q: max_tokens_field must be %q or %q", p.Name, MaxTokensField, MaxCompletionTokensField)
			}
		case FormatAnthropic:
			if p.MaxTokensField != "" {
				add("provider %q: max_tokens_field applies only to the openai format", p.Name)
			}
		default:
			add("provider %q: unsupported format %q", p.Name, p.Format)
		}
		if !absoluteURL(p.BaseURL) {
			add("provider %q: base_url must be an absolute http(s) URL", p.Name)
		}
		if p.UsageURL != "" && !absoluteURL(p.UsageURL) {
			add("provider %q: usage_url must be an absolute http(s) URL", p.Name)
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
		if !ValidPolicy(m.Policy) {
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

func absoluteURL(s string) bool {
	u, err := url.Parse(s)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
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
