package router

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/SamirVarma01/llm-gateway/internal/config"
	"github.com/SamirVarma01/llm-gateway/internal/provider/anthropic"
)

const routesYAML = `
providers:
  - { name: mock-a, format: openai, base_url: "http://mock/p/mock-a/v1" }
  - { name: mock-b, format: openai, base_url: "http://mock/p/mock-b/v1", api_key_env: MOCK_B_KEY }
  - { name: claude, format: anthropic, base_url: "http://mock/p/claude/v1" }
models:
  - alias: chat
    default_max_output: 100
    candidates:
      - { provider: mock-a, model: a-model, input_per_mtok: 0.15, output_per_mtok: 0.60, priority: 2, max_output: 50 }
      - { provider: mock-b, model: b-model, input_per_mtok: 0.20, output_per_mtok: 0.80, priority: 1, max_output: 500 }
      - { provider: mock-a, model: a-model-2, input_per_mtok: 1, output_per_mtok: 2, priority: 1, max_output: 500 }
  - alias: other
    candidates:
      - { provider: mock-a, model: x, max_output: 10 }
  - alias: claude
    candidates:
      - { provider: claude, model: claude-test, max_output: 10 }
`

func build(t *testing.T, env map[string]string) (*Router, error) {
	t.Helper()
	routes, err := config.ParseRoutes([]byte(routesYAML))
	if err != nil {
		t.Fatal(err)
	}
	providers, err := BuildProviders(routes, http.DefaultClient, func(k string) (string, bool) {
		v, ok := env[k]
		return v, ok
	})
	if err != nil {
		return nil, err
	}
	return New(routes, providers, Options{})
}

func TestResolve(t *testing.T) {
	r, err := build(t, map[string]string{"MOCK_B_KEY": "secret"})
	if err != nil {
		t.Fatal(err)
	}
	m, err := r.Resolve("chat")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, c := range m.Candidates {
		got = append(got, c.Target.Provider+"/"+c.Target.UpstreamModel)
	}
	// Priority ascending; ties keep file order.
	if want := "mock-b/b-model mock-a/a-model-2 mock-a/a-model"; strings.Join(got, " ") != want {
		t.Errorf("order = %v, want %s", got, want)
	}
	if p := m.Candidates[0].Target.Price; p.InputMicroPerM != 200_000 || p.OutputMicroPerM != 800_000 {
		t.Errorf("price = %+v", p)
	}
	if m.Candidates[0].Provider.Name() != "mock-b" {
		t.Error("candidate has the wrong adapter")
	}

	if _, err := r.Resolve("nope"); !errors.Is(err, ErrUnknownModel) {
		t.Errorf("err = %v, want ErrUnknownModel", err)
	}
	if ms := r.Models(); len(ms) != 3 || ms[0].Alias != "chat" || ms[1].Alias != "other" {
		t.Errorf("Models() = %v", ms)
	}
	c, _ := r.Resolve("claude")
	if _, ok := c.Candidates[0].Provider.(*anthropic.Provider); !ok {
		t.Errorf("anthropic-format provider built as %T", c.Candidates[0].Provider)
	}
}

func TestMaxOutput(t *testing.T) {
	r, err := build(t, map[string]string{"MOCK_B_KEY": "secret"})
	if err != nil {
		t.Fatal(err)
	}
	m, _ := r.Resolve("chat")
	small, large := m.Candidates[2], m.Candidates[0] // max_output 50 and 500
	ask := func(n int) *int { return &n }

	if got := m.MaxOutput(nil, large); got != 100 {
		t.Errorf("no client limit: got %d, want alias default 100", got)
	}
	if got := m.MaxOutput(nil, small); got != 50 {
		t.Errorf("default above target limit: got %d, want 50", got)
	}
	if got := m.MaxOutput(ask(300), large); got != 300 {
		t.Errorf("client limit: got %d, want 300", got)
	}
	if got := m.MaxOutput(ask(300), small); got != 50 {
		t.Errorf("client limit above target limit: got %d, want 50", got)
	}
}

func TestBuildProvidersRequiresAPIKeyEnv(t *testing.T) {
	_, err := build(t, nil)
	if err == nil || !strings.Contains(err.Error(), "MOCK_B_KEY") {
		t.Errorf("err = %v, want missing MOCK_B_KEY", err)
	}
}
