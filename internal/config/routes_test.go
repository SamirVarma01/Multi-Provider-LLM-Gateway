package config

import (
	"path/filepath"
	"strings"
	"testing"
)

const validRoutes = `
providers:
  - name: mock-a
    format: openai
    base_url: http://mock:9000/p/mock-a/v1
  - name: mock-b
    format: openai
    base_url: http://mock:9000/p/mock-b/v1
    api_key_env: MOCK_B_KEY
    max_tokens_field: max_completion_tokens
  - name: claude
    format: anthropic
    base_url: https://api.anthropic.com/v1
models:
  - alias: chat-small
    candidates:
      - { provider: mock-a, model: m-small, input_per_mtok: 0.15, output_per_mtok: 0.60, priority: 2, max_output: 4096 }
      - { provider: mock-b, model: m-small, input_per_mtok: 0.20, output_per_mtok: 0.80, priority: 1, max_output: 2048 }
  - alias: chat-cheap
    policy: cheapest
    candidates:
      - { provider: mock-a, model: m-small, max_output: 10 }
  - alias: chat-fast
    policy: fastest
    candidates:
      - { provider: mock-a, model: m-small, max_output: 10 }
`

func TestParseRoutes(t *testing.T) {
	r, err := ParseRoutes([]byte(validRoutes))
	if err != nil {
		t.Fatalf("ParseRoutes: %v", err)
	}
	m := r.Models[0]
	if m.Policy != PolicyPriority || m.DefaultMaxOutput != DefaultMaxOutput {
		t.Errorf("defaults not applied: %+v", m)
	}
	if r.Models[1].Policy != PolicyCheapest || r.Models[2].Policy != PolicyFastest {
		t.Errorf("policies = %q, %q", r.Models[1].Policy, r.Models[2].Policy)
	}
	if len(m.Candidates) != 2 || m.Candidates[1].Provider != "mock-b" || r.Providers[1].APIKeyEnv != "MOCK_B_KEY" ||
		r.Providers[1].MaxTokensField != MaxCompletionTokensField || r.Providers[2].Format != FormatAnthropic {
		t.Errorf("parsed = %+v", r)
	}
}

func TestParseRoutesErrors(t *testing.T) {
	tests := map[string]struct {
		yaml string
		want []string
	}{
		"unknown field": {
			yaml: "providers: []\nmodles: []\n",
			want: []string{"modles"},
		},
		"many problems at once": {
			yaml: `
providers:
  - { name: p, format: gemini, base_url: "ftp://x" }
  - { name: p, format: openai, base_url: "http://x/v1", max_tokens_field: tokens }
  - { name: q, format: anthropic, base_url: "http://x/v1", max_tokens_field: max_tokens }
models:
  - alias: a
    policy: quickest
    candidates:
      - { provider: nope, model: "", input_per_mtok: -1, max_output: 0 }
  - alias: a
    candidates: []
`,
			want: []string{
				`unsupported format "gemini"`, "base_url", `duplicate name "p"`,
				"max_tokens_field must be", "applies only to the openai format",
				`unsupported policy "quickest"`, `unknown provider "nope"`, "model is required",
				"prices must be", "max_output must be positive", `duplicate alias "a"`,
				"at least one candidate",
			},
		},
		"no models": {
			yaml: "providers: []\nmodels: []\n",
			want: []string{"at least one model"},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := ParseRoutes([]byte(tt.yaml))
			if err == nil {
				t.Fatal("expected error")
			}
			for _, w := range tt.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error does not mention %q:\n%v", w, err)
				}
			}
		})
	}
}

func TestMicroUSD(t *testing.T) {
	for usd, want := range map[float64]int64{0.15: 150_000, 0.6: 600_000, 15: 15_000_000, 0: 0, 0.0000014: 1} {
		if got := MicroUSD(usd); got != want {
			t.Errorf("MicroUSD(%v) = %d, want %d", usd, got, want)
		}
	}
}

// TestRepoRouteFiles keeps the route tables shipped in config/ valid.
func TestRepoRouteFiles(t *testing.T) {
	files, err := filepath.Glob("../../config/*.yaml")
	if err != nil || len(files) == 0 {
		t.Fatalf("no route files found: %v", err)
	}
	for _, f := range files {
		if _, err := LoadRoutes(f); err != nil {
			t.Errorf("%s: %v", f, err)
		}
	}
}
