package httpapi

import (
	"net/http"
	"testing"

	"github.com/SamirVarma01/llm-gateway/internal/mock"
)

func TestRoutePolicyHeader(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 3}, nil)
	policy := func(p string) http.Header { return http.Header{headerRoutePolicy: {p}} }

	tests := []struct {
		name, header, policy, provider string
	}{
		{"alias default", "", "priority", "claude-a"},
		{"cheapest", "cheapest", "cheapest", "mock-a"},
		{"explicit priority", "priority", "priority", "claude-a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var resp *http.Response
			if tt.header == "" {
				resp = e.chat(t, chatReq("mixed", nil))
			} else {
				resp = e.chat(t, chatReq("mixed", nil), policy(tt.header))
			}
			if resp.StatusCode != http.StatusOK || resp.Header.Get(headerProvider) != tt.provider {
				t.Errorf("status %d provider %q, want 200 %q", resp.StatusCode, resp.Header.Get(headerProvider), tt.provider)
			}
			if o := e.outcome(t); o.Policy != tt.policy {
				t.Errorf("outcome policy = %q, want %q", o.Policy, tt.policy)
			}
		})
	}

	wantError(t, e.chat(t, chatReq("mixed", nil), policy("random")), http.StatusBadRequest, "invalid_route_policy")
}

// A request one target cannot honor goes to a target that can (design D10):
// Anthropic targets reject a non-default temperature, so "mixed" serves it
// from the OpenAI-format candidate instead.
func TestUnsupportedTargetsAreSkipped(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 3}, nil)
	temp := 0.2
	req := chatReq("mixed", nil)
	req.Temperature = &temp
	resp := e.chat(t, req)
	if resp.StatusCode != http.StatusOK || resp.Header.Get(headerProvider) != "mock-a" {
		t.Fatalf("status %d provider %q, want 200 from mock-a", resp.StatusCode, resp.Header.Get(headerProvider))
	}
	if recs := e.mock.Records(resp.Header.Get("X-Request-ID")); len(recs) != 1 || recs[0].Provider != "mock-a" {
		t.Errorf("mock records = %+v", recs)
	}

	// With no capable target the request is rejected without an upstream call.
	only := chatReq("claude", nil)
	only.Temperature = &temp
	resp = e.chat(t, only)
	wantError(t, resp, http.StatusBadRequest, "unsupported_request")
	if n := len(e.mock.Records(resp.Header.Get("X-Request-ID"))); n != 0 {
		t.Errorf("unsupported request reached the provider %d times", n)
	}
}
