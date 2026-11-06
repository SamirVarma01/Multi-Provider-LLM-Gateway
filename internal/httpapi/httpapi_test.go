package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/SamirVarma01/llm-gateway/internal/auth"
	"github.com/SamirVarma01/llm-gateway/internal/config"
	"github.com/SamirVarma01/llm-gateway/internal/estimate"
	"github.com/SamirVarma01/llm-gateway/internal/mock"
	"github.com/SamirVarma01/llm-gateway/internal/oai"
	"github.com/SamirVarma01/llm-gateway/internal/obs"
	"github.com/SamirVarma01/llm-gateway/internal/router"
	"github.com/SamirVarma01/llm-gateway/internal/testutil"
)

func TestMain(m *testing.M) { testutil.Main(m) }

const adminToken = "test-admin-token"

// staticAuth accepts exactly one key.
type staticAuth struct {
	plaintext string
	key       auth.Key
}

func (s staticAuth) LookupByHash(_ context.Context, hash []byte) (auth.Key, error) {
	if bytes.Equal(hash, auth.HashKey(s.plaintext)) {
		return s.key, nil
	}
	return auth.Key{}, auth.ErrNotFound
}

type testEnv struct {
	mock     *mock.Server
	router   *router.Router
	srv      *Server
	ledger   *fakeLedger  // what Options.Ledger defaults to
	limiter  *fakeLimiter // what Options.Limiter defaults to
	public   *httptest.Server
	admin    *httptest.Server
	key      string
	outcomes chan outcome
}

// testRetry keeps failover tests fast: short stall timeouts and backoff.
var testRetry = router.RetryConfig{
	MaxAttempts: 3,
	TTFTTimeout: 500 * time.Millisecond,
	IdleTimeout: 500 * time.Millisecond,
	BackoffBase: time.Millisecond,
	BackoffMax:  5 * time.Millisecond,
}

// envTweak adjusts a test environment's router and server options.
type envTweak struct {
	router func(*router.Options)
	server func(*Options)
	mock   func(o *Options, mockURL string) // runs after server, with the mock's URL
}

// mockRoutes is the route table newEnv serves, with %[1]s the mock's URL.
// "chat" (OpenAI format, default_max_output 64, max_output 1000), "tiny"
// (max_output 5), "claude" (Anthropic format), "mixed" (Anthropic
// format first by priority, then a cheaper OpenAI-format target), and
// "pair" (mock-a, then mock-b).
const mockRoutes = `
providers:
  - { name: mock-a, format: openai, base_url: "%[1]s/p/mock-a/v1" }
  - { name: mock-b, format: openai, base_url: "%[1]s/p/mock-b/v1" }
  - { name: claude-a, format: anthropic, base_url: "%[1]s/p/claude-a/v1" }
models:
  - alias: chat
    default_max_output: 64
    candidates:
      - { provider: mock-a, model: m-small, input_per_mtok: 0.15, output_per_mtok: 0.60, max_output: 1000 }
  - alias: tiny
    candidates:
      - { provider: mock-a, model: m-tiny, input_per_mtok: 1, output_per_mtok: 2, max_output: 5 }
  - alias: claude
    default_max_output: 64
    candidates:
      - { provider: claude-a, model: claude-test, input_per_mtok: 3, output_per_mtok: 15, max_output: 1000 }
  - alias: mixed
    default_max_output: 64
    candidates:
      - { provider: claude-a, model: claude-test, input_per_mtok: 3, output_per_mtok: 15, priority: 1, max_output: 1000 }
      - { provider: mock-a, model: m-small, input_per_mtok: 0.15, output_per_mtok: 0.60, priority: 2, max_output: 1000 }
  - alias: pair
    default_max_output: 64
    candidates:
      - { provider: mock-a, model: m-pair, input_per_mtok: 1, output_per_mtok: 2, priority: 1, max_output: 1000 }
      - { provider: mock-b, model: m-pair, input_per_mtok: 1, output_per_mtok: 2, priority: 2, max_output: 1000 }
`

// newEnv starts a mock provider and a gateway in front of it (see mockRoutes).
func newEnv(t *testing.T, b mock.Behavior, tweak func(*Options)) *testEnv {
	t.Helper()
	return newEnvTweaked(t, b, envTweak{server: tweak})
}

// newEnvTweaked is newEnv with router options too.
func newEnvTweaked(t *testing.T, b mock.Behavior, tw envTweak) *testEnv {
	t.Helper()
	m := mock.New(mock.Options{Defaults: b, Seed: 3})
	mockTS := httptest.NewServer(m.Handler())
	t.Cleanup(mockTS.Close)
	if withMock := tw.mock; withMock != nil {
		server := tw.server
		tw.server = func(o *Options) {
			if server != nil {
				server(o)
			}
			withMock(o, mockTS.URL)
		}
	}
	e := newEnvRoutesTweaked(t, fmt.Sprintf(mockRoutes, mockTS.URL), tw)
	e.mock = m
	return e
}

// newEnvRoutes starts a gateway serving the given route table.
func newEnvRoutes(t *testing.T, routesYAML string, tweak func(*Options)) *testEnv {
	t.Helper()
	return newEnvRoutesTweaked(t, routesYAML, envTweak{server: tweak})
}

func newEnvRoutesTweaked(t *testing.T, routesYAML string, tw envTweak) *testEnv {
	t.Helper()
	routes, err := config.ParseRoutes([]byte(routesYAML))
	if err != nil {
		t.Fatal(err)
	}
	tr := &http.Transport{}
	t.Cleanup(tr.CloseIdleConnections)
	providers, err := router.BuildProviders(routes, &http.Client{Transport: tr}, func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	ropts := router.Options{Retry: testRetry, Logger: obs.NewLogger(io.Discard, 0)}
	if tw.router != nil {
		tw.router(&ropts)
	}
	rt, err := router.New(routes, providers, ropts)
	if err != nil {
		t.Fatal(err)
	}

	key, _ := auth.GenerateKey()
	fl, flim := &fakeLedger{}, &fakeLimiter{}
	opts := Options{
		Logger:         obs.NewLogger(io.Discard, 0),
		Auth:           staticAuth{plaintext: key, key: auth.Key{ID: uuid.New(), Name: "test"}},
		Router:         rt,
		Estimator:      estimate.Estimator{ImageTokens: 100, SafetyFactor: 1.25},
		Limiter:        flim,
		Ledger:         fl,
		AdminToken:     adminToken,
		RequestTimeout: 10 * time.Second,
		LedgerTimeout:  time.Second,
		MaxBodyBytes:   1 << 20,
	}
	if tw.server != nil {
		tw.server(&opts)
	}
	srv := New(opts)
	outcomes := make(chan outcome, 64)
	srv.onOutcome = func(o outcome) {
		select {
		case outcomes <- o:
		default: // a test that does not read outcomes must not block handlers
		}
	}
	// Runs after the servers close (cleanups are last in, first out).
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Drain(ctx)
	})
	public := httptest.NewServer(srv.PublicHandler())
	t.Cleanup(public.Close)
	admin := httptest.NewServer(srv.AdminHandler())
	t.Cleanup(admin.Close)
	return &testEnv{router: rt, srv: srv, ledger: fl, limiter: flim, public: public, admin: admin, key: key, outcomes: outcomes}
}

// outcome waits for the next request outcome.
func (e *testEnv) outcome(t *testing.T) outcome {
	t.Helper()
	select {
	case o := <-e.outcomes:
		return o
	case <-time.After(5 * time.Second):
		t.Fatal("no request outcome within 5s")
		return outcome{}
	}
}

func (e *testEnv) do(t *testing.T, method, url, bearer string, body any, headers ...http.Header) *http.Response {
	t.Helper()
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rd = strings.NewReader(b)
	default:
		raw, _ := json.Marshal(b)
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		t.Fatal(err)
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	for _, h := range headers {
		for k, vs := range h {
			req.Header[k] = vs
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func jsonBody(v any) io.Reader {
	raw, _ := json.Marshal(v)
	return bytes.NewReader(raw)
}

func (e *testEnv) chat(t *testing.T, body any, headers ...http.Header) *http.Response {
	t.Helper()
	return e.do(t, http.MethodPost, e.public.URL+"/v1/chat/completions", e.key, body, headers...)
}

func chatReq(model string, maxTokens *int) oai.ChatCompletionRequest {
	return oai.ChatCompletionRequest{
		Model: model,
		Messages: []oai.Message{
			{Role: "system", Content: oai.TextContent("be brief")},
			{Role: "user", Content: oai.Content{Parts: []oai.ContentPart{{Type: "text", Text: "hello gateway"}}}},
		},
		MaxTokens: maxTokens,
	}
}

func decode[T any](t *testing.T, resp *http.Response) T {
	t.Helper()
	var v T
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		t.Fatalf("decode %T: %v", v, err)
	}
	return v
}

func wantError(t *testing.T, resp *http.Response, status int, code string) {
	t.Helper()
	if resp.StatusCode != status {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want %d (body %s)", resp.StatusCode, status, body)
	}
	e := decode[oai.ErrorBody](t, resp)
	if e.Error.Code != code {
		t.Errorf("error code = %q, want %q (%s)", e.Error.Code, code, e.Error.Message)
	}
	if e.Error.RequestID == "" || e.Error.RequestID != resp.Header.Get(obs.RequestIDHeader) {
		t.Errorf("error request_id = %q, header = %q", e.Error.RequestID, resp.Header.Get(obs.RequestIDHeader))
	}
}

func TestChatHappyPath(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 12}, nil)
	resp := e.chat(t, chatReq("chat", nil))
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d: %s", resp.StatusCode, body)
	}
	out := decode[oai.ChatCompletionResponse](t, resp)
	reqID := resp.Header.Get(obs.RequestIDHeader)

	if out.ID != "chatcmpl-"+reqID || out.Object != "chat.completion" || out.Model != "chat" || len(out.Choices) != 1 {
		t.Errorf("response = %+v", out)
	}
	if resp.Header.Get(headerProvider) != "mock-a" || resp.Header.Get(headerAttempts) != "1" {
		t.Errorf("gateway headers = %v", resp.Header)
	}

	// The mock saw the gateway's request ID, and the usage the client got is
	// exactly what the mock recorded.
	recs := e.mock.Records(reqID)
	if len(recs) != 1 {
		t.Fatalf("mock records for %s = %+v", reqID, recs)
	}
	rec := recs[0]
	if rec.Model != "m-small" || rec.Attempt != 1 || !rec.Billable {
		t.Errorf("mock record = %+v", rec)
	}
	if out.Usage == nil || out.Usage.PromptTokens != rec.PromptTokens ||
		out.Usage.CompletionTokens != rec.CompletionTokens || out.Usage.TotalTokens != rec.PromptTokens+rec.CompletionTokens {
		t.Errorf("usage = %+v, mock recorded %+v", out.Usage, rec)
	}
	msg := out.Choices[0]
	if msg.Message.Role != "assistant" || len(strings.Fields(*msg.Message.Content)) != 12 || msg.FinishReason != "stop" {
		t.Errorf("choice = %+v", msg)
	}
}

func TestChatRequiresAuth(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 1}, nil)
	wantError(t, e.do(t, http.MethodPost, e.public.URL+"/v1/chat/completions", "", chatReq("chat", nil)),
		http.StatusUnauthorized, "missing_api_key")
	other, _ := auth.GenerateKey()
	wantError(t, e.do(t, http.MethodPost, e.public.URL+"/v1/chat/completions", other, chatReq("chat", nil)),
		http.StatusUnauthorized, "invalid_api_key")
	wantError(t, e.do(t, http.MethodGet, e.public.URL+"/v1/models", "", nil),
		http.StatusUnauthorized, "missing_api_key")
	if n := len(e.mock.AllRecords()); n != 0 {
		t.Errorf("unauthenticated requests reached the provider %d times", n)
	}
}

func TestChatValidation(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 1}, func(o *Options) { o.MaxBodyBytes = 4096 })
	two, zero, hot, half := 2, 0, 3.0, 0.5
	mut := func(f func(*oai.ChatCompletionRequest)) oai.ChatCompletionRequest {
		r := chatReq("chat", nil)
		f(&r)
		return r
	}
	tests := []struct {
		name   string
		body   any
		status int
		code   string
	}{
		{"invalid json", `{"model":`, 400, "invalid_json"},
		{"missing model", mut(func(r *oai.ChatCompletionRequest) { r.Model = "" }), 400, "missing_model"},
		{"no messages", mut(func(r *oai.ChatCompletionRequest) { r.Messages = nil }), 400, "missing_messages"},
		{"logprobs", `{"model":"chat","messages":[{"role":"user","content":"x"}],"logprobs":true}`, 400, "unsupported_parameter"},
		{"n=2", mut(func(r *oai.ChatCompletionRequest) { r.N = &two }), 400, "unsupported_parameter"},
		{"max_tokens 0", mut(func(r *oai.ChatCompletionRequest) { r.MaxTokens = &zero }), 400, "invalid_max_tokens"},
		{"temperature", mut(func(r *oai.ChatCompletionRequest) { r.Temperature = &hot }), 400, "invalid_temperature"},
		{"unknown role", mut(func(r *oai.ChatCompletionRequest) { r.Messages[0].Role = "narrator" }), 400, "unsupported_role"},
		{"tool message without tool_call_id", mut(func(r *oai.ChatCompletionRequest) { r.Messages[0].Role = "tool" }), 400, "missing_tool_call_id"},
		{"image part without url", mut(func(r *oai.ChatCompletionRequest) {
			r.Messages[1].Content.Parts[0].Type = "image_url"
		}), 400, "invalid_content"},
		{"audio part", mut(func(r *oai.ChatCompletionRequest) {
			r.Messages[1].Content.Parts[0].Type = "input_audio"
		}), 400, "unsupported_content"},
		{"null user content", `{"model":"chat","messages":[{"role":"user","content":null}]}`, 400, "missing_content"},
		{"tool_choice names an undeclared tool", `{"model":"chat","messages":[{"role":"user","content":"x"}],
			"tools":[{"type":"function","function":{"name":"a"}}],"tool_choice":{"type":"function","function":{"name":"b"}}}`,
			400, "invalid_tool_choice"},
		{"tool without name", `{"model":"chat","messages":[{"role":"user","content":"x"}],
			"tools":[{"type":"function","function":{}}]}`, 400, "invalid_tools"},
		{"anthropic target with temperature", mut(func(r *oai.ChatCompletionRequest) {
			r.Model, r.Temperature = "claude", &half
		}), 400, "unsupported_request"},
		{"unknown model", chatReq("gpt-nope", nil), 404, "model_not_found"},
		{"too large", mut(func(r *oai.ChatCompletionRequest) {
			r.Messages[1].Content = oai.TextContent(strings.Repeat("x", 5000))
		}), 413, "request_too_large"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wantError(t, e.chat(t, tt.body), tt.status, tt.code)
		})
	}
	if n := len(e.mock.AllRecords()); n != 0 {
		t.Errorf("invalid requests reached the provider %d times", n)
	}
}

func TestChatOutputCap(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 500}, nil)
	ask := func(n int) *int { return &n }
	tests := []struct {
		name       string
		req        oai.ChatCompletionRequest
		wantTokens int
	}{
		{"alias default when client sets none", chatReq("chat", nil), 64},
		{"client limit", chatReq("chat", ask(10)), 10},
		{"client limit above target max_output", chatReq("tiny", ask(100)), 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := e.chat(t, tt.req)
			out := decode[oai.ChatCompletionResponse](t, resp)
			if out.Usage == nil || out.Usage.CompletionTokens != tt.wantTokens || out.Choices[0].FinishReason != "length" {
				t.Errorf("usage = %+v finish = %q, want %d tokens / length", out.Usage, out.Choices[0].FinishReason, tt.wantTokens)
			}
		})
	}
}

func TestChatUpstreamErrors(t *testing.T) {
	tests := []struct {
		name   string
		b      mock.Behavior
		status int
		code   string
	}{
		{"provider 503", mock.Behavior{ErrorRate: 1, ErrorStatus: 503}, 502, "upstream_error"},
		{"provider 429", mock.Behavior{ErrorRate: 1, ErrorStatus: 429}, 502, "upstream_error"},
		{"provider 400", mock.Behavior{ErrorRate: 1, ErrorStatus: 400}, 400, "upstream_rejected"},
		{"mid-stream failure", mock.Behavior{OutputTokens: 10, FailAfterTokens: 3}, 502, "upstream_error"},
		{"provider hangs past the request timeout", mock.Behavior{Hang: true}, 504, "upstream_timeout"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t, tt.b, func(o *Options) { o.RequestTimeout = 200 * time.Millisecond })
			resp := e.chat(t, chatReq("chat", nil))
			wantError(t, resp, tt.status, tt.code)
			if resp.Header.Get(headerProvider) != "mock-a" {
				t.Errorf("%s header = %q", headerProvider, resp.Header.Get(headerProvider))
			}
		})
	}
}

func TestModels(t *testing.T) {
	e := newEnv(t, mock.Behavior{}, nil)
	resp := e.do(t, http.MethodGet, e.public.URL+"/v1/models", e.key, nil)
	out := decode[modelList](t, resp)
	if out.Object != "list" || len(out.Data) != 5 {
		t.Fatalf("models = %+v", out)
	}
	m := out.Data[0]
	if m.ID != "chat" || m.Object != "model" || m.Pricing.InputPerMTokUSD != 0.15 || m.Pricing.OutputPerMTokUSD != 0.6 {
		t.Errorf("first model = %+v", m)
	}
}

func TestHealth(t *testing.T) {
	e := newEnv(t, mock.Behavior{}, func(o *Options) {
		o.Ready = []ReadyCheck{
			{Name: "postgres", Check: func(context.Context) error { return nil }},
			{Name: "redis", Check: func(context.Context) error { return errors.New("connection refused") }},
		}
	})
	if resp := e.do(t, http.MethodGet, e.public.URL+"/healthz", "", nil); resp.StatusCode != http.StatusOK {
		t.Errorf("healthz = %d", resp.StatusCode)
	}
	resp := e.do(t, http.MethodGet, e.public.URL+"/readyz", "", nil)
	body := decode[map[string]any](t, resp)
	failed, _ := body["failed"].(map[string]any)
	if resp.StatusCode != http.StatusServiceUnavailable || failed["redis"] != "connection refused" || failed["postgres"] != nil {
		t.Errorf("readyz = %d %v", resp.StatusCode, body)
	}

	ok := newEnv(t, mock.Behavior{}, nil)
	if resp := ok.do(t, http.MethodGet, ok.public.URL+"/readyz", "", nil); resp.StatusCode != http.StatusOK {
		t.Errorf("readyz with no failing checks = %d", resp.StatusCode)
	}
	wantError(t, ok.do(t, http.MethodGet, ok.public.URL+"/nope", "", nil), http.StatusNotFound, "not_found")
}

func TestAdminAuth(t *testing.T) {
	e := newEnv(t, mock.Behavior{}, nil)
	wantError(t, e.do(t, http.MethodGet, e.admin.URL+"/admin/keys", "", nil), http.StatusUnauthorized, "invalid_admin_token")
	wantError(t, e.do(t, http.MethodGet, e.admin.URL+"/admin/keys", "wrong", nil), http.StatusUnauthorized, "invalid_admin_token")

	disabled := newEnv(t, mock.Behavior{}, func(o *Options) { o.AdminToken = "" })
	wantError(t, disabled.do(t, http.MethodGet, disabled.admin.URL+"/admin/keys", "", nil),
		http.StatusUnauthorized, "invalid_admin_token")
}

// TestEndToEndWithPostgres exercises the real key store: create a key through
// the admin API, use it, revoke it, and see it rejected.
func TestEndToEndWithPostgres(t *testing.T) {
	pool := testutil.NewDatabase(t)
	store := auth.NewPGStore(pool)
	e := newEnv(t, mock.Behavior{OutputTokens: 4}, func(o *Options) {
		o.Keys = store
		o.Auth = auth.NewCache(store, 0) // no caching: revocation is immediate
	})

	wantError(t, e.do(t, http.MethodPost, e.admin.URL+"/admin/keys", adminToken, `{"name": ""}`),
		http.StatusBadRequest, "missing_name")
	wantError(t, e.do(t, http.MethodPost, e.admin.URL+"/admin/keys", adminToken, `{"name": "x", "budget_usd": -1}`),
		http.StatusBadRequest, "invalid_budget")
	wantError(t, e.do(t, http.MethodPost, e.admin.URL+"/admin/keys", adminToken, `{"name": "x", "rpm_limit": 0}`),
		http.StatusBadRequest, "invalid_limit")

	resp := e.do(t, http.MethodPost, e.admin.URL+"/admin/keys", adminToken,
		`{"name": "alice", "rpm_limit": 60, "budget_usd": 12.5}`)
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("create key = %d: %s", resp.StatusCode, body)
	}
	created := decode[keyResponse](t, resp)
	if !strings.HasPrefix(created.Key, auth.KeyPrefix) || created.Name != "alice" ||
		*created.RPMLimit != 60 || *created.BudgetUSD != 12.5 || created.TPMLimit != nil {
		t.Errorf("created = %+v", created)
	}

	chat := func() *http.Response {
		return e.do(t, http.MethodPost, e.public.URL+"/v1/chat/completions", created.Key, chatReq("chat", nil))
	}
	if resp := chat(); resp.StatusCode != http.StatusOK {
		t.Fatalf("chat with new key = %d", resp.StatusCode)
	}

	list := decode[struct{ Data []keyResponse }](t, e.do(t, http.MethodGet, e.admin.URL+"/admin/keys", adminToken, nil))
	if len(list.Data) != 1 || list.Data[0].ID != created.ID || list.Data[0].Key != "" {
		t.Errorf("list = %+v (plaintext keys must never be listed)", list.Data)
	}

	resp = e.do(t, http.MethodPost, e.admin.URL+"/admin/keys/"+created.ID.String()+"/revoke", adminToken, nil)
	if revoked := decode[keyResponse](t, resp); revoked.RevokedAt == nil {
		t.Errorf("revoke = %+v", revoked)
	}
	wantError(t, chat(), http.StatusUnauthorized, "invalid_api_key")

	wantError(t, e.do(t, http.MethodPost, e.admin.URL+"/admin/keys/"+uuid.NewString()+"/revoke", adminToken, nil),
		http.StatusNotFound, "key_not_found")
	wantError(t, e.do(t, http.MethodPost, e.admin.URL+"/admin/keys/not-a-uuid/revoke", adminToken, nil),
		http.StatusBadRequest, "invalid_id")
}
