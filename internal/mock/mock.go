// Package mock implements a deterministic LLM provider for tests and
// benchmarks. It speaks two wire formats: OpenAI Chat Completions at
// /p/{name}/v1/chat/completions and Anthropic Messages at /p/{name}/v1/messages.
//
// One server hosts any number of virtual providers, each with its own
// Behavior (latency, decode rate, injected errors, mid-stream failures,
// hangs). Output is a pure function of (seed, provider, request ID,
// attempt), so repeated runs produce identical text and usage.
//
// Every request is recorded with the usage the mock reports, keyed by the
// caller's X-Request-ID and X-Attempt headers, so tests and the reconciler can
// compare what the gateway charged with what the "provider" billed. A record is
// billable if the request completed, or if at least one content token was
// flushed before it was interrupted; CompletionTokens counts flushed tokens.
package mock

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/SamirVarma01/llm-gateway/internal/oai"
	"github.com/SamirVarma01/llm-gateway/internal/sse"
)

// Headers the gateway sends upstream to identify a request attempt.
const (
	RequestIDHeader = "X-Request-ID"
	AttemptHeader   = "X-Attempt"
)

// DefaultProvider is the provider name served at /v1/chat/completions.
const DefaultProvider = "default"

// Fail modes for Behavior.FailMode.
const (
	FailAbort      = "abort"       // drop the connection
	FailErrorEvent = "error_event" // send an OpenAI error event and end the stream
)

// Behavior configures how a virtual provider responds.
type Behavior struct {
	TTFTMs          int     `json:"ttft_ms"`           // delay before the first byte
	TokensPerSec    float64 `json:"tokens_per_sec"`    // decode rate after the first token; 0 = unpaced
	OutputTokens    int     `json:"output_tokens"`     // tokens generated (capped by max_tokens)
	ErrorRate       float64 `json:"error_rate"`        // probability of failing before the first byte
	ErrorStatus     int     `json:"error_status"`      // HTTP status for injected errors; 0 = 500
	FailAfterTokens int     `json:"fail_after_tokens"` // fail after this many content tokens; 0 = never
	FailMode        string  `json:"fail_mode"`         // FailAbort (default) or FailErrorEvent
	Hang            bool    `json:"hang"`              // accept and never respond until the caller gives up
	OmitUsage       bool    `json:"omit_usage"`        // never report usage, like some providers
}

// Validate reports whether b is usable.
func (b Behavior) Validate() error {
	var errs []error
	if b.TTFTMs < 0 {
		errs = append(errs, errors.New("ttft_ms must be >= 0"))
	}
	if b.TokensPerSec < 0 {
		errs = append(errs, errors.New("tokens_per_sec must be >= 0"))
	}
	if b.OutputTokens < 0 {
		errs = append(errs, errors.New("output_tokens must be >= 0"))
	}
	if b.ErrorRate < 0 || b.ErrorRate > 1 {
		errs = append(errs, errors.New("error_rate must be in [0, 1]"))
	}
	if b.ErrorStatus != 0 && (b.ErrorStatus < 400 || b.ErrorStatus > 599) {
		errs = append(errs, errors.New("error_status must be 0 or 400-599"))
	}
	if b.FailAfterTokens < 0 {
		errs = append(errs, errors.New("fail_after_tokens must be >= 0"))
	}
	if b.FailMode != "" && b.FailMode != FailAbort && b.FailMode != FailErrorEvent {
		errs = append(errs, fmt.Errorf("fail_mode must be %q or %q", FailAbort, FailErrorEvent))
	}
	return errors.Join(errs...)
}

// Outcome is how a recorded request ended.
type Outcome string

// Outcomes.
const (
	OutcomeComplete         Outcome = "complete"
	OutcomeInjectedError    Outcome = "injected_error"
	OutcomeMidStreamFailure Outcome = "mid_stream_failure"
	OutcomeClientCancelled  Outcome = "client_cancelled"
)

// Record is the mock's account of one request attempt.
type Record struct {
	RequestID        string    `json:"request_id"`
	Attempt          int       `json:"attempt"`
	Provider         string    `json:"provider"`
	Model            string    `json:"model"`
	Stream           bool      `json:"stream"`
	PromptTokens     int       `json:"prompt_tokens"`
	CompletionTokens int       `json:"completion_tokens"`
	Billable         bool      `json:"billable"`
	Outcome          Outcome   `json:"outcome"`
	Status           int       `json:"status"`
	StartedAt        time.Time `json:"started_at"`
	EndedAt          time.Time `json:"ended_at"`
}

// Options configures a Server.
type Options struct {
	Defaults             Behavior     // behavior of providers with no explicit config
	Seed                 uint64       // base seed for all output
	AllowHeaderOverrides bool         // honor per-request X-Mock-* headers
	UsageLog             io.Writer    // optional JSONL sink for records
	Logger               *slog.Logger // nil = slog.Default()
}

// Server is the mock provider.
type Server struct {
	opts Options
	log  *slog.Logger

	mu        sync.Mutex
	providers map[string]Behavior
	records   []Record
	byRequest map[string][]int // request ID → indexes into records
}

// New returns a Server. It panics if opts.Defaults is invalid.
func New(opts Options) *Server {
	if err := opts.Defaults.Validate(); err != nil {
		panic(fmt.Sprintf("mock: invalid default behavior: %v", err))
	}
	l := opts.Logger
	if l == nil {
		l = slog.Default()
	}
	return &Server{
		opts:      opts,
		log:       l,
		providers: make(map[string]Behavior),
		byRequest: make(map[string][]int),
	}
}

// Handler returns the HTTP handler serving all mock routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /p/{provider}/v1/chat/completions", s.serve(openAIFormat{}))
	mux.HandleFunc("POST /v1/chat/completions", s.serve(openAIFormat{}))
	mux.HandleFunc("POST /p/{provider}/v1/messages", s.serve(anthropicFormat{}))
	mux.HandleFunc("POST /v1/messages", s.serve(anthropicFormat{}))
	mux.HandleFunc("GET /_mock/providers/{provider}", s.handleGetBehavior)
	mux.HandleFunc("PUT /_mock/providers/{provider}", s.handlePutBehavior)
	mux.HandleFunc("GET /_mock/usage", s.handleGetUsage)
	mux.HandleFunc("DELETE /_mock/usage", s.handleDeleteUsage)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	})
	return mux
}

// SetBehavior configures a virtual provider.
func (s *Server) SetBehavior(provider string, b Behavior) error {
	if err := b.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.providers[provider] = b
	return nil
}

// Behavior returns a provider's current behavior.
func (s *Server) Behavior(provider string) Behavior {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b, ok := s.providers[provider]; ok {
		return b
	}
	return s.opts.Defaults
}

// Records returns every attempt recorded for requestID, in arrival order of completion.
func (s *Server) Records(requestID string) []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Record, 0, len(s.byRequest[requestID]))
	for _, i := range s.byRequest[requestID] {
		out = append(out, s.records[i])
	}
	return out
}

// AllRecords returns every record.
func (s *Server) AllRecords() []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Record(nil), s.records...)
}

// ResetRecords discards all records.
func (s *Server) ResetRecords() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = nil
	s.byRequest = make(map[string][]int)
}

// Restore loads records an earlier run wrote to its UsageLog, so a restart
// does not forget what was billed: the reconciler reads a missing record as
// "the call never happened". Call it before the server takes traffic. It
// reports how many records it loaded and how many lines it skipped; a crash
// can leave a partial last line.
func (s *Server) Restore(r io.Reader) (restored, skipped int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		var rec Record
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			skipped++
			continue
		}
		s.byRequest[rec.RequestID] = append(s.byRequest[rec.RequestID], len(s.records))
		s.records = append(s.records, rec)
		restored++
	}
	return restored, skipped, sc.Err()
}

func (s *Server) addRecord(r Record) {
	r.Billable = r.Outcome == OutcomeComplete || r.CompletionTokens > 0
	r.EndedAt = time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byRequest[r.RequestID] = append(s.byRequest[r.RequestID], len(s.records))
	s.records = append(s.records, r)
	if s.opts.UsageLog != nil {
		line, _ := json.Marshal(r)
		if _, err := s.opts.UsageLog.Write(append(line, '\n')); err != nil {
			s.log.Error("mock: write usage log", "err", err)
		}
	}
}

// call is the state of one request attempt.
type call struct {
	rec      Record
	recorded bool
}

// finish records the call exactly once. Completed calls are recorded before
// their final bytes are written, so a caller that has read the whole response
// can always find the record.
func (s *Server) finish(c *call, outcome Outcome) {
	if c.recorded {
		return
	}
	c.recorded = true
	c.rec.Outcome = outcome
	s.addRecord(c.rec)
}

// request is the format-neutral view of an incoming chat request.
type request struct {
	model        string
	stream       bool
	includeUsage bool // send usage in the stream (OpenAI: stream_options.include_usage)
	outputLimit  *int
	promptTokens int
}

// completion is what a response reports.
type completion struct {
	id           string
	created      int64
	model        string
	text         string
	truncated    bool // stopped at the output limit
	promptTokens int
	outputTokens int
	withUsage    bool
}

// wireFormat is one provider API's request and response encoding. The shared
// flow in serve decides timing, failures, and recording; a format only
// encodes.
type wireFormat interface {
	parse(r *http.Request) (request, error)
	writeError(w http.ResponseWriter, status int, msg string)
	writeCompletion(w http.ResponseWriter, c completion)
	streamer(sw *sse.Writer, c completion, includeUsage bool) streamer
}

// streamer writes one streamed response.
type streamer interface {
	start() error
	token(i int, text string) error
	// finish writes every event after the last token except the final one.
	finish(outputTokens int) error
	// done writes the final event.
	done() error
	midStreamError() error
}

func (s *Server) serve(f wireFormat) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		provider := r.PathValue("provider")
		if provider == "" {
			provider = DefaultProvider
		}
		b := s.Behavior(provider)
		if s.opts.AllowHeaderOverrides {
			var err error
			if b, err = applyHeaderOverrides(b, r.Header); err != nil {
				f.writeError(w, http.StatusBadRequest, err.Error())
				return
			}
		}
		req, err := f.parse(r)
		if err != nil {
			f.writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		reqID := r.Header.Get(RequestIDHeader)
		if reqID == "" {
			reqID = uuid.NewString()
		}
		attempt := 1
		if v := r.Header.Get(AttemptHeader); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				attempt = n
			}
		}

		c := &call{rec: Record{
			RequestID:    reqID,
			Attempt:      attempt,
			Provider:     provider,
			Model:        req.model,
			Stream:       req.stream,
			PromptTokens: req.promptTokens,
			Status:       http.StatusOK,
			StartedAt:    time.Now(),
		}}
		// Any exit not explicitly recorded (client gone, abort panic) is a cancellation.
		defer s.finish(c, OutcomeClientCancelled)

		rng := rand.New(rand.NewPCG(s.opts.Seed, seedFor(provider, reqID, attempt)))
		injectError := b.ErrorRate > 0 && rng.Float64() < b.ErrorRate
		n, truncated := b.OutputTokens, false
		if lim := req.outputLimit; lim != nil && *lim < n {
			n, truncated = max(*lim, 0), true
		}
		tokens := generate(rng, n)

		ctx := r.Context()
		if b.Hang {
			<-ctx.Done()
			return
		}
		if injectError {
			status := b.ErrorStatus
			if status == 0 {
				status = http.StatusInternalServerError
			}
			c.rec.Status = status
			s.finish(c, OutcomeInjectedError)
			f.writeError(w, status, fmt.Sprintf("mock: injected error for provider %q", provider))
			return
		}
		if err := sleepUntil(ctx, time.Now().Add(time.Duration(b.TTFTMs)*time.Millisecond)); err != nil {
			return
		}

		comp := completion{
			id:           "mock-" + reqID,
			created:      time.Now().Unix(),
			model:        req.model,
			truncated:    truncated,
			promptTokens: req.promptTokens,
			withUsage:    !b.OmitUsage,
		}
		if req.stream {
			s.stream(w, r, c, b, f.streamer(sse.NewWriter(w), comp, req.includeUsage), tokens)
		} else {
			s.complete(w, r, c, b, f, comp, tokens)
		}
	}
}

func (s *Server) complete(w http.ResponseWriter, r *http.Request, c *call, b Behavior,
	f wireFormat, comp completion, tokens []string) {
	ctx := r.Context()
	start := time.Now()
	if b.FailAfterTokens > 0 && b.FailAfterTokens <= len(tokens) {
		// Nothing has been sent, so nothing is billable.
		if err := sleepUntil(ctx, tokenDue(start, b.FailAfterTokens, b.TokensPerSec)); err != nil {
			return
		}
		s.finish(c, OutcomeMidStreamFailure)
		panic(http.ErrAbortHandler)
	}
	if err := sleepUntil(ctx, tokenDue(start, len(tokens), b.TokensPerSec)); err != nil {
		return
	}
	comp.text = strings.Join(tokens, "")
	comp.outputTokens = len(tokens)
	c.rec.CompletionTokens = len(tokens)
	s.finish(c, OutcomeComplete)
	f.writeCompletion(w, comp)
}

func (s *Server) stream(w http.ResponseWriter, r *http.Request, c *call, b Behavior, st streamer, tokens []string) {
	ctx := r.Context()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)

	if err := st.start(); err != nil {
		return
	}
	start := time.Now()
	for i := 0; i <= len(tokens); i++ {
		if b.FailAfterTokens > 0 && i == b.FailAfterTokens {
			s.finish(c, OutcomeMidStreamFailure)
			if b.FailMode == FailErrorEvent {
				_ = st.midStreamError()
				return
			}
			panic(http.ErrAbortHandler)
		}
		if i == len(tokens) {
			break
		}
		if err := sleepUntil(ctx, tokenDue(start, i, b.TokensPerSec)); err != nil {
			return
		}
		if err := st.token(i, tokens[i]); err != nil {
			return
		}
		c.rec.CompletionTokens = i + 1
	}
	if err := st.finish(len(tokens)); err != nil {
		return
	}
	s.finish(c, OutcomeComplete)
	_ = st.done()
}

func (s *Server) handleGetBehavior(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.Behavior(r.PathValue("provider")))
}

// handlePutBehavior replaces a provider's behavior. Fields absent from the body
// take the server defaults.
func (s *Server) handlePutBehavior(w http.ResponseWriter, r *http.Request) {
	b := s.opts.Defaults
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&b); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	if err := s.SetBehavior(r.PathValue("provider"), b); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, b)
}

func (s *Server) handleGetUsage(w http.ResponseWriter, r *http.Request) {
	if id := r.URL.Query().Get("request_id"); id != "" {
		writeJSON(w, http.StatusOK, s.Records(id))
		return
	}
	writeJSON(w, http.StatusOK, s.AllRecords())
}

func (s *Server) handleDeleteUsage(w http.ResponseWriter, _ *http.Request) {
	s.ResetRecords()
	w.WriteHeader(http.StatusNoContent)
}

var vocabulary = strings.Fields(`the a gateway token stream model route budget ledger
	provider latency request response cache circuit breaker failover reserve settle
	release usage price health window bucket limit chunk event mock alpha beta gamma
	delta epsilon quick brown fox jumps over lazy dog river mountain cloud signal
	vector matrix kernel packet socket buffer queue worker thread context deadline`)

// generate returns n tokens; every token after the first starts with a space,
// so concatenation yields readable text and one chunk carries one token.
func generate(rng *rand.Rand, n int) []string {
	out := make([]string, n)
	for i := range out {
		w := vocabulary[rng.IntN(len(vocabulary))]
		if i > 0 {
			w = " " + w
		}
		out[i] = w
	}
	return out
}

func seedFor(provider, requestID string, attempt int) uint64 {
	h := fnv.New64a()
	_, _ = fmt.Fprintf(h, "%s\x00%s\x00%d", provider, requestID, attempt)
	return h.Sum64()
}

// tokenDue is when token i (0-based) should be emitted. The first token goes
// out at TTFT; later ones follow at the decode rate, scheduled from start so
// per-token overhead does not accumulate as drift.
func tokenDue(start time.Time, i int, tokensPerSec float64) time.Time {
	if tokensPerSec <= 0 || i == 0 {
		return start
	}
	return start.Add(time.Duration(float64(i) / tokensPerSec * float64(time.Second)))
}

// sleepUntil waits until t or until ctx is done, whichever is first.
func sleepUntil(ctx context.Context, t time.Time) error {
	d := time.Until(t)
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func applyHeaderOverrides(b Behavior, h http.Header) (Behavior, error) {
	var errs []error
	intHdr := func(name string, dst *int) {
		if v := h.Get(name); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", name, err))
			}
			*dst = n
		}
	}
	floatHdr := func(name string, dst *float64) {
		if v := h.Get(name); v != "" {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", name, err))
			}
			*dst = f
		}
	}
	boolHdr := func(name string, dst *bool) {
		if v := h.Get(name); v != "" {
			x, err := strconv.ParseBool(v)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", name, err))
			}
			*dst = x
		}
	}
	intHdr("X-Mock-TTFT-Ms", &b.TTFTMs)
	floatHdr("X-Mock-Tokens-Per-Sec", &b.TokensPerSec)
	intHdr("X-Mock-Output-Tokens", &b.OutputTokens)
	floatHdr("X-Mock-Error-Rate", &b.ErrorRate)
	intHdr("X-Mock-Error-Status", &b.ErrorStatus)
	intHdr("X-Mock-Fail-After-Tokens", &b.FailAfterTokens)
	if v := h.Get("X-Mock-Fail-Mode"); v != "" {
		b.FailMode = v
	}
	boolHdr("X-Mock-Hang", &b.Hang)
	boolHdr("X-Mock-Omit-Usage", &b.OmitUsage)
	if err := errors.Join(errs...); err != nil {
		return b, err
	}
	return b, b.Validate()
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError writes an OpenAI-shaped error; the control endpoints use it.
func writeError(w http.ResponseWriter, status int, typ, msg string) {
	writeJSON(w, status, oai.ErrorBody{Error: oai.ErrorDetail{Message: msg, Type: typ}})
}
