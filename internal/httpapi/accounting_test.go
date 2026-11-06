package httpapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/SamirVarma01/llm-gateway/internal/ledger"
	"github.com/SamirVarma01/llm-gateway/internal/mock"
	"github.com/SamirVarma01/llm-gateway/internal/obs"
	"github.com/SamirVarma01/llm-gateway/internal/provider"
	"github.com/SamirVarma01/llm-gateway/internal/ratelimit"
)

type fakeRelease struct {
	id     uuid.UUID
	reason string
}

// fakeLedger records what the gateway asks of the ledger.
type fakeLedger struct {
	mu         sync.Mutex
	reserveErr error
	settleErrs int           // fail this many settles first
	gate       chan struct{} // if set, Settle waits for it
	reserved   []ledger.Reservation
	settled    []ledger.Settlement
	released   []fakeRelease
}

func (f *fakeLedger) Reserve(_ context.Context, r ledger.Reservation) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.reserveErr != nil {
		return f.reserveErr
	}
	f.reserved = append(f.reserved, r)
	return nil
}

func (f *fakeLedger) Settle(ctx context.Context, s ledger.Settlement) (bool, error) {
	f.mu.Lock()
	gate := f.gate
	f.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.settleErrs != 0 {
		f.settleErrs--
		return false, errors.New("postgres is down")
	}
	f.settled = append(f.settled, s)
	return true, nil
}

func (f *fakeLedger) Release(_ context.Context, id uuid.UUID, reason, _ string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.released = append(f.released, fakeRelease{id, reason})
	return true, nil
}

// snapshot returns copies, safe to read while background work runs.
func (f *fakeLedger) snapshot() ([]ledger.Reservation, []ledger.Settlement, []fakeRelease) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.reserved), slices.Clone(f.settled), slices.Clone(f.released)
}

// fakeLimiter admits everything unless told otherwise.
type fakeLimiter struct {
	mu       sync.Mutex
	decision *ratelimit.Decision
	err      error
	admitted []int
	adjusted []int
}

func (f *fakeLimiter) Admit(_ context.Context, _ uuid.UUID, _ ratelimit.Limits, tokens int) (ratelimit.Decision, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.admitted = append(f.admitted, tokens)
	if f.err != nil {
		return ratelimit.Decision{}, f.err
	}
	if f.decision != nil {
		return *f.decision, nil
	}
	return ratelimit.Decision{Allowed: true}, nil
}

func (f *fakeLimiter) Adjust(_ context.Context, _ uuid.UUID, _ ratelimit.Limits, delta int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.adjusted = append(f.adjusted, delta)
	return nil
}

func (f *fakeLimiter) snapshot() ([]int, []int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.admitted), slices.Clone(f.adjusted)
}

var (
	claudePrice = provider.Price{InputMicroPerM: 3_000_000, OutputMicroPerM: 15_000_000}
	smallPrice  = provider.Price{InputMicroPerM: 150_000, OutputMicroPerM: 600_000}
)

// chatReq("mixed") has 2 messages of 8 and 13 bytes: an input estimate of
// 3 + (4+2) + (4+4) = 17, and 22 with the 1.25 safety factor.
const mixedEstIn = 22

func TestReservationCoversTheWorstTarget(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 5}, nil)
	resp := e.chat(t, chatReq("mixed", nil))
	reqID := resp.Header.Get(obs.RequestIDHeader)
	readBody(t, resp, false)
	e.outcome(t)

	reserved, settled, released := e.ledger.snapshot()
	if len(reserved) != 1 || len(settled) != 1 || len(released) != 0 {
		t.Fatalf("reserved %v settled %v released %v", reserved, settled, released)
	}
	r := reserved[0]
	if r.RequestID.String() != reqID || r.Alias != "mixed" || r.AmountNUSD != ledger.Cost(mixedEstIn, 64, claudePrice) {
		t.Errorf("reservation = %+v, want %d", r, ledger.Cost(mixedEstIn, 64, claudePrice))
	}
	if r.Meta.Policy != "priority" || r.Meta.EstInput != mixedEstIn || r.Meta.MaxOutput != 64 || len(r.Meta.Targets) != 2 {
		t.Errorf("meta = %+v", r.Meta)
	}
	if admitted, _ := e.limiter.snapshot(); len(admitted) != 1 || admitted[0] != mixedEstIn+64 {
		t.Errorf("tokens admitted = %v, want %d", admitted, mixedEstIn+64)
	}

	// The charge is exactly what the mock billed, at the serving target's price.
	rec := waitRecords(t, e.mock, reqID, 1)[0]
	s := settled[0]
	if s.RequestID.String() != reqID || s.Provider != "claude-a" || s.UpstreamModel != "claude-test" ||
		s.PromptTokens != rec.PromptTokens || s.CompletionTokens != rec.CompletionTokens || s.UsageEstimated ||
		s.Reason != ledger.ReasonCompleted || s.Source != ledger.SourceGateway ||
		s.AmountNUSD != ledger.Cost(rec.PromptTokens, rec.CompletionTokens, claudePrice) {
		t.Errorf("settlement = %+v, mock record %+v", s, rec)
	}
	// Unused tokens go back to the bucket.
	if _, adjusted := e.limiter.snapshot(); len(adjusted) != 1 ||
		adjusted[0] != mixedEstIn+64-rec.PromptTokens-rec.CompletionTokens {
		t.Errorf("adjusted = %v", adjusted)
	}
}

// Only targets the request can reach are reserved for: "mixed" with a
// temperature cannot use Anthropic, so the expensive target does not count.
func TestReservationIgnoresUnsupportedTargets(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 2}, nil)
	temp := 0.2
	req := chatReq("mixed", nil)
	req.Temperature = &temp
	readBody(t, e.chat(t, req), false)
	e.outcome(t)
	reserved, _, _ := e.ledger.snapshot()
	if want := ledger.Cost(mixedEstIn, 64, smallPrice); reserved[0].AmountNUSD != want {
		t.Errorf("reserved %d, want %d", reserved[0].AmountNUSD, want)
	}
}

func TestFailoverChargesTheServingTarget(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 3}, nil)
	setBehavior(t, e, "claude-a", mock.Behavior{ErrorRate: 1, ErrorStatus: 529})
	resp := e.chat(t, chatReq("mixed", nil))
	reqID := resp.Header.Get(obs.RequestIDHeader)
	readBody(t, resp, false)
	e.outcome(t)

	rec := waitRecords(t, e.mock, reqID, 2)[1]
	reserved, settled, _ := e.ledger.snapshot()
	if reserved[0].AmountNUSD != ledger.Cost(mixedEstIn, 64, claudePrice) {
		t.Errorf("reserved %d, want the expensive target's worst case", reserved[0].AmountNUSD)
	}
	if len(settled) != 1 || settled[0].Provider != "mock-a" ||
		settled[0].AmountNUSD != ledger.Cost(rec.PromptTokens, rec.CompletionTokens, smallPrice) {
		t.Errorf("settled = %+v, want mock-a's price for %+v", settled, rec)
	}
}

func TestFailedRequestsAreReleased(t *testing.T) {
	tests := []struct {
		name   string
		b      mock.Behavior
		reason string
	}{
		{"upstream failure", mock.Behavior{ErrorRate: 1, ErrorStatus: 503}, ledger.ReasonFailed},
		{"upstream rejects the request", mock.Behavior{ErrorRate: 1, ErrorStatus: 400}, ledger.ReasonRejected},
		{"non-stream fails mid-generation", mock.Behavior{OutputTokens: 10, FailAfterTokens: 3}, ledger.ReasonFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t, tt.b, nil)
			resp := e.chat(t, chatReq("chat", nil))
			reqID := resp.Header.Get(obs.RequestIDHeader)
			io.Copy(io.Discard, resp.Body)
			if o := e.outcome(t); o.Billable {
				t.Errorf("outcome = %+v", o)
			}
			_, settled, released := e.ledger.snapshot()
			if len(settled) != 0 || len(released) != 1 || released[0].id.String() != reqID || released[0].reason != tt.reason {
				t.Errorf("settled %v released %v, want one release (%s)", settled, released, tt.reason)
			}
			// Nothing was used, so every admitted token goes back.
			admitted, adjusted := e.limiter.snapshot()
			if len(adjusted) != 1 || adjusted[0] != admitted[0] {
				t.Errorf("admitted %v adjusted %v", admitted, adjusted)
			}
		})
	}
}

func TestInterruptedStreamIsSettledForWhatArrived(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 10, FailAfterTokens: 3}, nil)
	resp := e.chat(t, streamReq("chat", false))
	cs := readClientStream(resp.Body)
	o := e.outcome(t)
	_, settled, released := e.ledger.snapshot()
	if len(settled) != 1 || len(released) != 0 {
		t.Fatalf("settled %v released %v", settled, released)
	}
	s := settled[0]
	if s.Reason != ledger.ReasonInterrupted || !s.UsageEstimated || s.CompletionTokens != (len(cs.content)+3)/4 ||
		s.AmountNUSD != ledger.Cost(o.Usage.PromptTokens, o.Usage.CompletionTokens, smallPrice) {
		t.Errorf("settlement = %+v for %q", s, cs.content)
	}
}

func TestClientDisconnectAccounting(t *testing.T) {
	t.Run("before the commit point", func(t *testing.T) {
		e := newEnv(t, mock.Behavior{Hang: true}, nil)
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(100*time.Millisecond, cancel)
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, e.public.URL+"/v1/chat/completions",
			strings.NewReader(`{"model":"chat","messages":[{"role":"user","content":"hi"}],"stream":true}`))
		req.Header.Set("Authorization", "Bearer "+e.key)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			resp.Body.Close()
			t.Fatal("request should have been cancelled")
		}
		e.outcome(t)
		_, settled, released := e.ledger.snapshot()
		if len(settled) != 0 || len(released) != 1 || released[0].reason != ledger.ReasonCancelled {
			t.Errorf("settled %v released %v", settled, released)
		}
	})
	t.Run("mid-stream", func(t *testing.T) {
		e := newEnv(t, mock.Behavior{OutputTokens: 2000, TokensPerSec: 200}, nil)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, e.public.URL+"/v1/chat/completions",
			strings.NewReader(`{"model":"chat","messages":[{"role":"user","content":"hi"}],"stream":true}`))
		req.Header.Set("Authorization", "Bearer "+e.key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if _, err := resp.Body.Read(make([]byte, 256)); err != nil {
			t.Fatal(err)
		}
		cancel()
		o := e.outcome(t)
		_, settled, released := e.ledger.snapshot()
		if len(settled) != 1 || len(released) != 0 || settled[0].Reason != ledger.ReasonInterrupted {
			t.Fatalf("settled %v released %v", settled, released)
		}
		if o.Usage.CompletionTokens == 0 || settled[0].CompletionTokens != o.Usage.CompletionTokens {
			t.Errorf("outcome %+v, settlement %+v", o, settled[0])
		}
	})
}

func TestRateLimitRefusal(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 2}, nil)
	e.limiter.decision = &ratelimit.Decision{RetryAfter: 2100 * time.Millisecond}
	resp := e.chat(t, chatReq("chat", nil))
	if got := resp.Header.Get("Retry-After"); got != "3" {
		t.Errorf("Retry-After = %q, want 3", got)
	}
	wantError(t, resp, http.StatusTooManyRequests, "rate_limit_exceeded")

	e.limiter.decision = &ratelimit.Decision{RetryAfter: time.Millisecond}
	resp = e.chat(t, chatReq("chat", nil))
	if got := resp.Header.Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q, want at least 1", got)
	}
	wantError(t, resp, http.StatusTooManyRequests, "rate_limit_exceeded")

	e.limiter.decision = &ratelimit.Decision{TooLarge: true}
	resp = e.chat(t, chatReq("chat", nil))
	if got := resp.Header.Get("Retry-After"); got != "" {
		t.Errorf("Retry-After = %q: waiting cannot help", got)
	}
	wantError(t, resp, http.StatusTooManyRequests, "tokens_exceed_limit")

	if reserved, _, _ := e.ledger.snapshot(); len(reserved) != 0 {
		t.Errorf("a refused request reserved budget: %v", reserved)
	}
	if n := len(e.mock.AllRecords()); n != 0 {
		t.Errorf("a refused request reached the provider %d times", n)
	}
}

func TestBudgetRefusal(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 2}, nil)
	e.ledger.reserveErr = ledger.ErrInsufficientBudget
	wantError(t, e.chat(t, chatReq("chat", nil)), http.StatusPaymentRequired, "insufficient_budget")
	if n := len(e.mock.AllRecords()); n != 0 {
		t.Errorf("provider called %d times", n)
	}
	waitRefund(t, e)
}

func TestReserveFailureFailsClosed(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 2}, nil)
	e.ledger.reserveErr = errors.New("connection refused")
	wantError(t, e.chat(t, chatReq("chat", nil)), http.StatusServiceUnavailable, "ledger_unavailable")
	if n := len(e.mock.AllRecords()); n != 0 {
		t.Errorf("provider called %d times without a reservation", n)
	}
	waitRefund(t, e)
}

// waitRefund checks that a refused request's tokens went back to the bucket.
func waitRefund(t *testing.T, e *testEnv) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		admitted, adjusted := e.limiter.snapshot()
		if len(adjusted) == 1 {
			if adjusted[0] != admitted[0] {
				t.Errorf("refunded %d of %d tokens", adjusted[0], admitted[0])
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Error("tokens were not refunded")
}

func TestRateLimiterFailsOpen(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 2}, nil)
	e.limiter.err = errors.New("redis is down")
	readBody(t, e.chat(t, chatReq("chat", nil)), false)
	e.outcome(t)
	if _, settled, _ := e.ledger.snapshot(); len(settled) != 1 {
		t.Errorf("settled = %v: the budget still applies", settled)
	}
	if _, adjusted := e.limiter.snapshot(); len(adjusted) != 0 {
		t.Errorf("adjusted %v, but no tokens were taken", adjusted)
	}
}

func TestSettlementRetries(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 2}, nil)
	e.ledger.settleErrs = 3
	readBody(t, e.chat(t, chatReq("chat", nil)), false)
	e.outcome(t)
	if _, settled, _ := e.ledger.snapshot(); len(settled) != 1 {
		t.Errorf("settled = %v", settled)
	}
}

// The client's response must not wait for Postgres, however long settling
// takes: not its last byte, and not the next request on the connection.
func TestResponseDoesNotWaitForSettlement(t *testing.T) {
	for _, stream := range []bool{false, true} {
		e := newEnv(t, mock.Behavior{OutputTokens: 2}, nil)
		gate := make(chan struct{})
		e.ledger.gate = gate

		body := fmt.Sprintf(`{"model":"chat","messages":[{"role":"user","content":"hi"}],"stream":%v}`, stream)
		post := func() error {
			req, err := http.NewRequest(http.MethodPost, e.public.URL+"/v1/chat/completions", strings.NewReader(body))
			if err != nil {
				return err
			}
			req.Header.Set("Authorization", "Bearer "+e.key)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			_, err = io.Copy(io.Discard, resp.Body) // to EOF, past [DONE]
			return err
		}
		done := make(chan error, 1)
		go func() {
			err := post()
			if err == nil {
				err = post() // reuses the connection
			}
			done <- err
		}()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(500 * time.Millisecond):
			close(gate)
			t.Fatalf("stream=%v: the response waited for settlement", stream)
		}
		if _, settled, _ := e.ledger.snapshot(); len(settled) != 0 {
			t.Fatal("settled before the gate opened")
		}
		close(gate)
		e.outcome(t)
		e.outcome(t)
	}
}

func TestDrainStopsRetries(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 2}, nil)
	e.ledger.settleErrs = -1 // fails forever
	readBody(t, e.chat(t, chatReq("chat", nil)), false)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	e.srv.Drain(ctx)
	if el := time.Since(start); el > 2*time.Second {
		t.Errorf("Drain took %v, want about its 100ms budget", el)
	}
	if o := e.outcome(t); !o.Billable {
		t.Errorf("outcome = %+v", o)
	}
}
