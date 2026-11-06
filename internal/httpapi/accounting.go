package httpapi

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/SamirVarma01/llm-gateway/internal/auth"
	"github.com/SamirVarma01/llm-gateway/internal/ledger"
	"github.com/SamirVarma01/llm-gateway/internal/oai"
	"github.com/SamirVarma01/llm-gateway/internal/obs"
	"github.com/SamirVarma01/llm-gateway/internal/provider"
	"github.com/SamirVarma01/llm-gateway/internal/ratelimit"
	"github.com/SamirVarma01/llm-gateway/internal/router"
)

// Backoff between attempts to close a request in the ledger.
const (
	closeBackoffBase = 50 * time.Millisecond
	closeBackoffMax  = 5 * time.Second
)

// RateLimiter admits requests against a key's rates (design §7).
type RateLimiter interface {
	Admit(ctx context.Context, id uuid.UUID, lim ratelimit.Limits, tokens int) (ratelimit.Decision, error)
	Adjust(ctx context.Context, id uuid.UUID, lim ratelimit.Limits, delta int) error
}

// Ledger holds and charges budget (design §8).
type Ledger interface {
	Reserve(ctx context.Context, r ledger.Reservation) error
	Settle(ctx context.Context, s ledger.Settlement) (bool, error)
	Release(ctx context.Context, requestID uuid.UUID, reason, source string) (bool, error)
}

// admission is what a request took when it was let in, which closing it
// settles up.
type admission struct {
	requestID    uuid.UUID
	keyID        uuid.UUID
	limits       ratelimit.Limits
	tokens       int   // taken from the token bucket; 0 if rate limiting failed open
	reservedNUSD int64 // held against the budget
}

// admit rate-limits a request and then reserves its worst case: the most
// expensive target it could end up on, so failover never needs a second
// reservation (design D2). A refused request has no ledger entries, and any
// tokens it took are refunded, though it still counts as a request.
func (s *Server) admit(ctx context.Context, key auth.Key, plan *router.Plan, estIn int) (*admission, *apiError) {
	log := obs.Logger(ctx)
	id, err := uuid.Parse(obs.RequestID(ctx))
	if err != nil {
		log.Error("request id is not a UUID", "err", err)
		return nil, &apiError{status: http.StatusInternalServerError, typ: oai.ErrTypeAPI, code: "internal_error", msg: "Internal error."}
	}

	adm := &admission{requestID: id, keyID: key.ID, limits: ratelimit.Limits{RPM: key.RPMLimit, TPM: key.TPMLimit}}
	meta := ledger.Meta{Policy: plan.Policy, EstInput: estIn}
	for _, c := range plan.Choices {
		maxOut := *c.Req.MaxTokens
		meta.MaxOutput = max(meta.MaxOutput, maxOut)
		adm.reservedNUSD = max(adm.reservedNUSD, ledger.Cost(estIn, maxOut, c.Target.Price))
		meta.Targets = append(meta.Targets, ledger.TargetPrice{
			Provider: c.Target.Provider, Model: c.Target.UpstreamModel,
			InputMicroPerM: c.Target.Price.InputMicroPerM, OutputMicroPerM: c.Target.Price.OutputMicroPerM,
		})
	}

	tokens := estIn + meta.MaxOutput
	dec, err := s.opts.Limiter.Admit(ctx, adm.keyID, adm.limits, tokens)
	switch {
	case err != nil:
		// Fail open: budgets are in Postgres, so only rate fairness is lost.
		log.Warn("rate limiter unavailable; admitting without a limit", "err", err)
	case dec.TooLarge:
		return nil, &apiError{status: http.StatusTooManyRequests, typ: oai.ErrTypeRateLimit, code: "tokens_exceed_limit",
			msg: fmt.Sprintf("This request needs up to %d tokens, more than this key's tokens-per-minute limit. Lower max_tokens.", tokens)}
	case !dec.Allowed:
		wait := max(1, int(math.Ceil(dec.RetryAfter.Seconds())))
		return nil, &apiError{status: http.StatusTooManyRequests, typ: oai.ErrTypeRateLimit, code: "rate_limit_exceeded",
			msg: fmt.Sprintf("Rate limit exceeded. Try again in %d seconds.", wait), retryAfter: wait}
	default:
		adm.tokens = tokens
	}

	rctx, cancel := context.WithTimeout(ctx, s.opts.LedgerTimeout)
	defer cancel()
	err = s.opts.Ledger.Reserve(rctx, ledger.Reservation{
		RequestID: id, KeyID: adm.keyID, AmountNUSD: adm.reservedNUSD, Alias: plan.Model.Alias, Meta: meta,
	})
	if err == nil {
		return adm, nil
	}
	s.background(ctx, func(ctx context.Context) { s.adjustTokens(ctx, adm, adm.tokens) })
	if errors.Is(err, ledger.ErrInsufficientBudget) {
		return nil, &apiError{status: http.StatusPaymentRequired, typ: oai.ErrTypeBudget, code: "insufficient_budget",
			msg: fmt.Sprintf("This key's remaining budget cannot cover this request, which could cost up to $%.6f.",
				float64(adm.reservedNUSD)/1e9)}
	}
	// Fail closed: without a reservation the request could overspend.
	log.Error("reserve failed", "err", err)
	return nil, &apiError{status: http.StatusServiceUnavailable, typ: oai.ErrTypeAPI, code: "ledger_unavailable",
		msg: "The gateway cannot record usage right now, so it is not accepting requests."}
}

// conclude closes a finished request in the background. Nothing the client
// is waiting for depends on it, and running it in the handler would keep the
// response open: a stream's terminating chunk, and the connection's next
// request, would wait for Postgres, for as long as settlement retries.
func (s *Server) conclude(ctx context.Context, adm *admission, out outcome) {
	s.background(ctx, func(ctx context.Context) {
		s.account(ctx, adm, &out)
		s.finish(ctx, out)
	})
}

// account returns unused tokens to the bucket and closes the request in the
// ledger: charged if it was billable, released if not (design §10). The
// close retries until the request's deadline. If it still fails, the
// reservation stays open for the reconciler, which is safe because closing
// is idempotent.
func (s *Server) account(ctx context.Context, adm *admission, out *outcome) {
	used := 0
	if out.Billable {
		used = out.Usage.PromptTokens + out.Usage.CompletionTokens
		out.CostNUSD = ledger.Cost(out.Usage.PromptTokens, out.Usage.CompletionTokens, out.Price)
	}
	if adm.tokens > 0 {
		s.adjustTokens(ctx, adm, adm.tokens-used)
	}

	var closed bool
	err := s.retryClose(ctx, func(ctx context.Context) (err error) {
		if !out.Billable {
			closed, err = s.opts.Ledger.Release(ctx, adm.requestID, releaseReason(*out), ledger.SourceGateway)
			return err
		}
		reason := ledger.ReasonInterrupted
		if out.Result == resultCompleted {
			reason = ledger.ReasonCompleted
		}
		closed, err = s.opts.Ledger.Settle(ctx, ledger.Settlement{
			RequestID: adm.requestID, AmountNUSD: out.CostNUSD, Provider: out.Provider, UpstreamModel: out.UpstreamModel,
			PromptTokens: out.Usage.PromptTokens, CompletionTokens: out.Usage.CompletionTokens,
			UsageEstimated: out.Usage.Estimated, Reason: reason, Source: ledger.SourceGateway,
		})
		return err
	})
	log := obs.Logger(ctx)
	switch {
	case err != nil:
		log.Error("could not close the request; leaving it to the reconciler", "err", err)
	case !closed:
		log.Warn("request was already closed")
	}
}

func releaseReason(out outcome) string {
	var ue *provider.UpstreamError
	switch {
	case out.Result == resultClientDisconnected:
		return ledger.ReasonCancelled
	case errors.As(out.Err, &ue) && ue.Kind == provider.KindBadRequest:
		return ledger.ReasonRejected
	}
	return ledger.ReasonFailed
}

func (s *Server) adjustTokens(ctx context.Context, adm *admission, delta int) {
	if err := s.opts.Limiter.Adjust(ctx, adm.keyID, adm.limits, delta); err != nil {
		obs.Logger(ctx).Warn("could not adjust the token bucket", "err", err, "delta", delta)
	}
}

// retryClose runs op with full-jitter backoff until it succeeds or ctx ends.
func (s *Server) retryClose(ctx context.Context, op func(context.Context) error) error {
	ceiling := closeBackoffBase
	for {
		tctx, cancel := context.WithTimeout(ctx, s.opts.LedgerTimeout)
		err := op(tctx)
		cancel()
		if err == nil {
			return nil
		}
		obs.Logger(ctx).Warn("ledger close failed; retrying", "err", err)
		t := time.NewTimer(time.Duration(rand.Float64() * float64(ceiling)))
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return err
		}
		ceiling = min(2*ceiling, closeBackoffMax)
	}
}

// background runs f off the request path, with ctx's values but not its
// cancellation: the client leaving must not abort the accounting of what it
// used. f may run until the request's deadline, or for LedgerTimeout if that
// is later, and Drain tracks it.
func (s *Server) background(ctx context.Context, f func(context.Context)) {
	deadline := s.now().Add(s.opts.LedgerTimeout)
	if d, ok := ctx.Deadline(); ok && d.After(deadline) {
		deadline = d
	}
	detached := context.WithoutCancel(ctx)
	s.inflight.Add(1)
	go func() {
		defer s.inflight.Add(-1)
		ctx, cancel := context.WithDeadline(detached, deadline)
		defer cancel()
		defer context.AfterFunc(s.stopping, cancel)()
		f(ctx)
	}()
}

// Drain waits for background accounting to finish. If ctx ends first, it
// cancels what is left and waits for that to stop; requests it leaves open
// are closed by the reconciler. Call it once the HTTP servers have stopped.
func (s *Server) Drain(ctx context.Context) {
	t := time.NewTicker(5 * time.Millisecond)
	defer t.Stop()
	for s.inflight.Load() > 0 {
		select {
		case <-t.C:
		case <-ctx.Done():
			s.stop()
			<-t.C
		}
	}
}
