package router

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/SamirVarma01/llm-gateway/internal/obs"
	"github.com/SamirVarma01/llm-gateway/internal/provider"
)

// RetryConfig configures the attempt loop (design §6.4).
type RetryConfig struct {
	MaxAttempts int           // attempts per request, across all candidates
	TTFTTimeout time.Duration // from the upstream call to its first output
	IdleTimeout time.Duration // between upstream chunks after the first output
	BackoffBase time.Duration // backoff before the second pass over the candidates
	BackoffMax  time.Duration // cap for the doubling backoff
}

// DefaultRetryConfig returns the defaults from design §6.4.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts: 3,
		TTFTTimeout: 10 * time.Second,
		IdleTimeout: 30 * time.Second,
		BackoffBase: 100 * time.Millisecond,
		BackoffMax:  2 * time.Second,
	}
}

// ErrNoHealthyTarget means every capable target's breaker refused the
// request, so no attempt was made.
var ErrNoHealthyTarget = errors.New("router: every target's circuit breaker is open")

// Failure is why Execute committed nothing. It unwraps to the last attempt's
// error, ErrNoHealthyTarget, or, if the request context ended, that
// context's error joined with the last attempt's.
type Failure struct {
	Attempts   int           // upstream attempts made
	RetryAfter time.Duration // with ErrNoHealthyTarget: until the first breaker admits a probe
	Err        error
}

func (f *Failure) Error() string {
	return fmt.Sprintf("router: no attempt committed after %d attempt(s): %v", f.Attempts, f.Err)
}

func (f *Failure) Unwrap() error { return f.Err }

// Execute runs the attempt loop for a plan (design §6.4).
//
// It walks the plan's choices in order, skipping targets whose breaker
// refuses, and for each attempt calls the upstream and then commit. commit
// reads Attempt.Stream until the response can be committed to the client:
// up to the first output chunk for a streaming request, the whole response
// for a non-streaming one. If commit returns nil, Execute returns the
// attempt, still open; the caller reads the rest of the stream and must end
// the attempt with exactly one of Succeeded, Failed, or Abandoned.
//
// If the upstream call or commit fails, the attempt is recorded and closed.
// A failure that allows failover moves on to the next choice at once; after
// a full pass Execute waits a full-jitter exponential backoff and starts
// again from the top, up to MaxAttempts in total. It stops early when the
// error does not allow failover, when the request context ends, or when
// less than TTFTTimeout remains before the request's deadline.
func (r *Router) Execute(ctx context.Context, p *Plan, commit func(*Attempt) error) (*Attempt, error) {
	cfg := r.opts.Retry
	log := obs.Logger(ctx)
	n := 0
	var lastErr error
	stop := func() (*Attempt, error) {
		if err := ctx.Err(); err != nil {
			return nil, &Failure{Attempts: n, Err: errors.Join(err, lastErr)}
		}
		return nil, &Failure{Attempts: n, Err: lastErr}
	}

	for pass := 0; ; pass++ {
		if pass > 0 {
			if n >= cfg.MaxAttempts || !r.timeForAnother(ctx) {
				return stop()
			}
			waited := r.opts.Now()
			err := r.backoff(ctx, pass)
			p.UpstreamWait += r.opts.Now().Sub(waited)
			if err != nil {
				return stop()
			}
		}
		admitted := false
		for _, c := range p.Choices {
			if n >= cfg.MaxAttempts || (n > 0 && !r.timeForAnother(ctx)) {
				return stop()
			}
			tk, ok := c.health.breaker.allow()
			if !ok {
				continue
			}
			admitted = true
			n++
			a, err := r.attempt(ctx, p, c, n, tk, commit)
			if err == nil {
				return a, nil
			}
			lastErr = err
			if ctx.Err() != nil {
				return stop()
			}
			failover := retryable(err)
			log.Warn("upstream attempt failed", "attempt", n, "provider", c.Target.Provider,
				"upstream_model", c.Target.UpstreamModel, "err", err, "failover", failover)
			if !failover {
				return stop()
			}
		}
		if !admitted {
			if n == 0 {
				return nil, &Failure{Err: ErrNoHealthyTarget, RetryAfter: retryAfter(p)}
			}
			return stop()
		}
	}
}

// retryable reports whether another target might serve a request that
// failed with err (design §6.3).
func retryable(err error) bool {
	var ue *provider.UpstreamError
	return errors.As(err, &ue) && ue.Retryable
}

// timeForAnother reports whether the request's deadline leaves room for
// another attempt to reach its first output.
func (r *Router) timeForAnother(ctx context.Context) bool {
	dl, ok := ctx.Deadline()
	return !ok || time.Until(dl) >= r.opts.Retry.TTFTTimeout
}

// backoff waits before pass number pass (1-based): a uniformly random time
// up to min(BackoffMax, BackoffBase·2^(pass-1)), so concurrent requests
// retrying the same targets spread out.
func (r *Router) backoff(ctx context.Context, pass int) error {
	cfg := r.opts.Retry
	ceiling := cfg.BackoffMax
	if shift := pass - 1; shift < 30 {
		ceiling = min(cfg.BackoffBase<<shift, cfg.BackoffMax)
	}
	t := time.NewTimer(time.Duration(r.opts.Rand() * float64(ceiling)))
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// retryAfter is how long until the first of the plan's breakers admits a probe.
func retryAfter(p *Plan) time.Duration {
	soonest := time.Duration(-1)
	for _, c := range p.Choices {
		state, in := c.health.breaker.snapshot()
		if state != StateOpen {
			in = 0 // half-open with every probe slot taken: try again soon
		}
		if soonest < 0 || in < soonest {
			soonest = in
		}
	}
	return max(soonest, 0)
}

func (r *Router) attempt(ctx context.Context, p *Plan, c Choice, n int, tk *ticket, commit func(*Attempt) error) (*Attempt, error) {
	actx, cancel := context.WithCancelCause(provider.WithAttempt(ctx, n))
	ts := &timedStream{
		provider: c.Target.Provider,
		parent:   ctx,
		ctx:      actx,
		cancel:   cancel,
		ttft:     r.opts.Retry.TTFTTimeout,
		idle:     r.opts.Retry.IdleTimeout,
		now:      r.opts.Now,
		waited:   &p.UpstreamWait,
	}
	a := &Attempt{N: n, Choice: c, r: r, model: p.Model, parent: ctx, ticket: tk, ts: ts, start: r.opts.Now()}
	ts.timer = time.AfterFunc(ts.ttft, ts.stall)

	st, err := c.Provider.Stream(actx, c.Target, c.Req)
	p.UpstreamWait += r.opts.Now().Sub(a.start)
	if err != nil {
		err = ts.classify(err)
		a.Failed(err)
		return nil, err
	}
	ts.inner = st
	a.Stream = ts
	if err := commit(a); err != nil {
		a.Failed(err)
		return nil, err
	}
	return a, nil
}

// Attempt is one upstream call made for a request.
type Attempt struct {
	N      int             // 1-based attempt number, sent upstream as X-Attempt
	Choice Choice          // the target called
	Stream provider.Stream // the upstream response, with the TTFT and idle timeouts applied

	r        *Router
	model    *Model
	parent   context.Context
	ticket   *ticket
	ts       *timedStream
	start    time.Time
	finished bool
}

// TTFT is the time from the upstream call to its first output chunk, or 0
// if no output arrived.
func (a *Attempt) TTFT() time.Duration {
	if !a.ts.gotOutput.Load() {
		return 0
	}
	return a.ts.firstOutput.Sub(a.start)
}

// Succeeded ends an attempt whose upstream completed its response with
// completionTokens output tokens. The target's breaker records a success,
// and its latency averages a TTFT and decode-rate sample.
func (a *Attempt) Succeeded(completionTokens int) {
	if !a.end() {
		return
	}
	a.ticket.success()
	if a.ts.gotOutput.Load() {
		a.Choice.health.latency.observeDecode(completionTokens, a.ts.lastOutput.Sub(a.ts.firstOutput), a.r.opts.Now())
	}
	a.model.output.observe(completionTokens)
}

// Failed ends an attempt whose upstream failed with err. The target's
// breaker records a failure unless err is the client's own fault
// (KindBadRequest) or the request itself ended (the client left or the
// request timed out), which say nothing about the target.
func (a *Attempt) Failed(err error) {
	if !a.end() {
		return
	}
	var ue *provider.UpstreamError
	if a.parent.Err() != nil || (errors.As(err, &ue) && ue.Kind == provider.KindBadRequest) {
		a.ticket.release()
		return
	}
	a.ticket.failure()
}

// Abandoned ends an attempt the caller stopped for its own reasons, such as
// the client disconnecting. The breaker records nothing.
func (a *Attempt) Abandoned() {
	if a.end() {
		a.ticket.release()
	}
}

// end closes the attempt and records its TTFT. It reports false if the
// attempt had already ended.
func (a *Attempt) end() bool {
	if a.finished {
		return false
	}
	a.finished = true
	_ = a.ts.Close()
	if d := a.TTFT(); d > 0 {
		a.Choice.health.latency.observeTTFT(d, a.ts.firstOutput)
	}
	return true
}

// stallError is the cancellation cause when an attempt's upstream goes quiet.
type stallError struct {
	waitingFor string
	limit      time.Duration
}

func (e *stallError) Error() string { return fmt.Sprintf("no %s within %v", e.waitingFor, e.limit) }

// timedStream applies the TTFT and idle timeouts to an upstream stream
// (design §6.4). One timer cancels the attempt's context: it first runs for
// the TTFT timeout, and once output has arrived it restarts with the idle
// timeout after every chunk. The resulting cancellation is reported as a
// KindTimeout upstream error, which allows failover before the commit point.
type timedStream struct {
	inner    provider.Stream
	provider string
	parent   context.Context
	ctx      context.Context
	cancel   context.CancelCauseFunc
	timer    *time.Timer
	ttft     time.Duration
	idle     time.Duration
	now      func() time.Time
	waited   *time.Duration // the plan's UpstreamWait

	gotOutput   atomic.Bool // read by the timer goroutine
	firstOutput time.Time
	lastOutput  time.Time
	closed      bool
}

func (s *timedStream) stall() {
	if s.gotOutput.Load() {
		s.cancel(&stallError{"upstream data", s.idle})
		return
	}
	s.cancel(&stallError{"output from the upstream", s.ttft})
}

func (s *timedStream) Recv() (*provider.Chunk, error) {
	start := s.now()
	c, err := s.inner.Recv()
	now := s.now()
	*s.waited += now.Sub(start)
	if err != nil {
		return nil, s.classify(err)
	}
	if c.HasOutput() {
		if !s.gotOutput.Load() {
			s.firstOutput = now
			s.gotOutput.Store(true)
		}
		s.lastOutput = now
	}
	if s.gotOutput.Load() {
		s.timer.Reset(s.idle)
	}
	return c, nil
}

func (s *timedStream) Usage() provider.Reported { return s.inner.Usage() }

func (s *timedStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.timer.Stop()
	var err error
	if s.inner != nil {
		err = s.inner.Close()
	}
	s.cancel(nil)
	return err
}

// classify turns a cancellation caused by a stall into a timeout error. A
// clean end, the request's own cancellation, and every other error pass
// through unchanged.
func (s *timedStream) classify(err error) error {
	if errors.Is(err, io.EOF) || s.parent.Err() != nil {
		return err
	}
	var se *stallError
	if errors.As(context.Cause(s.ctx), &se) {
		return &provider.UpstreamError{Provider: s.provider, Kind: provider.KindTimeout, Retryable: true, Msg: se.Error()}
	}
	return err
}
