package router

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/SamirVarma01/llm-gateway/internal/provider"
)

// errHang makes a fakeStream block until its context ends.
var errHang = errors.New("hang")

type step struct {
	delay time.Duration
	chunk provider.Chunk
}

// fakeStream replays steps, then ends with end (io.EOF for a clean end).
// Like the real adapters, it returns the context's error when cancelled.
type fakeStream struct {
	ctx    context.Context
	steps  []step
	end    error
	rep    provider.Reported
	closed bool
}

func (s *fakeStream) Recv() (*provider.Chunk, error) {
	if len(s.steps) == 0 {
		if s.end == errHang {
			<-s.ctx.Done()
			return nil, s.ctx.Err()
		}
		return nil, s.end
	}
	st := s.steps[0]
	s.steps = s.steps[1:]
	if st.delay > 0 {
		t := time.NewTimer(st.delay)
		defer t.Stop()
		select {
		case <-t.C:
		case <-s.ctx.Done():
			return nil, s.ctx.Err()
		}
	}
	c := st.chunk
	return &c, nil
}

func (s *fakeStream) Usage() provider.Reported { return s.rep }
func (s *fakeStream) Close() error             { s.closed = true; return nil }

// reply is a stream that answers with the given words and stops.
func reply(words ...string) func(context.Context, int) (provider.Stream, error) {
	return func(ctx context.Context, _ int) (provider.Stream, error) {
		steps := []step{{chunk: provider.Chunk{Role: "assistant"}}}
		for _, w := range words {
			steps = append(steps, step{chunk: provider.Chunk{Content: w}})
		}
		steps = append(steps, step{chunk: provider.Chunk{FinishReason: provider.FinishStop}})
		return &fakeStream{ctx: ctx, steps: steps, end: io.EOF,
			rep: provider.Reported{CompletionTokens: len(words), HasCompletion: true}}, nil
	}
}

// refuse fails every call with an upstream status.
func refuse(name string, status int) func(context.Context, int) (provider.Stream, error) {
	return func(context.Context, int) (provider.Stream, error) {
		kind, retryable := provider.ClassifyStatus(status)
		return nil, &provider.UpstreamError{Provider: name, Status: status, Kind: kind, Retryable: retryable, Msg: "refused"}
	}
}

// hang accepts the call and never sends anything.
func hang(ctx context.Context, _ int) (provider.Stream, error) {
	return &fakeStream{ctx: ctx, end: errHang}, nil
}

// untilOutput is the streaming commit rule: read to the first output chunk.
func untilOutput(a *Attempt) error {
	for {
		c, err := a.Stream.Recv()
		if errors.Is(err, io.EOF) {
			return &provider.UpstreamError{Provider: a.Choice.Target.Provider, Kind: provider.KindProtocol,
				Retryable: true, Msg: "stream ended without output"}
		}
		if err != nil {
			return err
		}
		if c.HasOutput() {
			return nil
		}
	}
}

// collect is the non-streaming commit rule: read the whole response.
func collect(a *Attempt) error {
	_, err := provider.Collect(a.Stream)
	return err
}

var fastRetry = RetryConfig{
	MaxAttempts: 3,
	TTFTTimeout: 100 * time.Millisecond,
	IdleTimeout: 100 * time.Millisecond,
	BackoffBase: time.Millisecond,
	BackoffMax:  2 * time.Millisecond,
}

// calls records which providers were called, and with which attempt number.
type calls []string

func (c *calls) wrap(name string, f func(context.Context, int) (provider.Stream, error)) func(context.Context, int) (provider.Stream, error) {
	return func(ctx context.Context, n int) (provider.Stream, error) {
		*c = append(*c, name+string(rune('0'+n)))
		return f(ctx, n)
	}
}

func execute(t *testing.T, r *Router, policy string, commit func(*Attempt) error) (*Attempt, error) {
	t.Helper()
	m, _ := r.Resolve("m")
	p, err := r.Plan(m, Request{Chat: chatRequest(true), Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	return r.Execute(context.Background(), p, commit)
}

func failures(r *Router, name string) int {
	m, _ := r.Resolve("m")
	for _, c := range m.Candidates {
		if c.Target.Provider == name {
			b := c.health.breaker
			b.mu.Lock()
			defer b.mu.Unlock()
			n := 0
			for _, s := range b.window {
				if s.valid && s.failed {
					n++
				}
			}
			return n
		}
	}
	panic("no provider " + name)
}

func TestExecuteFailsOverBeforeCommit(t *testing.T) {
	for _, status := range []int{500, 503, 529, 429, 401, 404} {
		r, fakes := newTestRouter(t, policyRoutes, Options{Retry: fastRetry})
		var got calls
		fakes["a"].stream = got.wrap("a", refuse("a", status))
		fakes["b"].stream = got.wrap("b", reply("hi"))

		a, err := execute(t, r, "", untilOutput)
		if err != nil {
			t.Fatalf("status %d: %v", status, err)
		}
		if a.N != 2 || a.Choice.Target.Provider != "b" || strings.Join(got, ",") != "a1,b2" {
			t.Errorf("status %d: attempt %d on %s, calls %v", status, a.N, a.Choice.Target.Provider, got)
		}
		a.Succeeded(1)
		if failures(r, "a") != 1 {
			t.Errorf("status %d: a's breaker recorded %d failures, want 1", status, failures(r, "a"))
		}
	}
}

func TestExecuteDoesNotFailOverOnBadRequest(t *testing.T) {
	r, fakes := newTestRouter(t, policyRoutes, Options{Retry: fastRetry})
	var got calls
	fakes["a"].stream = got.wrap("a", refuse("a", 400))
	fakes["b"].stream = got.wrap("b", reply("hi"))

	_, err := execute(t, r, "", untilOutput)
	var f *Failure
	var ue *provider.UpstreamError
	if !errors.As(err, &f) || f.Attempts != 1 || !errors.As(err, &ue) || ue.Status != 400 {
		t.Fatalf("err = %v, want a 400 after 1 attempt", err)
	}
	if strings.Join(got, ",") != "a1" {
		t.Errorf("calls = %v, want only a1", got)
	}
	if failures(r, "a") != 0 {
		t.Error("a client error counted against the target's breaker")
	}
}

func TestExecuteBacksOffAndRetriesFromTheTop(t *testing.T) {
	r, fakes := newTestRouter(t, policyRoutes, Options{Retry: fastRetry})
	var got calls
	fails := 1
	fakes["a"].stream = got.wrap("a", func(ctx context.Context, n int) (provider.Stream, error) {
		if fails > 0 {
			fails--
			return refuse("a", 503)(ctx, n)
		}
		return reply("ok")(ctx, n)
	})
	fakes["b"].stream = got.wrap("b", refuse("b", 503))
	fakes["c"].decline = errors.New("not this one")

	a, err := execute(t, r, "", untilOutput)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Succeeded(1)
	// Pass 1: a, b. Pass 2 after a backoff: a again.
	if a.N != 3 || strings.Join(got, ",") != "a1,b2,a3" {
		t.Errorf("attempt %d, calls %v; want a1,b2,a3", a.N, got)
	}
}

func TestExecuteGivesUpAfterMaxAttempts(t *testing.T) {
	r, fakes := newTestRouter(t, policyRoutes, Options{Retry: fastRetry})
	var got calls
	for _, name := range []string{"a", "b", "c"} {
		fakes[name].stream = got.wrap(name, refuse(name, 502))
	}
	_, err := execute(t, r, "", untilOutput)
	var f *Failure
	var ue *provider.UpstreamError
	if !errors.As(err, &f) || f.Attempts != 3 || !errors.As(err, &ue) || ue.Provider != "c" {
		t.Fatalf("err = %v, want the last of 3 attempts", err)
	}
	if strings.Join(got, ",") != "a1,b2,c3" {
		t.Errorf("calls = %v", got)
	}
}

func TestExecuteSkipsOpenBreakers(t *testing.T) {
	r, fakes := newTestRouter(t, policyRoutes, Options{Retry: fastRetry})
	m, _ := r.Resolve("m")
	trip := func(i int) {
		for range 5 {
			tk, _ := m.Candidates[i].health.breaker.allow()
			tk.failure()
		}
	}
	var got calls
	fakes["a"].stream = got.wrap("a", reply("hi"))
	fakes["b"].stream = got.wrap("b", reply("hi"))
	fakes["c"].stream = got.wrap("c", reply("hi"))
	trip(0)

	a, err := execute(t, r, "", untilOutput)
	if err != nil {
		t.Fatal(err)
	}
	a.Succeeded(1)
	// Skipping a target is not an attempt.
	if a.N != 1 || a.Choice.Target.Provider != "b" || strings.Join(got, ",") != "b1" {
		t.Errorf("attempt %d on %s, calls %v", a.N, a.Choice.Target.Provider, got)
	}

	trip(1)
	trip(2)
	_, err = execute(t, r, "", untilOutput)
	var f *Failure
	if !errors.As(err, &f) || !errors.Is(err, ErrNoHealthyTarget) || f.Attempts != 0 {
		t.Fatalf("err = %v, want ErrNoHealthyTarget with no attempts", err)
	}
	if f.RetryAfter <= 0 || f.RetryAfter > 5*time.Second {
		t.Errorf("RetryAfter = %v, want within the 5s cooldown", f.RetryAfter)
	}
}

func TestExecuteTTFTTimeoutFailsOver(t *testing.T) {
	r, fakes := newTestRouter(t, policyRoutes, Options{Retry: fastRetry})
	var got calls
	// a sends its role chunk and then nothing: no output, so still pre-commit.
	fakes["a"].stream = got.wrap("a", func(ctx context.Context, _ int) (provider.Stream, error) {
		return &fakeStream{ctx: ctx, steps: []step{{chunk: provider.Chunk{Role: "assistant"}}}, end: errHang}, nil
	})
	fakes["b"].stream = got.wrap("b", reply("hi"))

	start := time.Now()
	a, err := execute(t, r, "", untilOutput)
	if err != nil {
		t.Fatal(err)
	}
	a.Succeeded(1)
	if el := time.Since(start); el < fastRetry.TTFTTimeout || el > 2*time.Second {
		t.Errorf("failover took %v, want about the 100ms TTFT timeout", el)
	}
	if a.Choice.Target.Provider != "b" || failures(r, "a") != 1 {
		t.Errorf("served by %s; a's failures = %d", a.Choice.Target.Provider, failures(r, "a"))
	}
}

func TestExecuteAllTimeOut(t *testing.T) {
	r, fakes := newTestRouter(t, policyRoutes, Options{Retry: fastRetry})
	for _, name := range []string{"a", "b", "c"} {
		fakes[name].stream = hang
	}
	_, err := execute(t, r, "", untilOutput)
	var ue *provider.UpstreamError
	if !errors.As(err, &ue) || ue.Kind != provider.KindTimeout || !strings.Contains(ue.Msg, "100ms") {
		t.Fatalf("err = %v, want a TTFT timeout", err)
	}
}

func TestIdleTimeoutAfterCommit(t *testing.T) {
	r, fakes := newTestRouter(t, policyRoutes, Options{Retry: fastRetry})
	fakes["a"].stream = func(ctx context.Context, _ int) (provider.Stream, error) {
		return &fakeStream{ctx: ctx, steps: []step{
			{chunk: provider.Chunk{Content: "one"}},
			{delay: 50 * time.Millisecond, chunk: provider.Chunk{Content: "two"}}, // within the idle timeout
		}, end: errHang}, nil
	}
	a, err := execute(t, r, "", untilOutput)
	if err != nil {
		t.Fatal(err)
	}
	if c, err := a.Stream.Recv(); err != nil || c.Content != "two" {
		t.Fatalf("Recv = %v, %v", c, err)
	}
	_, err = a.Stream.Recv()
	var ue *provider.UpstreamError
	if !errors.As(err, &ue) || ue.Kind != provider.KindTimeout || !strings.Contains(ue.Msg, "upstream data") {
		t.Fatalf("err = %v, want an idle timeout", err)
	}
	a.Failed(err)
	if failures(r, "a") != 1 {
		t.Error("mid-stream timeout not recorded against the target")
	}
}

func TestNonStreamFailsOverAfterPartialOutput(t *testing.T) {
	r, fakes := newTestRouter(t, policyRoutes, Options{Retry: fastRetry})
	var got calls
	fakes["a"].stream = got.wrap("a", func(ctx context.Context, _ int) (provider.Stream, error) {
		return &fakeStream{ctx: ctx, steps: []step{{chunk: provider.Chunk{Content: "partial"}}},
			end: &provider.UpstreamError{Provider: "a", Kind: provider.KindTransport, Retryable: true, Msg: "reset"}}, nil
	})
	fakes["b"].stream = got.wrap("b", reply("whole"))
	a, err := execute(t, r, "", collect)
	if err != nil {
		t.Fatal(err)
	}
	a.Succeeded(1)
	if a.Choice.Target.Provider != "b" || strings.Join(got, ",") != "a1,b2" {
		t.Errorf("served by %s, calls %v", a.Choice.Target.Provider, got)
	}
}

func TestExecuteStopsWhenTheClientLeaves(t *testing.T) {
	r, fakes := newTestRouter(t, policyRoutes, Options{Retry: RetryConfig{
		MaxAttempts: 3, TTFTTimeout: time.Minute, IdleTimeout: time.Minute, BackoffBase: time.Millisecond, BackoffMax: time.Millisecond,
	}})
	var got calls
	fakes["a"].stream = got.wrap("a", hang)
	fakes["b"].stream = got.wrap("b", reply("hi"))
	m, _ := r.Resolve("m")
	p, _ := r.Plan(m, Request{Chat: chatRequest(true)})

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	_, err := r.Execute(ctx, p, untilOutput)
	var f *Failure
	if !errors.As(err, &f) || !errors.Is(err, context.Canceled) || f.Attempts != 1 {
		t.Fatalf("err = %v, want Canceled after 1 attempt", err)
	}
	if strings.Join(got, ",") != "a1" || failures(r, "a") != 0 {
		t.Errorf("calls %v; a's failures %d (a client leaving is not the target's fault)", got, failures(r, "a"))
	}
}

func TestExecuteStopsNearTheDeadline(t *testing.T) {
	r, fakes := newTestRouter(t, policyRoutes, Options{Retry: fastRetry})
	var got calls
	fakes["a"].stream = got.wrap("a", refuse("a", 503))
	fakes["b"].stream = got.wrap("b", reply("hi"))
	m, _ := r.Resolve("m")
	p, _ := r.Plan(m, Request{Chat: chatRequest(true)})

	// 50ms left is less than the 100ms TTFT timeout: no second attempt.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := r.Execute(ctx, p, untilOutput)
	var f *Failure
	if !errors.As(err, &f) || f.Attempts != 1 || strings.Join(got, ",") != "a1" {
		t.Fatalf("err = %v, calls %v; want 1 attempt", err, got)
	}
}

func TestAttemptRecordsLatency(t *testing.T) {
	r, fakes := newTestRouter(t, policyRoutes, Options{Retry: fastRetry})
	fakes["a"].stream = func(ctx context.Context, _ int) (provider.Stream, error) {
		return &fakeStream{ctx: ctx, steps: []step{
			{delay: 20 * time.Millisecond, chunk: provider.Chunk{Content: "a"}},
			{delay: 20 * time.Millisecond, chunk: provider.Chunk{Content: "b"}},
			{delay: 20 * time.Millisecond, chunk: provider.Chunk{Content: "c", FinishReason: "stop"}},
		}, end: io.EOF}, nil
	}
	a, err := execute(t, r, "", untilOutput)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Collect(a.Stream); err != nil {
		t.Fatal(err)
	}
	if a.TTFT() < 20*time.Millisecond {
		t.Errorf("TTFT = %v, want >= 20ms", a.TTFT())
	}
	a.Succeeded(3)
	a.Failed(errors.New("ignored: the attempt already ended"))

	m, _ := r.Resolve("m")
	ttft, ok, tps, tpsOK := m.Candidates[0].health.latency.estimates(time.Now())
	// 2 tokens after the first, over about 40ms.
	if !ok || ttft < 0.02 || !tpsOK || tps <= 0 || tps > 2/0.04 {
		t.Errorf("ttft %v (%v), tps %v (%v)", ttft, ok, tps, tpsOK)
	}
	if avg, ok := m.output.get(); !ok || avg != 3 {
		t.Errorf("alias output average = %v, %v; want 3", avg, ok)
	}
	if failures(r, "a") != 0 {
		t.Error("a second report on an ended attempt was counted")
	}
}

func TestHalfOpenProbeIsHeldUntilTheAttemptEnds(t *testing.T) {
	clk := newFakeClock()
	r, fakes := newTestRouter(t, policyRoutes, Options{Retry: fastRetry, Now: clk.now})
	m, _ := r.Resolve("m")
	for range 5 {
		tk, _ := m.Candidates[0].health.breaker.allow()
		tk.failure()
	}
	clk.advance(5 * time.Second)
	fakes["a"].stream = reply("probe")
	fakes["b"].stream = reply("other")

	probe, err := execute(t, r, "", untilOutput)
	if err != nil || probe.Choice.Target.Provider != "a" {
		t.Fatalf("probe: %v on %v", err, probe)
	}
	// While the probe is in flight, a is skipped.
	other, err := execute(t, r, "", untilOutput)
	if err != nil || other.Choice.Target.Provider != "b" {
		t.Fatalf("second request: %v", err)
	}
	other.Succeeded(1)
	probe.Abandoned() // the client left: the slot frees without a verdict
	next, err := execute(t, r, "", untilOutput)
	if err != nil || next.Choice.Target.Provider != "a" {
		t.Fatalf("after release: %v", err)
	}
	next.Succeeded(1)
}
