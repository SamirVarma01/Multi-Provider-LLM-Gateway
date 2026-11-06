package ledger

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/SamirVarma01/llm-gateway/internal/config"
	"github.com/SamirVarma01/llm-gateway/internal/mock"
	"github.com/SamirVarma01/llm-gateway/internal/provider"
)

// stubUsage answers lookups from a table.
type stubUsage struct {
	records map[uuid.UUID][]UsageRecord
	err     error
	calls   int
}

func (s *stubUsage) Usage(_ context.Context, id uuid.UUID) ([]UsageRecord, error) {
	s.calls++
	return s.records[id], s.err
}

var quietLog = slog.New(slog.NewTextHandler(io.Discard, nil))

// Prices in the snapshot, deliberately unlike anything the route table has.
var (
	cheap = TargetPrice{Provider: "cheap", Model: "c1", InputMicroPerM: 1000, OutputMicroPerM: 2000}
	dear  = TargetPrice{Provider: "dear", Model: "d1", InputMicroPerM: 5000, OutputMicroPerM: 9000}
)

func (f fixture) orphan(t *testing.T, amount int64, targets ...TargetPrice) uuid.UUID {
	t.Helper()
	r := f.reservation(amount)
	r.Meta.Targets = targets
	if err := f.l.Reserve(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	return r.RequestID
}

func (f fixture) reconciler(sources map[string]UsageSource) *Reconciler {
	return NewReconciler(f.l, sources, 0, quietLog)
}

func pass(t *testing.T, r *Reconciler) Result {
	t.Helper()
	res, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func (f fixture) closeRow(t *testing.T, id uuid.UUID) (reason, source string) {
	t.Helper()
	if err := f.pool.QueryRow(context.Background(),
		`SELECT reason, source FROM ledger_entries WHERE request_id = $1 AND kind = 'release'`, id).Scan(&reason, &source); err != nil {
		t.Fatalf("release entry for %s: %v", id, err)
	}
	return reason, source
}

func TestReconcileChargesWhatTheProviderBilled(t *testing.T) {
	f := newFixture(t, nil)
	id := f.orphan(t, 500_000, cheap, dear)
	src := &stubUsage{records: map[uuid.UUID][]UsageRecord{id: {
		{Provider: "dear", Model: "d1", Attempt: 1, PromptTokens: 10, CompletionTokens: 20, Billable: true, Outcome: "complete"},
	}}}

	res := pass(t, f.reconciler(map[string]UsageSource{"cheap": src, "dear": src}))
	if res.Orphans != 1 || res.Settled != 1 {
		t.Fatalf("result = %+v", res)
	}
	if src.calls != 1 {
		t.Errorf("%d lookups; targets sharing a source need one", src.calls)
	}
	want := Cost(10, 20, provider.Price{InputMicroPerM: 5000, OutputMicroPerM: 9000})
	f.wantTotals(t, want, 0)
	got := f.kinds(t, id)
	if got["settle"] != want || got["release"] != 500_000-want {
		t.Errorf("entries = %v, want a charge of %d from the price snapshot", got, want)
	}
	if reason, source := f.closeRow(t, id); reason != ReasonCompleted || source != SourceReconciler {
		t.Errorf("closed with %s by %s", reason, source)
	}
}

func TestReconcileUsesTheLastBillableAttempt(t *testing.T) {
	f := newFixture(t, nil)
	id := f.orphan(t, 500_000, cheap, dear)
	src := &stubUsage{records: map[uuid.UUID][]UsageRecord{id: {
		// Billed by the provider, then failed over: not charged to the client.
		{Provider: "cheap", Model: "c1", Attempt: 1, PromptTokens: 10, CompletionTokens: 3, Billable: true, Outcome: "mid_stream_failure"},
		{Provider: "dear", Model: "d1", Attempt: 2, PromptTokens: 10, CompletionTokens: 5, Billable: true, Outcome: "client_cancelled"},
		{Provider: "cheap", Model: "c1", Attempt: 3, PromptTokens: 10, Outcome: "injected_error"},
	}}}
	pass(t, f.reconciler(map[string]UsageSource{"cheap": src, "dear": src}))
	if got, want := f.kinds(t, id)["settle"], Cost(10, 5, provider.Price{InputMicroPerM: 5000, OutputMicroPerM: 9000}); got != want {
		t.Errorf("charged %d, want attempt 2's %d", got, want)
	}
	if reason, _ := f.closeRow(t, id); reason != ReasonInterrupted {
		t.Errorf("reason = %s: attempt 2 did not finish", reason)
	}
}

func TestReconcileReleasesWhenNothingWasBilled(t *testing.T) {
	f := newFixture(t, nil)
	never := f.orphan(t, 400, cheap)
	failed := f.orphan(t, 400, cheap)
	src := &stubUsage{records: map[uuid.UUID][]UsageRecord{
		failed: {{Provider: "cheap", Model: "c1", Attempt: 1, Outcome: "injected_error"}},
	}}
	res := pass(t, f.reconciler(map[string]UsageSource{"cheap": src}))
	if res.ReleasedNoCall != 2 {
		t.Fatalf("result = %+v", res)
	}
	f.wantTotals(t, 0, 0)
	for _, id := range []uuid.UUID{never, failed} {
		if reason, source := f.closeRow(t, id); reason != ReasonOrphanNoCall || source != SourceReconciler {
			t.Errorf("closed with %s by %s", reason, source)
		}
	}
}

// With a target it cannot ask, no record does not mean no call.
func TestReconcileUnknownUsageIsAnUndercharge(t *testing.T) {
	f := newFixture(t, nil)
	askNobody := f.orphan(t, 400, dear)
	mixed := f.orphan(t, 400, cheap, dear)
	billedAnyway := f.orphan(t, 400, cheap, dear)
	src := &stubUsage{records: map[uuid.UUID][]UsageRecord{billedAnyway: {
		{Provider: "cheap", Model: "c1", Attempt: 1, PromptTokens: 1, Billable: true, Outcome: "complete"},
	}}}
	res := pass(t, f.reconciler(map[string]UsageSource{"cheap": src})) // "dear" has no usage API
	if res.ReleasedUnknownUsage != 2 || res.Settled != 1 {
		t.Fatalf("result = %+v", res)
	}
	for _, id := range []uuid.UUID{askNobody, mixed} {
		if reason, _ := f.closeRow(t, id); reason != ReasonOrphanUnknownUsage {
			t.Errorf("reason = %s", reason)
		}
	}
	if reason, _ := f.closeRow(t, billedAnyway); reason != ReasonCompleted {
		t.Errorf("reason = %s: a known charge beats unknown usage", reason)
	}
}

func TestReconcileLeavesOrphansOpenWhenLookupFails(t *testing.T) {
	f := newFixture(t, nil)
	id := f.orphan(t, 400, cheap)
	src := &stubUsage{err: errors.New("mock is down"), records: map[uuid.UUID][]UsageRecord{id: {
		{Provider: "cheap", Model: "c1", Attempt: 1, PromptTokens: 2, Billable: true, Outcome: "complete"},
	}}}
	r := f.reconciler(map[string]UsageSource{"cheap": src})
	if res := pass(t, r); res.Failed != 1 || res.Settled+res.ReleasedNoCall != 0 {
		t.Fatalf("result = %+v", res)
	}
	f.wantTotals(t, 0, 400)

	src.err = nil
	if res := pass(t, r); res.Settled != 1 {
		t.Errorf("after recovery: %+v", res)
	}
	f.wantTotals(t, Cost(2, 0, provider.Price{InputMicroPerM: 1000, OutputMicroPerM: 2000}), 0)
}

func TestReconcileRefusesARecordOutsideTheSnapshot(t *testing.T) {
	f := newFixture(t, nil)
	id := f.orphan(t, 400, cheap)
	src := &stubUsage{records: map[uuid.UUID][]UsageRecord{id: {
		{Provider: "cheap", Model: "other-model", Attempt: 1, PromptTokens: 2, Billable: true, Outcome: "complete"},
	}}}
	if res := pass(t, f.reconciler(map[string]UsageSource{"cheap": src})); res.Failed != 1 {
		t.Errorf("result = %+v: with no price it must not guess", res)
	}
	f.wantTotals(t, 0, 400)
}

func TestReconcileIgnoresLiveAndClosedRequests(t *testing.T) {
	f := newFixture(t, nil)
	ctx := context.Background()
	young := f.orphan(t, 400, cheap)
	src := &stubUsage{}
	sources := map[string]UsageSource{"cheap": src}

	if res := pass(t, NewReconciler(f.l, sources, time.Hour, quietLog)); res.Orphans != 0 {
		t.Errorf("a reservation younger than the limit was picked up: %+v", res)
	}
	if got := f.kinds(t, young); len(got) != 1 {
		t.Errorf("entries = %v", got)
	}

	if _, err := f.l.Settle(ctx, settlement(young, 100)); err != nil {
		t.Fatal(err)
	}
	if res := pass(t, f.reconciler(sources)); res.Orphans != 0 || src.calls != 0 {
		t.Errorf("a closed request was picked up: %+v", res)
	}
}

func TestReconcileRunsOneAtATime(t *testing.T) {
	f := newFixture(t, nil)
	ctx := context.Background()
	f.orphan(t, 400, cheap)
	r := f.reconciler(map[string]UsageSource{"cheap": &stubUsage{}})

	other, err := f.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Release()
	if _, err := other.Exec(ctx, `SELECT pg_advisory_lock($1)`, reconcileLockKey); err != nil {
		t.Fatal(err)
	}
	if res := pass(t, r); !res.Skipped || res.Orphans != 0 {
		t.Fatalf("result = %+v", res)
	}
	if _, err := other.Exec(ctx, `SELECT pg_advisory_unlock($1)`, reconcileLockKey); err != nil {
		t.Fatal(err)
	}
	if res := pass(t, r); res.Skipped || res.ReleasedNoCall != 1 {
		t.Errorf("result = %+v", res)
	}
	// A finished pass gives the lock back.
	if res := pass(t, r); res.Skipped {
		t.Errorf("result = %+v", res)
	}
}

// One pass clears any number of orphans, and ones it cannot resolve do not
// stop it from reaching the rest.
func TestReconcilePagesPastUnresolvedOrphans(t *testing.T) {
	f := newFixture(t, nil)
	const stuck, fine = orphanBatch + 5, 30
	sources := map[string]UsageSource{"dear": &stubUsage{err: errors.New("down")}, "cheap": &stubUsage{}}
	for range stuck {
		f.orphan(t, 10, dear)
	}
	for range fine {
		f.orphan(t, 10, cheap)
	}
	res := pass(t, f.reconciler(sources))
	if res.Orphans != stuck+fine || res.Failed != stuck || res.ReleasedNoCall != fine {
		t.Errorf("result = %+v", res)
	}
	f.wantTotals(t, 0, stuck*10)
}

func TestReconcilerRunStopsWithItsContext(t *testing.T) {
	f := newFixture(t, nil)
	id := f.orphan(t, 400, cheap)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.reconciler(map[string]UsageSource{"cheap": &stubUsage{}}).Run(ctx, 10*time.Millisecond)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for len(f.kinds(t, id)) < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	if got := f.kinds(t, id); len(got) != 2 {
		t.Errorf("entries = %v: the loop should have closed it", got)
	}
}

// Against the real mock: what it records is what gets charged. An attempt
// cut off mid-stream is the crash case, and is billed for what it flushed.
func TestReconcileAgainstTheMock(t *testing.T) {
	f := newFixture(t, nil)
	m := mock.New(mock.Options{Defaults: mock.Behavior{OutputTokens: 6}, Seed: 1})
	srv := httptest.NewServer(m.Handler())
	t.Cleanup(srv.Close)

	tr := &http.Transport{}
	t.Cleanup(tr.CloseIdleConnections)
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	sources := UsageSources([]config.ProviderSpec{
		{Name: "cheap", UsageURL: srv.URL + "/"},
		{Name: "dear", UsageURL: srv.URL},
		{Name: "real"},
	}, client)
	if len(sources) != 2 || sources["cheap"] != sources["dear"] {
		t.Fatalf("sources = %v: one per URL, none without one", sources)
	}

	served := f.orphan(t, 500_000, cheap)
	unused := f.orphan(t, 500_000, cheap)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/p/cheap/v1/chat/completions",
		strings.NewReader(`{"model":"c1","messages":[{"role":"user","content":"one two three"}]}`))
	req.Header.Set(mock.RequestIDHeader, served.String())
	req.Header.Set(mock.AttemptHeader, strconv.Itoa(1))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	res := pass(t, f.reconciler(sources))
	if res.Settled != 1 || res.ReleasedNoCall != 1 {
		t.Fatalf("result = %+v", res)
	}
	rec := m.Records(served.String())[0]
	want := Cost(rec.PromptTokens, rec.CompletionTokens, provider.Price{InputMicroPerM: 1000, OutputMicroPerM: 2000})
	if got := f.kinds(t, served)["settle"]; got != want || rec.CompletionTokens != 6 {
		t.Errorf("charged %d, want %d for %+v", got, want, rec)
	}
	if reason, _ := f.closeRow(t, unused); reason != ReasonOrphanNoCall {
		t.Errorf("reason = %s", reason)
	}
}
