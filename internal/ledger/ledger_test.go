package ledger

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SamirVarma01/llm-gateway/internal/auth"
	"github.com/SamirVarma01/llm-gateway/internal/testutil"
)

func TestMain(m *testing.M) { testutil.Main(m) }

type fixture struct {
	pool *pgxpool.Pool
	l    *Ledger
	key  uuid.UUID
}

// newFixture creates a database and a key with the given budget (nil for
// unlimited).
func newFixture(t *testing.T, budget *int64) fixture {
	t.Helper()
	pool := testutil.NewDatabase(t)
	k, _, err := auth.NewPGStore(pool).CreateKey(context.Background(), auth.NewKey{Name: "t", BudgetNUSD: budget})
	if err != nil {
		t.Fatal(err)
	}
	return fixture{pool: pool, l: New(pool), key: k.ID}
}

func budget(n int64) *int64 { return &n }

func (f fixture) reservation(amount int64) Reservation {
	return Reservation{
		RequestID: uuid.Must(uuid.NewV7()), KeyID: f.key, AmountNUSD: amount, Alias: "chat",
		Meta: Meta{Policy: "priority", EstInput: 10, MaxOutput: 20,
			Targets: []TargetPrice{{Provider: "p", Model: "m", InputMicroPerM: 1, OutputMicroPerM: 2}}},
	}
}

func (f fixture) reserve(t *testing.T, amount int64) uuid.UUID {
	t.Helper()
	r := f.reservation(amount)
	if err := f.l.Reserve(context.Background(), r); err != nil {
		t.Fatalf("Reserve(%d): %v", amount, err)
	}
	return r.RequestID
}

func settlement(id uuid.UUID, amount int64) Settlement {
	return Settlement{
		RequestID: id, AmountNUSD: amount, Provider: "p", UpstreamModel: "m", PromptTokens: 7,
		CompletionTokens: 3, Reason: ReasonCompleted, Source: SourceGateway,
	}
}

// wantTotals checks the account row, and that it equals what the entries
// derive (design §8.4: key_accounts is a cache of the ledger).
func (f fixture) wantTotals(t *testing.T, spent, held int64) {
	t.Helper()
	ctx := context.Background()
	var gotSpent, gotHeld int64
	if err := f.pool.QueryRow(ctx, `SELECT spent_nusd, held_nusd FROM key_accounts WHERE key_id = $1`, f.key).
		Scan(&gotSpent, &gotHeld); err != nil {
		t.Fatal(err)
	}
	if gotSpent != spent || gotHeld != held {
		t.Errorf("account: spent %d held %d, want spent %d held %d", gotSpent, gotHeld, spent, held)
	}
	var derivedSpent, derivedHeld int64
	if err := f.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(amount_nusd) FILTER (WHERE kind = 'settle'), 0),
		       COALESCE(SUM(amount_nusd) FILTER (WHERE kind = 'reserve' AND NOT EXISTS (
		         SELECT 1 FROM ledger_entries x WHERE x.request_id = e.request_id AND x.kind = 'release')), 0)
		  FROM ledger_entries e WHERE key_id = $1`, f.key).Scan(&derivedSpent, &derivedHeld); err != nil {
		t.Fatal(err)
	}
	if derivedSpent != gotSpent || derivedHeld != gotHeld {
		t.Errorf("ledger derives spent %d held %d; account says %d, %d", derivedSpent, derivedHeld, gotSpent, gotHeld)
	}
}

func (f fixture) kinds(t *testing.T, id uuid.UUID) map[string]int64 {
	t.Helper()
	rows, err := f.pool.Query(context.Background(),
		`SELECT kind::text, amount_nusd FROM ledger_entries WHERE request_id = $1`, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var kind string
		var amount int64
		if err := rows.Scan(&kind, &amount); err != nil {
			t.Fatal(err)
		}
		out[kind] = amount
	}
	return out
}

func TestReserveAndSettle(t *testing.T) {
	f := newFixture(t, budget(1000))
	ctx := context.Background()
	id := f.reserve(t, 400)
	f.wantTotals(t, 0, 400)

	closed, err := f.l.Settle(ctx, settlement(id, 150))
	if err != nil || !closed {
		t.Fatalf("Settle = %v, %v", closed, err)
	}
	f.wantTotals(t, 150, 0)
	got := f.kinds(t, id)
	if len(got) != 3 || got["reserve"] != 400 || got["settle"] != 150 || got["release"] != 250 {
		t.Errorf("entries = %v", got)
	}

	var alias, provider, model, reason string
	var prompt, completion int
	var estimated bool
	if err := f.pool.QueryRow(ctx, `
		SELECT s.alias, s.provider, s.upstream_model, s.prompt_tokens, s.completion_tokens, s.usage_estimated, r.reason
		  FROM ledger_entries s JOIN ledger_entries r USING (request_id)
		 WHERE s.request_id = $1 AND s.kind = 'settle' AND r.kind = 'release'`, id).
		Scan(&alias, &provider, &model, &prompt, &completion, &estimated, &reason); err != nil {
		t.Fatal(err)
	}
	if alias != "chat" || provider != "p" || model != "m" || prompt != 7 || completion != 3 || estimated || reason != ReasonCompleted {
		t.Errorf("settle row: %s %s %s %d %d %v %s", alias, provider, model, prompt, completion, estimated, reason)
	}

	var targets int
	if err := f.pool.QueryRow(ctx, `
		SELECT jsonb_array_length(meta->'targets') FROM ledger_entries WHERE request_id = $1 AND kind = 'reserve'`, id).
		Scan(&targets); err != nil || targets != 1 {
		t.Errorf("reserve meta targets = %d, %v", targets, err)
	}
}

func TestReserveEnforcesBudget(t *testing.T) {
	f := newFixture(t, budget(1000))
	ctx := context.Background()
	first := f.reserve(t, 600)
	f.reserve(t, 400) // exactly up to the budget

	r := f.reservation(1)
	if err := f.l.Reserve(ctx, r); !errors.Is(err, ErrInsufficientBudget) {
		t.Fatalf("Reserve past the budget: %v", err)
	}
	if got := f.kinds(t, r.RequestID); len(got) != 0 {
		t.Errorf("a refused reservation wrote entries: %v", got)
	}
	f.wantTotals(t, 0, 1000)

	// Settling for less frees the rest; spent money stays spent.
	if _, err := f.l.Settle(ctx, settlement(first, 100)); err != nil {
		t.Fatal(err)
	}
	f.wantTotals(t, 100, 400)
	f.reserve(t, 500)
	if err := f.l.Reserve(ctx, f.reservation(1)); !errors.Is(err, ErrInsufficientBudget) {
		t.Errorf("Reserve past the budget after settling: %v", err)
	}
}

func TestUnlimitedKeyIsStillRecorded(t *testing.T) {
	f := newFixture(t, nil)
	id := f.reserve(t, 1<<40)
	f.wantTotals(t, 0, 1<<40)
	if got := f.kinds(t, id); got["reserve"] != 1<<40 {
		t.Errorf("entries = %v: orphans are found through reserve entries", got)
	}
}

func TestReleaseChargesNothing(t *testing.T) {
	f := newFixture(t, budget(1000))
	id := f.reserve(t, 400)
	closed, err := f.l.Release(context.Background(), id, ReasonFailed, SourceGateway)
	if err != nil || !closed {
		t.Fatalf("Release = %v, %v", closed, err)
	}
	f.wantTotals(t, 0, 0)
	if got := f.kinds(t, id); len(got) != 2 || got["release"] != 400 {
		t.Errorf("entries = %v", got)
	}
}

func TestOverageIsChargedInFull(t *testing.T) {
	f := newFixture(t, budget(1000))
	ctx := context.Background()
	id := f.reserve(t, 900)
	if _, err := f.l.Settle(ctx, settlement(id, 1200)); err != nil {
		t.Fatal(err)
	}
	f.wantTotals(t, 1200, 0)
	if got := f.kinds(t, id); got["release"] != 0 || got["settle"] != 1200 {
		t.Errorf("entries = %v", got)
	}
	if err := f.l.Reserve(ctx, f.reservation(0)); !errors.Is(err, ErrInsufficientBudget) {
		t.Errorf("Reserve on an overspent key: %v", err)
	}
}

func TestClosingTwiceIsANoOp(t *testing.T) {
	f := newFixture(t, budget(1000))
	ctx := context.Background()
	id := f.reserve(t, 400)
	if closed, err := f.l.Settle(ctx, settlement(id, 100)); err != nil || !closed {
		t.Fatalf("first settle = %v, %v", closed, err)
	}
	if closed, err := f.l.Settle(ctx, settlement(id, 300)); err != nil || closed {
		t.Errorf("second settle = %v, %v", closed, err)
	}
	if closed, err := f.l.Release(ctx, id, ReasonFailed, SourceReconciler); err != nil || closed {
		t.Errorf("release after settle = %v, %v", closed, err)
	}
	f.wantTotals(t, 100, 0)
	if got := f.kinds(t, id); got["settle"] != 100 || got["release"] != 300 {
		t.Errorf("entries = %v", got)
	}
}

func TestClosingAnUnknownRequestIsANoOp(t *testing.T) {
	f := newFixture(t, budget(1000))
	ctx := context.Background()
	if closed, err := f.l.Settle(ctx, settlement(uuid.New(), 100)); err != nil || closed {
		t.Errorf("Settle = %v, %v", closed, err)
	}
	if closed, err := f.l.Release(ctx, uuid.New(), ReasonFailed, SourceGateway); err != nil || closed {
		t.Errorf("Release = %v, %v", closed, err)
	}
	f.wantTotals(t, 0, 0)
}

// Concurrent reservations serialize on the account row: however they
// interleave, exactly as many fit as the budget allows.
func TestConcurrentReservationsCannotPassTheBudget(t *testing.T) {
	const fits, tries, each = 10, 40, 100
	f := newFixture(t, budget(fits*each))
	var ok, refused atomic.Int64
	var wg sync.WaitGroup
	for range tries {
		wg.Go(func() {
			switch err := f.l.Reserve(context.Background(), f.reservation(each)); {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, ErrInsufficientBudget):
				refused.Add(1)
			default:
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if ok.Load() != fits || refused.Load() != tries-fits {
		t.Errorf("admitted %d, refused %d; want %d, %d", ok.Load(), refused.Load(), fits, tries-fits)
	}
	f.wantTotals(t, 0, fits*each)
}

// A gateway settle racing a reconciler release, many at once: each request
// closes exactly once, and the totals match the winners.
func TestConcurrentClosesCloseOnce(t *testing.T) {
	const requests, amount = 25, 100
	f := newFixture(t, nil)
	ctx := context.Background()
	ids := make([]uuid.UUID, requests)
	for i := range ids {
		ids[i] = f.reserve(t, amount)
	}
	var settled, released atomic.Int64
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Go(func() {
			closed, err := f.l.Settle(ctx, settlement(id, 40))
			if err != nil {
				t.Error(err)
			}
			if closed {
				settled.Add(1)
			}
		})
		wg.Go(func() {
			closed, err := f.l.Release(ctx, id, ReasonFailed, SourceReconciler)
			if err != nil {
				t.Error(err)
			}
			if closed {
				released.Add(1)
			}
		})
	}
	wg.Wait()
	if settled.Load()+released.Load() != requests {
		t.Fatalf("%d settled + %d released, want %d closes", settled.Load(), released.Load(), requests)
	}
	f.wantTotals(t, settled.Load()*40, 0)
}

func TestLedgerIsAppendOnly(t *testing.T) {
	f := newFixture(t, nil)
	ctx := context.Background()
	id := f.reserve(t, 100)
	if _, err := f.pool.Exec(ctx, `UPDATE ledger_entries SET amount_nusd = 0 WHERE request_id = $1`, id); err == nil {
		t.Error("UPDATE on the ledger succeeded")
	}
	if _, err := f.pool.Exec(ctx, `DELETE FROM ledger_entries WHERE request_id = $1`, id); err == nil {
		t.Error("DELETE on the ledger succeeded")
	}
}
