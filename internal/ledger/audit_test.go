package ledger

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/SamirVarma01/llm-gateway/internal/provider"
)

var cheapPrice = provider.Price{InputMicroPerM: 1000, OutputMicroPerM: 2000}

// settled reserves and settles a request on the cheap target.
func (f fixture) settled(t *testing.T, prompt, completion int, source string) uuid.UUID {
	t.Helper()
	id := f.orphan(t, 500_000, cheap)
	s := Settlement{
		RequestID: id, AmountNUSD: Cost(prompt, completion, cheapPrice), Provider: "cheap", UpstreamModel: "c1",
		PromptTokens: prompt, CompletionTokens: completion, Reason: ReasonCompleted, Source: source,
	}
	if _, err := f.l.Settle(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	return id
}

func billedRecord(id uuid.UUID, attempt, prompt, completion int) UsageRecord {
	return UsageRecord{RequestID: id.String(), Provider: "cheap", Model: "c1", Attempt: attempt,
		PromptTokens: prompt, CompletionTokens: completion, Billable: true, Outcome: "complete"}
}

func (f fixture) audit(t *testing.T, opts AuditOptions) *AuditReport {
	t.Helper()
	rep, err := f.l.Audit(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

func wantViolation(t *testing.T, rep *AuditReport, check, sample string) {
	t.Helper()
	if rep.Violations[check] != 1 {
		t.Errorf("violations = %v, want one %s (samples %v)", rep.Violations, check, rep.Samples)
		return
	}
	if !strings.Contains(strings.Join(rep.Samples, "\n"), sample) {
		t.Errorf("samples %v do not mention %q", rep.Samples, sample)
	}
}

func TestAuditPassesACleanLedger(t *testing.T) {
	f := newFixture(t, nil)
	ctx := context.Background()
	gw := f.settled(t, 10, 20, SourceGateway)
	rec := f.settled(t, 5, 5, SourceReconciler)
	failed := f.orphan(t, 400, cheap)
	if _, err := f.l.Release(ctx, failed, ReasonFailed, SourceGateway); err != nil {
		t.Fatal(err)
	}
	estimated := f.orphan(t, 500_000, cheap)
	if _, err := f.l.Settle(ctx, Settlement{
		RequestID: estimated, AmountNUSD: Cost(3, 4, cheapPrice), Provider: "cheap", UpstreamModel: "c1",
		PromptTokens: 3, CompletionTokens: 4, UsageEstimated: true, Reason: ReasonInterrupted, Source: SourceGateway,
	}); err != nil {
		t.Fatal(err)
	}
	f.orphan(t, 100, cheap) // open, which is allowed here

	rep := f.audit(t, AuditOptions{ProviderRecords: []UsageRecord{
		billedRecord(gw, 1, 99, 99), // a failed-over attempt, not the one charged
		billedRecord(gw, 2, 10, 20),
		billedRecord(rec, 1, 5, 5),
		billedRecord(failed, 1, 7, 7), // the provider billed it; the client was not charged
	}})
	if !rep.OK() {
		t.Fatalf("violations = %v, samples %v", rep.Violations, rep.Samples)
	}
	if rep.Keys != 1 || rep.Requests != 5 || rep.Open != 1 || rep.BilledNotCharged != 1 || rep.Released[ReasonFailed] != 1 {
		t.Errorf("report = %+v", rep)
	}
	if rep.Gateway != (SettleStats{Settled: 2, Estimated: 1, Compared: 1}) {
		t.Errorf("gateway = %+v", rep.Gateway)
	}
	if rep.Reconciler != (SettleStats{Settled: 1, Compared: 1}) {
		t.Errorf("reconciler = %+v", rep.Reconciler)
	}
}

func TestAuditCatchesWrongTotals(t *testing.T) {
	f := newFixture(t, nil)
	f.settled(t, 10, 20, SourceGateway)
	if _, err := f.pool.Exec(context.Background(), `UPDATE key_accounts SET spent_nusd = spent_nusd + 1 WHERE key_id = $1`, f.key); err != nil {
		t.Fatal(err)
	}
	wantViolation(t, f.audit(t, AuditOptions{}), CheckAccountTotals, "account says spent")
}

// forge writes an entry directly, as a bug or a tampering could.
func (f fixture) forge(t *testing.T, id uuid.UUID, kind string, amount int64, extra ...any) {
	t.Helper()
	cols, vals := `request_id, key_id, kind, amount_nusd, source`, `$1, $2, $3, $4, 'gateway'`
	args := []any{id, f.key, kind, amount}
	if len(extra) > 0 {
		cols += `, provider, upstream_model, prompt_tokens, completion_tokens`
		vals += `, $5, $6, $7, $8`
		args = append(args, extra...)
	}
	if _, err := f.pool.Exec(context.Background(),
		`INSERT INTO ledger_entries (`+cols+`) VALUES (`+vals+`)`, args...); err != nil {
		t.Fatal(err)
	}
}

func TestAuditCatchesStrayEntries(t *testing.T) {
	f := newFixture(t, nil)
	f.forge(t, uuid.New(), "release", 0)
	wantViolation(t, f.audit(t, AuditOptions{}), CheckStructure, "no reserve")
}

func TestAuditCatchesAChargeWithNoClose(t *testing.T) {
	f := newFixture(t, nil)
	id := f.orphan(t, 400, cheap)
	f.forge(t, id, "settle", Cost(1, 1, cheapPrice), "cheap", "c1", 1, 1)
	rep := f.audit(t, AuditOptions{})
	if rep.Violations[CheckStructure] != 1 {
		t.Errorf("violations = %v", rep.Violations)
	}
}

func TestAuditCatchesAWrongRelease(t *testing.T) {
	f := newFixture(t, nil)
	id := f.orphan(t, 400, cheap)
	f.forge(t, id, "release", 399)
	wantViolation(t, f.audit(t, AuditOptions{}), CheckCloseAmount, "released 399 of a 400 reservation")
}

func TestAuditCatchesAWrongCharge(t *testing.T) {
	f := newFixture(t, nil)
	id := f.orphan(t, 500_000, cheap)
	want := Cost(10, 10, cheapPrice)
	if _, err := f.l.Settle(context.Background(), Settlement{
		RequestID: id, AmountNUSD: want + 1, Provider: "cheap", UpstreamModel: "c1",
		PromptTokens: 10, CompletionTokens: 10, Reason: ReasonCompleted, Source: SourceGateway,
	}); err != nil {
		t.Fatal(err)
	}
	wantViolation(t, f.audit(t, AuditOptions{}), CheckCost, "charged")
}

func TestAuditCatchesAChargeForAnUnlistedTarget(t *testing.T) {
	f := newFixture(t, nil)
	id := f.orphan(t, 500_000, cheap)
	if _, err := f.l.Settle(context.Background(), Settlement{
		RequestID: id, AmountNUSD: 1, Provider: "elsewhere", UpstreamModel: "x",
		Reason: ReasonCompleted, Source: SourceGateway,
	}); err != nil {
		t.Fatal(err)
	}
	wantViolation(t, f.audit(t, AuditOptions{}), CheckCost, "not among its targets")
}

func TestAuditComparesChargesWithTheProvider(t *testing.T) {
	f := newFixture(t, nil)
	mismatch := f.settled(t, 10, 20, SourceGateway)
	f.settled(t, 1, 1, SourceGateway) // no provider record at all
	rep := f.audit(t, AuditOptions{ProviderRecords: []UsageRecord{billedRecord(mismatch, 1, 10, 21)}})
	if rep.Violations[CheckProviderUsage] != 2 || rep.Gateway.Mismatched != 2 || rep.Gateway.Compared != 2 {
		t.Fatalf("report = %+v", rep)
	}
	samples := strings.Join(rep.Samples, "\n")
	for _, want := range []string{"completion tokens: provider recorded 21", "no billable provider record"} {
		if !strings.Contains(samples, want) {
			t.Errorf("samples %q do not mention %q", samples, want)
		}
	}
}

func TestAuditRequireClosed(t *testing.T) {
	f := newFixture(t, nil)
	id := f.orphan(t, 400, cheap)
	if rep := f.audit(t, AuditOptions{}); !rep.OK() || rep.Open != 1 {
		t.Errorf("open reservations are allowed by default: %+v", rep)
	}
	wantViolation(t, f.audit(t, AuditOptions{RequireClosed: true}), CheckOpen, id.String())
}

func TestAuditCapsSamples(t *testing.T) {
	f := newFixture(t, nil)
	const n = maxAuditSamples + 10
	for range n {
		f.orphan(t, 1, cheap)
	}
	rep := f.audit(t, AuditOptions{RequireClosed: true})
	if rep.Violations[CheckOpen] != n || len(rep.Samples) != maxAuditSamples {
		t.Errorf("%d violations, %d samples", rep.Violations[CheckOpen], len(rep.Samples))
	}
}
