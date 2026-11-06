package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Audit checks, each named in Audit.Violations.
const (
	CheckAccountTotals = "account_totals" // key_accounts differs from what the entries derive
	CheckStructure     = "structure"      // entries that cannot exist: a close with no reserve, a charge with no close
	CheckCloseAmount   = "close_amount"   // a release that does not add up with its reserve and charge
	CheckCost          = "cost"           // a charge that does not equal its tokens at the snapshot price
	CheckProviderUsage = "provider_usage" // a charge that differs from what the provider recorded
	CheckOpen          = "open"           // a reservation left open, when none should be
)

// maxAuditSamples bounds how many violations an Audit describes in full.
const maxAuditSamples = 50

// AuditOptions configures Audit.
type AuditOptions struct {
	// ProviderRecords, if set, is what the provider recorded, to compare
	// charges with. Only the mock can supply it.
	ProviderRecords []UsageRecord
	// RequireClosed reports open reservations as violations. Set it once
	// traffic has stopped and the reconciler has run.
	RequireClosed bool
}

// SettleStats summarizes charges from one source. Audit keeps the sources
// apart because they prove different things: the reconciler charges from the
// provider's records, so its charges agree with them by construction, while
// agreement on the gateway's charges is evidence.
type SettleStats struct {
	Settled    int64 `json:"settled"`
	Estimated  int64 `json:"estimated"`  // not compared: the provider never reported these counts
	Compared   int64 `json:"compared"`   // matched against a provider record
	Mismatched int64 `json:"mismatched"` // of those compared
}

// AuditReport is the result of Audit.
type AuditReport struct {
	Keys             int64            `json:"keys"`
	Requests         int64            `json:"requests"`
	Open             int64            `json:"open"`
	Gateway          SettleStats      `json:"gateway"`
	Reconciler       SettleStats      `json:"reconciler"`
	Released         map[string]int64 `json:"released"`           // by reason
	BilledNotCharged int64            `json:"billed_not_charged"` // provider records with a charge that went unclaimed (design §12)
	Violations       map[string]int64 `json:"violations"`         // by check
	Samples          []string         `json:"samples"`            // the first violations, described
}

// OK reports whether no check failed.
func (r *AuditReport) OK() bool { return len(r.Violations) == 0 }

func (r *AuditReport) violate(check, format string, args ...any) {
	r.Violations[check]++
	if len(r.Samples) < maxAuditSamples {
		r.Samples = append(r.Samples, check+": "+fmt.Sprintf(format, args...))
	}
}

// Audit recomputes everything the ledger claims from its entries, in one
// read-only pass (design §8.4). Run it with traffic stopped: requests in
// flight between two of its queries can show up as false differences.
func (l *Ledger) Audit(ctx context.Context, opts AuditOptions) (*AuditReport, error) {
	rep := &AuditReport{Released: map[string]int64{}, Violations: map[string]int64{}}
	records := map[string][]UsageRecord{}
	for _, r := range opts.ProviderRecords {
		records[r.RequestID] = append(records[r.RequestID], r)
	}

	if err := l.auditAccounts(ctx, rep); err != nil {
		return nil, fmt.Errorf("ledger: audit: %w", err)
	}
	if err := l.auditStrays(ctx, rep); err != nil {
		return nil, fmt.Errorf("ledger: audit: %w", err)
	}
	if err := l.auditRequests(ctx, rep, records, opts); err != nil {
		return nil, fmt.Errorf("ledger: audit: %w", err)
	}
	return rep, nil
}

func (l *Ledger) auditAccounts(ctx context.Context, rep *AuditReport) error {
	rows, err := l.pool.Query(ctx, `
		SELECT a.key_id, a.spent_nusd, a.held_nusd, COALESCE(s.spent, 0)::bigint, COALESCE(h.held, 0)::bigint
		  FROM key_accounts a
		  LEFT JOIN (SELECT key_id, SUM(amount_nusd) AS spent FROM ledger_entries
		              WHERE kind = 'settle' GROUP BY key_id) s USING (key_id)
		  LEFT JOIN (SELECT r.key_id, SUM(r.amount_nusd) AS held FROM ledger_entries r
		              WHERE r.kind = 'reserve' AND NOT EXISTS (
		                SELECT 1 FROM ledger_entries x WHERE x.request_id = r.request_id AND x.kind = 'release')
		              GROUP BY r.key_id) h USING (key_id)`)
	if err != nil {
		return err
	}
	var id uuid.UUID
	var spent, held, wantSpent, wantHeld int64
	_, err = pgx.ForEachRow(rows, []any{&id, &spent, &held, &wantSpent, &wantHeld}, func() error {
		rep.Keys++
		if spent != wantSpent || held != wantHeld {
			rep.violate(CheckAccountTotals, "key %s: account says spent %d held %d; entries derive %d and %d",
				id, spent, held, wantSpent, wantHeld)
		}
		return nil
	})
	return err
}

// auditStrays finds closes and charges with no reserve entry.
func (l *Ledger) auditStrays(ctx context.Context, rep *AuditReport) error {
	rows, err := l.pool.Query(ctx, `
		SELECT e.request_id, e.kind::text FROM ledger_entries e
		 WHERE e.kind <> 'reserve' AND NOT EXISTS (
		   SELECT 1 FROM ledger_entries r WHERE r.request_id = e.request_id AND r.kind = 'reserve')`)
	if err != nil {
		return err
	}
	var id uuid.UUID
	var kind string
	_, err = pgx.ForEachRow(rows, []any{&id, &kind}, func() error {
		rep.violate(CheckStructure, "request %s has a %s entry but no reserve", id, kind)
		return nil
	})
	return err
}

func (l *Ledger) auditRequests(ctx context.Context, rep *AuditReport, records map[string][]UsageRecord, opts AuditOptions) error {
	rows, err := l.pool.Query(ctx, `
		SELECT r.request_id, r.amount_nusd, r.meta,
		       c.amount_nusd, c.reason,
		       s.amount_nusd, s.provider, s.upstream_model, s.prompt_tokens, s.completion_tokens,
		       s.usage_estimated, s.source
		  FROM ledger_entries r
		  LEFT JOIN ledger_entries c ON c.request_id = r.request_id AND c.kind = 'release'
		  LEFT JOIN ledger_entries s ON s.request_id = r.request_id AND s.kind = 'settle'
		 WHERE r.kind = 'reserve'`)
	if err != nil {
		return err
	}
	var q auditRow
	_, err = pgx.ForEachRow(rows, []any{&q.id, &q.reserved, &q.meta, &q.released, &q.reason, &q.charged, &q.provider,
		&q.model, &q.prompt, &q.completion, &q.estimated, &q.source}, func() error {
		q.audit(rep, records[q.id.String()], opts)
		return nil
	})
	return err
}

// auditRow is one request with its close and charge, if any.
type auditRow struct {
	id                 uuid.UUID
	reserved           int64
	meta               []byte
	released           *int64
	reason             *string
	charged            *int64
	provider, model    *string
	prompt, completion *int
	estimated          *bool
	source             *string
}

func (q *auditRow) audit(rep *AuditReport, recs []UsageRecord, opts AuditOptions) {
	rep.Requests++
	if q.released == nil {
		rep.Open++
		if opts.RequireClosed {
			rep.violate(CheckOpen, "request %s is still open", q.id)
		}
		if q.charged != nil {
			rep.violate(CheckStructure, "request %s has a charge but no close", q.id)
		}
		return
	}
	if q.charged == nil {
		if *q.released != q.reserved {
			rep.violate(CheckCloseAmount, "request %s released %d of a %d reservation with no charge", q.id, *q.released, q.reserved)
		}
		if q.reason != nil {
			rep.Released[*q.reason]++
		}
		if opts.ProviderRecords != nil {
			if last := lastBillable(recs); last != nil {
				rep.BilledNotCharged++
			}
		}
		return
	}

	if want := max(q.reserved-*q.charged, 0); *q.released != want {
		rep.violate(CheckCloseAmount, "request %s released %d, want %d (reserved %d, charged %d)",
			q.id, *q.released, want, q.reserved, *q.charged)
	}
	stats := &rep.Gateway
	if q.source != nil && *q.source == SourceReconciler {
		stats = &rep.Reconciler
	}
	stats.Settled++
	q.auditCost(rep)
	if q.estimated != nil && *q.estimated {
		stats.Estimated++
		return
	}
	if opts.ProviderRecords != nil {
		q.auditProviderUsage(rep, stats, recs)
	}
}

// auditCost recomputes the charge from the reservation's price snapshot.
func (q *auditRow) auditCost(rep *AuditReport) {
	if q.provider == nil || q.model == nil || q.prompt == nil || q.completion == nil {
		rep.violate(CheckCost, "request %s has a charge with no usage on it", q.id)
		return
	}
	var meta Meta
	if err := json.Unmarshal(q.meta, &meta); err != nil {
		rep.violate(CheckCost, "request %s: reserve meta: %v", q.id, err)
		return
	}
	price, ok := meta.price(*q.provider, *q.model)
	if !ok {
		rep.violate(CheckCost, "request %s was charged for %s/%s, which is not among its targets", q.id, *q.provider, *q.model)
		return
	}
	if want := Cost(*q.prompt, *q.completion, price); *q.charged != want {
		rep.violate(CheckCost, "request %s charged %d for %d+%d tokens, want %d", q.id, *q.charged, *q.prompt, *q.completion, want)
	}
}

// auditProviderUsage compares a charge with the provider's record of the
// attempt that served it.
func (q *auditRow) auditProviderUsage(rep *AuditReport, stats *SettleStats, recs []UsageRecord) {
	stats.Compared++
	last := lastBillable(recs)
	var diff []string
	switch {
	case last == nil:
		diff = append(diff, "no billable provider record")
	default:
		if q.provider == nil || last.Provider != *q.provider || q.model == nil || last.Model != *q.model {
			diff = append(diff, fmt.Sprintf("target: provider recorded %s/%s", last.Provider, last.Model))
		}
		if q.prompt == nil || last.PromptTokens != *q.prompt {
			diff = append(diff, fmt.Sprintf("prompt tokens: provider recorded %d", last.PromptTokens))
		}
		if q.completion == nil || last.CompletionTokens != *q.completion {
			diff = append(diff, fmt.Sprintf("completion tokens: provider recorded %d", last.CompletionTokens))
		}
	}
	if len(diff) > 0 {
		stats.Mismatched++
		rep.violate(CheckProviderUsage, "request %s: %s", q.id, strings.Join(diff, "; "))
	}
}

// lastBillable returns the billable record of a request's last attempt.
func lastBillable(recs []UsageRecord) *UsageRecord {
	var last *UsageRecord
	for i := range recs {
		if recs[i].Billable && (last == nil || recs[i].Attempt > last.Attempt) {
			last = &recs[i]
		}
	}
	return last
}

// AllUsage fetches every record from the mock's usage API at baseURL.
func AllUsage(ctx context.Context, client *http.Client, baseURL string) ([]UsageRecord, error) {
	recs, err := fetchUsage(ctx, client, strings.TrimRight(baseURL, "/")+"/_mock/usage")
	if err != nil {
		return nil, err
	}
	if recs == nil {
		recs = []UsageRecord{}
	}
	return recs, nil
}
