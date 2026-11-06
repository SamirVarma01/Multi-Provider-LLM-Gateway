// Package ledger holds budget reservations and charges in Postgres
// (design §8).
//
// Every admitted request writes a reserve entry and is closed exactly once:
// Settle charges it, Release ends it with no charge. Both insert a release
// entry first, and UNIQUE (request_id, kind) lets only one close win, so a
// repeated settle, or a settle racing a release, is a no-op. Each operation
// is one SQL statement, which makes it atomic and one round trip; the
// key_accounts totals change in the same statement as the entries they
// summarize.
package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrInsufficientBudget means the key's budget cannot cover a reservation.
var ErrInsufficientBudget = errors.New("ledger: insufficient budget")

// Sources of a ledger entry.
const (
	SourceGateway    = "gateway"
	SourceReconciler = "reconciler"
)

// Reasons a request was closed, stored on its release entry.
const (
	ReasonCompleted   = "completed"   // settled: the upstream finished
	ReasonInterrupted = "interrupted" // settled: partial output was delivered
	ReasonFailed      = "failed"      // released: no attempt committed
	ReasonRejected    = "rejected"    // released: the upstream refused the client's request
	ReasonCancelled   = "cancelled"   // released: the client left before the commit point

	ReasonOrphanNoCall       = "orphan_no_call"       // reconciler released: the upstream billed nothing
	ReasonOrphanUnknownUsage = "orphan_unknown_usage" // reconciler released: usage cannot be known, an undercharge
)

// TargetPrice is a target's price at reservation time. The reconciler
// charges orphaned requests from this snapshot, so a route table edit
// cannot change what an in-flight request costs.
type TargetPrice struct {
	Provider        string `json:"provider"`
	Model           string `json:"model"`
	InputMicroPerM  int64  `json:"input_micro_per_m"`
	OutputMicroPerM int64  `json:"output_micro_per_m"`
}

// Meta is stored on a reserve entry.
type Meta struct {
	Policy    string        `json:"policy"`
	EstInput  int           `json:"est_input"`
	MaxOutput int           `json:"max_output"`
	Targets   []TargetPrice `json:"targets"`
}

// Reservation holds budget for one request.
type Reservation struct {
	RequestID  uuid.UUID
	KeyID      uuid.UUID
	AmountNUSD int64
	Alias      string
	Meta       Meta
}

// Settlement charges a reserved request.
type Settlement struct {
	RequestID        uuid.UUID
	AmountNUSD       int64
	Provider         string
	UpstreamModel    string
	PromptTokens     int
	CompletionTokens int
	UsageEstimated   bool
	Reason           string // ReasonCompleted or ReasonInterrupted
	Source           string
}

// Ledger is the Postgres-backed ledger.
type Ledger struct {
	pool *pgxpool.Pool
}

// New returns a Ledger.
func New(pool *pgxpool.Pool) *Ledger { return &Ledger{pool: pool} }

// reserveSQL holds budget and records it (design §8.4). The UPDATE takes the
// account's row lock. Under READ COMMITTED Postgres re-checks the WHERE
// against the latest row version after waiting for that lock, so concurrent
// reservations serialize per key and their sum cannot pass the budget.
const reserveSQL = `
WITH acct AS (
  UPDATE key_accounts
     SET held_nusd = held_nusd + $3, updated_at = now()
   WHERE key_id = $2
     AND (budget_nusd IS NULL OR spent_nusd + held_nusd + $3 <= budget_nusd)
  RETURNING key_id
)
INSERT INTO ledger_entries (request_id, key_id, kind, amount_nusd, alias, meta, source)
SELECT $1, key_id, 'reserve', $3, $4, $5, 'gateway' FROM acct`

// Reserve holds r.AmountNUSD against the key's budget. It returns
// ErrInsufficientBudget, with nothing written, if the budget cannot cover it.
func (l *Ledger) Reserve(ctx context.Context, r Reservation) error {
	meta, err := json.Marshal(r.Meta)
	if err != nil {
		return fmt.Errorf("ledger: reserve: %w", err)
	}
	tag, err := l.pool.Exec(ctx, reserveSQL, r.RequestID, r.KeyID, r.AmountNUSD, r.Alias, meta)
	if err != nil {
		return fmt.Errorf("ledger: reserve: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrInsufficientBudget
	}
	return nil
}

// settleSQL closes a request with a charge. The reserved amount comes from
// the reserve entry, never from the caller. "closed" is empty if the request
// was already closed, or was never reserved, and then nothing downstream of
// it writes anything. Data-modifying CTEs always run, so "charged" needs no
// reference from the final statement.
const settleSQL = `
WITH r AS (
  SELECT key_id, amount_nusd, alias FROM ledger_entries WHERE request_id = $1 AND kind = 'reserve'
), closed AS (
  INSERT INTO ledger_entries (request_id, key_id, kind, amount_nusd, alias, reason, source)
  SELECT $1, key_id, 'release', GREATEST(amount_nusd - $2, 0), alias, $3, $4 FROM r
  ON CONFLICT (request_id, kind) DO NOTHING
  RETURNING key_id
), charged AS (
  INSERT INTO ledger_entries (request_id, key_id, kind, amount_nusd, alias, provider, upstream_model,
                              prompt_tokens, completion_tokens, usage_estimated, source)
  SELECT $1, r.key_id, 'settle', $2, r.alias, $5, $6, $7, $8, $9, $4
    FROM r JOIN closed USING (key_id)
)
UPDATE key_accounts a
   SET held_nusd = a.held_nusd - r.amount_nusd, spent_nusd = a.spent_nusd + $2, updated_at = now()
  FROM r JOIN closed USING (key_id)
 WHERE a.key_id = r.key_id`

// Settle charges a reserved request and releases the rest of its hold. A
// charge above the reservation is applied in full, which can take the key
// past its budget (design §8.4). It reports whether this call closed the
// request; false means another close got there first, and nothing changed.
func (l *Ledger) Settle(ctx context.Context, s Settlement) (bool, error) {
	tag, err := l.pool.Exec(ctx, settleSQL, s.RequestID, s.AmountNUSD, s.Reason, s.Source,
		s.Provider, s.UpstreamModel, s.PromptTokens, s.CompletionTokens, s.UsageEstimated)
	if err != nil {
		return false, fmt.Errorf("ledger: settle: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// releaseSQL closes a request with no charge.
const releaseSQL = `
WITH r AS (
  SELECT key_id, amount_nusd, alias FROM ledger_entries WHERE request_id = $1 AND kind = 'reserve'
), closed AS (
  INSERT INTO ledger_entries (request_id, key_id, kind, amount_nusd, alias, reason, source)
  SELECT $1, key_id, 'release', amount_nusd, alias, $2, $3 FROM r
  ON CONFLICT (request_id, kind) DO NOTHING
  RETURNING key_id
)
UPDATE key_accounts a
   SET held_nusd = a.held_nusd - r.amount_nusd, updated_at = now()
  FROM r JOIN closed USING (key_id)
 WHERE a.key_id = r.key_id`

// Release ends a reserved request with no charge. It reports whether this
// call closed the request; false means another close got there first.
func (l *Ledger) Release(ctx context.Context, requestID uuid.UUID, reason, source string) (bool, error) {
	tag, err := l.pool.Exec(ctx, releaseSQL, requestID, reason, source)
	if err != nil {
		return false, fmt.Errorf("ledger: release: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}
