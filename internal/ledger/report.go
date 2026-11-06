package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ErrKeyNotFound means no account exists for a key.
var ErrKeyNotFound = errors.New("ledger: key not found")

// Group is a key's charges for one alias, provider, and model.
type Group struct {
	Alias            string
	Provider         string
	UpstreamModel    string
	Requests         int64
	PromptTokens     int64
	CompletionTokens int64
	SpentNUSD        int64
}

// Usage is a key's usage, derived from its ledger entries.
type Usage struct {
	// Current, over the key's whole history.
	BudgetNUSD     *int64 // nil = unlimited
	TotalSpentNUSD int64
	HeldNUSD       int64

	// Within the requested window.
	SpentNUSD int64
	Requests  int64 // reservations, whether or not they were charged
	Groups    []Group
}

// AvailableNUSD is what the key can still reserve, or nil if unlimited. It
// is negative if a charge above its reservation overspent the budget.
func (u Usage) AvailableNUSD() *int64 {
	if u.BudgetNUSD == nil {
		return nil
	}
	v := *u.BudgetNUSD - u.TotalSpentNUSD - u.HeldNUSD
	return &v
}

// Usage reports a key's usage from its ledger entries, not from the
// materialized totals in key_accounts. A nil from or to leaves that end of
// the window open; from is inclusive and to is exclusive.
func (l *Ledger) Usage(ctx context.Context, keyID uuid.UUID, from, to *time.Time) (Usage, error) {
	var u Usage
	err := l.pool.QueryRow(ctx, `
		SELECT a.budget_nusd,
		       COALESCE((SELECT SUM(amount_nusd) FROM ledger_entries WHERE key_id = a.key_id AND kind = 'settle'), 0)::bigint,
		       COALESCE((SELECT SUM(r.amount_nusd) FROM ledger_entries r
		                  WHERE r.key_id = a.key_id AND r.kind = 'reserve' AND NOT EXISTS (
		                    SELECT 1 FROM ledger_entries x WHERE x.request_id = r.request_id AND x.kind = 'release')), 0)::bigint,
		       (SELECT count(*) FROM ledger_entries
		         WHERE key_id = a.key_id AND kind = 'reserve'
		           AND ($2::timestamptz IS NULL OR created_at >= $2) AND ($3::timestamptz IS NULL OR created_at < $3))
		  FROM key_accounts a WHERE a.key_id = $1`, keyID, from, to).
		Scan(&u.BudgetNUSD, &u.TotalSpentNUSD, &u.HeldNUSD, &u.Requests)
	if errors.Is(err, pgx.ErrNoRows) {
		return Usage{}, ErrKeyNotFound
	}
	if err != nil {
		return Usage{}, fmt.Errorf("ledger: usage: %w", err)
	}

	rows, err := l.pool.Query(ctx, `
		SELECT COALESCE(alias, ''), COALESCE(provider, ''), COALESCE(upstream_model, ''), count(*),
		       COALESCE(SUM(prompt_tokens), 0)::bigint, COALESCE(SUM(completion_tokens), 0)::bigint, SUM(amount_nusd)::bigint
		  FROM ledger_entries
		 WHERE key_id = $1 AND kind = 'settle'
		   AND ($2::timestamptz IS NULL OR created_at >= $2) AND ($3::timestamptz IS NULL OR created_at < $3)
		 GROUP BY 1, 2, 3 ORDER BY 1, 2, 3`, keyID, from, to)
	if err != nil {
		return Usage{}, fmt.Errorf("ledger: usage: %w", err)
	}
	u.Groups, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (Group, error) {
		var g Group
		err := r.Scan(&g.Alias, &g.Provider, &g.UpstreamModel, &g.Requests, &g.PromptTokens, &g.CompletionTokens, &g.SpentNUSD)
		return g, err
	})
	if err != nil {
		return Usage{}, fmt.Errorf("ledger: usage: %w", err)
	}
	for _, g := range u.Groups {
		u.SpentNUSD += g.SpentNUSD
	}
	return u, nil
}

// Entry is one raw ledger entry.
type Entry struct {
	ID               int64           `json:"id"`
	RequestID        uuid.UUID       `json:"request_id"`
	Kind             string          `json:"kind"`
	AmountNUSD       int64           `json:"amount_nusd"`
	Alias            *string         `json:"alias"`
	Provider         *string         `json:"provider"`
	UpstreamModel    *string         `json:"upstream_model"`
	PromptTokens     *int            `json:"prompt_tokens"`
	CompletionTokens *int            `json:"completion_tokens"`
	UsageEstimated   *bool           `json:"usage_estimated"`
	Source           string          `json:"source"`
	Reason           *string         `json:"reason"`
	Meta             json.RawMessage `json:"meta,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
}

// Entries returns a key's newest entries with an ID below before, or its
// newest entries if before is 0.
func (l *Ledger) Entries(ctx context.Context, keyID uuid.UUID, limit int, before int64) ([]Entry, error) {
	var exists bool
	if err := l.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM key_accounts WHERE key_id = $1)`, keyID).Scan(&exists); err != nil {
		return nil, fmt.Errorf("ledger: entries: %w", err)
	}
	if !exists {
		return nil, ErrKeyNotFound
	}
	rows, err := l.pool.Query(ctx, `
		SELECT id, request_id, kind::text, amount_nusd, alias, provider, upstream_model,
		       prompt_tokens, completion_tokens, usage_estimated, source, reason, meta, created_at
		  FROM ledger_entries
		 WHERE key_id = $1 AND ($2 = 0 OR id < $2)
		 ORDER BY id DESC LIMIT $3`, keyID, before, limit)
	if err != nil {
		return nil, fmt.Errorf("ledger: entries: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Entry, error) {
		var e Entry
		err := r.Scan(&e.ID, &e.RequestID, &e.Kind, &e.AmountNUSD, &e.Alias, &e.Provider, &e.UpstreamModel,
			&e.PromptTokens, &e.CompletionTokens, &e.UsageEstimated, &e.Source, &e.Reason, &e.Meta, &e.CreatedAt)
		return e, err
	})
	if err != nil {
		return nil, fmt.Errorf("ledger: entries: %w", err)
	}
	return out, nil
}
