package auth

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NewKey describes a key to create.
type NewKey struct {
	Name       string
	RPMLimit   *int
	TPMLimit   *int
	BudgetNUSD *int64
}

// PGStore keeps keys in Postgres.
type PGStore struct {
	pool *pgxpool.Pool
}

// NewPGStore returns a Postgres-backed key store.
func NewPGStore(pool *pgxpool.Pool) *PGStore { return &PGStore{pool: pool} }

const selectKey = `
SELECT k.id, k.name, k.key_prefix, k.rpm_limit, k.tpm_limit, a.budget_nusd, k.created_at, k.revoked_at
  FROM api_keys k
  JOIN key_accounts a ON a.key_id = k.id`

func scanKey(row pgx.Row) (Key, error) {
	var k Key
	err := row.Scan(&k.ID, &k.Name, &k.Prefix, &k.RPMLimit, &k.TPMLimit, &k.BudgetNUSD, &k.CreatedAt, &k.RevokedAt)
	return k, err
}

// CreateKey creates a key and its account row in one transaction and returns
// the key's metadata and its plaintext, which is not recoverable afterwards.
func (s *PGStore) CreateKey(ctx context.Context, nk NewKey) (Key, string, error) {
	plaintext, err := GenerateKey()
	if err != nil {
		return Key{}, "", err
	}
	id := uuid.Must(uuid.NewV7())

	var k Key
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO api_keys (id, name, key_prefix, key_hash, rpm_limit, tpm_limit)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			id, nk.Name, DisplayPrefix(plaintext), HashKey(plaintext), nk.RPMLimit, nk.TPMLimit,
		); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO key_accounts (key_id, budget_nusd) VALUES ($1, $2)`, id, nk.BudgetNUSD,
		); err != nil {
			return err
		}
		k, err = scanKey(tx.QueryRow(ctx, selectKey+` WHERE k.id = $1`, id))
		return err
	})
	if err != nil {
		return Key{}, "", fmt.Errorf("auth: create key: %w", err)
	}
	return k, plaintext, nil
}

// LookupByHash implements Lookuper.
func (s *PGStore) LookupByHash(ctx context.Context, hash []byte) (Key, error) {
	k, err := scanKey(s.pool.QueryRow(ctx, selectKey+` WHERE k.key_hash = $1`, hash))
	if errors.Is(err, pgx.ErrNoRows) {
		return Key{}, ErrNotFound
	}
	if err != nil {
		return Key{}, fmt.Errorf("auth: lookup: %w", err)
	}
	return k, nil
}

// ListKeys returns all keys, oldest first.
func (s *PGStore) ListKeys(ctx context.Context) ([]Key, error) {
	rows, err := s.pool.Query(ctx, selectKey+` ORDER BY k.created_at, k.id`)
	if err != nil {
		return nil, fmt.Errorf("auth: list: %w", err)
	}
	keys, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Key, error) { return scanKey(r) })
	if err != nil {
		return nil, fmt.Errorf("auth: list: %w", err)
	}
	return keys, nil
}

// KeyUpdate changes a key's limits. A Set flag applies its value, where nil
// means unlimited; a field whose flag is false keeps its current value.
type KeyUpdate struct {
	SetRPMLimit bool
	RPMLimit    *int
	SetTPMLimit bool
	TPMLimit    *int
	SetBudget   bool
	BudgetNUSD  *int64
}

// UpdateKey applies u in one transaction and returns the updated key. A
// budget below what the key has already spent is allowed: its next
// reservation is refused. New rate limits reach requests within the auth
// cache's TTL; a new budget applies at once, because reservations read it
// from the account row.
func (s *PGStore) UpdateKey(ctx context.Context, id uuid.UUID, u KeyUpdate) (Key, error) {
	var k Key
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			UPDATE api_keys
			   SET rpm_limit = CASE WHEN $2::boolean THEN $3::integer ELSE rpm_limit END,
			       tpm_limit = CASE WHEN $4::boolean THEN $5::integer ELSE tpm_limit END
			 WHERE id = $1`, id, u.SetRPMLimit, u.RPMLimit, u.SetTPMLimit, u.TPMLimit); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE key_accounts
			   SET budget_nusd = CASE WHEN $2::boolean THEN $3::bigint ELSE budget_nusd END, updated_at = now()
			 WHERE key_id = $1`, id, u.SetBudget, u.BudgetNUSD); err != nil {
			return err
		}
		var err error
		k, err = scanKey(tx.QueryRow(ctx, selectKey+` WHERE k.id = $1`, id))
		return err
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Key{}, ErrNotFound
	}
	if err != nil {
		return Key{}, fmt.Errorf("auth: update: %w", err)
	}
	return k, nil
}

// RevokeKey revokes a key. Revoking an already revoked key is a no-op.
func (s *PGStore) RevokeKey(ctx context.Context, id uuid.UUID) (Key, error) {
	_, err := s.pool.Exec(ctx,
		`UPDATE api_keys SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL`, id)
	if err != nil {
		return Key{}, fmt.Errorf("auth: revoke: %w", err)
	}
	k, err := scanKey(s.pool.QueryRow(ctx, selectKey+` WHERE k.id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Key{}, ErrNotFound
	}
	if err != nil {
		return Key{}, fmt.Errorf("auth: revoke: %w", err)
	}
	return k, nil
}
