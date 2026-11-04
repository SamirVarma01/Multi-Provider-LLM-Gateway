// Package db opens the Postgres pool and applies schema migrations.
package db

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Open connects to Postgres and verifies the connection.
func Open(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("db: parse url: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("db: connect: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("db: ping: %w", err)
	}
	return pool, nil
}

// migrationLockID is an arbitrary constant for pg_advisory_lock so concurrent
// gateway starts apply migrations one at a time.
const migrationLockID = 0x6c6c6d67 // "llmg"

// Migrate applies every *.sql file in migrations that has not been applied yet,
// in lexical filename order, each in its own transaction.
func Migrate(ctx context.Context, pool *pgxpool.Pool, migrations fs.FS) (applied []string, err error) {
	names, err := fs.Glob(migrations, "*.sql")
	if err != nil {
		return nil, err
	}
	slices.Sort(names)

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("db: acquire: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockID); err != nil {
		return nil, fmt.Errorf("db: migration lock: %w", err)
	}
	defer func() {
		// Use a fresh context: ctx may already be cancelled, and a held session
		// lock would block every later migration.
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, uerr := conn.Exec(unlockCtx, "SELECT pg_advisory_unlock($1)", migrationLockID); uerr != nil {
			err = errors.Join(err, fmt.Errorf("db: migration unlock: %w", uerr))
		}
	}()

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    text PRIMARY KEY,
		applied_at timestamptz NOT NULL DEFAULT now()
	)`); err != nil {
		return nil, fmt.Errorf("db: create schema_migrations: %w", err)
	}

	for _, name := range names {
		version := strings.TrimSuffix(path.Base(name), ".sql")
		var done bool
		if err := conn.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)", version,
		).Scan(&done); err != nil {
			return applied, fmt.Errorf("db: check %s: %w", version, err)
		}
		if done {
			continue
		}
		body, err := fs.ReadFile(migrations, name)
		if err != nil {
			return applied, err
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return applied, fmt.Errorf("db: begin %s: %w", version, err)
		}
		// No arguments, so pgx uses the simple protocol and multi-statement files work.
		if _, err := tx.Exec(ctx, string(body)); err != nil {
			_ = tx.Rollback(ctx)
			return applied, fmt.Errorf("db: apply %s: %w", version, err)
		}
		if _, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", version); err != nil {
			_ = tx.Rollback(ctx)
			return applied, fmt.Errorf("db: record %s: %w", version, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return applied, fmt.Errorf("db: commit %s: %w", version, err)
		}
		applied = append(applied, version)
	}
	return applied, nil
}
