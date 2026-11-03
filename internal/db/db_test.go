package db_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/SamirVarma01/llm-gateway/internal/db"
	"github.com/SamirVarma01/llm-gateway/internal/testutil"
	"github.com/SamirVarma01/llm-gateway/migrations"
)

func TestMain(m *testing.M) { testutil.Main(m) }

func TestMigrateIsIdempotent(t *testing.T) {
	pool := testutil.NewDatabase(t) // already migrated once
	applied, err := db.Migrate(context.Background(), pool, migrations.FS)
	if err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	if len(applied) != 0 {
		t.Errorf("second Migrate applied %v, want nothing", applied)
	}
}

func TestLedgerIsAppendOnly(t *testing.T) {
	pool := testutil.NewDatabase(t)
	ctx := context.Background()
	keyID := uuid.New()
	if _, err := pool.Exec(ctx,
		`INSERT INTO api_keys (id, name, key_prefix, key_hash) VALUES ($1, 'k', 'gw_x', '\x00')`, keyID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO ledger_entries (request_id, key_id, kind, amount_nusd, source)
		 VALUES ($1, $2, 'reserve', 100, 'gateway')`, uuid.New(), keyID); err != nil {
		t.Fatal(err)
	}

	for _, stmt := range []string{
		"UPDATE ledger_entries SET amount_nusd = 0",
		"DELETE FROM ledger_entries",
		"TRUNCATE ledger_entries",
	} {
		_, err := pool.Exec(ctx, stmt)
		if err == nil || !strings.Contains(err.Error(), "append-only") {
			t.Errorf("%s: err = %v, want append-only violation", stmt, err)
		}
	}
}

func TestLedgerIdempotencyKey(t *testing.T) {
	pool := testutil.NewDatabase(t)
	ctx := context.Background()
	keyID, reqID := uuid.New(), uuid.New()
	if _, err := pool.Exec(ctx,
		`INSERT INTO api_keys (id, name, key_prefix, key_hash) VALUES ($1, 'k', 'gw_x', '\x00')`, keyID); err != nil {
		t.Fatal(err)
	}
	insert := `INSERT INTO ledger_entries (request_id, key_id, kind, amount_nusd, source)
	           VALUES ($1, $2, 'release', 5, 'gateway') ON CONFLICT (request_id, kind) DO NOTHING`
	for i, want := range []int64{1, 0} {
		tag, err := pool.Exec(ctx, insert, reqID, keyID)
		if err != nil {
			t.Fatal(err)
		}
		if tag.RowsAffected() != want {
			t.Errorf("insert #%d affected %d rows, want %d", i+1, tag.RowsAffected(), want)
		}
	}
}
