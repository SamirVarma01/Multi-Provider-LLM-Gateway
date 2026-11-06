package ledger

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestUsage(t *testing.T) {
	f := newFixture(t, budget(10_000))
	ctx := context.Background()

	settle := func(alias, providerName string, amount int64) {
		t.Helper()
		r := f.reservation(1000)
		r.Alias = alias
		if err := f.l.Reserve(ctx, r); err != nil {
			t.Fatal(err)
		}
		s := settlement(r.RequestID, amount)
		s.Provider = providerName
		if _, err := f.l.Settle(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	settle("chat", "a", 100)
	settle("chat", "a", 200)
	settle("chat", "b", 400)
	settle("other", "a", 800)
	failed := f.reserve(t, 1000)
	if _, err := f.l.Release(ctx, failed, ReasonFailed, SourceGateway); err != nil {
		t.Fatal(err)
	}
	f.reserve(t, 300) // still open

	u, err := f.l.Usage(ctx, f.key, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if u.TotalSpentNUSD != 1500 || u.SpentNUSD != 1500 || u.HeldNUSD != 300 || u.Requests != 6 {
		t.Errorf("usage = %+v", u)
	}
	if got := u.AvailableNUSD(); got == nil || *got != 10_000-1500-300 {
		t.Errorf("available = %v", got)
	}
	want := []Group{
		{"chat", "a", "m", 2, 14, 6, 300},
		{"chat", "b", "m", 1, 7, 3, 400},
		{"other", "a", "m", 1, 7, 3, 800},
	}
	if len(u.Groups) != len(want) {
		t.Fatalf("groups = %+v", u.Groups)
	}
	for i := range want {
		if u.Groups[i] != want[i] {
			t.Errorf("group %d = %+v, want %+v", i, u.Groups[i], want[i])
		}
	}
}

// The report comes from the entries, so it stays right if the cached totals
// are wrong.
func TestUsageIsDerivedFromTheLedger(t *testing.T) {
	f := newFixture(t, nil)
	ctx := context.Background()
	id := f.reserve(t, 1000)
	if _, err := f.l.Settle(ctx, settlement(id, 400)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `UPDATE key_accounts SET spent_nusd = 999, held_nusd = 999 WHERE key_id = $1`, f.key); err != nil {
		t.Fatal(err)
	}
	u, err := f.l.Usage(ctx, f.key, nil, nil)
	if err != nil || u.TotalSpentNUSD != 400 || u.HeldNUSD != 0 || u.AvailableNUSD() != nil {
		t.Errorf("usage = %+v, %v", u, err)
	}
}

func TestUsageWindow(t *testing.T) {
	f := newFixture(t, nil)
	ctx := context.Background()
	id := f.reserve(t, 1000)
	if _, err := f.l.Settle(ctx, settlement(id, 400)); err != nil {
		t.Fatal(err)
	}
	past, future := time.Now().Add(-time.Hour), time.Now().Add(time.Hour)

	u, err := f.l.Usage(ctx, f.key, &past, &future)
	if err != nil || u.SpentNUSD != 400 || u.Requests != 1 {
		t.Errorf("window around it: %+v, %v", u, err)
	}
	u, err = f.l.Usage(ctx, f.key, &future, nil)
	if err != nil || u.SpentNUSD != 0 || u.Requests != 0 || len(u.Groups) != 0 {
		t.Errorf("window after it: %+v, %v", u, err)
	}
	if u.TotalSpentNUSD != 400 {
		t.Errorf("total spent = %d: it does not depend on the window", u.TotalSpentNUSD)
	}
	u, err = f.l.Usage(ctx, f.key, nil, &past)
	if err != nil || u.SpentNUSD != 0 {
		t.Errorf("window before it: %+v, %v", u, err)
	}
}

func TestEntries(t *testing.T) {
	f := newFixture(t, nil)
	ctx := context.Background()
	id := f.reserve(t, 1000)
	if _, err := f.l.Settle(ctx, settlement(id, 400)); err != nil {
		t.Fatal(err)
	}
	all, err := f.l.Entries(ctx, f.key, 10, 0)
	if err != nil || len(all) != 3 {
		t.Fatalf("entries = %+v, %v", all, err)
	}
	if all[0].ID < all[1].ID || all[1].ID < all[2].ID {
		t.Errorf("entries are not newest first: %+v", all)
	}
	if all[2].Kind != "reserve" || all[2].AmountNUSD != 1000 || len(all[2].Meta) == 0 {
		t.Errorf("reserve entry = %+v", all[2])
	}

	page, err := f.l.Entries(ctx, f.key, 2, 0)
	if err != nil || len(page) != 2 {
		t.Fatalf("first page = %+v, %v", page, err)
	}
	rest, err := f.l.Entries(ctx, f.key, 2, page[1].ID)
	if err != nil || len(rest) != 1 || rest[0].ID != all[2].ID {
		t.Errorf("second page = %+v, %v", rest, err)
	}
}

func TestReportsForAnUnknownKey(t *testing.T) {
	f := newFixture(t, nil)
	ctx := context.Background()
	if _, err := f.l.Usage(ctx, uuid.New(), nil, nil); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("Usage: %v", err)
	}
	if _, err := f.l.Entries(ctx, uuid.New(), 10, 0); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("Entries: %v", err)
	}
}
