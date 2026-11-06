// Command reconcile_check audits the ledger (design §8.4): it recomputes
// account totals, close amounts, and every charge from the entries, and, with
// -mock-url, compares charges with what the mock provider recorded. It only
// reads. It exits 1 if any check fails.
//
// Run it with traffic stopped. Use -require-closed once the reconciler has
// run, to also fail on reservations left open.
//
//	go run ./scripts/reconcile_check -require-closed
//	make reconcile-check
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"time"

	"github.com/SamirVarma01/llm-gateway/internal/db"
	"github.com/SamirVarma01/llm-gateway/internal/ledger"
)

func main() {
	code, err := run()
	if err != nil {
		fmt.Fprintln(os.Stderr, "reconcile_check:", err)
		os.Exit(2)
	}
	os.Exit(code)
}

func run() (int, error) {
	databaseURL := flag.String("database-url", os.Getenv("DATABASE_URL"), "Postgres URL (default $DATABASE_URL)")
	mockURL := flag.String("mock-url", os.Getenv("MOCK_URL"), "mock provider URL; empty skips the comparison (default $MOCK_URL)")
	requireClosed := flag.Bool("require-closed", false, "fail on reservations left open")
	asJSON := flag.Bool("json", false, "print the report as JSON")
	flag.Parse()
	if *databaseURL == "" {
		return 0, fmt.Errorf("set -database-url or DATABASE_URL")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	pool, err := db.Open(ctx, *databaseURL)
	if err != nil {
		return 0, err
	}
	defer pool.Close()

	opts := ledger.AuditOptions{RequireClosed: *requireClosed}
	if *mockURL != "" {
		client := &http.Client{Timeout: time.Minute}
		if opts.ProviderRecords, err = ledger.AllUsage(ctx, client, *mockURL); err != nil {
			return 0, fmt.Errorf("mock usage: %w", err)
		}
	}
	rep, err := ledger.New(pool).Audit(ctx, opts)
	if err != nil {
		return 0, err
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(rep); err != nil {
			return 0, err
		}
	} else {
		print(rep, opts.ProviderRecords != nil)
	}
	if !rep.OK() {
		return 1, nil
	}
	return 0, nil
}

func print(r *ledger.AuditReport, compared bool) {
	fmt.Printf("keys %d, requests %d, open %d\n", r.Keys, r.Requests, r.Open)
	settled := func(name string, s ledger.SettleStats, note string) {
		fmt.Printf("%-11s settled %d, estimated %d", name, s.Settled, s.Estimated)
		if compared {
			fmt.Printf(", compared with the provider %d, mismatched %d%s", s.Compared, s.Mismatched, note)
		}
		fmt.Println()
	}
	settled("gateway:", r.Gateway, "")
	settled("reconciler:", r.Reconciler, " (agrees by construction)")
	for _, reason := range sortedKeys(r.Released) {
		fmt.Printf("released %s: %d\n", reason, r.Released[reason])
	}
	if compared {
		fmt.Printf("billed by the provider, not charged: %d\n", r.BilledNotCharged)
	}
	if r.OK() {
		fmt.Println("OK")
		return
	}
	for _, check := range sortedKeys(r.Violations) {
		fmt.Printf("FAILED %s: %d\n", check, r.Violations[check])
	}
	for _, s := range r.Samples {
		fmt.Println(" ", s)
	}
}

func sortedKeys(m map[string]int64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
