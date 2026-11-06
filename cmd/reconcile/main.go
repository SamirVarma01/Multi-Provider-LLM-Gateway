// Command reconcile makes one reconciler pass and prints what it resolved
// (design §9). The gateway runs passes itself; this is for when it is down.
// It reads the same environment as the gateway, and exits non-zero if
// another pass was running or an orphan could not be resolved.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/SamirVarma01/llm-gateway/internal/config"
	"github.com/SamirVarma01/llm-gateway/internal/db"
	"github.com/SamirVarma01/llm-gateway/internal/ledger"
	"github.com/SamirVarma01/llm-gateway/internal/obs"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "reconcile:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}
	routes, err := config.LoadRoutes(cfg.RoutesFile)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	usage := ledger.UsageSources(routes.Providers, &http.Client{Timeout: 5 * time.Second})
	logger := obs.NewLogger(os.Stderr, cfg.LogLevel)
	reconciler := ledger.NewReconciler(ledger.New(pool), usage, cfg.RequestTimeout+cfg.ReconcileGrace, logger)
	res, err := reconciler.RunOnce(ctx)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(os.Stdout).Encode(res); err != nil {
		return err
	}
	switch {
	case res.Skipped:
		return errors.New("another reconciler pass is running")
	case res.Failed > 0:
		return fmt.Errorf("%d orphaned reservations could not be resolved", res.Failed)
	}
	return nil
}
