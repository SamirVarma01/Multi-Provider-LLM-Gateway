// Command mockprovider runs the deterministic OpenAI-compatible mock provider.
//
// Environment:
//
//	MOCK_ADDR                    listen address (default :9000)
//	MOCK_TTFT_MS                 default time to first token in ms (default 0)
//	MOCK_TOKENS_PER_SEC          default decode rate; 0 = unpaced (default 0)
//	MOCK_OUTPUT_TOKENS           default tokens per response (default 16)
//	MOCK_ERROR_RATE              default injected error probability (default 0)
//	MOCK_ERROR_STATUS            default injected error status (default 500)
//	MOCK_SEED                    output seed (default 1)
//	MOCK_ALLOW_HEADER_OVERRIDES  honor X-Mock-* request headers (default true)
//	MOCK_USAGE_FILE              keep usage records in this JSONL file across restarts (optional)
//	LOG_LEVEL                    debug|info|warn|error (default info)
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/SamirVarma01/llm-gateway/internal/mock"
	"github.com/SamirVarma01/llm-gateway/internal/obs"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "mockprovider:", err)
		os.Exit(1)
	}
}

func run() error {
	var errs []error
	env := func(name, def string) string {
		if v := os.Getenv(name); v != "" {
			return v
		}
		return def
	}
	intEnv := func(name string, def int) int {
		n, err := strconv.Atoi(env(name, strconv.Itoa(def)))
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
		}
		return n
	}
	floatEnv := func(name string, def float64) float64 {
		f, err := strconv.ParseFloat(env(name, strconv.FormatFloat(def, 'f', -1, 64)), 64)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
		}
		return f
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(env("LOG_LEVEL", "info"))); err != nil {
		errs = append(errs, fmt.Errorf("LOG_LEVEL: %w", err))
	}
	allow, err := strconv.ParseBool(env("MOCK_ALLOW_HEADER_OVERRIDES", "true"))
	if err != nil {
		errs = append(errs, fmt.Errorf("MOCK_ALLOW_HEADER_OVERRIDES: %w", err))
	}
	seed, err := strconv.ParseUint(env("MOCK_SEED", "1"), 10, 64)
	if err != nil {
		errs = append(errs, fmt.Errorf("MOCK_SEED: %w", err))
	}
	defaults := mock.Behavior{
		TTFTMs:       intEnv("MOCK_TTFT_MS", 0),
		TokensPerSec: floatEnv("MOCK_TOKENS_PER_SEC", 0),
		OutputTokens: intEnv("MOCK_OUTPUT_TOKENS", 16),
		ErrorRate:    floatEnv("MOCK_ERROR_RATE", 0),
		ErrorStatus:  intEnv("MOCK_ERROR_STATUS", 500),
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	if err := defaults.Validate(); err != nil {
		return err
	}

	logger := obs.NewLogger(os.Stdout, level)
	opts := mock.Options{Defaults: defaults, Seed: seed, AllowHeaderOverrides: allow, Logger: logger}
	usagePath := os.Getenv("MOCK_USAGE_FILE")
	if usagePath != "" {
		f, err := os.OpenFile(usagePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		defer func() {
			// The usage file is the record of what was billed, so a failed
			// close, which can mean lost writes, must not pass silently.
			if err := f.Close(); err != nil {
				logger.Error("close usage file", "err", err)
			}
		}()
		opts.UsageLog = f
	}
	server := mock.New(opts)
	if usagePath != "" {
		f, err := os.Open(usagePath)
		if err != nil {
			return err
		}
		restored, skipped, err := server.Restore(f)
		_ = f.Close() // read only
		if err != nil {
			return fmt.Errorf("restore usage: %w", err)
		}
		logger.Info("restored usage records", "records", restored, "skipped_lines", skipped)
	}

	srv := &http.Server{
		Addr:              env("MOCK_ADDR", ":9000"),
		Handler:           server.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	logger.Info("mock provider listening", "addr", srv.Addr, "defaults", defaults)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		// Hanging requests never finish on their own; cut them off.
		_ = srv.Close()
	}
	logger.Info("mock provider stopped")
	return nil
}
