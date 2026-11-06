package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		"DATABASE_URL": "postgres://x",
		"REDIS_URL":    "redis://x",
		"ROUTES_FILE":  "routes.yaml",
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.PublicAddr != ":8080" || cfg.AdminAddr != ":8081" {
		t.Errorf("addrs = %q, %q", cfg.PublicAddr, cfg.AdminAddr)
	}
	if cfg.AuthCacheTTL != 10*time.Second || cfg.RequestTimeout != 10*time.Minute {
		t.Errorf("durations = %v, %v", cfg.AuthCacheTTL, cfg.RequestTimeout)
	}
	if !cfg.MigrateOnStart || cfg.LogLevel != slog.LevelInfo || cfg.MaxBodyBytes != 4<<20 || cfg.EstImageTokens != 1000 {
		t.Errorf("unexpected defaults: %+v", cfg)
	}
	if cfg.EstSafetyFactor != 1.25 || cfg.RateLimitTimeout != 250*time.Millisecond || cfg.LedgerTimeout != 5*time.Second {
		t.Errorf("accounting defaults: %+v", cfg)
	}
	// Routing defaults from design §6.
	if cfg.MaxAttempts != 3 || cfg.AttemptTTFTTimeout != 10*time.Second || cfg.StreamIdleTimeout != 30*time.Second ||
		cfg.RetryBackoffBase != 100*time.Millisecond || cfg.RetryBackoffMax != 2*time.Second {
		t.Errorf("failover defaults: %+v", cfg)
	}
	if cfg.EWMAAlpha != 0.2 || cfg.LatencyStaleAfter != 30*time.Second || cfg.RouteExploreRate != 0.05 {
		t.Errorf("latency defaults: %+v", cfg)
	}
	if cfg.BreakerWindowSize != 20 || cfg.BreakerWindow != 10*time.Second || cfg.BreakerFailureRate != 0.5 ||
		cfg.BreakerMinRequests != 5 || cfg.BreakerConsecutiveFailures != 5 || cfg.BreakerCooldown != 5*time.Second ||
		cfg.BreakerMaxCooldown != 60*time.Second || cfg.BreakerHalfOpenProbes != 1 || cfg.BreakerHalfOpenSuccesses != 2 {
		t.Errorf("breaker defaults: %+v", cfg)
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		"DATABASE_URL":     "postgres://x",
		"REDIS_URL":        "redis://x",
		"ROUTES_FILE":      "routes.yaml",
		"ADMIN_TOKEN":      " secret ",
		"LOG_LEVEL":        "debug",
		"AUTH_CACHE_TTL":   "250ms",
		"MIGRATE_ON_START": "false",
		"MAX_BODY_BYTES":   "1024",
		"EWMA_ALPHA":       "0.5",
		"MAX_ATTEMPTS":     "1",
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.AdminToken != "secret" {
		t.Errorf("AdminToken = %q, want trimmed", cfg.AdminToken)
	}
	if cfg.LogLevel != slog.LevelDebug || cfg.AuthCacheTTL != 250*time.Millisecond ||
		cfg.MigrateOnStart || cfg.MaxBodyBytes != 1024 || cfg.EWMAAlpha != 0.5 || cfg.MaxAttempts != 1 {
		t.Errorf("overrides not applied: %+v", cfg)
	}
}

func TestLoadReportsAllErrors(t *testing.T) {
	_, err := Load(env(map[string]string{
		"AUTH_CACHE_TTL":       "soon",
		"LOG_LEVEL":            "loud",
		"EWMA_ALPHA":           "high",
		"ROUTE_EXPLORE_RATE":   "1.5",
		"MAX_ATTEMPTS":         "0",
		"BREAKER_COOLDOWN":     "10s",
		"BREAKER_MAX_COOLDOWN": "5s",
	}))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"DATABASE_URL", "REDIS_URL", "ROUTES_FILE", "AUTH_CACHE_TTL", "LOG_LEVEL",
		"EWMA_ALPHA", "ROUTE_EXPLORE_RATE", "MAX_ATTEMPTS", "BREAKER_MAX_COOLDOWN"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}
