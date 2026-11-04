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
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.AdminToken != "secret" {
		t.Errorf("AdminToken = %q, want trimmed", cfg.AdminToken)
	}
	if cfg.LogLevel != slog.LevelDebug || cfg.AuthCacheTTL != 250*time.Millisecond ||
		cfg.MigrateOnStart || cfg.MaxBodyBytes != 1024 {
		t.Errorf("overrides not applied: %+v", cfg)
	}
}

func TestLoadReportsAllErrors(t *testing.T) {
	_, err := Load(env(map[string]string{
		"AUTH_CACHE_TTL": "soon",
		"LOG_LEVEL":      "loud",
	}))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"DATABASE_URL", "REDIS_URL", "ROUTES_FILE", "AUTH_CACHE_TTL", "LOG_LEVEL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}
