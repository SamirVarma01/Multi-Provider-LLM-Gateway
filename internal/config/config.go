// Package config loads gateway configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the gateway's process-level configuration.
type Config struct {
	PublicAddr string // GATEWAY_PUBLIC_ADDR
	AdminAddr  string // GATEWAY_ADMIN_ADDR
	AdminToken string // ADMIN_TOKEN; empty disables the admin listener

	DatabaseURL    string // DATABASE_URL
	RedisURL       string // REDIS_URL
	RoutesFile     string // ROUTES_FILE
	MigrateOnStart bool   // MIGRATE_ON_START

	LogLevel slog.Level // LOG_LEVEL: debug|info|warn|error

	AuthCacheTTL         time.Duration // AUTH_CACHE_TTL
	RequestTimeout       time.Duration // REQUEST_TIMEOUT
	ShutdownDrainTimeout time.Duration // SHUTDOWN_DRAIN_TIMEOUT
	MaxBodyBytes         int64         // MAX_BODY_BYTES
}

// FromEnv loads Config from the process environment.
func FromEnv() (Config, error) {
	return Load(os.LookupEnv)
}

// Load builds a Config using lookup to read variables. All problems are
// reported together.
func Load(lookup func(string) (string, bool)) (Config, error) {
	r := reader{lookup: lookup}
	cfg := Config{
		PublicAddr:           r.str("GATEWAY_PUBLIC_ADDR", ":8080"),
		AdminAddr:            r.str("GATEWAY_ADMIN_ADDR", ":8081"),
		AdminToken:           r.str("ADMIN_TOKEN", ""),
		DatabaseURL:          r.required("DATABASE_URL"),
		RedisURL:             r.required("REDIS_URL"),
		RoutesFile:           r.required("ROUTES_FILE"),
		MigrateOnStart:       r.boolean("MIGRATE_ON_START", true),
		LogLevel:             r.level("LOG_LEVEL", slog.LevelInfo),
		AuthCacheTTL:         r.duration("AUTH_CACHE_TTL", 10*time.Second),
		RequestTimeout:       r.duration("REQUEST_TIMEOUT", 10*time.Minute),
		ShutdownDrainTimeout: r.duration("SHUTDOWN_DRAIN_TIMEOUT", 30*time.Second),
		MaxBodyBytes:         r.int64("MAX_BODY_BYTES", 4<<20),
	}
	if cfg.RequestTimeout <= 0 {
		r.errs = append(r.errs, errors.New("REQUEST_TIMEOUT must be positive"))
	}
	if cfg.MaxBodyBytes <= 0 {
		r.errs = append(r.errs, errors.New("MAX_BODY_BYTES must be positive"))
	}
	if err := errors.Join(r.errs...); err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	return cfg, nil
}

type reader struct {
	lookup func(string) (string, bool)
	errs   []error
}

func (r *reader) get(name string) (string, bool) {
	v, ok := r.lookup(name)
	v = strings.TrimSpace(v)
	return v, ok && v != ""
}

func (r *reader) str(name, def string) string {
	if v, ok := r.get(name); ok {
		return v
	}
	return def
}

func (r *reader) required(name string) string {
	v, ok := r.get(name)
	if !ok {
		r.errs = append(r.errs, fmt.Errorf("%s is required", name))
	}
	return v
}

func (r *reader) boolean(name string, def bool) bool {
	v, ok := r.get(name)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %w", name, err))
	}
	return b
}

func (r *reader) duration(name string, def time.Duration) time.Duration {
	v, ok := r.get(name)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %w", name, err))
	}
	return d
}

func (r *reader) int64(name string, def int64) int64 {
	v, ok := r.get(name)
	if !ok {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %w", name, err))
	}
	return n
}

func (r *reader) level(name string, def slog.Level) slog.Level {
	v, ok := r.get(name)
	if !ok {
		return def
	}
	var l slog.Level
	if err := l.UnmarshalText([]byte(v)); err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %w", name, err))
	}
	return l
}
