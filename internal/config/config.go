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

	// MetricsAddr is METRICS_ADDR, where /metrics is served; empty disables
	// it. It is a listener of its own because the metrics carry per-key spend.
	MetricsAddr string

	DatabaseURL    string // DATABASE_URL
	RedisURL       string // REDIS_URL
	RoutesFile     string // ROUTES_FILE
	MigrateOnStart bool   // MIGRATE_ON_START

	LogLevel slog.Level // LOG_LEVEL: debug|info|warn|error

	AuthCacheTTL         time.Duration // AUTH_CACHE_TTL
	RequestTimeout       time.Duration // REQUEST_TIMEOUT
	ShutdownDrainTimeout time.Duration // SHUTDOWN_DRAIN_TIMEOUT
	MaxBodyBytes         int64         // MAX_BODY_BYTES

	EstImageTokens  int     // EST_IMAGE_TOKENS: tokens assumed per input image when estimating
	EstSafetyFactor float64 // EST_SAFETY_FACTOR: margin on the input estimate used to reserve

	RateLimitTimeout time.Duration // RATELIMIT_TIMEOUT: the most a Redis call may add to a request
	LedgerTimeout    time.Duration // LEDGER_TIMEOUT: each Postgres call on the money path

	ReconcileInterval time.Duration // RECONCILE_INTERVAL: time between passes; 0 runs them only on demand
	ReconcileGrace    time.Duration // RECONCILE_GRACE: margin past REQUEST_TIMEOUT before a reservation counts as orphaned

	// Failover (design §6.4).
	MaxAttempts        int           // MAX_ATTEMPTS: upstream attempts per request, across all candidates
	AttemptTTFTTimeout time.Duration // ATTEMPT_TTFT_TIMEOUT: from the upstream call to its first output
	StreamIdleTimeout  time.Duration // STREAM_IDLE_TIMEOUT: between upstream chunks after the first output
	RetryBackoffBase   time.Duration // RETRY_BACKOFF_BASE: backoff before the second pass over the candidates
	RetryBackoffMax    time.Duration // RETRY_BACKOFF_MAX: backoff cap

	// Latency tracking for the fastest policy (design §6.2).
	EWMAAlpha         float64       // EWMA_ALPHA: weight of the newest sample
	LatencyStaleAfter time.Duration // LATENCY_STALE_AFTER: samples older than this are ignored
	RouteExploreRate  float64       // ROUTE_EXPLORE_RATE: share of fastest-policy requests sent to a non-best target

	// Circuit breakers, one per target (design §6.3).
	BreakerWindowSize          int           // BREAKER_WINDOW_SIZE: outcomes kept for the failure rate
	BreakerWindow              time.Duration // BREAKER_WINDOW: outcomes older than this are ignored
	BreakerFailureRate         float64       // BREAKER_FAILURE_RATE: trip at or above this failure rate...
	BreakerMinRequests         int           // BREAKER_MIN_REQUESTS: ...over at least this many outcomes
	BreakerConsecutiveFailures int           // BREAKER_CONSECUTIVE_FAILURES: or after this many failures in a row
	BreakerCooldown            time.Duration // BREAKER_COOLDOWN: open time before the first probe
	BreakerMaxCooldown         time.Duration // BREAKER_MAX_COOLDOWN: cap for the doubling cooldown
	BreakerHalfOpenProbes      int           // BREAKER_HALF_OPEN_PROBES: concurrent probes while half-open
	BreakerHalfOpenSuccesses   int           // BREAKER_HALF_OPEN_SUCCESSES: probe successes that close the breaker
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
		MetricsAddr:          r.str("METRICS_ADDR", ":9090"),
		DatabaseURL:          r.required("DATABASE_URL"),
		RedisURL:             r.required("REDIS_URL"),
		RoutesFile:           r.required("ROUTES_FILE"),
		MigrateOnStart:       r.boolean("MIGRATE_ON_START", true),
		LogLevel:             r.level("LOG_LEVEL", slog.LevelInfo),
		AuthCacheTTL:         r.duration("AUTH_CACHE_TTL", 10*time.Second),
		RequestTimeout:       r.duration("REQUEST_TIMEOUT", 10*time.Minute),
		ShutdownDrainTimeout: r.duration("SHUTDOWN_DRAIN_TIMEOUT", 30*time.Second),
		MaxBodyBytes:         r.int64("MAX_BODY_BYTES", 4<<20),
		EstImageTokens:       int(r.int64("EST_IMAGE_TOKENS", 1000)),
		EstSafetyFactor:      r.float("EST_SAFETY_FACTOR", 1.25),

		RateLimitTimeout: r.duration("RATELIMIT_TIMEOUT", 250*time.Millisecond),
		LedgerTimeout:    r.duration("LEDGER_TIMEOUT", 5*time.Second),

		ReconcileInterval: r.duration("RECONCILE_INTERVAL", 30*time.Second),
		ReconcileGrace:    r.duration("RECONCILE_GRACE", 30*time.Second),

		MaxAttempts:        int(r.int64("MAX_ATTEMPTS", 3)),
		AttemptTTFTTimeout: r.duration("ATTEMPT_TTFT_TIMEOUT", 10*time.Second),
		StreamIdleTimeout:  r.duration("STREAM_IDLE_TIMEOUT", 30*time.Second),
		RetryBackoffBase:   r.duration("RETRY_BACKOFF_BASE", 100*time.Millisecond),
		RetryBackoffMax:    r.duration("RETRY_BACKOFF_MAX", 2*time.Second),

		EWMAAlpha:         r.float("EWMA_ALPHA", 0.2),
		LatencyStaleAfter: r.duration("LATENCY_STALE_AFTER", 30*time.Second),
		RouteExploreRate:  r.float("ROUTE_EXPLORE_RATE", 0.05),

		BreakerWindowSize:          int(r.int64("BREAKER_WINDOW_SIZE", 20)),
		BreakerWindow:              r.duration("BREAKER_WINDOW", 10*time.Second),
		BreakerFailureRate:         r.float("BREAKER_FAILURE_RATE", 0.5),
		BreakerMinRequests:         int(r.int64("BREAKER_MIN_REQUESTS", 5)),
		BreakerConsecutiveFailures: int(r.int64("BREAKER_CONSECUTIVE_FAILURES", 5)),
		BreakerCooldown:            r.duration("BREAKER_COOLDOWN", 5*time.Second),
		BreakerMaxCooldown:         r.duration("BREAKER_MAX_COOLDOWN", 60*time.Second),
		BreakerHalfOpenProbes:      int(r.int64("BREAKER_HALF_OPEN_PROBES", 1)),
		BreakerHalfOpenSuccesses:   int(r.int64("BREAKER_HALF_OPEN_SUCCESSES", 2)),
	}
	check := func(ok bool, msg string) {
		if !ok {
			r.errs = append(r.errs, errors.New(msg))
		}
	}
	check(cfg.RequestTimeout > 0, "REQUEST_TIMEOUT must be positive")
	check(cfg.MaxBodyBytes > 0, "MAX_BODY_BYTES must be positive")
	check(cfg.EstImageTokens >= 0, "EST_IMAGE_TOKENS must be >= 0")
	check(cfg.EstSafetyFactor >= 1, "EST_SAFETY_FACTOR must be at least 1")

	check(cfg.RateLimitTimeout > 0, "RATELIMIT_TIMEOUT must be positive")
	check(cfg.LedgerTimeout > 0, "LEDGER_TIMEOUT must be positive")

	check(cfg.ReconcileInterval >= 0, "RECONCILE_INTERVAL must be >= 0")
	// The gateway's own close may run LEDGER_TIMEOUT past the request deadline.
	check(cfg.ReconcileGrace >= cfg.LedgerTimeout, "RECONCILE_GRACE must be >= LEDGER_TIMEOUT")

	check(cfg.MaxAttempts >= 1, "MAX_ATTEMPTS must be at least 1")
	check(cfg.AttemptTTFTTimeout > 0, "ATTEMPT_TTFT_TIMEOUT must be positive")
	check(cfg.StreamIdleTimeout > 0, "STREAM_IDLE_TIMEOUT must be positive")
	check(cfg.RetryBackoffBase > 0, "RETRY_BACKOFF_BASE must be positive")
	check(cfg.RetryBackoffMax >= cfg.RetryBackoffBase, "RETRY_BACKOFF_MAX must be >= RETRY_BACKOFF_BASE")

	check(cfg.EWMAAlpha > 0 && cfg.EWMAAlpha <= 1, "EWMA_ALPHA must be in (0, 1]")
	check(cfg.LatencyStaleAfter > 0, "LATENCY_STALE_AFTER must be positive")
	check(cfg.RouteExploreRate >= 0 && cfg.RouteExploreRate <= 1, "ROUTE_EXPLORE_RATE must be in [0, 1]")

	check(cfg.BreakerWindowSize >= 1, "BREAKER_WINDOW_SIZE must be at least 1")
	check(cfg.BreakerWindow > 0, "BREAKER_WINDOW must be positive")
	check(cfg.BreakerFailureRate > 0 && cfg.BreakerFailureRate <= 1, "BREAKER_FAILURE_RATE must be in (0, 1]")
	check(cfg.BreakerMinRequests >= 1, "BREAKER_MIN_REQUESTS must be at least 1")
	check(cfg.BreakerConsecutiveFailures >= 1, "BREAKER_CONSECUTIVE_FAILURES must be at least 1")
	check(cfg.BreakerCooldown > 0, "BREAKER_COOLDOWN must be positive")
	check(cfg.BreakerMaxCooldown >= cfg.BreakerCooldown, "BREAKER_MAX_COOLDOWN must be >= BREAKER_COOLDOWN")
	check(cfg.BreakerHalfOpenProbes >= 1, "BREAKER_HALF_OPEN_PROBES must be at least 1")
	check(cfg.BreakerHalfOpenSuccesses >= 1, "BREAKER_HALF_OPEN_SUCCESSES must be at least 1")
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

func (r *reader) float(name string, def float64) float64 {
	v, ok := r.get(name)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %w", name, err))
	}
	return f
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
