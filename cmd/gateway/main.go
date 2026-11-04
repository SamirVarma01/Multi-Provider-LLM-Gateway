// Command gateway runs the LLM gateway: the public OpenAI-compatible API and,
// when ADMIN_TOKEN is set, the admin API on a separate listener.
// Configuration is read from the environment; see internal/config.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"

	"github.com/SamirVarma01/llm-gateway/internal/auth"
	"github.com/SamirVarma01/llm-gateway/internal/config"
	"github.com/SamirVarma01/llm-gateway/internal/db"
	"github.com/SamirVarma01/llm-gateway/internal/estimate"
	"github.com/SamirVarma01/llm-gateway/internal/httpapi"
	"github.com/SamirVarma01/llm-gateway/internal/obs"
	"github.com/SamirVarma01/llm-gateway/internal/router"
	"github.com/SamirVarma01/llm-gateway/migrations"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "gateway:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}
	logger := obs.NewLogger(os.Stdout, cfg.LogLevel)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	routes, err := config.LoadRoutes(cfg.RoutesFile)
	if err != nil {
		return err
	}
	providers, err := router.BuildProviders(routes, newUpstreamClient(), os.LookupEnv)
	if err != nil {
		return err
	}
	rt, err := router.New(routes, providers)
	if err != nil {
		return err
	}

	pool, err := db.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if cfg.MigrateOnStart {
		applied, err := db.Migrate(ctx, pool, migrations.FS)
		if err != nil {
			return err
		}
		logger.Info("migrations applied", "versions", applied)
	}

	redisOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return fmt.Errorf("REDIS_URL: %w", err)
	}
	rdb := redis.NewClient(redisOpts)
	defer rdb.Close()

	keys := auth.NewPGStore(pool)
	api := httpapi.New(httpapi.Options{
		Logger:    logger,
		Auth:      auth.NewCache(keys, cfg.AuthCacheTTL),
		Keys:      keys,
		Router:    rt,
		Estimator: estimate.Estimator{ImageTokens: cfg.EstImageTokens},
		Ready: []httpapi.ReadyCheck{
			{Name: "postgres", Check: pool.Ping},
			{Name: "redis", Check: func(ctx context.Context) error { return rdb.Ping(ctx).Err() }},
		},
		AdminToken:     cfg.AdminToken,
		RequestTimeout: cfg.RequestTimeout,
		MaxBodyBytes:   cfg.MaxBodyBytes,
	})

	servers := []*http.Server{newServer(cfg.PublicAddr, api.PublicHandler())}
	if cfg.AdminToken != "" {
		servers = append(servers, newServer(cfg.AdminAddr, api.AdminHandler()))
	} else {
		logger.Warn("ADMIN_TOKEN is not set; admin API disabled")
	}

	g, gctx := errgroup.WithContext(ctx)
	for _, srv := range servers {
		g.Go(func() error {
			logger.Info("listening", "addr", srv.Addr)
			if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				return fmt.Errorf("listen %s: %w", srv.Addr, err)
			}
			return nil
		})
	}
	g.Go(func() error {
		<-gctx.Done()
		logger.Info("shutting down", "drain_timeout", cfg.ShutdownDrainTimeout)
		shutdown(servers, cfg.ShutdownDrainTimeout, logger)
		return nil
	})
	err = g.Wait()
	logger.Info("stopped")
	return err
}

func newServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
}

// shutdown stops accepting connections and waits up to drain for in-flight
// requests, then closes whatever is left, which cancels those requests'
// contexts and, through them, their upstream calls.
func shutdown(servers []*http.Server, drain time.Duration, logger *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), drain)
	defer cancel()
	g := new(errgroup.Group)
	for _, srv := range servers {
		g.Go(func() error {
			if err := srv.Shutdown(ctx); err != nil {
				logger.Warn("drain timed out; closing remaining connections", "addr", srv.Addr, "err", err)
				return srv.Close()
			}
			return nil
		})
	}
	_ = g.Wait()
}

// newUpstreamClient returns the HTTP client for provider calls. It has no
// overall timeout because streams are long-lived; request contexts bound
// every call. The idle pool is sized for many concurrent requests to the same
// few hosts, which the default (2 per host) would serialize on dialing.
func newUpstreamClient() *http.Client {
	return &http.Client{Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          1024,
		MaxIdleConnsPerHost:   256,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: time.Second,
	}}
}
