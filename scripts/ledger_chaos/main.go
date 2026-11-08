// Command ledger_chaos checks the ledger through crashes. It sends mixed load
// through the gateway while repeatedly killing and restarting the gateway's
// container, waits for the reconciler to close what the crashes orphaned,
// then audits the ledger against the mock provider's records (design §9,
// §12). The target is no discrepancies, no double charges and no reservation
// left open.
//
// It runs on the host, because it drives Docker: scripts/bench.sh chaos. It
// writes its results as JSON to stdout and progress to stderr; -render prints
// a results file as Markdown.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SamirVarma01/llm-gateway/internal/db"
	"github.com/SamirVarma01/llm-gateway/internal/ledger"
	"github.com/SamirVarma01/llm-gateway/internal/mock"
	"github.com/SamirVarma01/llm-gateway/scripts/internal/bench"
)

// Results is the output of a run.
type Results struct {
	Meta    bench.Meta          `json:"meta"`
	Load    LoadStats           `json:"load"`
	Kills   []Kill              `json:"kills"`
	Audit   *ledger.AuditReport `json:"audit"`
	Checks  Checks              `json:"checks"`
	Samples []string            `json:"samples,omitempty"` // the first failures of Checks, described
}

// LoadStats is what the clients saw.
type LoadStats struct {
	Sent       int            `json:"sent"`
	OK         int            `json:"ok"`
	Errors     map[string]int `json:"errors"` // by kind
	DurationMs float64        `json:"duration_ms"`
}

// Kill is one kill and restart of the gateway.
type Kill struct {
	AtMs     float64 `json:"at_ms"` // from the start of the load
	Signal   string  `json:"signal"`
	InFlight int64   `json:"in_flight"` // client requests open at the kill
	DownMs   float64 `json:"down_ms"`   // from the kill until /readyz answered again
}

// Checks are what this test verifies beyond ledger.Audit.
type Checks struct {
	// DoubleCharges counts requests with more than one charge entry, and
	// DoubleCloses those with more than one close entry.
	DoubleCharges int64 `json:"double_charges"`
	DoubleCloses  int64 `json:"double_closes"`
	// BilledUnknown counts requests the mock billed that have no
	// reservation at all; Audit only looks at requests the ledger knows.
	BilledUnknown int64 `json:"billed_unknown"`
	// Client checks compare usage that complete responses reported with
	// what the ledger charged.
	ClientMatched    int64 `json:"client_matched"`
	ClientMismatched int64 `json:"client_mismatched"`
	ClientNoUsage    int64 `json:"client_no_usage"` // streams that did not ask for usage
	// OpenBeforeReconcile is what was still open once the gateway's own
	// closes had finished and before the final reconciler pass. The
	// in-process reconciler also ran throughout the test.
	OpenBeforeReconcile int64 `json:"open_before_reconcile"`
	ReconcilePasses     int   `json:"reconcile_passes"`
}

// Headline is the three numbers the test is for.
type Headline struct {
	Discrepancies int64 `json:"discrepancies"`
	DoubleCharges int64 `json:"double_charges"`
	Orphans       int64 `json:"orphans"`
}

// Headline derives the three numbers from the audit and the checks.
func (r *Results) Headline() Headline {
	return Headline{
		Discrepancies: r.Audit.Violations[ledger.CheckProviderUsage] + r.Audit.BilledNotCharged + r.Checks.BilledUnknown,
		DoubleCharges: r.Checks.DoubleCharges,
		Orphans:       r.Audit.Open,
	}
}

type config struct {
	gateway, admin, adminToken, mock, databaseURL, container string
	rate                                                     float64
	kills                                                    int
	signals                                                  []string
	upMin, upMax, downMin, downMax, tail                     time.Duration
	requestTimeout, reconcileGrace                           time.Duration
	keys                                                     int
	seed                                                     uint64
	behavior                                                 mock.Behavior
	primaryErrorRate                                         float64
}

// aliases are the routes the load spreads over (config/routes.bench.yaml).
var aliases = []string{"bench", "failover", "bench-claude"}

func main() {
	log.SetFlags(0)
	log.SetPrefix("ledger_chaos: ")
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	var c config
	flag.StringVar(&c.gateway, "gateway", "http://127.0.0.1:18080", "gateway public URL")
	flag.StringVar(&c.admin, "admin", "http://127.0.0.1:18081", "gateway admin URL")
	flag.StringVar(&c.adminToken, "admin-token", envOr("ADMIN_TOKEN", "dev-admin-token"), "admin token")
	flag.StringVar(&c.mock, "mock", "http://127.0.0.1:19000", "mock provider URL")
	flag.StringVar(&c.databaseURL, "database-url", os.Getenv("DATABASE_URL"), "Postgres URL")
	flag.StringVar(&c.container, "container", "", "the gateway's Docker container")
	flag.Float64Var(&c.rate, "rate", 40, "requests per second")
	flag.IntVar(&c.kills, "kills", 20, "how many times to kill the gateway")
	signals := flag.String("signals", "KILL", "signals to kill with, used in turn")
	flag.DurationVar(&c.upMin, "up-min", 3*time.Second, "shortest time the gateway runs between kills")
	flag.DurationVar(&c.upMax, "up-max", 8*time.Second, "longest time the gateway runs between kills")
	flag.DurationVar(&c.downMin, "down-min", 0, "shortest time the gateway stays down")
	flag.DurationVar(&c.downMax, "down-max", 2*time.Second, "longest time the gateway stays down")
	flag.DurationVar(&c.tail, "tail", 5*time.Second, "load after the last restart")
	flag.DurationVar(&c.requestTimeout, "request-timeout", 20*time.Second, "the gateway's REQUEST_TIMEOUT")
	flag.DurationVar(&c.reconcileGrace, "reconcile-grace", 10*time.Second, "the gateway's RECONCILE_GRACE")
	flag.IntVar(&c.keys, "keys", 4, "API keys the load spreads over")
	flag.Uint64Var(&c.seed, "seed", 1, "seed for the request mix and the kill timing")
	ttft := flag.Duration("ttft", 100*time.Millisecond, "mock time to first token")
	flag.Float64Var(&c.behavior.TokensPerSec, "tps", 50, "mock tokens per second")
	flag.IntVar(&c.behavior.OutputTokens, "tokens", 48, "mock output tokens per response")
	flag.Float64Var(&c.primaryErrorRate, "primary-error-rate", 0.1, "share of the failover alias's primary attempts that fail before output")
	render := flag.String("render", "", "print this results file as Markdown, then exit")
	flag.Parse()

	if *render != "" {
		return renderFile(*render, os.Stdout)
	}
	if c.databaseURL == "" || c.container == "" {
		return errors.New("-database-url and -container are required")
	}
	c.behavior.TTFTMs = int(ttft.Milliseconds())
	for _, s := range strings.Split(*signals, ",") {
		if s = strings.TrimSpace(s); s != "" {
			c.signals = append(c.signals, s)
		}
	}
	if len(c.signals) == 0 {
		return errors.New("-signals is empty")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	res, err := chaos(ctx, c)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(res); err != nil {
		return err
	}
	h := res.Headline()
	log.Printf("discrepancies %d, double charges %d, orphaned reservations %d", h.Discrepancies, h.DoubleCharges, h.Orphans)
	return nil
}

func chaos(ctx context.Context, c config) (*Results, error) {
	client := bench.NewHTTPClient()
	setupCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if err := bench.WaitReady(setupCtx, client, c.gateway+"/readyz"); err != nil {
		return nil, err
	}
	for _, p := range []string{"bench", "primary", "backup", "bench-claude"} {
		b := c.behavior
		if p == "primary" {
			b.ErrorRate = c.primaryErrorRate
		}
		if err := bench.SetBehavior(setupCtx, client, c.mock, p, b); err != nil {
			return nil, err
		}
	}
	admin := bench.Admin{URL: c.admin, Token: c.adminToken, Client: client}
	var keys []string
	for i := range c.keys {
		k, err := admin.CreateKey(setupCtx, fmt.Sprintf("chaos-%d", i), bench.HighLimits())
		if err != nil {
			return nil, fmt.Errorf("create key: %w", err)
		}
		keys = append(keys, k.Secret)
	}

	res := &Results{Meta: bench.NewMeta("ledger_chaos", map[string]string{
		"rate":            fmt.Sprint(c.rate),
		"kills":           fmt.Sprint(c.kills),
		"signals":         strings.Join(c.signals, ","),
		"up":              fmt.Sprintf("%s–%s", c.upMin, c.upMax),
		"down":            fmt.Sprintf("%s–%s", c.downMin, c.downMax),
		"tail":            c.tail.String(),
		"keys":            fmt.Sprint(c.keys),
		"seed":            fmt.Sprint(c.seed),
		"request_timeout": c.requestTimeout.String(),
		"reconcile_grace": c.reconcileGrace.String(),
		"mock_ttft":       (time.Duration(c.behavior.TTFTMs) * time.Millisecond).String(),
		"mock_tps":        fmt.Sprint(c.behavior.TokensPerSec),
		"mock_tokens":     fmt.Sprint(c.behavior.OutputTokens),
		"primary_errors":  fmt.Sprint(c.primaryErrorRate),
		"aliases":         strings.Join(aliases, ","),
	})}

	// The request mix is fixed by the seed: each request's alias, mode,
	// max_tokens and key depend only on its sequence number.
	var inFlight atomic.Int64
	send := func(ctx context.Context, seq int) bench.Result {
		inFlight.Add(1)
		defer inFlight.Add(-1)
		rng := rand.New(rand.NewPCG(c.seed, uint64(seq)))
		stream := rng.IntN(2) == 1
		req := bench.ChatRequest{
			Model:     aliases[rng.IntN(len(aliases))],
			Messages:  []bench.Message{{Role: "user", Content: fmt.Sprintf("Chaos request %d: explain what a ledger is.", seq)}},
			MaxTokens: 16 + rng.IntN(81),
			Stream:    stream,
		}
		if stream && rng.IntN(2) == 1 {
			req.StreamOptions = &bench.StreamOptions{IncludeUsage: true}
		}
		body, _ := json.Marshal(req)
		rctx, cancel := context.WithTimeout(ctx, c.requestTimeout+10*time.Second)
		defer cancel()
		return bench.Chat(rctx, client, c.gateway+"/v1/chat/completions", keys[seq%len(keys)], body, stream)
	}

	start := time.Now()
	stopLoad := make(chan struct{})
	runc := make(chan bench.Run, 1)
	go func() {
		runc <- bench.Generate(ctx, bench.Load{Rate: c.rate, Duration: 24 * time.Hour, Stop: stopLoad}, send)
	}()
	rng := rand.New(rand.NewPCG(c.seed, 1<<63))
	between := func(lo, hi time.Duration) time.Duration {
		if hi <= lo {
			return lo
		}
		return lo + time.Duration(rng.Int64N(int64(hi-lo)))
	}
	for i := range c.kills {
		if err := sleep(ctx, between(c.upMin, c.upMax)); err != nil {
			close(stopLoad)
			return nil, err
		}
		sig := c.signals[i%len(c.signals)]
		k := Kill{AtMs: bench.Ms(time.Since(start)), Signal: sig, InFlight: inFlight.Load()}
		killedAt := time.Now()
		if err := restart(ctx, client, c, sig, between(c.downMin, c.downMax)); err != nil {
			close(stopLoad)
			return nil, fmt.Errorf("kill %d: %w", i+1, err)
		}
		k.DownMs = bench.Ms(time.Since(killedAt))
		res.Kills = append(res.Kills, k)
		log.Printf("kill %d/%d (%s): %d requests in flight, back after %.1fs", i+1, c.kills, sig, k.InFlight, k.DownMs/1000)
	}
	if err := sleep(ctx, c.tail); err != nil {
		close(stopLoad)
		return nil, err
	}
	close(stopLoad)
	stoppedAt := time.Now()
	run := <-runc
	res.Load = LoadStats{Sent: len(run.Results), Errors: map[string]int{}, DurationMs: bench.Ms(stoppedAt.Sub(start))}
	for _, r := range run.Results {
		if r.OK {
			res.Load.OK++
		} else {
			res.Load.Errors[errorKind(r.Err)]++
		}
	}
	log.Printf("load stopped: %d sent, %d complete", res.Load.Sent, res.Load.OK)

	pool, err := db.Open(ctx, c.databaseURL)
	if err != nil {
		return nil, err
	}
	defer pool.Close()
	if err := settle(ctx, c, admin, pool, stoppedAt, res); err != nil {
		return nil, err
	}

	records, err := ledger.AllUsage(ctx, &http.Client{Timeout: time.Minute}, c.mock)
	if err != nil {
		return nil, fmt.Errorf("mock usage: %w", err)
	}
	res.Audit, err = ledger.New(pool).Audit(ctx, ledger.AuditOptions{ProviderRecords: records, RequireClosed: true})
	if err != nil {
		return nil, err
	}
	if err := check(ctx, pool, records, run.Results, res); err != nil {
		return nil, err
	}
	return res, nil
}

// restart kills the gateway's container with sig, waits for it to stop,
// leaves it down for down, then starts it and waits until it is ready.
func restart(ctx context.Context, client *http.Client, c config, sig string, down time.Duration) error {
	if err := docker(ctx, "kill", "--signal", sig, c.container); err != nil {
		return err
	}
	stopCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	for {
		out, err := exec.CommandContext(stopCtx, "docker", "inspect", "-f", "{{.State.Running}}", c.container).Output()
		if err != nil {
			return fmt.Errorf("docker inspect: %w", err)
		}
		if strings.TrimSpace(string(out)) == "false" {
			break
		}
		if err := sleep(stopCtx, 100*time.Millisecond); err != nil {
			return fmt.Errorf("container did not stop: %w", err)
		}
	}
	if err := sleep(ctx, down); err != nil {
		return err
	}
	if err := docker(ctx, "start", c.container); err != nil {
		return err
	}
	readyCtx, cancel2 := context.WithTimeout(ctx, time.Minute)
	defer cancel2()
	return bench.WaitReady(readyCtx, client, c.gateway+"/readyz")
}

func docker(ctx context.Context, args ...string) error {
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

// settle waits until every reservation is old enough for the reconciler,
// then runs passes until none is left open or a pass changes nothing.
func settle(ctx context.Context, c config, admin bench.Admin, pool *pgxpool.Pool, stoppedAt time.Time, res *Results) error {
	// Requests sent before the load stopped end within REQUEST_TIMEOUT; the
	// gateway's own closes have finished by then. Anything still open after
	// that was orphaned by a kill.
	if err := sleep(ctx, time.Until(stoppedAt.Add(c.requestTimeout+2*time.Second))); err != nil {
		return err
	}
	var err error
	if res.Checks.OpenBeforeReconcile, err = openCount(ctx, pool); err != nil {
		return err
	}
	log.Printf("%d reservations open; waiting until they are older than %s", res.Checks.OpenBeforeReconcile, c.requestTimeout+c.reconcileGrace)
	if err := sleep(ctx, time.Until(stoppedAt.Add(c.requestTimeout+c.reconcileGrace+2*time.Second))); err != nil {
		return err
	}
	for range 10 {
		ran, err := admin.Reconcile(ctx)
		if err != nil {
			return fmt.Errorf("reconcile: %w", err)
		}
		if ran {
			res.Checks.ReconcilePasses++
		}
		open, err := openCount(ctx, pool)
		if err != nil {
			return err
		}
		if open == 0 {
			break
		}
		log.Printf("%d reservations still open", open)
		if err := sleep(ctx, time.Second); err != nil {
			return err
		}
	}
	return nil
}

func openCount(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	var n int64
	err := pool.QueryRow(ctx, `
		SELECT count(*) FROM ledger_entries r
		 WHERE r.kind = 'reserve' AND NOT EXISTS (
		   SELECT 1 FROM ledger_entries x WHERE x.request_id = r.request_id AND x.kind = 'release')`).Scan(&n)
	return n, err
}

// check runs the checks ledger.Audit does not.
func check(ctx context.Context, pool *pgxpool.Pool, records []ledger.UsageRecord, results []bench.Result, res *Results) error {
	ch := &res.Checks
	sample := func(format string, args ...any) {
		if len(res.Samples) < 50 {
			res.Samples = append(res.Samples, fmt.Sprintf(format, args...))
		}
	}
	err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE kind = 'settle'), count(*) FILTER (WHERE kind = 'release')
		  FROM (SELECT request_id, kind FROM ledger_entries WHERE kind IN ('settle', 'release')
		         GROUP BY request_id, kind HAVING count(*) > 1) d`).Scan(&ch.DoubleCharges, &ch.DoubleCloses)
	if err != nil {
		return err
	}

	reserved := map[string]bool{}
	rows, err := pool.Query(ctx, `SELECT request_id FROM ledger_entries WHERE kind = 'reserve'`)
	if err != nil {
		return err
	}
	var id uuid.UUID
	if _, err := pgx.ForEachRow(rows, []any{&id}, func() error { reserved[id.String()] = true; return nil }); err != nil {
		return err
	}
	billed := map[string]bool{}
	for _, r := range records {
		if r.Billable && !billed[r.RequestID] {
			billed[r.RequestID] = true
			if !reserved[r.RequestID] {
				ch.BilledUnknown++
				sample("billed_unknown: the mock billed request %s, which has no reservation", r.RequestID)
			}
		}
	}

	type charge struct{ prompt, completion int }
	charged := map[string]charge{}
	rows, err = pool.Query(ctx, `SELECT request_id, prompt_tokens, completion_tokens FROM ledger_entries WHERE kind = 'settle'`)
	if err != nil {
		return err
	}
	var p, comp int
	if _, err := pgx.ForEachRow(rows, []any{&id, &p, &comp}, func() error {
		charged[id.String()] = charge{p, comp}
		return nil
	}); err != nil {
		return err
	}
	for _, r := range results {
		if !r.OK {
			continue
		}
		if r.Usage == nil {
			ch.ClientNoUsage++
			continue
		}
		got, ok := charged[r.RequestID]
		switch {
		case !ok:
			ch.ClientMismatched++
			sample("client: request %s got a complete response, and the ledger has no charge for it", r.RequestID)
		case got.prompt != r.Usage.PromptTokens || got.completion != r.Usage.CompletionTokens:
			ch.ClientMismatched++
			sample("client: request %s reported %d+%d tokens; the ledger charged %d+%d", r.RequestID,
				r.Usage.PromptTokens, r.Usage.CompletionTokens, got.prompt, got.completion)
		default:
			ch.ClientMatched++
		}
	}
	return nil
}

// errorKind groups client errors: a killed gateway produces many slightly
// different network errors.
func errorKind(err string) string {
	switch {
	case strings.Contains(err, "connection refused"):
		return "connection refused"
	case strings.Contains(err, "connection reset"), strings.Contains(err, "EOF"), strings.Contains(err, "broken pipe"):
		return "connection dropped"
	case strings.HasPrefix(err, "stream"):
		return "stream cut off"
	case strings.HasPrefix(err, "status "):
		return err
	}
	return "other: " + err
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}
