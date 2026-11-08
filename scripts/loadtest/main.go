// Command loadtest measures what the gateway adds to request latency. It
// sends the same load to the mock provider directly and through the gateway,
// with the mock's latency fixed, and reports the difference at p50, p95 and
// p99, streaming and non-streaming, at several fixed rates and concurrency
// levels. It also compares one hot key with load spread over many keys,
// because every request on a key updates that key's account row.
//
// It is meant to run inside the Compose network, so that both paths cross
// the same network: scripts/bench.sh loadtest. It writes its results as JSON
// to stdout and progress to stderr; -render prints a results file as
// Markdown.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/SamirVarma01/llm-gateway/internal/mock"
	"github.com/SamirVarma01/llm-gateway/scripts/internal/bench"
)

// Results is the output of a run.
type Results struct {
	Meta  bench.Meta `json:"meta"`
	Cells []Cell     `json:"cells"`
}

// Cell is one load level against one target.
type Cell struct {
	Target  string  `json:"target"` // "direct" or "gateway"
	Stream  bool    `json:"stream"`
	Rate    float64 `json:"rate,omitempty"`    // open loop
	Workers int     `json:"workers,omitempty"` // closed loop
	Keys    int     `json:"keys,omitempty"`    // gateway keys the requests rotate through

	Start time.Time `json:"start"`
	End   time.Time `json:"end"`

	Requests     int            `json:"requests"`
	Errors       int            `json:"errors"`
	ErrorKinds   map[string]int `json:"error_kinds,omitempty"`
	Throughput   float64        `json:"throughput"` // successful responses per second
	InFlightMean float64        `json:"in_flight_mean"`
	InFlightMax  int64          `json:"in_flight_max"`
	MaxLagMs     float64        `json:"max_lag_ms"`
	TTFT         *bench.Dist    `json:"ttft,omitempty"` // streaming, successful responses
	Latency      bench.Dist     `json:"latency"`        // successful responses
	// ServerOverheadMs is the mean of the gateway's own overhead metric over
	// the cell (design §4, D23), for comparison with what the client measured.
	ServerOverheadMs *float64 `json:"server_overhead_mean_ms,omitempty"`
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("loadtest: ")
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

type config struct {
	gateway, admin, adminToken, metrics, mock string
	alias, provider, upstreamModel            string
	behavior                                  mock.Behavior
	maxTokens                                 int
	rates                                     []float64
	workers                                   []int
	modes                                     []bool // stream?
	spreadKeys                                int
	spreadRates                               []float64
	warmup, duration, pause                   time.Duration
}

func run() error {
	var c config
	flag.StringVar(&c.gateway, "gateway", "http://gateway:8080", "gateway public URL")
	flag.StringVar(&c.admin, "admin", "http://gateway:8081", "gateway admin URL")
	flag.StringVar(&c.adminToken, "admin-token", envOr("ADMIN_TOKEN", "dev-admin-token"), "admin token")
	flag.StringVar(&c.metrics, "metrics", "http://gateway:9090/metrics", "gateway metrics URL; empty skips the server-side overhead")
	flag.StringVar(&c.mock, "mock", "http://mock:9000", "mock provider URL")
	flag.StringVar(&c.alias, "model", "bench", "gateway alias, routed only to -provider")
	flag.StringVar(&c.provider, "provider", "bench", "mock provider the alias routes to")
	flag.StringVar(&c.upstreamModel, "upstream-model", "m-bench", "model name sent to the mock directly")
	ttft := flag.Duration("ttft", 50*time.Millisecond, "mock time to first token")
	flag.Float64Var(&c.behavior.TokensPerSec, "tps", 100, "mock tokens per second after the first")
	flag.IntVar(&c.behavior.OutputTokens, "tokens", 32, "mock output tokens per response")
	rates := flag.String("rates", "50,200,500,1000", "open-loop rates, requests per second")
	workers := flag.String("concurrency", "10,50,200", "closed-loop concurrency levels")
	modes := flag.String("modes", "stream,nonstream", "stream, nonstream, or both")
	flag.IntVar(&c.spreadKeys, "spread-keys", 16, "also run -spread-rates through the gateway with load spread over this many keys; 0 skips")
	spreadRates := flag.String("spread-rates", "500,1000", "rates for the -spread-keys comparison")
	flag.DurationVar(&c.warmup, "warmup", 3*time.Second, "unmeasured time at the start of each cell")
	flag.DurationVar(&c.duration, "duration", 20*time.Second, "measured time per cell")
	flag.DurationVar(&c.pause, "pause", 2*time.Second, "idle time between cells")
	render := flag.String("render", "", "print this results file as Markdown, then exit")
	flag.Parse()

	if *render != "" {
		return renderFile(*render, os.Stdout)
	}
	c.behavior.TTFTMs = int(ttft.Milliseconds())
	c.maxTokens = c.behavior.OutputTokens * 2
	var err error
	if c.rates, err = parseList(*rates, parseFloat); err != nil {
		return fmt.Errorf("-rates: %w", err)
	}
	if c.workers, err = parseList(*workers, strconv.Atoi); err != nil {
		return fmt.Errorf("-concurrency: %w", err)
	}
	if c.spreadRates, err = parseList(*spreadRates, parseFloat); err != nil {
		return fmt.Errorf("-spread-rates: %w", err)
	}
	for _, m := range strings.Split(*modes, ",") {
		switch strings.TrimSpace(m) {
		case "stream":
			c.modes = append(c.modes, true)
		case "nonstream":
			c.modes = append(c.modes, false)
		case "":
		default:
			return fmt.Errorf("-modes: unknown mode %q", m)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	res, err := benchmark(ctx, c)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(res)
}

func benchmark(ctx context.Context, c config) (*Results, error) {
	client := bench.NewHTTPClient()
	setupCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if err := bench.WaitReady(setupCtx, client, c.gateway+"/readyz"); err != nil {
		return nil, err
	}
	if err := bench.SetBehavior(setupCtx, client, c.mock, c.provider, c.behavior); err != nil {
		return nil, err
	}
	admin := bench.Admin{URL: c.admin, Token: c.adminToken, Client: client}
	var keys []string
	for i := range max(c.spreadKeys, 1) {
		k, err := admin.CreateKey(setupCtx, fmt.Sprintf("loadtest-%d", i), bench.HighLimits())
		if err != nil {
			return nil, fmt.Errorf("create key: %w", err)
		}
		keys = append(keys, k.Secret)
	}

	res := &Results{Meta: bench.NewMeta("loadtest", map[string]string{
		"mock_ttft":   (time.Duration(c.behavior.TTFTMs) * time.Millisecond).String(),
		"mock_tps":    fmt.Sprint(c.behavior.TokensPerSec),
		"mock_tokens": strconv.Itoa(c.behavior.OutputTokens),
		"max_tokens":  strconv.Itoa(c.maxTokens),
		"warmup":      c.warmup.String(),
		"duration":    c.duration.String(),
	})}
	runCell := func(cell Cell) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Printf("%s", describe(cell))
		out, err := measure(ctx, c, client, keys, cell)
		if err != nil {
			return err
		}
		log.Printf("  ok %d, errors %d, p50 %.2f ms, p99 %.2f ms", out.Latency.N, out.Errors, out.Latency.P50, out.Latency.P99)
		res.Cells = append(res.Cells, out)
		time.Sleep(c.pause)
		return nil
	}
	// Direct and gateway runs alternate at each level, so drift on the host
	// affects both alike.
	for _, stream := range c.modes {
		for _, rate := range c.rates {
			for _, target := range []string{"direct", "gateway"} {
				if err := runCell(Cell{Target: target, Stream: stream, Rate: rate, Keys: keysFor(target, 1)}); err != nil {
					return nil, err
				}
			}
		}
		for _, w := range c.workers {
			for _, target := range []string{"direct", "gateway"} {
				if err := runCell(Cell{Target: target, Stream: stream, Workers: w, Keys: keysFor(target, 1)}); err != nil {
					return nil, err
				}
			}
		}
		if c.spreadKeys > 1 {
			for _, rate := range c.spreadRates {
				if err := runCell(Cell{Target: "gateway", Stream: stream, Rate: rate, Keys: c.spreadKeys}); err != nil {
					return nil, err
				}
			}
		}
	}
	return res, nil
}

func keysFor(target string, n int) int {
	if target == "gateway" {
		return n
	}
	return 0
}

func describe(c Cell) string {
	mode := "nonstream"
	if c.Stream {
		mode = "stream"
	}
	load := fmt.Sprintf("%g req/s", c.Rate)
	if c.Rate == 0 {
		load = fmt.Sprintf("%d workers", c.Workers)
	}
	s := fmt.Sprintf("%-7s %-9s %s", c.Target, mode, load)
	if c.Keys > 1 {
		s += fmt.Sprintf(", %d keys", c.Keys)
	}
	return s
}

func measure(ctx context.Context, c config, client *http.Client, keys []string, cell Cell) (Cell, error) {
	url, model, keyed := c.mock+"/p/"+c.provider+"/v1/chat/completions", c.upstreamModel, false
	if cell.Target == "gateway" {
		url, model, keyed = c.gateway+"/v1/chat/completions", c.alias, true
	}
	req := bench.ChatRequest{
		Model:     model,
		Messages:  []bench.Message{{Role: "user", Content: "Summarize how a gateway routes one request, in a short paragraph."}},
		MaxTokens: c.maxTokens,
		Stream:    cell.Stream,
	}
	if cell.Stream {
		req.StreamOptions = &bench.StreamOptions{IncludeUsage: true}
	}
	body, _ := json.Marshal(req)
	send := func(ctx context.Context, seq int) bench.Result {
		key := ""
		if keyed {
			key = keys[seq%max(cell.Keys, 1)]
		}
		rctx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()
		return bench.Chat(rctx, client, url, key, body, cell.Stream)
	}

	load := bench.Load{Rate: cell.Rate, Workers: cell.Workers, Warmup: c.warmup, Duration: c.duration}
	var before chan overheadSnapshot
	if cell.Target == "gateway" && c.metrics != "" {
		before = make(chan overheadSnapshot, 1)
		load.OnMeasure = func() { before <- scrapeOverhead(ctx, client, c.metrics, cell.Stream) }
	}
	cell.Start = time.Now().UTC()
	run := bench.Generate(ctx, load, send)
	cell.End = time.Now().UTC()
	if ctx.Err() != nil {
		return cell, ctx.Err()
	}

	var ttft, lat []time.Duration
	for _, r := range run.Results {
		cell.Requests++
		if !r.OK {
			cell.Errors++
			if cell.ErrorKinds == nil {
				cell.ErrorKinds = map[string]int{}
			}
			cell.ErrorKinds[r.Err]++
			continue
		}
		lat = append(lat, r.Latency())
		if d, ok := r.TTFT(); ok {
			ttft = append(ttft, d)
		}
	}
	cell.Latency = bench.Summarize(lat)
	if cell.Stream {
		d := bench.Summarize(ttft)
		cell.TTFT = &d
	}
	cell.Throughput = float64(len(lat)) / run.Elapsed.Seconds()
	cell.InFlightMean, cell.InFlightMax = run.InFlightMean, run.InFlightMax
	cell.MaxLagMs = bench.Ms(run.MaxLag)
	if before != nil {
		s0 := <-before
		time.Sleep(200 * time.Millisecond) // let the last requests' metrics land
		s1 := scrapeOverhead(ctx, client, c.metrics, cell.Stream)
		if s0.ok && s1.ok && s1.count > s0.count {
			mean := (s1.sum - s0.sum) / (s1.count - s0.count) * 1000
			cell.ServerOverheadMs = &mean
		}
	}
	return cell, nil
}

// overheadSnapshot is the gateway's overhead histogram for one mode.
type overheadSnapshot struct {
	sum, count float64
	ok         bool
}

func scrapeOverhead(ctx context.Context, client *http.Client, url string, stream bool) overheadSnapshot {
	m, err := bench.Scrape(ctx, client, url)
	if err != nil {
		log.Printf("  scrape metrics: %v", err)
		return overheadSnapshot{}
	}
	label := fmt.Sprintf(`{stream="%t"}`, stream)
	sum, okSum := m["gateway_overhead_seconds_sum"+label]
	n, okN := m["gateway_overhead_seconds_count"+label]
	return overheadSnapshot{sum: sum, count: n, ok: okSum && okN}
}

func parseFloat(s string) (float64, error) { return strconv.ParseFloat(s, 64) }

func parseList[T any](s string, parse func(string) (T, error)) ([]T, error) {
	var out []T
	for _, f := range strings.Split(s, ",") {
		if f = strings.TrimSpace(f); f == "" {
			continue
		}
		v, err := parse(f)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func renderFile(path string, w io.Writer) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var res Results
	if err := json.Unmarshal(b, &res); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	if len(res.Cells) == 0 {
		return errors.New(path + ": no cells")
	}
	return renderMarkdown(&res, w)
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
