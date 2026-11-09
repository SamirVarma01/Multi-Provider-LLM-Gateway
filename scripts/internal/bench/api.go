package bench

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/SamirVarma01/llm-gateway/internal/mock"
)

// Admin is a client for the gateway's admin API.
type Admin struct {
	URL, Token string
	Client     *http.Client
}

// Key is a created API key.
type Key struct {
	ID     string `json:"id"`
	Secret string `json:"key"`
}

// KeyLimits are the limits a created key gets. Benchmark keys get limits far
// above the load they see, so every request still takes the rate-limit and
// budget paths: a key with no limits skips Redis entirely.
type KeyLimits struct {
	RPM       *int     `json:"rpm_limit"`
	TPM       *int     `json:"tpm_limit"`
	BudgetUSD *float64 `json:"budget_usd"`
}

// HighLimits returns limits the benchmarks never reach.
func HighLimits() KeyLimits {
	rpm, tpm, budget := 100_000_000, 1_000_000_000, 1_000_000.0
	return KeyLimits{RPM: &rpm, TPM: &tpm, BudgetUSD: &budget}
}

// CreateKey creates an API key.
func (a Admin) CreateKey(ctx context.Context, name string, lim KeyLimits) (Key, error) {
	body, _ := json.Marshal(struct {
		Name string `json:"name"`
		KeyLimits
	}{name, lim})
	var k Key
	err := a.do(ctx, http.MethodPost, "/admin/keys", body, http.StatusCreated, &k)
	return k, err
}

// Reconcile runs one reconciler pass. It reports false, with no error, if a
// pass was already running.
func (a Admin) Reconcile(ctx context.Context) (bool, error) {
	err := a.do(ctx, http.MethodPost, "/admin/reconcile", nil, http.StatusOK, nil)
	var se *StatusError
	if errors.As(err, &se) && se.Status == http.StatusConflict {
		return false, nil
	}
	return err == nil, err
}

// StatusError is an unexpected HTTP status.
type StatusError struct {
	Status int
	Body   string
}

func (e *StatusError) Error() string { return fmt.Sprintf("status %d: %s", e.Status, e.Body) }

func (a Admin) do(ctx context.Context, method, path string, body []byte, want int, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(a.URL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+a.Token)
	req.Header.Set("Content-Type", "application/json")
	return doJSON(a.Client, req, want, out)
}

func doJSON(client *http.Client, req *http.Request, want int, out any) error {
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != want {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return &StatusError{Status: resp.StatusCode, Body: strings.TrimSpace(string(b))}
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// SetBehavior configures one of the mock's virtual providers.
func SetBehavior(ctx context.Context, client *http.Client, mockURL, provider string, b mock.Behavior) error {
	body, _ := json.Marshal(b)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut,
		strings.TrimRight(mockURL, "/")+"/_mock/providers/"+provider, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if err := doJSON(client, req, http.StatusOK, nil); err != nil {
		return fmt.Errorf("set mock behavior for %s: %w", provider, err)
	}
	return nil
}

// WaitReady polls url until it answers 200 or ctx ends.
func WaitReady(ctx context.Context, client *http.Client, url string) error {
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s not ready: %w", url, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// Scrape fetches a Prometheus text exposition and returns each sample's
// value keyed by its series as written, such as
// `gateway_breaker_state{provider="a",upstream_model="m"}`. The gateway's
// client library writes label names sorted, so callers can build keys.
func Scrape(ctx context.Context, client *http.Client, url string) (map[string]float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, &StatusError{Status: resp.StatusCode}
	}
	out := map[string]float64{}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		i := strings.LastIndexByte(line, ' ')
		if i < 0 {
			continue
		}
		v, err := strconv.ParseFloat(line[i+1:], 64)
		if err != nil {
			continue
		}
		out[line[:i]] = v
	}
	return out, sc.Err()
}

// Meta describes where and how a benchmark ran.
type Meta struct {
	Tool      string            `json:"tool"`
	Date      string            `json:"date"`
	Commit    string            `json:"commit,omitempty"` // $BENCH_COMMIT
	Host      string            `json:"host,omitempty"`   // $BENCH_HOST
	NumCPU    int               `json:"num_cpu"`          // as the benchmark process saw it
	GoVersion string            `json:"go_version"`
	Settings  map[string]string `json:"settings"`
}

// NewMeta returns a Meta for tool, now.
func NewMeta(tool string, settings map[string]string) Meta {
	return Meta{
		Tool:      tool,
		Date:      time.Now().UTC().Format(time.RFC3339),
		Commit:    os.Getenv("BENCH_COMMIT"),
		Host:      os.Getenv("BENCH_HOST"),
		NumCPU:    runtime.NumCPU(),
		GoVersion: runtime.Version(),
		Settings:  settings,
	}
}
