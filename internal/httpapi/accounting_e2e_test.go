package httpapi

import (
	"context"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/SamirVarma01/llm-gateway/internal/auth"
	"github.com/SamirVarma01/llm-gateway/internal/config"
	"github.com/SamirVarma01/llm-gateway/internal/ledger"
	"github.com/SamirVarma01/llm-gateway/internal/mock"
	"github.com/SamirVarma01/llm-gateway/internal/obs"
	"github.com/SamirVarma01/llm-gateway/internal/provider"
	"github.com/SamirVarma01/llm-gateway/internal/ratelimit"
	"github.com/SamirVarma01/llm-gateway/internal/testutil"
)

// realEnv is a gateway on real Postgres and Redis, in front of the mock.
type realEnv struct {
	*testEnv
	pool  *pgxpool.Pool
	store *auth.PGStore
}

func newRealEnv(t *testing.T, b mock.Behavior, limiter RateLimiter) *realEnv {
	t.Helper()
	return newRealEnvTweaked(t, b, limiter, nil)
}

func newRealEnvTweaked(t *testing.T, b mock.Behavior, limiter RateLimiter, tweak func(o *Options, pool *pgxpool.Pool, mockURL string)) *realEnv {
	t.Helper()
	pool := testutil.NewDatabase(t)
	store := auth.NewPGStore(pool)
	if limiter == nil {
		limiter = ratelimit.New(testutil.NewRedis(t), 2*time.Second)
	}
	e := newEnvTweaked(t, b, envTweak{
		server: func(o *Options) {
			o.Keys = store
			o.Auth = auth.NewCache(store, time.Minute)
			o.Ledger = ledger.New(pool)
			o.Limiter = limiter
		},
		mock: func(o *Options, mockURL string) {
			if tweak != nil {
				tweak(o, pool, mockURL)
			}
		},
	})
	return &realEnv{testEnv: e, pool: pool, store: store}
}

func (e *realEnv) newKey(t *testing.T, nk auth.NewKey) (auth.Key, string) {
	t.Helper()
	nk.Name = "t"
	k, plaintext, err := e.store.CreateKey(context.Background(), nk)
	if err != nil {
		t.Fatal(err)
	}
	return k, plaintext
}

func (e *realEnv) chatAs(t *testing.T, key string, body any) *http.Response {
	t.Helper()
	return e.do(t, http.MethodPost, e.public.URL+"/v1/chat/completions", key, body)
}

func (e *realEnv) totals(t *testing.T, id uuid.UUID) (spent, held int64) {
	t.Helper()
	if err := e.pool.QueryRow(context.Background(),
		`SELECT spent_nusd, held_nusd FROM key_accounts WHERE key_id = $1`, id).Scan(&spent, &held); err != nil {
		t.Fatal(err)
	}
	return spent, held
}

func (e *realEnv) entryCount(t *testing.T, requestID string) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ledger_entries WHERE request_id = $1`, requestID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestE2EBudgetRunsOutExactly(t *testing.T) {
	e := newRealEnv(t, mock.Behavior{OutputTokens: 4}, nil)
	budget := int64(20_000)
	k, plaintext := e.newKey(t, auth.NewKey{BudgetNUSD: &budget})
	ten := 10

	var want int64
	served := 0
	for {
		resp := e.chatAs(t, plaintext, chatReq("chat", &ten))
		reqID := resp.Header.Get(obs.RequestIDHeader)
		if resp.StatusCode == http.StatusPaymentRequired {
			wantError(t, resp, http.StatusPaymentRequired, "insufficient_budget")
			if n := e.entryCount(t, reqID); n != 0 {
				t.Errorf("a refused request wrote %d ledger entries", n)
			}
			if recs := e.mock.Records(reqID); len(recs) != 0 {
				t.Errorf("a refused request reached the provider: %+v", recs)
			}
			break
		}
		readBody(t, resp, false)
		e.outcome(t) // settled
		served++
		if served > 20 {
			t.Fatal("the budget never ran out")
		}
		rec := waitRecords(t, e.mock, reqID, 1)[0]
		want += ledger.Cost(rec.PromptTokens, rec.CompletionTokens, smallPrice)
		if n := e.entryCount(t, reqID); n != 3 {
			t.Errorf("request %s has %d ledger entries, want reserve, settle, release", reqID, n)
		}
		spent, held := e.totals(t, k.ID)
		if spent != want || held != 0 {
			t.Fatalf("after %d requests: spent %d held %d, want %d and 0 (the mock's records)", served, spent, held, want)
		}
	}
	if served == 0 {
		t.Fatal("no request fit the budget")
	}
	if spent, _ := e.totals(t, k.ID); spent > budget {
		t.Errorf("spent %d, past the budget of %d", spent, budget)
	}
}

// Many requests at once on one key: reservations serialize on the account
// row, so what is admitted can never pass the budget, and every request
// closes.
func TestE2EConcurrentRequestsCannotOverspend(t *testing.T) {
	e := newRealEnv(t, mock.Behavior{OutputTokens: 4, TTFTMs: 20}, nil)
	budget := int64(50_000)
	k, plaintext := e.newKey(t, auth.NewKey{BudgetNUSD: &budget})

	const requests = 40
	ten := 10
	body := chatReq("chat", &ten)
	var ok, refused, other atomic.Int64
	var wg sync.WaitGroup
	for range requests {
		wg.Go(func() {
			raw, _ := http.NewRequest(http.MethodPost, e.public.URL+"/v1/chat/completions", jsonBody(body))
			raw.Header.Set("Authorization", "Bearer "+plaintext)
			resp, err := http.DefaultClient.Do(raw)
			if err != nil {
				other.Add(1)
				return
			}
			defer resp.Body.Close()
			_, _ = io.Copy(io.Discard, resp.Body)
			switch resp.StatusCode {
			case http.StatusOK:
				ok.Add(1)
			case http.StatusPaymentRequired:
				refused.Add(1)
			default:
				other.Add(1)
			}
		})
	}
	wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	e.srv.Drain(ctx)

	if other.Load() != 0 || ok.Load() == 0 || refused.Load() == 0 {
		t.Fatalf("200s %d, 402s %d, other %d: want some of each of the first two only", ok.Load(), refused.Load(), other.Load())
	}
	spent, held := e.totals(t, k.ID)
	if spent > budget || held != 0 {
		t.Errorf("spent %d of %d, held %d", spent, budget, held)
	}
	var settles, open int
	if err := e.pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE kind = 'settle'),
		       count(*) FILTER (WHERE kind = 'reserve' AND NOT EXISTS (
		         SELECT 1 FROM ledger_entries x WHERE x.request_id = e.request_id AND x.kind = 'release'))
		  FROM ledger_entries e WHERE key_id = $1`, k.ID).Scan(&settles, &open); err != nil {
		t.Fatal(err)
	}
	if settles != int(ok.Load()) || open != 0 {
		t.Errorf("%d settles for %d served requests, %d left open", settles, ok.Load(), open)
	}
}

func TestE2ERequestRateLimit(t *testing.T) {
	e := newRealEnv(t, mock.Behavior{OutputTokens: 2}, nil)
	rpm := 2
	_, plaintext := e.newKey(t, auth.NewKey{RPMLimit: &rpm})
	for range rpm {
		readBody(t, e.chatAs(t, plaintext, chatReq("chat", nil)), false)
	}
	resp := e.chatAs(t, plaintext, chatReq("chat", nil))
	reqID := resp.Header.Get(obs.RequestIDHeader)
	// 2 per minute is one every 30s, and the 2 requests above just took the last.
	if ra, err := strconv.Atoi(resp.Header.Get("Retry-After")); err != nil || ra < 25 || ra > 30 {
		t.Errorf("Retry-After = %q, want about 30", resp.Header.Get("Retry-After"))
	}
	wantError(t, resp, http.StatusTooManyRequests, "rate_limit_exceeded")
	if n := e.entryCount(t, reqID); n != 0 {
		t.Errorf("a rate-limited request wrote %d ledger entries", n)
	}
}

// Requests are admitted at their worst case and refunded the difference, so
// the bucket tracks real usage.
func TestE2ETokenLimitTracksActualUsage(t *testing.T) {
	e := newRealEnv(t, mock.Behavior{OutputTokens: 2}, nil)
	tpm := 1000
	_, plaintext := e.newKey(t, auth.NewKey{TPMLimit: &tpm})

	big := 900
	for i := range 3 { // 3 x 900 is far past 1000: this works only through refunds
		resp := e.chatAs(t, plaintext, chatReq("chat", &big))
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("request %d: %d %s", i+1, resp.StatusCode, body)
		}
		readBody(t, resp, false)
		e.outcome(t) // refunded
	}

	// Admission counts the output cap that will be sent, not what the client
	// asked for: "tiny" caps output at 5, so this fits.
	huge := 2000
	resp := e.chatAs(t, plaintext, chatReq("tiny", &huge))
	readBody(t, resp, false)
	e.outcome(t)

	// The input estimate plus 1000 output tokens can never fit in 1000.
	resp = e.chatAs(t, plaintext, chatReq("chat", &tpm))
	if resp.Header.Get("Retry-After") != "" {
		t.Errorf("Retry-After = %q: waiting cannot help", resp.Header.Get("Retry-After"))
	}
	wantError(t, resp, http.StatusTooManyRequests, "tokens_exceed_limit")
}

func TestE2ERedisDownFailsOpen(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	rdb := redis.NewClient(&redis.Options{Addr: addr, ContextTimeoutEnabled: true, MaxRetries: -1})
	t.Cleanup(func() { rdb.Close() })

	e := newRealEnv(t, mock.Behavior{OutputTokens: 2}, ratelimit.New(rdb, 200*time.Millisecond))
	rpm := 1
	k, plaintext := e.newKey(t, auth.NewKey{RPMLimit: &rpm})
	for range 3 { // past the limit, which cannot be enforced
		readBody(t, e.chatAs(t, plaintext, chatReq("chat", nil)), false)
		e.outcome(t)
	}
	if spent, held := e.totals(t, k.ID); spent == 0 || held != 0 {
		t.Errorf("spent %d held %d: money must stay accounted for", spent, held)
	}
}

func TestE2EPostgresDownFailsClosed(t *testing.T) {
	e := newRealEnv(t, mock.Behavior{OutputTokens: 2}, nil)
	_, plaintext := e.newKey(t, auth.NewKey{})
	readBody(t, e.chatAs(t, plaintext, chatReq("chat", nil)), false) // caches the key
	e.outcome(t)

	e.pool.Close()
	resp := e.chatAs(t, plaintext, chatReq("chat", nil))
	reqID := resp.Header.Get(obs.RequestIDHeader)
	wantError(t, resp, http.StatusServiceUnavailable, "ledger_unavailable")
	if recs := e.mock.Records(reqID); len(recs) != 0 {
		t.Errorf("the provider was called with no reservation: %+v", recs)
	}
}

func TestE2EUpdateKey(t *testing.T) {
	e := newRealEnv(t, mock.Behavior{OutputTokens: 2}, nil)
	budget, rpm := int64(5_000_000_000), 10 // $5
	k, plaintext := e.newKey(t, auth.NewKey{RPMLimit: &rpm, BudgetNUSD: &budget})
	url := e.admin.URL + "/admin/keys/" + k.ID.String()
	patch := func(body string) *http.Response {
		return e.do(t, http.MethodPatch, url, adminToken, body)
	}

	// A field left out stays; a field set changes.
	got := decode[keyResponse](t, patch(`{"tpm_limit": 500}`))
	if *got.RPMLimit != 10 || *got.TPMLimit != 500 || *got.BudgetUSD != 5 {
		t.Errorf("after setting tpm_limit: %+v", got)
	}
	// null means unlimited.
	got = decode[keyResponse](t, patch(`{"rpm_limit": null, "tpm_limit": null}`))
	if got.RPMLimit != nil || got.TPMLimit != nil || *got.BudgetUSD != 5 {
		t.Errorf("after clearing limits: %+v", got)
	}
	got = decode[keyResponse](t, patch(`{}`))
	if *got.BudgetUSD != 5 {
		t.Errorf("after an empty patch: %+v", got)
	}

	wantError(t, patch(`{"rpm_limit": 0}`), http.StatusBadRequest, "invalid_limit")
	wantError(t, patch(`{"tpm_limit": "lots"}`), http.StatusBadRequest, "invalid_limit")
	wantError(t, patch(`{"budget_usd": -1}`), http.StatusBadRequest, "invalid_budget")
	wantError(t, patch(`{"budget_usd": "free"}`), http.StatusBadRequest, "invalid_budget")
	wantError(t, patch(`{"name": "x"}`), http.StatusBadRequest, "invalid_json")
	wantError(t, patch(`not json`), http.StatusBadRequest, "invalid_json")
	wantError(t, e.do(t, http.MethodPatch, e.admin.URL+"/admin/keys/"+uuid.NewString(), adminToken, `{}`),
		http.StatusNotFound, "key_not_found")
	wantError(t, e.do(t, http.MethodPatch, e.admin.URL+"/admin/keys/nope", adminToken, `{}`),
		http.StatusBadRequest, "invalid_id")

	// A new budget applies at once, even with the key cached.
	readBody(t, e.chatAs(t, plaintext, chatReq("chat", nil)), false)
	e.outcome(t)
	if got := decode[keyResponse](t, patch(`{"budget_usd": 0}`)); *got.BudgetUSD != 0 {
		t.Errorf("budget = %v", *got.BudgetUSD)
	}
	wantError(t, e.chatAs(t, plaintext, chatReq("chat", nil)), http.StatusPaymentRequired, "insufficient_budget")
	patch(`{"budget_usd": null}`)
	readBody(t, e.chatAs(t, plaintext, chatReq("chat", nil)), false)
}

// crashedLedger keeps reservations but loses every close, which is what the
// ledger sees when the gateway dies after serving a request.
type crashedLedger struct{ *ledger.Ledger }

func (crashedLedger) Settle(context.Context, ledger.Settlement) (bool, error) { return true, nil }
func (crashedLedger) Release(context.Context, uuid.UUID, string, string) (bool, error) {
	return true, nil
}

func TestE2EReconcilerClosesWhatACrashLeftOpen(t *testing.T) {
	e := newRealEnvTweaked(t, mock.Behavior{OutputTokens: 4}, nil, func(o *Options, pool *pgxpool.Pool, mockURL string) {
		led := ledger.New(pool)
		o.Ledger = crashedLedger{led}
		tr := &http.Transport{}
		t.Cleanup(tr.CloseIdleConnections)
		sources := ledger.UsageSources([]config.ProviderSpec{
			{Name: "mock-a", UsageURL: mockURL}, {Name: "mock-b", UsageURL: mockURL},
		}, &http.Client{Transport: tr, Timeout: 5 * time.Second})
		o.Reconciler = ledger.NewReconciler(led, sources, 0, obs.NewLogger(io.Discard, 0))
	})
	k, plaintext := e.newKey(t, auth.NewKey{})

	// Served in full.
	resp := e.chatAs(t, plaintext, chatReq("pair", nil))
	complete := resp.Header.Get(obs.RequestIDHeader)
	readBody(t, resp, false)
	e.outcome(t)

	// Cut off after 3 tokens: billed for those, by the provider and by us.
	setBehavior(t, e.testEnv, "mock-a", mock.Behavior{OutputTokens: 10, FailAfterTokens: 3})
	body := chatReq("pair", nil)
	body.Stream = true
	resp = e.chatAs(t, plaintext, body)
	cut := resp.Header.Get(obs.RequestIDHeader)
	readClientStream(resp.Body)
	e.outcome(t)

	// Every attempt failed before any output: nothing billed.
	failing := mock.Behavior{ErrorRate: 1, ErrorStatus: 503}
	setBehavior(t, e.testEnv, "mock-a", failing)
	setBehavior(t, e.testEnv, "mock-b", failing)
	resp = e.chatAs(t, plaintext, chatReq("pair", nil))
	failed := resp.Header.Get(obs.RequestIDHeader)
	_, _ = io.Copy(io.Discard, resp.Body)
	e.outcome(t)

	if spent, held := e.totals(t, k.ID); spent != 0 || held == 0 {
		t.Fatalf("before reconciling: spent %d held %d", spent, held)
	}
	reconcile := func() ledger.Result {
		return decode[ledger.Result](t, e.do(t, http.MethodPost, e.admin.URL+"/admin/reconcile", adminToken, nil))
	}
	if res := reconcile(); res.Orphans != 3 || res.Settled != 2 || res.ReleasedNoCall != 1 || res.Failed != 0 {
		t.Fatalf("result = %+v", res)
	}

	var want int64
	for _, id := range []string{complete, cut} {
		rec := e.mock.Records(id)[0]
		want += ledger.Cost(rec.PromptTokens, rec.CompletionTokens, provider.Price{InputMicroPerM: 1_000_000, OutputMicroPerM: 2_000_000})
	}
	if spent, held := e.totals(t, k.ID); spent != want || held != 0 {
		t.Errorf("spent %d held %d, want %d and 0", spent, held, want)
	}
	if n := e.entryCount(t, failed); n != 2 {
		t.Errorf("a request that billed nothing has %d entries, want reserve and release", n)
	}
	if res := reconcile(); res.Orphans != 0 {
		t.Errorf("second pass found %+v", res)
	}
}
