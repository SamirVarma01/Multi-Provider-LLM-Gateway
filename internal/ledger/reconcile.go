package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/SamirVarma01/llm-gateway/internal/config"
	"github.com/SamirVarma01/llm-gateway/internal/provider"
)

// UsageRecord is a provider's account of one attempt at a request.
type UsageRecord struct {
	Provider         string `json:"provider"`
	Model            string `json:"model"`
	Attempt          int    `json:"attempt"`
	PromptTokens     int    `json:"prompt_tokens"`
	CompletionTokens int    `json:"completion_tokens"`
	Billable         bool   `json:"billable"`
	Outcome          string `json:"outcome"`
}

// usageComplete is the Outcome of an attempt that finished.
const usageComplete = "complete"

// UsageSource can say what a provider billed for a request.
type UsageSource interface {
	Usage(ctx context.Context, requestID uuid.UUID) ([]UsageRecord, error)
}

// HTTPUsage reads the mock provider's usage API. Real providers have no
// per-request usage API, so nothing else implements UsageSource.
type HTTPUsage struct {
	client  *http.Client
	baseURL string
}

// Usage implements UsageSource.
func (u *HTTPUsage) Usage(ctx context.Context, requestID uuid.UUID) ([]UsageRecord, error) {
	endpoint := u.baseURL + "/_mock/usage?request_id=" + url.QueryEscape(requestID.String())
	return fetchUsage(ctx, u.client, endpoint)
}

func fetchUsage(ctx context.Context, client *http.Client, endpoint string) ([]UsageRecord, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		return nil, fmt.Errorf("usage lookup: HTTP %d", resp.StatusCode)
	}
	var recs []UsageRecord
	if err := json.NewDecoder(resp.Body).Decode(&recs); err != nil {
		return nil, fmt.Errorf("usage lookup: %w", err)
	}
	return recs, nil
}

// UsageSources returns a source for each provider with a usage_url, sharing
// one per URL so a request is looked up once however many of its targets
// live behind that URL.
func UsageSources(providers []config.ProviderSpec, client *http.Client) map[string]UsageSource {
	byURL := map[string]*HTTPUsage{}
	out := map[string]UsageSource{}
	for _, p := range providers {
		if p.UsageURL == "" {
			continue
		}
		base := strings.TrimRight(p.UsageURL, "/")
		if byURL[base] == nil {
			byURL[base] = &HTTPUsage{client: client, baseURL: base}
		}
		out[p.Name] = byURL[base]
	}
	return out
}

// orphanBatch is how many open reservations one query fetches.
const orphanBatch = 100

// reconcileLockKey is the advisory lock that lets one reconciler run at a
// time across gateway instances.
const reconcileLockKey int64 = 0x6c6c6d6777 // "llmgw"

// orphansSQL finds reservations with no close, older than any live request
// can be. Postgres's now() is the only clock involved. Paging by key, not by
// offset, lets a pass step past orphans it could not resolve.
const orphansSQL = `
SELECT r.id, r.created_at, r.request_id, r.meta
  FROM ledger_entries r
 WHERE r.kind = 'reserve'
   AND r.created_at < now() - make_interval(secs => $1)
   AND (r.created_at, r.id) > ($2, $3)
   AND NOT EXISTS (SELECT 1 FROM ledger_entries x WHERE x.request_id = r.request_id AND x.kind = 'release')
 ORDER BY r.created_at, r.id
 LIMIT $4`

// Result is what one reconciler pass did.
type Result struct {
	Skipped              bool `json:"skipped"` // another reconciler holds the lock
	Orphans              int  `json:"orphans"`
	Settled              int  `json:"settled"`
	ReleasedNoCall       int  `json:"released_no_call"`
	ReleasedUnknownUsage int  `json:"released_unknown_usage"` // undercharged, by design
	AlreadyClosed        int  `json:"already_closed"`         // closed by the gateway during the pass
	Failed               int  `json:"failed"`                 // left open for the next pass
}

// Reconciler closes reservations the gateway never closed, because it
// crashed or could not reach Postgres (design §9).
type Reconciler struct {
	ledger  *Ledger
	sources map[string]UsageSource // by provider name
	after   time.Duration
	log     *slog.Logger
}

// NewReconciler returns a Reconciler. after is how old an open reservation
// must be to count as orphaned: it must exceed the longest a request can
// live, REQUEST_TIMEOUT, plus the time its own close may take. sources maps
// provider names to where their usage can be looked up.
func NewReconciler(l *Ledger, sources map[string]UsageSource, after time.Duration, log *slog.Logger) *Reconciler {
	return &Reconciler{ledger: l, sources: sources, after: after, log: log}
}

// Run makes a pass every interval until ctx ends.
func (r *Reconciler) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := r.RunOnce(ctx); err != nil && ctx.Err() == nil {
				r.log.Error("reconcile pass failed", "err", err)
			}
		}
	}
}

// RunOnce makes one pass over all orphaned reservations. If another
// reconciler is running, it does nothing and reports Skipped. An orphan that
// cannot be resolved stays open for the next pass, so an unreachable usage
// source delays a close and never guesses at one.
func (r *Reconciler) RunOnce(ctx context.Context) (res Result, err error) {
	conn, err := r.ledger.pool.Acquire(ctx)
	if err != nil {
		return res, fmt.Errorf("ledger: reconcile: %w", err)
	}
	defer conn.Release()
	var locked bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, reconcileLockKey).Scan(&locked); err != nil {
		return res, fmt.Errorf("ledger: reconcile: %w", err)
	}
	if !locked {
		return Result{Skipped: true}, nil
	}
	defer func() {
		// The lock belongs to this session, and the connection goes back to
		// the pool. If it cannot be unlocked, closing the session frees it.
		uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if _, err := conn.Exec(uctx, `SELECT pg_advisory_unlock($1)`, reconcileLockKey); err != nil {
			_ = conn.Conn().Close(uctx)
		}
	}()

	var lastAt time.Time
	var lastID int64
	for {
		batch, err := r.orphans(ctx, lastAt, lastID)
		if err != nil {
			return res, err
		}
		for _, o := range batch {
			res.Orphans++
			if err := r.resolve(ctx, o, &res); err != nil {
				res.Failed++
				r.log.Error("could not reconcile request; leaving it open", "request_id", o.requestID, "err", err)
			}
			lastAt, lastID = o.createdAt, o.id
		}
		if len(batch) < orphanBatch {
			break
		}
	}
	if res.Orphans > 0 {
		r.log.Info("reconcile pass", "orphans", res.Orphans, "settled", res.Settled, "released_no_call", res.ReleasedNoCall,
			"released_unknown_usage", res.ReleasedUnknownUsage, "already_closed", res.AlreadyClosed, "failed", res.Failed)
	}
	return res, nil
}

type orphan struct {
	id        int64
	createdAt time.Time
	requestID uuid.UUID
	meta      Meta
	metaErr   error
}

func (r *Reconciler) orphans(ctx context.Context, afterAt time.Time, afterID int64) ([]orphan, error) {
	rows, err := r.ledger.pool.Query(ctx, orphansSQL, r.after.Seconds(), afterAt, afterID, orphanBatch)
	if err != nil {
		return nil, fmt.Errorf("ledger: find orphans: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (orphan, error) {
		var o orphan
		var meta []byte
		if err := row.Scan(&o.id, &o.createdAt, &o.requestID, &meta); err != nil {
			return o, err
		}
		o.metaErr = json.Unmarshal(meta, &o.meta)
		return o, nil
	})
	if err != nil {
		return nil, fmt.Errorf("ledger: find orphans: %w", err)
	}
	return out, nil
}

// resolve closes one orphan (design §9): with a charge if a provider billed
// it, or else with a release that says whether the call is known not to have
// happened.
func (r *Reconciler) resolve(ctx context.Context, o orphan, res *Result) error {
	if o.metaErr != nil {
		return fmt.Errorf("reserve meta: %w", o.metaErr)
	}
	billed, unknown, err := r.lookup(ctx, o)
	if err != nil {
		return err
	}

	var closed bool
	switch {
	case billed != nil:
		price, ok := o.meta.price(billed.Provider, billed.Model)
		if !ok {
			return fmt.Errorf("billed by %s/%s, which is not among its targets", billed.Provider, billed.Model)
		}
		reason := ReasonInterrupted
		if billed.Outcome == usageComplete {
			reason = ReasonCompleted
		}
		closed, err = r.ledger.Settle(ctx, Settlement{
			RequestID: o.requestID, AmountNUSD: Cost(billed.PromptTokens, billed.CompletionTokens, price),
			Provider: billed.Provider, UpstreamModel: billed.Model,
			PromptTokens: billed.PromptTokens, CompletionTokens: billed.CompletionTokens,
			Reason: reason, Source: SourceReconciler,
		})
		if err == nil && closed {
			res.Settled++
		}
	case unknown:
		closed, err = r.ledger.Release(ctx, o.requestID, ReasonOrphanUnknownUsage, SourceReconciler)
		if err == nil && closed {
			res.ReleasedUnknownUsage++
			r.log.Warn("released a request with unknown usage: this is an undercharge", "request_id", o.requestID)
		}
	default:
		closed, err = r.ledger.Release(ctx, o.requestID, ReasonOrphanNoCall, SourceReconciler)
		if err == nil && closed {
			res.ReleasedNoCall++
		}
	}
	if err != nil {
		return err
	}
	if !closed {
		res.AlreadyClosed++
	}
	return nil
}

// lookup asks every source the orphan's targets have. billed is the billable
// record of the last attempt, the one that was serving the client when the
// gateway stopped; earlier attempts failed over, and those are not charged.
// unknown means a target cannot be asked, so the absence of a record does not
// show that no call happened.
func (r *Reconciler) lookup(ctx context.Context, o orphan) (billed *UsageRecord, unknown bool, err error) {
	asked := map[UsageSource]bool{}
	for _, t := range o.meta.Targets {
		src, ok := r.sources[t.Provider]
		if !ok {
			unknown = true
			continue
		}
		if asked[src] {
			continue
		}
		asked[src] = true
		recs, err := src.Usage(ctx, o.requestID)
		if err != nil {
			return nil, false, errors.Join(fmt.Errorf("usage lookup for %s", t.Provider), err)
		}
		for i := range recs {
			if recs[i].Billable && (billed == nil || recs[i].Attempt > billed.Attempt) {
				billed = &recs[i]
			}
		}
	}
	return billed, unknown, nil
}

func (m Meta) price(providerName, model string) (provider.Price, bool) {
	for _, t := range m.Targets {
		if t.Provider == providerName && t.Model == model {
			return provider.Price{InputMicroPerM: t.InputMicroPerM, OutputMicroPerM: t.OutputMicroPerM}, true
		}
	}
	return provider.Price{}, false
}
