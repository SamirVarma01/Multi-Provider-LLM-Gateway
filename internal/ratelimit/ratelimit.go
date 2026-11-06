// Package ratelimit enforces per-key request and token rates with token
// buckets in Redis (design §7).
//
// Each key has up to two buckets, requests and tokens per minute. Capacity is
// the per-minute limit and refill is limit/60 per second. A request is
// admitted by one Lua script, atomically across both buckets, so concurrent
// gateways cannot over-admit and a request refused by one bucket takes
// nothing from the other. The scripts read Redis's clock, not the gateway's.
//
// Redis being unavailable is an error from Admit, and the caller fails open:
// budgets live in Postgres, so only rate fairness is lost.
package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Limits are a key's rates; nil means unlimited.
type Limits struct {
	RPM *int
	TPM *int
}

// Decision is the result of Admit.
type Decision struct {
	Allowed    bool
	RetryAfter time.Duration // refused: when the buckets could cover the request
	TooLarge   bool          // refused: the request needs more tokens than the TPM limit, so it can never pass
}

// Limiter is the Redis-backed rate limiter.
type Limiter struct {
	rdb     redis.Scripter
	timeout time.Duration
}

// New returns a Limiter. Each Redis call gets at most timeout, which bounds
// what an unresponsive Redis adds to a request. The client must have
// ContextTimeoutEnabled set for that bound to reach its socket reads.
func New(rdb redis.Scripter, timeout time.Duration) *Limiter {
	return &Limiter{rdb: rdb, timeout: timeout}
}

// bucketLua defines the shared bucket helpers. A missing bucket is a full
// one. store sets a TTL of exactly the time the bucket takes to refill, so
// a key expires only once expiry changes nothing, and token debt from an
// underestimated request is never forgiven early.
const bucketLua = `
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)

local function level(key, cap)
  local v = redis.call('HMGET', key, 'tokens', 'ts_ms')
  local tokens, ts = tonumber(v[1]), tonumber(v[2])
  if not tokens then return cap end
  return math.min(cap, tokens + math.max(now - ts, 0) * cap / 60000)
end

local function store(key, cap, tokens)
  redis.call('HSET', key, 'tokens', tokens, 'ts_ms', now)
  redis.call('PEXPIRE', key, math.ceil((cap - tokens) * 60000 / cap) + 1)
end
`

// admitScript takes 1 request and ARGV[3] tokens, or nothing. A limit of 0
// skips that bucket. It returns {1, 0}, or {0, wait_ms} where wait_ms is when
// both buckets could cover the request.
var admitScript = redis.NewScript(bucketLua + `
local rpm_limit, tpm_limit, tok_cost = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local wait, rpm, tpm = 0, 0, 0
if rpm_limit > 0 then
  rpm = level(KEYS[1], rpm_limit)
  if rpm < 1 then wait = math.ceil((1 - rpm) * 60000 / rpm_limit) end
end
if tpm_limit > 0 then
  tpm = level(KEYS[2], tpm_limit)
  if tpm < tok_cost then wait = math.max(wait, math.ceil((tok_cost - tpm) * 60000 / tpm_limit)) end
end
if wait > 0 then return {0, wait} end
if rpm_limit > 0 then store(KEYS[1], rpm_limit, rpm - 1) end
if tpm_limit > 0 then store(KEYS[2], tpm_limit, tpm - tok_cost) end
return {1, 0}
`)

// adjustScript adds ARGV[2] tokens to the bucket, which may be negative,
// capped at capacity. Below zero is debt that delays later requests, so the
// rate holds on average even when actual usage passes the estimate.
var adjustScript = redis.NewScript(bucketLua + `
local cap, delta = tonumber(ARGV[1]), tonumber(ARGV[2])
local tokens = math.min(cap, level(KEYS[1], cap) + delta)
if tokens >= cap then
  redis.call('DEL', KEYS[1])
else
  store(KEYS[1], cap, tokens)
end
return 1
`)

// The hash tag keeps a key's buckets in one Redis Cluster slot, which a
// script touching both requires.
func bucketKeys(id uuid.UUID) []string {
	return []string{"rl:{" + id.String() + "}:rpm", "rl:{" + id.String() + "}:tpm"}
}

func limitArg(limit *int) int {
	if limit == nil {
		return 0
	}
	return *limit
}

// Admit takes one request and tokens tokens from the key's buckets, or
// nothing if they cannot cover it. Tokens are the request's worst case; call
// Adjust once its real usage is known.
func (l *Limiter) Admit(ctx context.Context, id uuid.UUID, lim Limits, tokens int) (Decision, error) {
	if lim.RPM == nil && lim.TPM == nil {
		return Decision{Allowed: true}, nil
	}
	if lim.TPM != nil && tokens > *lim.TPM {
		return Decision{TooLarge: true}, nil
	}
	ctx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	res, err := admitScript.Run(ctx, l.rdb, bucketKeys(id), limitArg(lim.RPM), limitArg(lim.TPM), tokens).Int64Slice()
	if err != nil {
		return Decision{}, fmt.Errorf("ratelimit: admit: %w", err)
	}
	if len(res) != 2 {
		return Decision{}, fmt.Errorf("ratelimit: admit: unexpected reply %v", res)
	}
	if res[0] == 1 {
		return Decision{Allowed: true}, nil
	}
	return Decision{RetryAfter: time.Duration(res[1]) * time.Millisecond}, nil
}

// Adjust corrects the key's token bucket by delta: positive refunds tokens
// taken by Admit and not used, negative charges tokens used beyond them.
func (l *Limiter) Adjust(ctx context.Context, id uuid.UUID, lim Limits, delta int) error {
	if lim.TPM == nil || delta == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	if err := adjustScript.Run(ctx, l.rdb, bucketKeys(id)[1:], *lim.TPM, delta).Err(); err != nil {
		return fmt.Errorf("ratelimit: adjust: %w", err)
	}
	return nil
}
