# LLM Gateway: Design

Status: **draft for review (Phase 0)**. None of this is implemented yet. Defaults shown
for config values are starting points, not measurements. All performance numbers will
come from `scripts/` in Phase 7.

## 1. Scope

An OpenAI-compatible HTTP gateway in front of several LLM providers. It provides:

- streaming passthrough with low added latency;
- routing by price, live latency, or static priority, with circuit breakers and failover;
- per-key rate limits (requests/min, tokens/min) and spend budgets;
- an append-only usage ledger where every request is charged **at most once**, and is
  charged **exactly once** whenever its usage is knowable (§9, §12).

Out of scope: marketplace UI, real payments, provider catalog sync, user accounts beyond
API keys, multi-region deployment.

## 2. Components

```mermaid
flowchart LR
  C[Client<br/>OpenAI SDK] -->|/v1/*| G[Gateway<br/>public :8080]
  A[Operator] -->|/admin/*| GA[Gateway<br/>admin :8081]
  G --> R[(Redis<br/>rate-limit buckets)]
  G --> P[(Postgres<br/>keys, accounts, ledger)]
  GA --> P
  G -->|always streams| U1[Provider: OpenAI-format]
  G -->|always streams| U2[Provider: Anthropic-format]
  G -.tests/bench.-> M[Mock provider]
  REC[Reconciler<br/>in-process loop + CLI] --> P
  REC -.usage lookup.-> M
```

| Store | Holds | Why there |
|---|---|---|
| Postgres | API key hashes, per-key accounts, ledger | Money must be transactional and durable. Reserve and settle are single ACID transactions. |
| Redis | Rate-limit token buckets | Hot path, approximate, safe to lose. Atomic Lua gives correct behavior under concurrency. |
| Gateway memory | Route table, latency EWMAs, circuit breakers, auth cache | Read on every request, so they need zero network hops (see §14, D3). |

## 3. Request lifecycle

```mermaid
sequenceDiagram
  autonumber
  participant C as Client
  participant G as Gateway
  participant RL as Redis
  participant PG as Postgres
  participant UP as Provider
  C->>G: POST /v1/chat/completions
  G->>G: request ID, parse, validate
  G->>G: auth (hash lookup, cached)
  G->>G: resolve alias → ordered candidates, estimate cost
  G->>RL: Lua: take 1 req + est tokens
  G->>PG: reserve(max cost over candidates)
  loop until committed or attempts exhausted
    G->>UP: streaming request (attempt n)
    UP-->>G: first chunk
  end
  Note over G,C: COMMIT POINT: first byte to client
  UP-->>G: chunks…
  G-->>C: chunks… (stream) / single JSON (non-stream)
  G->>G: capture usage (reported or estimated)
  G-->>C: final bytes flushed
  G->>PG: settle(actual) + release(remainder)
  G->>RL: Lua: adjust token bucket by (actual − estimate)
```

| # | Step | Failure → client sees | Ledger effect |
|---|---|---|---|
| 1 | Assign request ID (UUIDv7, gateway-generated; a client `X-Request-ID` is logged as `client_request_id` only) | — | — |
| 2 | Parse and validate (body size limit, `n>1` rejected) | 400 | none |
| 3 | Auth: `sha256(bearer)` → `api_keys` lookup, cached in-process for `AUTH_CACHE_TTL` (default 10s) | 401 | none |
| 4 | Resolve alias → candidate targets, filtered by capability and ordered by policy (§6) | 404 `model_not_found` / 400 if no candidate supports the request | none |
| 5 | Estimate: `est_in` (§8.3), `max_out`, reservation `R` = max cost over candidates | — | — |
| 6 | Rate limit: one Lua call takes 1 request and `est_in + max_out` tokens (§7) | 429 + `Retry-After` | none |
| 7 | Reserve `R` (single SQL statement, §8.4) | 402 if budget insufficient; 503 if Postgres is unavailable (fail closed) | `reserve` |
| 8 | Attempt loop with failover, allowed **only before the commit point** (§6.4) | 502/503/504 if every attempt fails | `release(R)` |
| 9 | Stream to client, or aggregate for non-stream | after commit: SSE error event (§6.5) | — |
| 10 | Capture usage from the provider's stream, or estimate it and set `estimated=true` | — | — |
| 11 | Flush the final bytes to the client, **then** settle using a detached context | not visible to client; retried with backoff until the request deadline, then left to the reconciler | `settle(A)` + `release(max(R−A,0))` |
| 12 | Adjust the TPM bucket by `actual_tokens − admitted_tokens` | — | — |

Settling *after* the last byte keeps the Postgres round trip off the client's critical
path. If the gateway crashes between the flush and the settle, the reservation stays
open and the reconciler resolves it (§9).

**Always stream upstream.** The gateway asks the provider for a stream even when the
client asked for a non-streaming response, and builds the JSON body itself. That leaves
one code path, gives a TTFT measurement on every request (used by the `fastest`
policy), and gives partial usage when a request is cut off. The cost is building
non-stream responses ourselves; Phase 7 measures what that costs.

## 4. API surface

All error bodies use the OpenAI shape: `{"error":{"message","type","code","request_id"}}`.

### Public listener (`:8080`)

| Method & path | Notes |
|---|---|
| `POST /v1/chat/completions` | OpenAI Chat Completions request, streaming (`stream: true` → SSE) or not. Accepts `max_tokens` or `max_completion_tokens`, tools, `tool_choice`, `parallel_tool_calls`, tool messages, and image parts. Usage carries a gateway extension, `estimated: true`, when a count was estimated (§5.5). Optional `X-Route-Policy: cheapest\|fastest\|priority` overrides the alias default (Phase 3). Response headers: `X-Request-ID`, `X-Gateway-Provider`, `X-Gateway-Attempts`. |
| `GET /v1/models` | Lists aliases in OpenAI list format, with an extra `pricing` object (max input/output price per 1M tokens across candidates). |
| `GET /healthz` | Liveness: the process is up. |
| `GET /readyz` | Readiness: Postgres and Redis are reachable. |
| `GET /metrics` | Prometheus (Phase 6). |

### Admin listener (`:8081`, `Authorization: Bearer $ADMIN_TOKEN`)

| Method & path | Body / result |
|---|---|
| `POST /admin/keys` | `{name, rpm_limit?, tpm_limit?, budget_usd?}` → `{id, key}`. The plaintext key is returned **once only**. |
| `GET /admin/keys` | List: id, name, prefix, limits, budget, created/revoked. No secrets. |
| `PATCH /admin/keys/{id}` | `{rpm_limit?, tpm_limit?, budget_usd?}`. `null` means unlimited. |
| `POST /admin/keys/{id}/revoke` | Sets `revoked_at`. Takes effect within `AUTH_CACHE_TTL`. |
| `GET /admin/keys/{id}/usage?from&to` | Spent, held, available, request count, and tokens grouped by alias and provider, all derived from the ledger. |
| `GET /admin/keys/{id}/ledger?limit&before` | Raw ledger entries (paginated). |
| `POST /admin/reconcile` | Runs one reconciler pass now and returns what it resolved (used by scripts). |

The admin API is on a separate listener so that it is not exposed when only `:8080`
is published.

**Key format:** `gw_` + 32 random bytes, base64url-encoded. Only `sha256(key)` and a
short display prefix are stored. A plain SHA-256 is enough because the keys are
high-entropy random strings, not passwords, so a slow KDF would add per-request cost for
no benefit.

## 5. Provider adapters and the internal format

### 5.1 Interface

```go
// A Target is one routable (provider, upstream model) pair with its pricing.
type Target struct {
    Provider      string
    UpstreamModel string
    Price         Price // integer micro-USD per 1M tokens, input and output
    MaxOutput     int
}

type Provider interface {
    Name() string
    // Supports reports whether this provider can honor the request. A non-nil error
    // says what it cannot do; requests are never silently altered to fit.
    Supports(req *ChatRequest) error
    // Stream starts an upstream streaming call. It returns after the upstream response
    // status is known; the error is classified (see UpstreamError).
    Stream(ctx context.Context, t Target, req *ChatRequest) (Stream, error)
}

type Stream interface {
    Recv() (*Chunk, error) // io.EOF on clean end; *UpstreamError on failure; ctx.Err() if the caller cancelled
    Usage() Reported       // what the provider has reported so far
    Close() error          // idempotent; releases the upstream connection
}

// Providers can report the two counts at different times (Anthropic sends input
// tokens first and output tokens at the end), so each has its own flag.
type Reported struct {
    PromptTokens, CompletionTokens int
    HasPrompt, HasCompletion       bool
}

type UpstreamError struct {
    Provider  string
    Status    int     // upstream HTTP status, 0 for transport errors
    Kind      ErrKind // Transport, Timeout, RateLimited, Server, Config, BadRequest, Protocol
    Retryable bool    // failover to another target may help
    Msg       string
}
```

A caller cancellation is returned as `ctx.Err()`, never as an `UpstreamError`, so a
client leaving is not counted against the provider's health.

`Stream` creates no goroutines; reads are bounded by the request context. (The
per-attempt TTFT and idle timeouts in §6.4 arrive in Phase 3.) Tests check for leaks
with `goleak`.

### 5.2 Internal format (OpenAI-shaped)

The public API is OpenAI's, so the internal types follow it closely and Anthropic is
translated in both directions:

```go
type ChatRequest struct {
    Model             string
    Messages          []Message
    Tools             []Tool // {Name, Description, Parameters (JSON Schema), Strict}
    ToolChoice        *ToolChoice
    ParallelToolCalls *bool
    MaxTokens         *int // effective output cap; always sent upstream
    Temperature       *float64
    TopP              *float64
    Stop              []string
    Stream            bool // client mode; upstream always streams
    IncludeUsage      bool // client set stream_options.include_usage
    User              string
    Extra             map[string]json.RawMessage // unmodeled fields; forwarded to OpenAI-format targets only
}
type Message struct {
    Role       Role          // system (also accepts "developer") | user | assistant | tool
    Content    []ContentPart // text | image (https URL or base64 data URI)
    ToolCalls  []ToolCall    // assistant only
    ToolCallID string        // tool only
}
type ToolCall struct{ ID, Name, Arguments string } // Arguments is JSON text
type Chunk struct {
    Role         string
    Content      string
    ToolCalls    []ToolCallDelta // {Index, ID, Name, ArgumentsFragment}
    FinishReason string          // stop | length | tool_calls | content_filter
}
type Usage struct { // what the gateway accounts for one request
    PromptTokens, CompletionTokens int
    Estimated bool // at least one count was estimated
}
```

The public API rejects request fields whose response data the gateway would drop while
re-encoding chunks: `logprobs`, `top_logprobs`, `audio`, `modalities`, and the legacy
`functions`/`function_call`. Other unknown fields go into `Extra`.

### 5.3 OpenAI-format adapter

Nearly a passthrough. Upstream requests always set `stream: true` and
`stream_options.include_usage: true`; the usage-only final chunk is consumed by the
adapter, and the gateway sends its own usage chunk only if the client asked for one.
`Extra` is forwarded as is (known fields win on a name clash).

The output cap is sent as `max_tokens` by default, which most OpenAI-compatible
servers accept. A provider can set `max_tokens_field: max_completion_tokens` in the
route table, which newer OpenAI models require.

Chunks are parsed into `Chunk` and re-encoded. That decode/encode per chunk costs CPU;
if Phase 7 shows it matters, we switch to forwarding the raw bytes for OpenAI→OpenAI
and scanning only for `usage`.

### 5.4 Anthropic adapter (Messages API)

The adapter calls the HTTP API directly with `anthropic-version: 2026-10-21` and
`x-api-key`, not through the Go SDK. The gateway must own retries (the SDK retries 429
and 5xx itself, which would hide failures from failover and the circuit breakers),
point the adapter at any base URL (the mock), and see raw stream bytes for timing.

| OpenAI (internal) | Anthropic |
|---|---|
| `system` / `developer` messages | joined (blank line between) into top-level `system`, wherever they appear |
| consecutive same-role messages | merged (Anthropic requires roles to alternate) |
| empty text parts | dropped (Anthropic rejects empty text blocks) |
| `image_url` (base64 data URI / http(s) URL) | `image` block with `base64` / `url` source; other URLs are rejected |
| assistant `tool_calls[]` | assistant `tool_use` blocks `{id, name, input: parsed(arguments)}`; arguments must be a JSON object |
| `tool` message(s) | `tool_result` blocks `{tool_use_id, content}` in a `user` message |
| `tools[].function{name,description,parameters,strict}` | `tools[]{name, description, input_schema, strict}`; no parameters → `{"type":"object","properties":{}}` |
| `tool_choice`: `auto` / `none` / `required` / `{function:{name}}` | `{type:auto}` / `{type:none}` / `{type:any}` / `{type:tool,name}` |
| `parallel_tool_calls: false` | `disable_parallel_tool_use: true` on the tool choice |
| `max_tokens` (optional) | `max_tokens` (required → always the effective cap) |
| `stop` | `stop_sequences` |
| `user` | `metadata.user_id` |
| `temperature`, `top_p` | API version 2026-10-21 rejects them on every model. The default value (1) means the same as omitting it, so it is accepted and not sent; any other value is rejected by `Supports` |
| fields in `Extra` (e.g. `seed`, `response_format`) | rejected by `Supports` |

Forced tool choice (`required`, or a named function) is passed through as `any`/`tool`.
The newest Claude models reject that with a 400, which reaches the client as
`upstream_rejected`.

Mapping the finish reason: `end_turn`/`stop_sequence`/`pause_turn` → `stop`,
`max_tokens`/`model_context_window_exceeded` → `length`, `tool_use` → `tool_calls`,
`refusal` → `content_filter`.

Mapping stream events:

| Anthropic event | Internal `Chunk` |
|---|---|
| `message_start` | `Role=assistant`; record input usage = `input_tokens` + `cache_creation_input_tokens` + `cache_read_input_tokens`. The `output_tokens` here is provisional and ignored |
| `content_block_start` (text) | nothing, unless it carries text |
| `content_block_start` (tool_use) | `ToolCalls[{Index: k, ID, Name}]`, where k counts tool_use blocks |
| `content_block_start` (thinking, redacted_thinking, others) | nothing; their deltas are skipped too |
| `content_block_delta` `text_delta` | `Content` |
| `content_block_delta` `input_json_delta` | `ToolCalls[{Index: k, ArgumentsFragment}]` |
| `content_block_stop` of a tool_use with no input deltas | `ArgumentsFragment: "{}"`, so clients always get valid JSON |
| `message_delta` | `FinishReason`; record the final `usage.output_tokens` |
| `message_stop` | EOF |
| `ping`, unknown event types | ignored (the API may add event types) |
| `error` | `*UpstreamError` (`overloaded_error` → retryable server error) |

Thinking content is never forwarded: OpenAI's format has no place for it, and on current
models the API returns it empty by default anyway. Its tokens are still part of
`output_tokens`, so they are billed. Because OpenAI-format clients cannot send thinking
blocks back, multi-turn tool use reaches Anthropic without them. The live test
(`make test-live`) checks that the API accepts such a history.

Simplification: Anthropic cache-read and cache-write tokens are billed at the normal
input price. Real Anthropic prices them differently, so this can overcharge relative to
the provider. It is listed as a non-guarantee.

### 5.5 Usage capture

- Each count the provider reports is used as is: the final OpenAI usage chunk, or
  Anthropic's `message_start` (input) and `message_delta` (output).
- Each count the provider does not report is estimated, and the usage is marked
  `Estimated`. That happens when the provider omitted usage, the client disconnected,
  or the stream failed mid-way. For an interrupted Anthropic stream this usually means
  only the output count is estimated.
- The estimator (`internal/estimate`) counts one token per 4 bytes of UTF-8. It counts
  bytes, not characters, so multi-byte scripts are underestimated less. It adds 4
  tokens per message and 3 per request, and counts `EST_IMAGE_TOKENS` per image
  instead of the URL or base64 length. Input is estimated from the request; output from
  the content and tool arguments the gateway actually received.
- Clients see the flag as a gateway extension, `usage.estimated: true` (omitted when
  false). The ledger will persist it (Phase 4).

## 6. Routing, health, and failover

### 6.1 Route table (`ROUTES_FILE`, YAML)

```yaml
providers:
  - name: mock-a
    format: openai            # adapter
    base_url: http://mock:9000/p/mock-a/v1   # up to and including /v1
    api_key_env: MOCK_A_KEY   # optional
models:
  - alias: fast-chat
    policy: cheapest          # default; X-Route-Policy can override per request
    default_max_output: 1024
    candidates:
      - { provider: mock-a, model: m-small, input_per_mtok: 0.15, output_per_mtok: 0.60, priority: 1, max_output: 4096 }
      - { provider: mock-b, model: m-small, input_per_mtok: 0.20, output_per_mtok: 0.80, priority: 2, max_output: 4096 }
```

Scalar config comes from env; the route table is a file whose path comes from env. Prices
are converted to integer micro-USD per 1M tokens at load time.

### 6.2 Policies

Each policy orders the capable candidates. Targets with an open breaker are skipped when
attempts are made (§6.3).

- **`priority`**: static `priority` from config.
- **`cheapest`**: estimated cost of *this* request, `est_in·in_price + max_out·out_price`.
  Ties are broken by priority.
- **`fastest`**: uses per-target EWMAs (α = `EWMA_ALPHA`, default 0.2) of
  **TTFT** and **decode rate** (output tokens/s after the first token). All requests have
  both, because upstream always streams.
  - Streaming request: score = `ewma_ttft`.
  - Non-streaming request: score = `ewma_ttft + expected_out / ewma_tps`, where
    `expected_out` = the alias-level EWMA of completion tokens, capped at `max_out`.
  - Targets with no samples, or whose newest sample is older than `LATENCY_STALE_AFTER`
    (default 30s), get an optimistic score equal to the best known score, so they get
    tried. In addition, `ROUTE_EXPLORE_RATE` (default 0.05) of requests put a random
    non-best healthy candidate first, to keep estimates fresh.

### 6.3 Circuit breaker (per target)

```mermaid
stateDiagram-v2
  [*] --> Closed
  Closed --> Open: failure rate ≥ threshold over window (min volume)<br/>or N consecutive failures
  Open --> HalfOpen: cooldown elapsed
  HalfOpen --> Closed: probe successes ≥ K
  HalfOpen --> Open: any probe failure (cooldown doubles, capped)
```

Defaults (config): window = last 20 outcomes or 10s, threshold 50% with at least 5
requests, or 5 consecutive failures. Cooldown starts at 5s, doubles on each re-trip, caps
at 60s, and resets after a full close. Half-open allows 1 concurrent probe and closes
after 2 successes.

| Outcome | Counts as breaker failure? | Failover allowed (pre-commit)? |
|---|---|---|
| Transport error, connect/TTFT timeout | yes | yes |
| Upstream 5xx, Anthropic `overloaded_error` | yes | yes |
| Upstream 429 | yes | yes |
| Upstream 401/402/403/404 (`KindConfig`: our credentials, account, or configured model) | yes | yes |
| Upstream 400/413/422 and other 4xx (client's request is bad) | **no** | **no**: passed through to the client as 400 |
| Mid-stream error / idle timeout after first chunk | yes | **no** (post-commit) |
| Client cancel | no | — |

404 counts as configuration, not a client error: clients name a gateway alias, and the
upstream model ID comes from the route table. An upstream 404 is therefore the gateway's
mistake, and another target may still serve the request (§14, D11).

Health state lives in each gateway process (see §14, D3).

### 6.4 Failover and the commit point

**Commit point** is the moment the first byte goes to the client:

- **Streaming:** when the first upstream chunk that carries output arrives: content, a
  tool call, or a finish reason. Role-only chunks before it (OpenAI's opening chunk,
  Anthropic's `message_start`) are held back, so an upstream that accepts the request
  but fails before producing anything is still a pre-commit failure (§14, D12). Only
  then do we write `200` + `text/event-stream`, an opening role chunk, and that chunk.
  Before then the client has received nothing, not even headers. We cannot send SSE
  keepalive comments before committing, because they would commit the `200`.
- **Non-streaming:** when the full JSON response is written. Any failure while
  aggregating counts as pre-commit, so a non-stream request can fail over even after the
  upstream produced tokens. The provider may bill for those tokens; we do not charge the
  client for them (§12).

Attempt loop:
1. Take the next candidate from the policy order whose breaker admits a request.
2. Attempt context = request context, plus a TTFT timeout (`ATTEMPT_TTFT_TIMEOUT`,
   default 10s), plus an idle timeout between chunks (`STREAM_IDLE_TIMEOUT`, default 30s).
3. On a pre-commit error that allows failover: record it in the breaker and move to the
   next candidate **immediately**. Switching providers needs no wait. Once every
   candidate has been tried, start again from the top after full-jitter exponential
   backoff (base 100ms, cap 2s), up to `MAX_ATTEMPTS` (default 3) total. Stop early if
   the remaining request deadline is shorter than the TTFT timeout.
4. If nothing commits, return the last error mapped as follows: timeouts → 504, all
   breakers open → 503, everything else → 502. Then release the reservation.

Tradeoff: while failover is running, a streaming client sees no bytes at all. The wait is
bounded by `MAX_ATTEMPTS × ATTEMPT_TTFT_TIMEOUT` plus backoff.

### 6.5 After the commit point

No failover. On an upstream error or idle timeout mid-stream, the gateway:
1. writes one terminal event,
   `data: {"error":{"message":"…","type":"upstream_error","code":"stream_interrupted","request_id":"…"}}`,
2. **does not** send `data: [DONE]`, and closes the stream. The official OpenAI SDKs raise
   an exception on an `error` payload, and clients that only watch for `[DONE]` can tell
   the stream was truncated because it never arrives.
3. records a breaker failure, and settles input plus the output received so far
   (estimated if the provider did not report it).

## 7. Rate limiting

Each key has two token buckets in Redis: `rl:{<key_id>}:rpm` and `rl:{<key_id>}:tpm`. The
hash tag keeps both in the same Redis Cluster slot. Each bucket is a hash
`{tokens, ts_ms}`. Capacity = the per-minute limit, refill = limit/60 per second. A
`null` limit means that bucket is skipped.

**Admission:** one Lua script, atomic across both buckets:

```lua
-- KEYS: rpm, tpm   ARGV: rpm_limit, tpm_limit, tok_cost
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)       -- server clock, not the gateway's
-- refill both: tokens = min(cap, tokens + (now - ts) * rate)
-- if rpm >= 1 and tpm >= tok_cost: deduct both, PEXPIRE 120000, return {1, 0}
-- else: return {0, wait_ms}  where wait_ms = max(time for rpm to reach 1, time for tpm to reach tok_cost)
-- neither bucket is deducted on reject
```

- `tok_cost = est_in + max_out`, so a request is admitted against its worst case.
- **Adjust** (a second small script, after settle): add `admitted − actual` back to the
  TPM bucket, capped at capacity. If actual > admitted the bucket can go negative. That
  debt delays later requests, so TPM holds on average.
- If the request is rejected later with 402, its tokens are refunded but it still counts
  as a request.
- 429 sets `Retry-After = ceil(wait_ms / 1000)`, minimum 1. If `tok_cost > tpm_limit`
  the request can never be admitted, so it gets 429 `tokens_exceed_limit` with no
  `Retry-After`.
- **Redis unavailable → fail open**, with the `ratelimit_errors_total` metric and a
  warning log. Budgets are in Postgres, so money stays protected; only rate fairness is
  lost while Redis is down.

## 8. Budgets and the ledger

### 8.1 Money

Integer **nano-USD** (`*_nusd`, 1e-9 USD) in `bigint`. int64 holds about 9.2e9 USD.
Prices are integer micro-USD per 1M tokens, and

```
cost_nusd(in, out, price) = ceil((in · price.in_micro_per_m + out · price.out_micro_per_m) / 1000)
```

This is integer math with rounding of at most 1 nano-USD per request. The gateway,
reconciler, and `reconcile_check` all use this one function, so comparisons are exact.

### 8.2 Schema

```sql
CREATE TABLE api_keys (
  id          uuid PRIMARY KEY,
  name        text NOT NULL,
  key_prefix  text NOT NULL,
  key_hash    bytea NOT NULL UNIQUE,          -- sha256(key)
  rpm_limit   integer,                        -- NULL = unlimited
  tpm_limit   integer,
  created_at  timestamptz NOT NULL DEFAULT now(),
  revoked_at  timestamptz
);

-- Materialized per-key totals. Must always equal what the ledger derives (checked by reconcile_check).
CREATE TABLE key_accounts (
  key_id      uuid PRIMARY KEY REFERENCES api_keys(id),
  budget_nusd bigint,                         -- NULL = unlimited (a spend cap, not a balance)
  spent_nusd  bigint NOT NULL DEFAULT 0,      -- Σ settle
  held_nusd   bigint NOT NULL DEFAULT 0,      -- Σ reserve of requests with no release
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TYPE ledger_kind AS ENUM ('reserve', 'settle', 'release');

CREATE TABLE ledger_entries (
  id                bigserial PRIMARY KEY,
  request_id        uuid NOT NULL,
  key_id            uuid NOT NULL REFERENCES api_keys(id),
  kind              ledger_kind NOT NULL,
  amount_nusd       bigint NOT NULL CHECK (amount_nusd >= 0),
  alias             text,
  provider          text,                     -- settle
  upstream_model    text,                     -- settle
  prompt_tokens     integer,                  -- settle
  completion_tokens integer,                  -- settle
  usage_estimated   boolean,                  -- settle
  source            text NOT NULL,            -- 'gateway' | 'reconciler'
  reason            text,                     -- release: completed | failed | rejected | orphan_no_call | orphan_unknown_usage
  meta              jsonb,                    -- reserve: candidate targets + price snapshot
  created_at        timestamptz NOT NULL DEFAULT now(),
  UNIQUE (request_id, kind)                   -- the idempotency key
);
CREATE INDEX ON ledger_entries (key_id, created_at);
CREATE INDEX ON ledger_entries (created_at) WHERE kind = 'reserve';
-- Append-only: a trigger raises on UPDATE/DELETE, and the app role has no UPDATE/DELETE grant.
```

### 8.3 Estimating the reservation

- `est_in = ceil(chars(messages, tools) / 4 · EST_SAFETY_FACTOR)` (default 1.25). Each
  image part counts as `EST_IMAGE_TOKENS` instead of its base64 length. The
  `TokenEstimator` interface lets a real tokenizer replace this later.
- `max_out = min(client max_tokens or alias default_max_output, target max_output)`.
- `R = max over candidates of cost_nusd(est_in, max_out, price)`. Reserving the maximum
  means failover to a more expensive candidate never needs a second reservation. The cost
  is holding more budget than needed while the request runs.

### 8.4 Reserve → settle → release

Every request that passes rate limiting writes a `reserve` entry, **including keys with
no budget**, because orphaned requests can only be found through open reservations.

**Reserve** is one statement and one round trip:

```sql
WITH acct AS (
  UPDATE key_accounts
     SET held_nusd = held_nusd + $R, updated_at = now()
   WHERE key_id = $key
     AND (budget_nusd IS NULL OR spent_nusd + held_nusd + $R <= budget_nusd)
  RETURNING key_id
)
INSERT INTO ledger_entries (request_id, key_id, kind, amount_nusd, alias, meta, source)
SELECT $req, key_id, 'reserve', $R, $alias, $meta, 'gateway' FROM acct;
-- 0 rows inserted → 402
```

The `UPDATE` takes the row lock. Under READ COMMITTED, Postgres re-checks the `WHERE`
against the latest row version after waiting for the lock. Concurrent reservations
therefore serialize per key, and the sum of reservations can never pass the budget.
The cost: requests for the same key queue on one row lock for the length of a short
transaction. Throughput for a single hot key is limited by that, and Phase 7 measures it.

**Settle** (gateway or reconciler, one transaction):

```sql
BEGIN;
-- 1. Close token. Every closing path inserts a release; UNIQUE(request_id, kind) lets only one win.
INSERT INTO ledger_entries (request_id, key_id, kind, amount_nusd, reason, source)
SELECT request_id, key_id, 'release', GREATEST(amount_nusd - $A, 0), 'completed', $src
  FROM ledger_entries WHERE request_id = $req AND kind = 'reserve'
ON CONFLICT (request_id, kind) DO NOTHING
RETURNING key_id, amount_nusd;                 -- 0 rows → already closed → ROLLBACK, return no-op
-- 2. The charge.
INSERT INTO ledger_entries (request_id, key_id, kind, amount_nusd, provider, upstream_model,
                            prompt_tokens, completion_tokens, usage_estimated, source)
VALUES (...);
-- 3. Materialized totals (R read from the reserve row, never trusted from memory).
UPDATE key_accounts SET held_nusd = held_nusd - $R, spent_nusd = spent_nusd + $A WHERE key_id = $key;
COMMIT;
```

**Release-only** (call failed, rejected, or never happened): the same close token with
amount `R`, and `held -= R`. No `settle` row.

Properties:
- **Exactly one close per request.** Every closing path inserts `release`, and
  `UNIQUE(request_id, kind)` lets only one succeed. A repeated settle, a gateway settle
  racing the reconciler, or settle racing release: the loser is a no-op.
- **Derived balances:** `spent = Σ settle`, `held = Σ reserve where no release exists`,
  `available = budget − spent − held`. `key_accounts` is a cache of these, updated in the
  same transaction and verified by `reconcile_check`.
- **Overage:** if the actual cost `A > R` (the input estimate was too low, or a provider
  ignored the max-tokens limit), we still settle the full `A`. The release amount is 0,
  and the balance may go below the budget. The next reservation then fails with 402.
  Overspend is bounded by the estimation error of requests that were in flight together.

## 9. Reconciler

The reconciler runs as an in-process loop (`RECONCILE_INTERVAL`, default 30s). It is
guarded by `pg_try_advisory_lock`, so only one gateway instance runs it at a time. It is
also available as a one-shot CLI and as `POST /admin/reconcile`.

```sql
SELECT r.request_id, r.key_id, r.amount_nusd, r.meta FROM ledger_entries r
 WHERE r.kind = 'reserve' AND r.created_at < now() - $T
   AND NOT EXISTS (SELECT 1 FROM ledger_entries x WHERE x.request_id = r.request_id AND x.kind = 'release')
 ORDER BY r.created_at LIMIT 100;
```

`T = REQUEST_TIMEOUT + RECONCILE_GRACE`. The gateway enforces `REQUEST_TIMEOUT` as a
hard deadline on every request, so a reservation older than `T` cannot belong to a live
request. Postgres `now()` is the only clock involved. If the timeout assumption were
ever violated, the close token would still prevent a double charge.

For each orphan, it asks the `UsageSource` for that request ID:

| Lookup result | Action |
|---|---|
| Mock: billable record found | `settle` using the record's provider, model, and tokens, priced with the snapshot in the reserve's `meta`; `source='reconciler'` |
| Mock: no billable record (call never happened, or failed before the first token) | `release(R)`, reason `orphan_no_call` |
| Real provider (no per-request usage API) | `release(R)`, reason `orphan_unknown_usage`; metric + log. **This is an undercharge, by design** |
| Lookup error (e.g. mock unreachable) | skip; retry on the next pass |

**Change from what I proposed earlier:** I had suggested a separate `usage_captured`
entry written before settling. Because settle runs right after usage is captured, that
entry would be a second write with the same crash window, so it adds nothing. It would
only help if we checkpointed usage *during* long streams. That is a possible later
addition, not part of v1.

## 10. Failure semantics

"Received output" means output tokens the gateway actually read from upstream.

| Situation | Upstream | Client sees | Charged |
|---|---|---|---|
| Bad auth / unknown model / invalid body | not called | 401 / 404 / 400 | nothing, no ledger rows |
| Rate limited | not called | 429 + `Retry-After` | nothing, no ledger rows |
| Budget insufficient | not called | 402 | nothing, no ledger rows |
| Postgres down at reserve | not called | 503 | nothing (fail closed) |
| Redis down | called | normal | normal (rate limits fail open) |
| Provider 400-class (bad request) | called once | 400 passthrough | release R, 0 |
| All attempts fail pre-commit | called ≤ `MAX_ATTEMPTS` | 502 / 503 / 504 | release R, 0 |
| Failover then success | several | normal response | only the attempt that succeeded |
| Client disconnects pre-commit | cancelled via ctx | — | release R, 0 |
| Client disconnects mid-stream | cancelled via ctx | — | input + received output, `estimated` unless usage was reported |
| Upstream error mid-stream | — | SSE error event, no `[DONE]` | input + received output, usually `estimated` |
| Non-stream upstream fails mid-generation | fail over | normal response from next attempt | only the successful attempt |
| `REQUEST_TIMEOUT` hit | cancelled | as pre- or post-commit error | as the rows above |
| Postgres down at settle | completed | normal response | settle retried until the deadline, then the reconciler handles it |
| Gateway crash before upstream call | not called | connection drop | reconciler: release R |
| Gateway crash mid-stream or before settle | partial / complete | connection drop or full response | reconciler: mock → recorded usage; real → release (undercharge) |
| Actual cost > reservation | — | normal | full actual cost; balance may go below budget |
| Graceful shutdown (SIGTERM) | in-flight allowed to finish within `SHUTDOWN_DRAIN_TIMEOUT`, then cancelled | finished, or SSE error event | as completed or as mid-stream error |

Client disconnect is detected through `r.Context()`, which is cancelled when the
connection closes. The upstream request uses that context, so it is cancelled too.
Settlement then runs under `context.WithoutCancel(ctx)` with its own timeout, so the
cancellation that ended the request does not also abort its accounting.

## 11. Mock provider

A separate service (`cmd/mockprovider`), speaking OpenAI format (Anthropic format is
added in Phase 2 for adapter tests).

- **Multiple virtual providers on one service:** `/p/{name}/v1/chat/completions`. Each
  name has its own behavior, so failover tests run against one container.
- **Behavior:** `ttft_ms`, `tokens_per_sec`, `output_tokens`, `error_rate` +
  `error_status`, `fail_after_tokens` (mid-stream failure), `hang` (accept, never
  respond). Set through env defaults, `PUT /_mock/providers/{name}` at runtime, and
  per-request `X-Mock-*` headers.
- **Deterministic:** seeded RNG per (provider, request ID). The output text is a function
  of the seed. Prompt tokens are counted with a rule that differs on purpose from the
  gateway's estimator (word-based), so tests can tell reported usage from estimated.
- **Records usage:** keyed by the gateway's `X-Request-ID` and `X-Attempt` headers. Each
  record holds `{provider, model, prompt_tokens, completion_tokens, billable, outcome}`.
  An attempt is **billable iff it completed or flushed at least one content token**. A
  completed response reports usage, so the gateway charges its input even with zero
  output. Its completion tokens are the tokens flushed. A completed attempt is
  recorded *before* its final bytes (`[DONE]` / the JSON body) are written, so a caller
  that has read the whole response can always find the record. Queried with
  `GET /_mock/usage?request_id=` and exported with `GET /_mock/usage`.
- Records are kept in memory and optionally appended to JSONL, so they survive a mock
  restart. Chaos tests kill only the gateway.

## 12. Guarantees and non-guarantees

**Guaranteed**
- No request is ever charged twice (`UNIQUE(request_id, kind)` close token).
- Concurrent requests on one key cannot reserve more than its budget (row-locked
  conditional update).
- No failover after the first byte reaches the client. The client never receives output
  from two providers in one response.
- Every reservation is eventually closed by the gateway or the reconciler. No reservation
  stays open longer than `T` + one reconciler interval while Postgres is up.
- Against the mock, the settled cost per request equals the mock's recorded usage, except
  for requests marked `estimated`.

**Not guaranteed**
- Charges for disconnected or interrupted streams are estimates (flagged). The provider
  may have generated tokens that were still in flight and never reached us.
- With real providers, requests orphaned by a crash are released, which **undercharges**
  the client.
- Attempts abandoned during failover may be billed by the provider but are not charged to
  the client. That cost falls on the gateway operator.
- Spend can exceed the budget by the estimation error of in-flight requests.
- Rate limits are not enforced while Redis is down.
- Revocation takes effect within `AUTH_CACHE_TTL`, not immediately.
- Anthropic cache tokens are priced as normal input tokens.
- Estimated counts come from a 4-bytes-per-token heuristic, not the provider's tokenizer.
  How far off it is depends on the text. It is not measured here, because the mock's
  tokenizer is synthetic.
- Thinking content from Anthropic models is not forwarded, and OpenAI-format clients
  cannot send thinking blocks back in later turns.
- There is no client idempotency: a client retry is a new request and a new charge.

**What the Phase 7 ledger chaos test can and can't show:** requests resolved by the
reconciler are compared with the mock's records, and they were settled *from* those
records, so they agree by construction. The test is meaningful for (a) requests settled
by the gateway, compared against the mock, (b) no request being closed twice, and
(c) no reservation left open after the drain. The report will show these separately.

## 13. Repository layout (preview of Phase 1)

```
cmd/gateway/          main: public + admin listeners, reconciler loop
cmd/mockprovider/     mock provider service
cmd/reconcile/        one-shot reconciler CLI
internal/config/      env + routes file
internal/httpapi/     handlers, OpenAI wire types, SSE writer, error mapping
internal/auth/        key hashing, lookup, cache
internal/provider/    interface + internal types; openai/, anthropic/ adapters
internal/router/      policies, EWMA, breaker, attempt loop
internal/ratelimit/   Redis Lua buckets
internal/ledger/      reserve/settle/release, reconciler, pricing math
internal/estimate/    TokenEstimator
internal/mock/        mock provider implementation
internal/obs/         slog setup, request-ID middleware, metrics
migrations/           SQL migrations
scripts/              loadtest/, failover_test/, ledger_chaos/, reconcile_check/
bench/results/        committed benchmark output
```

## 14. Decisions to confirm in review

| # | Decision | Differs from the plan? | Reason |
|---|---|---|---|
| D1 | Budgets and reservations live in **Postgres** (row-locked conditional update), not Redis | Yes: the plan put budgets in Redis | With one transactional store there is no window where Redis and Postgres disagree. The cost is a per-key row lock. |
| D2 | Resolve candidates **before** reserving, and reserve the max cost across them | Order changed: the plan had reserve → route | The reservation depends on candidate prices, and failover must never need a second reservation. |
| D3 | Latency and breaker state live **in-process**, not in Redis | Yes: the plan put health state in Redis | They are read and written on every request, and Compose runs one gateway. Sharing them across instances is a later addition if we scale out. |
| D4 | **Always stream upstream**, and build non-stream responses in the gateway | New | One code path, TTFT on every request, partial usage on interruption. |
| D5 | After an error event mid-stream, **omit `[DONE]`** | Changes what I said earlier | A missing `[DONE]` is the truncation signal for simple clients. |
| D6 | Dropped the separate `usage_captured` entry | Changes recommendation 2 | It has the same crash window as settle (§9). |
| D7 | Settle **after** the last byte is flushed | New | Keeps a Postgres round trip off client latency; the crash window is covered by the reconciler. |
| D8 | Money in integer nano-USD; prices in micro-USD per 1M tokens | New | Exact integer comparisons in `reconcile_check`. |

Decided during Phase 2:

| # | Decision | Reason |
|---|---|---|
| D9 | The Anthropic adapter calls the HTTP API directly, not through the Go SDK | The SDK's built-in retries would hide 429/5xx from failover and the breakers. The gateway also needs any base URL (the mock) and raw stream timing. Cost: we maintain the wire types ourselves; the mock enforces the 2026-10-21 rules to catch drift. |
| D10 | Anthropic targets reject non-default `temperature`/`top_p` and unmodeled fields instead of dropping them | API version 2026-10-21 rejects sampling parameters on every model. Silently dropping a client's `temperature: 0.2` would change its output without telling it. Cost: clients that always send a non-default temperature (some SDK wrappers do) cannot use Anthropic targets until Phase 3, where `Supports` filters candidates and such requests route to OpenAI-format targets. |
| D11 | Upstream 401/402/403/404 are configuration errors that allow failover | The client never names the upstream model, so these are the gateway's credentials, account, or route table, not the client's request. |
| D12 | The streaming commit point is the first chunk with output, not the first chunk | An upstream that sends `message_start` and then fails can still be failed over. |
