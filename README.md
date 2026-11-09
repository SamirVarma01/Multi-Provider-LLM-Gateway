# LLM Gateway

An OpenAI-compatible gateway, written in Go, that sits between your applications and LLM
providers. Clients use the OpenAI Chat Completions API, streaming or not, with a gateway
API key. The gateway:

- routes each request to an OpenAI-format or Anthropic-format provider, by priority,
  price, or measured latency;
- fails over to another provider when one errors or hangs, until the first byte reaches
  the client;
- enforces per-key rate limits (requests and tokens per minute) in Redis;
- enforces per-key budgets with a reserve-then-settle ledger in Postgres, which a
  reconciler keeps exact through crashes.

A mock provider that speaks both formats, with controllable latency and failures, runs
the tests and benchmarks. It also lets you try everything here without a provider
account.

The full design is in [docs/design.md](docs/design.md), including every decision and
its reason (§14) and the benchmark results (§15).

## Quickstart

You need Docker with Compose v2, plus Go 1.26 for the tests and benchmark scripts.

```sh
make up        # Postgres, Redis, the mock provider and the gateway, on 127.0.0.1
```

Create an API key with a $1 budget and a limit of 60 requests a minute. The admin API is
on its own port, and the key is shown only once:

```sh
curl -s -X POST http://127.0.0.1:8081/admin/keys \
  -H "Authorization: Bearer dev-admin-token" \
  -d '{"name":"demo","budget_usd":1,"rpm_limit":60}'
# {"id":"01a1c0dc-…","key":"gw_…","name":"demo","prefix":"gw_…","rpm_limit":60,…}

export GW_KEY=gw_…   # the "key" from the response
```

Send a request. The route table that Compose uses ([config/routes.compose.yaml](config/routes.compose.yaml))
defines three aliases on the mock: `mock-small`, `mock-large` and `mock-claude`.

```sh
curl -s -i http://127.0.0.1:8080/v1/chat/completions \
  -H "Authorization: Bearer $GW_KEY" -H "Content-Type: application/json" \
  -d '{"model":"mock-small","messages":[{"role":"user","content":"hello"}],"max_tokens":16}'
```

```text
HTTP/1.1 200 OK
X-Gateway-Attempts: 1
X-Gateway-Provider: mock-a
X-Request-Id: 01a1c0dc-0c64-75f0-bb9a-0ff61caaab61

{"id":"chatcmpl-…","object":"chat.completion","model":"mock-small","choices":[…],
 "usage":{"prompt_tokens":7,"completion_tokens":16,"total_tokens":23}}
```

Any OpenAI SDK works. This streams from `mock-claude`, which speaks Anthropic's
Messages API; the gateway translates it:

```python
import os
from openai import OpenAI

client = OpenAI(base_url="http://127.0.0.1:8080/v1", api_key=os.environ["GW_KEY"])
stream = client.chat.completions.create(
    model="mock-claude",
    messages=[{"role": "user", "content": "hello"}],
    max_tokens=16,
    stream=True,
    stream_options={"include_usage": True},
)
for chunk in stream:
    if chunk.choices:
        print(chunk.choices[0].delta.content or "", end="")
    if chunk.usage:
        print("\n", chunk.usage)
```

See what the key has spent, from the ledger:

```sh
curl -s http://127.0.0.1:8081/admin/keys/<id>/usage -H "Authorization: Bearer dev-admin-token"
# {"budget_usd":1,"total_spent_usd":0.00027165,"held_usd":0,"available_usd":0.99972835,
#  "requests":2,"groups":[{"alias":"mock-claude",…},{"alias":"mock-small",…}]}
```

### Watch it fail over

`mock-small` prefers `mock-a`, with `mock-b` as the fallback. Make `mock-a` fail every
request through the mock's control API, then send a few requests:

```sh
curl -s -X PUT http://127.0.0.1:9000/_mock/providers/mock-a -d '{"error_rate":1,"error_status":500}'

for i in 1 2 3 4 5 6 7; do
  curl -s -o /dev/null -D - http://127.0.0.1:8080/v1/chat/completions \
    -H "Authorization: Bearer $GW_KEY" -H "Content-Type: application/json" \
    -d '{"model":"mock-small","messages":[{"role":"user","content":"hello"}],"max_tokens":16}' |
    grep -i 'x-gateway' | tr -d '\r' | tr '\n' ' '; echo
done
```

```text
X-Gateway-Attempts: 2 X-Gateway-Provider: mock-b     (five times)
X-Gateway-Attempts: 1 X-Gateway-Provider: mock-b
X-Gateway-Attempts: 1 X-Gateway-Provider: mock-b
```

Every request succeeds. The first five try `mock-a`, fail, and are served by `mock-b`.
After five failures in a row, `mock-a`'s circuit breaker opens, and requests go straight
to `mock-b`. `PUT` with `{}` puts `mock-a` back to its defaults, and once the breaker's
probes succeed, traffic returns to it.

`make down` stops the stack. `make smoke` runs a scripted version of these steps.

### Real providers

Compose bakes the mock's route table into the image. For real providers, copy
[config/routes.example.yaml](config/routes.example.yaml), set its models and prices, and
run the gateway with it, here against the Compose stack's Postgres and Redis:

```sh
docker compose up -d --wait postgres redis
DATABASE_URL='postgres://gateway:gateway@127.0.0.1:5432/gateway?sslmode=disable' \
REDIS_URL=redis://127.0.0.1:6379/0 ROUTES_FILE=my-routes.yaml ADMIN_TOKEN=change-me \
ANTHROPIC_API_KEY=… OPENAI_API_KEY=… \
go run ./cmd/gateway
```

Each provider names the environment variable that holds its API key (`api_key_env`).

## Architecture

```mermaid
flowchart LR
  C[Client<br/>OpenAI SDK] -->|/v1/*| G[Gateway<br/>public :8080]
  A[Operator] -->|/admin/*| GA[Gateway<br/>admin :8081]
  G --> R[(Redis<br/>rate-limit buckets)]
  G --> P[(Postgres<br/>keys, accounts, ledger)]
  GA --> P
  G -->|always streams| U1[OpenAI-format providers]
  G -->|always streams| U2[Anthropic-format providers]
  REC[Reconciler<br/>in-process loop + CLI] --> P
  G -.tests, benchmarks.-> M[Mock provider]
  REC -.usage lookup.-> M
```

| Where | What | Why there |
|---|---|---|
| Postgres | API key hashes, each key's account (budget, spent, held), the ledger | Money must be transactional and durable. |
| Redis | Rate-limit token buckets | On the hot path, approximate, and safe to lose. |
| Gateway memory | Route table, latency averages, circuit breakers, auth cache | Read on every request, so no network hop. |

The gateway also serves Prometheus metrics on a third port (`:9090`), kept separate
because they include per-key spend. Compose doesn't publish it.

## Request lifecycle

```mermaid
sequenceDiagram
  participant C as Client
  participant G as Gateway
  participant RL as Redis
  participant PG as Postgres
  participant UP as Provider
  C->>G: POST /v1/chat/completions
  G->>G: authenticate, resolve alias to candidates, estimate cost
  G->>RL: take 1 request + estimated tokens (one Lua call)
  G->>PG: reserve the worst-case cost (one UPDATE)
  loop until the first output, or attempts run out
    G->>UP: streaming request
    UP-->>G: first output
  end
  Note over G,C: commit point: first byte to the client
  UP-->>G: chunks
  G-->>C: chunks (stream) or one JSON body
  G->>RL: return unused tokens to the bucket
  G->>PG: settle the actual cost, release the rest
```

1. **Authenticate:** the bearer key is hashed with SHA-256, looked up in Postgres, and
   cached for 10 s.
2. **Route:** the alias resolves to candidate targets, ordered by the alias's policy
   (`priority`, `cheapest` or `fastest`; `X-Route-Policy` overrides it per request).
3. **Rate limit:** one Lua script takes a request and the worst-case token count from
   both buckets, or neither. Over the limit, the client gets 429 with `Retry-After`.
4. **Reserve:** the worst-case cost across all candidates is held against the key's
   budget. Not enough budget, and the client gets 402.
5. **Attempt:** the gateway always streams from the provider. Until the first output
   reaches the client, a failed, slow or hung attempt moves on to the next candidate.
6. **Respond:** the gateway forwards a stream, or builds the non-streaming JSON body
   itself.
7. **Close:** after the response, a background task settles the actual cost and
   releases the rest of the reservation, so Postgres stays off the client's critical
   path. If the gateway crashes first, the reconciler closes the reservation later.

## Guarantees and non-guarantees

**Guaranteed**
- No request is charged twice. Each close carries a unique `(request_id, kind)` token.
- Concurrent requests on one key cannot reserve more than its budget.
- A response never mixes output from two providers. There is no failover after the
  first byte.
- Every reservation is closed, by the gateway or by the reconciler, within
  `REQUEST_TIMEOUT` + `RECONCILE_GRACE` + one reconciler interval, while Postgres and
  the providers' usage sources are up.
- Against the mock, each request's charge equals the mock's recorded usage, except
  requests marked `estimated`.

**Not guaranteed**
- When a provider omits usage, or a stream is cut off, the charge is an estimate (about
  4 bytes a token), flagged with `"estimated": true` in the response's usage. Tokens
  still in flight when a stream broke are never counted.
- A real provider can't be asked afterwards what a crash-orphaned request used, so the
  reconciler releases it, which undercharges the client.
- Attempts abandoned during failover may be billed by the provider but aren't charged
  to the client. The operator pays for them.
- Spend can exceed a budget by the estimation error of requests in flight.
- Rate limits are not enforced while Redis is down: the gateway fails open, while
  budgets keep protecting money.
- Revoking a key takes effect within the auth cache's 10 s.
- There is no client idempotency: a retry is a new request and a new charge.

The full list is in [design §12](docs/design.md#12-guarantees-and-non-guarantees).

## Measured results

Every number below comes from a report in [bench/results/](bench/results/), written by
the script it names. All five reports come from one run at commit `5b8bab0`, on an Apple
M2 laptop with Docker Desktop (8 CPUs, 3.8 GiB). The load generator, gateway, mock,
Postgres and Redis all share that machine, so the numbers compare the gateway with
calling the mock directly. They are not a server's capacity. [Design §15](docs/design.md#15-benchmarks)
explains them in full.

### Gateway overhead

From [loadtest.md](bench/results/loadtest.md) (`make bench-load`). Each rate runs twice,
straight to the mock and through the gateway, through one key. The mock answers after
50 ms, then sends 32 tokens at 100 tokens/s. Overhead is the gateway's percentile minus
the direct run's, so at p99 it is noisy and can come out negative. There were no errors
in 256,657 requests.

| Rate (req/s) | Streaming, time to first token: p50 / p95 / p99 (ms) | Non-streaming, full response: p50 / p95 / p99 (ms) |
|---:|---:|---:|
| 50 | 1.65 / 2.54 / 5.66 | 0.70 / 0.44 / 3.53 |
| 200 | 0.49 / 0.57 / 1.37 | 0.39 / −0.21 / −0.71 |
| 500 | 0.56 / 0.72 / 27.80 | 0.02 / 0.82 / 11.17 |
| 1000 | 0.80 / 52.38 / 186.14 | 0.64 / 34.17 / 98.16 |

### Where the tail at 1000 req/s comes from

The same 1000 req/s, run three ways: through one key; through one key with Postgres not
waiting for the disk ([loadtest-synchronous-commit-off.md](bench/results/loadtest-synchronous-commit-off.md));
and spread over 16 keys.

| Overhead, p95 / p99 (ms) | One key | One key, commits don't wait for the disk | 16 keys |
|---|---:|---:|---:|
| Streaming, time to first token | 52.38 / 186.14 | 5.03 / 38.29 | 3.47 / 26.75 |
| Non-streaming, full response | 34.17 / 98.16 | 0.10 / −4.02 | 5.84 / 11.98 |

Each reservation and settlement holds its key's account row lock until its commit is on
disk. So under heavy load on one key, a slow disk flush delays every request queued on
that row. The gateway keeps durable commits anyway: without them, a Postgres crash
could lose reservations for requests already sent to a provider. Spreading load over
several keys avoids the queue.

### Failover

From [failover.md](bench/results/failover.md) (`make bench-failover`): 50 req/s, with the
primary provider faulty for 30 s.

| Fault on the primary | Breaker opened after | Client errors during the fault | Breaker closed after the fix |
|---|---:|---:|---:|
| HTTP 500 before any output | 0.20 s | 0 of 1500 | 5.65 s |
| Accepts requests and never answers | 10.10 s | 0 of 1500 | 5.55 s |
| Cuts every stream off after 4 tokens | 0.30 s | 8 of 1500, all streaming | 5.95 s |

A hang is only noticed when the 10 s first-output timeout fires, so requests sent in
those 10 s waited for it before the backup served them. A stream cut off after output had
reached the client can't fail over; non-streaming requests had received nothing, so
they were retried. When the breaker closes depends on where its back-off stands
(5 s, doubling up to 60 s).

### The ledger through crashes

From [ledger_chaos.md](bench/results/ledger_chaos.md) (`make bench-chaos`). The gateway was
killed with SIGKILL 20 times, every 3–8 s, under 40 req/s of mixed traffic: both
provider formats, streaming and non-streaming, and a primary failing 10% of attempts.
After draining and reconciling:

| Ledger discrepancies vs. the mock's usage records | Double charges | Orphaned reservations |
|---:|---:|---:|
| **0** | **0** | **0** |

Of 5662 requests sent, 4554 were reserved. The gateway charged 3827, each matching the
mock's record. The reconciler charged 646 from the mock's records and released 81 that
never reached a provider.

## Design decisions

The full decision log, D1–D29, is in [design §14](docs/design.md#14-decisions-to-confirm-in-review).

**Reserve, then settle.** Before calling a provider, the gateway reserves the request's
worst-case cost against the key's budget: the highest cost among its candidates, for
its estimated input times 1.25 plus its maximum output. That is one conditional `UPDATE`
on the key's account row, so concurrent requests serialize on the row lock and can never
reserve past the budget. When the request ends, one statement charges the actual cost
and releases the rest. The ledger's unique `(request_id, kind)` index makes closes
idempotent, so the gateway and the reconciler can never both charge a request. The
reconciler finds reservations still open past the request timeout and settles them
from the provider's usage records, or releases them if the provider has none. The cost
is the per-key queue described under the results.

**Failover only before the first byte.** The gateway always asks providers for a
stream, even when the client didn't. Until output first reaches the client, any failure
is invisible: the gateway tries the next candidate, skipping providers whose circuit
breaker is open. A non-streaming client receives nothing until the response is
complete, so it can fail over even partway through generation. Once output has reached
the client, the response is committed to one provider. A failure becomes an SSE error event, and `[DONE]` is left off so that
simple clients see the truncation. The alternative, buffering or splicing output from
two providers, either adds latency to every stream or produces a response no single
model wrote.

**A Lua rate limiter.** Each key has two token buckets in Redis, requests per minute and
tokens per minute. One Lua script refills and checks both, using Redis's clock rather
than the gateway's, and takes from both or neither, so it is atomic without
transactions or locks. A request is admitted against its worst-case token count, and
the difference is refunded when it ends; a request that used more leaves the bucket in
debt. If Redis is down, the gateway admits requests anyway: rate limits protect
fairness, and the budgets in Postgres still protect money.

## Configuration

The gateway reads its configuration from environment variables. Durations use Go
syntax (`10s`, `5m`).

| Variable | Default | |
|---|---|---|
| `DATABASE_URL` | required | Postgres. Migrations run at startup unless `MIGRATE_ON_START=false`. |
| `REDIS_URL` | required | Redis, for rate limits. |
| `ROUTES_FILE` | required | The route table: providers, aliases, prices. |
| `ADMIN_TOKEN` | empty | Bearer token for the admin API. Empty turns the admin listener off. |
| `GATEWAY_PUBLIC_ADDR` / `GATEWAY_ADMIN_ADDR` / `METRICS_ADDR` | `:8080` / `:8081` / `:9090` | Listeners. Empty `METRICS_ADDR` turns metrics off. |
| `REQUEST_TIMEOUT` | `10m` | Whole-request deadline. |
| `ATTEMPT_TTFT_TIMEOUT` | `10s` | How long an attempt may take to produce its first output before failover. |
| `STREAM_IDLE_TIMEOUT` | `30s` | Longest gap between chunks after the first output. |
| `MAX_ATTEMPTS` | `3` | Upstream attempts per request, across all candidates. |
| `EST_SAFETY_FACTOR` | `1.25` | Margin on the input estimate when reserving. |
| `AUTH_CACHE_TTL` | `10s` | How long a key lookup is cached, and so how long revocation takes. |
| `RECONCILE_INTERVAL` / `RECONCILE_GRACE` | `30s` / `30s` | Reconciler frequency, and how far past `REQUEST_TIMEOUT` a reservation must be to count as orphaned. |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`. JSON logs go to stdout. |

The circuit breaker, backoff, latency tracking and timeouts for Redis and Postgres
have settings too. [internal/config/config.go](internal/config/config.go) lists them
all, with their defaults.

A route table lists providers and aliases. Each alias has candidate targets with
prices in USD per million tokens:

```yaml
providers:
  - { name: anthropic, format: anthropic, base_url: https://api.anthropic.com/v1, api_key_env: ANTHROPIC_API_KEY }
models:
  - alias: claude-haiku
    policy: priority          # default; or cheapest, or fastest
    candidates:
      - { provider: anthropic, model: claude-haiku-5-5, input_per_mtok: 0.10, output_per_mtok: 0.50, max_output: 128000 }
```

## Development

| Command | Does |
|---|---|
| `make test` | Unit and integration tests, with the race detector. Integration tests start throwaway Postgres and Redis containers, and are skipped without Docker. |
| `make test-integration` | The same, but fails instead of skipping when Docker is unavailable. |
| `make lint` | golangci-lint v2 ([.golangci.yml](.golangci.yml)). |
| `make check` | `go vet`, lint and the integration tests: what to run before a commit. |
| `make test-live` | Calls real providers, and costs money. Runs only with `ANTHROPIC_API_KEY`, or `OPENAI_API_KEY` and `OPENAI_LIVE_MODEL`, set. |
| `make bench` | All the benchmarks, each on a fresh Compose project of its own; reports go to `bench/results/`. About 25 minutes. |
| `make reconcile` | One reconciler pass, for when the gateway is down. |
| `make reconcile-check` | Audits the Compose stack's ledger against the mock's records. |

Every test runs against the mock provider; nothing needs a provider account. Tests
check for leaked goroutines with goleak.

```text
cmd/gateway, cmd/mockprovider, cmd/reconcile   the binaries
internal/httpapi    handlers, OpenAI wire types, SSE writer, error mapping
internal/router     routing policies, latency tracking, circuit breakers, the attempt loop
internal/provider   the provider interface, with openai/ and anthropic/ adapters
internal/ratelimit  the Redis Lua token buckets
internal/ledger     reserve, settle, release, the reconciler and the ledger audit
internal/mock       the mock provider
scripts/            benchmarks (loadtest, failover_test, ledger_chaos), bench.sh, smoke.sh
docs/design.md      the design, decisions, and benchmark write-up
```
