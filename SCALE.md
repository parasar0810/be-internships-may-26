# Scale Plan — 10 k RPS

## Current Baseline

Single Node.js process + `better-sqlite3` (synchronous, embedded, WAL mode).
SQLite sustains ~5–10 k writes/s on NVMe locally, but it cannot be shared
across multiple hosts and serialises all writes through one file lock.
This design is correct for a single-node prototype; the sections below describe
the path to multi-instance, high-throughput production operation.

---

## Data Model & Indexes

| Index | Query it serves |
|---|---|
| `idx_user_created ON signals(user_id, created_at)` | `GET /v1/signals?userId=…` — range scan then `ORDER BY created_at DESC LIMIT n` |
| `UNIQUE(idempotency_key)` | Atomic deduplication; also serves `SELECT … WHERE idempotency_key = ?` |

For Postgres at scale:
- Partition `signals` by `created_at` range (monthly).  Old partitions become
  append-only and can be detached and archived to S3/Glacier.
- If `payload` grows large: store it in a separate `signal_payloads(signal_id, body)`
  table with TOAST compression so the hot index pages stay compact.
- Add a partial index `WHERE idempotency_key IS NOT NULL` to keep the unique
  index small (many rows will have `NULL`).

---

## Idempotency Across Instances

**Current (single-node):** `INSERT OR IGNORE` + `SELECT` inside one SQLite
connection.  The `UNIQUE` constraint makes it atomic at the DB level — no
check-then-insert race even for concurrent requests on the same process.

**Multi-instance:** Move deduplication to a shared, durable store.

Option A — **Redis `SET NX EX`** (fast, ephemeral):
```
SET idem:{key} {serialised-response} NX EX 86400
```
First writer wins; every subsequent call reads the cached response.
TTL of 24 h covers all realistic retry windows.

Option B — **Postgres `INSERT … ON CONFLICT DO NOTHING RETURNING *`** (durable):
```sql
INSERT INTO signals (user_id, type, payload, idempotency_key, created_at)
VALUES ($1,$2,$3,$4,$5)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING *;
```
If `RETURNING` returns no rows, a follow-up `SELECT` fetches the existing one.
All within a single round-trip to a shared Postgres primary — cross-pod safe.

In both cases, **no in-memory check-then-insert** — the external store is the
single source of truth.

---

## Rate Limiting Across Instances

**Current (single-node):** An in-process `Map` is inherently atomic within one
Node.js event loop.  It will over-count or under-count across pods.

**Multi-instance — Redis sliding window (Lua script, one round-trip):**

```lua
-- KEYS[1]=userId, ARGV[1]=nowMs, ARGV[2]=windowMs, ARGV[3]=limit
local key   = 'rl:' .. KEYS[1]
local now   = tonumber(ARGV[1])
local win   = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])

redis.call('ZREMRANGEBYSCORE', key, 0, now - win)   -- drop old entries
local cnt = redis.call('ZCARD', key)

if cnt < limit then
  redis.call('ZADD', key, now, now .. ':' .. math.random())
  redis.call('PEXPIRE', key, win)
  return {1, limit - cnt - 1}    -- {allowed, remaining}
end
return {0, 0}
```

`EVAL` executes this atomically on the Redis primary.  All pods call the same
key — no per-pod drift.  The `PEXPIRE` auto-cleans keys, so no sweep loop is
needed.

Alternative (simpler, fixed-window): `INCR rl:{userId}:{floor(nowMs/winMs)}`
with `EXPIRE` set only on first increment.

---

## Observability

**Structured logs** (Fastify JSON logger, already in place):
- Ship to ELK / Datadog / CloudWatch Logs.
- Include `requestId`, `userId`, `idempotencyKey`, `durationMs` on every line.

**Prometheus metrics** (expose `/metrics` via `prom-client`):

| Metric | Type | Labels |
|---|---|---|
| `signals_created_total` | counter | `userId` |
| `rate_limit_hits_total` | counter | `userId` |
| `db_retries_total` | counter | `attempt` |
| `db_errors_total` | counter | `op` |
| `http_request_duration_ms` | histogram | `route`, `status` |

**Alerts:**
- p99 latency > 200 ms for 2 consecutive minutes
- Error rate > 1 % over any 1-minute window
- Rate-limit hit rate spikes > 10× baseline (potential abuse)
- Redis connection errors (fail-open, but alert)

**Distributed tracing:** Propagate `x-request-id`; emit OpenTelemetry spans to
Jaeger or Datadog APM.

---

## Failure Modes

| Failure | Handling |
|---|---|
| Transient `SQLITE_BUSY` / Postgres timeout | Exponential back-off + full jitter, max 3 attempts (50 ms base) |
| DB fully down | Return `503` with `Retry-After: 5` header after all retries exhausted |
| Duplicate insert on retry | `INSERT OR IGNORE` / `ON CONFLICT DO NOTHING` — idempotent by design; no extra row |
| Redis rate-limiter down | **Fail open** — allow the request, log a warning, alert on-call |
| Redis idempotency-cache miss after restart | Fall back to Postgres `ON CONFLICT` check as the source of truth |
| Memory growth (in-process Map) | Sweep timer every 60 s; disappears entirely once Redis is adopted |

---

## 10 k RPS Design Sketch

### Target
- 10 000 requests / second sustained, p99 < 50 ms end-to-end.
- Zero duplicate signals on idempotent retries across restarts and pods.

### Topology

```
                ┌───────────────────────────────────────┐
Clients ──────▶ │  CDN / WAF  (L3 rate limit, DDoS)    │
                └──────────────┬────────────────────────┘
                               │
                ┌──────────────▼────────────────────────┐
                │  Load Balancer  (ALB / nginx)          │
                └───┬──────────────┬────────────────────┘
                    │              │              … 8 pods
           ┌────────▼──┐  ┌───────▼────┐
           │ Node Pod  │  │ Node Pod   │   Fastify, stateless
           └────┬──────┘  └──────┬─────┘
                │                │
           ┌────▼────────────────▼──────────────────────┐
           │  Redis Cluster  (rate + idempotency cache)  │  r6g.large × 3
           └──────────────────────┬─────────────────────┘
                                  │
           ┌──────────────────────▼─────────────────────┐
           │  Postgres  (primary + 2 read replicas)      │  rds r6g.2xlarge
           │  Writes → primary   Reads → replicas        │
           └────────────────────────────────────────────┘
```

### Capacity Math

| Layer | Throughput | Sizing |
|---|---|---|
| Fastify pods (8 × c6g.large) | ~1 250 RPS each | 8 pods = 10 k RPS |
| Redis (r6g.large × 3) | > 100 k ops/s | Headroom × 10 |
| Postgres writes | 10 k inserts/s | r6g.2xlarge; use `COPY`-based micro-batching or BullMQ write queue to fan-in |
| Connection pool | 10 k / 8 pods = 1 250 req/pod; 10 ms avg DB time → ~13 concurrent connections/pod | Pool of 20/pod, 160 total |

### Write Queue (optional, sustained 10 k+)

Enqueue signals in **BullMQ + Redis** and flush to Postgres in micro-batches
(every 10 ms or 100 items).  Decouples HTTP latency from DB write latency.
Idempotency keys are still checked atomically in Redis before enqueue, so
no duplicate signals even on retry.

### Cost Ballpark (AWS us-east-1, ~$USD/month)

| Component | Spec | Est. Cost |
|---|---|---|
| ECS/EC2 Node pods (8) | c6g.large | $240 |
| RDS Postgres | r6g.2xlarge + 2 read replicas | $900 |
| ElastiCache Redis | r6g.large × 3 | $450 |
| ALB + data transfer | 10 k RPS | $70 |
| CloudWatch / logs | — | $40 |
| **Total** | | **≈ $1 700 / mo** |
