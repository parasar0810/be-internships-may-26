# Signals Challenge (Node.js + Fastify)

Build a minimal production-leaning service that can **handle load**, **rate limit**, and **avoid duplicates** via idempotency.

## Endpoints (to keep)
- `POST /v1/signals`
  - body: `{ "userId": "string", "type": "string", "payload": "string" }`
  - headers: `X-API-Key`, `Idempotency-Key` (optional)
  - behaviors:
    - **Rate limit** per `userId`: `RATE_LIMIT_PER_MIN` per minute (default 5).
    - **Idempotency**: same `Idempotency-Key` should not create duplicates.
- `GET /v1/signals?userId=...&limit=...`
- `GET /healthz`

## Your Tasks
1. **Implement a robust rate limiter** in `src/rateLimit.js`. ✅
2. **Make idempotency safe across scale** in `src/signals.js`. ✅
3. **Handle DB failure** gracefully with retry/backoff. ✅
4. **Think for 10k RPS.** Add a `SCALE.md`. ✅
5. **Finish the tests** in `tests/*.test.js`. ✅

## Deliverables
- Working service, passing tests, updated README, SCALE.md.
- Optional deploy link.
---

## Implementation Notes

### 1. Rate Limiter (`src/rateLimit.js`)
- **Fixed-window** counter per `userId` stored in a `Map`.
- Window expires after `RATE_LIMIT_PER_MIN` window (60 s); a new window
  starts on the first request after expiry.
- A background `setInterval` sweeps expired entries every 60 s to prevent
  unbounded memory growth.
- **Multi-instance path**: the in-process Map is documented with a drop-in
  Redis Lua sliding-window script that is atomically correct across pods.
  See `src/rateLimit.js` and `SCALE.md`.

### 2. Atomic Idempotency (`src/signals.js` + `src/db.js`)
- Replaced the check-then-insert pattern with `INSERT OR IGNORE` followed by
  `SELECT` (`upsertSignal` in `src/db.js`).
- Both concurrent requests racing on the same `Idempotency-Key` use the
  SQLite `UNIQUE` constraint as the single source of truth — only one row is
  ever written, and both callers receive the canonical response.
- No in-process lock is required; the DB-level constraint is sufficient.

### 3. DB Retry / Backoff (`src/signals.js`)
- `withRetry(fn)` wraps every DB call with up to **3 attempts**.
- Delay = `Math.random() * BASE * 2^attempt` (exponential back-off with
  full jitter, base 50 ms).
- Because `upsertSignal` uses `INSERT OR IGNORE`, retrying after a transient
  failure **never creates a duplicate row**.

### 4. SQLite WAL Mode (`src/db.js`)
- `PRAGMA journal_mode = WAL` allows concurrent readers during writes —
  significantly better throughput under the test harness.
- `PRAGMA busy_timeout = 3000` makes SQLite wait up to 3 s before throwing
  `SQLITE_BUSY`, giving the retry logic time to succeed on transient lock
  contention.

### 5. Tests (`tests/`)
| File | Tests |
|---|---|
| `idempotency.test.js` | Same key → same resource; **concurrent race** (10 parallel requests, 1 record); no key → new record each time |
| `rate-limit.test.js` | Basic 5-allow / 6th-429; per-user isolation; **concurrent burst** (10 parallel → ≤ 5 allowed); auth rejection (401); health endpoint (200); GET returns records |

All tests use an isolated `DATABASE_URL=:memory:` SQLite database so they
do not interfere with each other or with `./data/signals.db`.

## Running

```bash
# Install
npm install

# Start dev server
npm run dev

# Run all tests
npm test

# Quick load test (server must be running)
npm run bench
```

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `API_KEY` | `change-me` | Shared secret sent in `X-API-Key` header |
| `PORT` | `8080` | HTTP listen port |
| `DATABASE_URL` | `./data/signals.db` | SQLite file path (`:memory:` for tests) |
| `RATE_LIMIT_PER_MIN` | `5` | Max requests per user per 60-second window |
| `DB_FAIL_RATE` | `0` | Fraction of DB calls that simulate `SQLITE_BUSY` (0–1) |

## Extra Production Constraints (must pass)

- **Atomic Idempotency:** Survive concurrent requests and restarts. Avoid check-then-insert races; use a DB-level unique constraint or atomic upsert pattern. Return the same resource for identical `Idempotency-Key`.
- **Concurrency-Safe Rate Limit:** Must behave correctly under burst and parallel calls. Naive in-memory counters that race will fail hidden checks. Explain how this becomes multi-instance safe.
- **Transient DB Failures:** Implement retry/backoff (with jitter) or circuit breaker when DB errors occur (we simulate via `DB_FAIL_RATE`). No duplicates on retry.
- **Scale Plan (10k RPS):** Fill `SCALE.md` with a clear, concise approach (indexes, pooling, caching, queues, horizontal scale, idempotency store).

> We will run additional **hidden concurrency/multi-instance tests** during evaluation.
