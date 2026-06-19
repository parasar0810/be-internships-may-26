import { insertSignal, upsertSignal, listSignals } from './db.js';
import { checkAndConsume } from './rateLimit.js';

// ---------------------------------------------------------------------------
// Retry helper — exponential back-off with full jitter.
// Retries on any error; the caller handles the final failure.
// "No duplicates on retry" is guaranteed because insertions are either
// idempotent (upsertSignal) or produce intentionally distinct rows (insertSignal).
// ---------------------------------------------------------------------------
const MAX_RETRIES   = 3;
const BASE_DELAY_MS = 50;

async function withRetry(fn) {
  let lastErr;
  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      return fn(); // better-sqlite3 is synchronous
    } catch (e) {
      lastErr = e;
      if (attempt < MAX_RETRIES - 1) {
        // Exponential back-off: 50 ms, 100 ms, … ± full jitter.
        const cap   = BASE_DELAY_MS * 2 ** attempt;
        const delay = Math.random() * cap;
        await new Promise((r) => setTimeout(r, delay));
      }
    }
  }
  throw lastErr;
}

// ---------------------------------------------------------------------------
// POST /v1/signals
// ---------------------------------------------------------------------------
export async function postSignal(req, reply) {
  const nowMs = Date.now(); // single timestamp for the entire request
  const idem  = req.headers['idempotency-key'] ?? null;
  const { userId, type, payload } = req.body ?? {};

  if (!userId || !type || typeof payload === 'undefined') {
    return reply.code(400).send({ error: 'invalid_body' });
  }

  // Rate limit — applied to all requests (including idempotent retries).
  const { ok, remaining, resetMs } = checkAndConsume(userId, nowMs);
  if (!ok) {
    return reply.code(429).send({ error: 'rate_limited', remaining, resetMs });
  }

  try {
    if (idem) {
      // Atomic upsert: INSERT OR IGNORE followed by SELECT.
      // Both concurrent requests racing on the same key will get the same row
      // back — no check-then-insert gap, no duplicate rows.
      const row = await withRetry(() => upsertSignal(userId, type, payload, idem, nowMs));
      return row;
    }

    // No idempotency key — every call intentionally creates a distinct record.
    const info = await withRetry(() =>
      insertSignal(userId, type, payload, null, nowMs)
    );
    return {
      id:             Number(info.lastInsertRowid),
      userId,
      type,
      payload:        String(payload),
      idempotencyKey: null,
      createdAt:      nowMs,
    };
  } catch (e) {
    req.log.error({ err: e, ctx: 'postSignal' });
    return reply.code(503).send({ error: 'db_unavailable' });
  }
}

// ---------------------------------------------------------------------------
// GET /v1/signals
// ---------------------------------------------------------------------------
export async function getSignals(req, reply) {
  const { userId, limit = 20 } = req.query ?? {};
  if (!userId) return reply.code(400).send({ error: 'missing_userId' });

  // Clamp: reject non-positive values and cap at 100.
  const lim = Math.min(Math.max(Number(limit) || 20, 1), 100);

  try {
    const rows = await withRetry(() => listSignals(userId, lim));
    return { items: rows };
  } catch (e) {
    req.log.error({ err: e, ctx: 'getSignals' });
    return reply.code(503).send({ error: 'db_unavailable' });
  }
}
