/**
 * Fixed-window in-process rate limiter.
 *
 * Single-instance correctness:
 *   Node.js is single-threaded, so Map operations are inherently atomic —
 *   no two callbacks interleave mid-function.  The fixed-window counter is
 *   therefore race-free within one process.
 *
 * Multi-instance safety (how to scale beyond one pod):
 *   Replace the Map with a shared Redis counter using a Lua script so all
 *   pods share the same window atomically:
 *
 *     local key   = 'rl:' .. KEYS[1]
 *     local now   = tonumber(ARGV[1])
 *     local win   = tonumber(ARGV[2])
 *     local limit = tonumber(ARGV[3])
 *     redis.call('ZREMRANGEBYSCORE', key, 0, now - win)
 *     local cnt = redis.call('ZCARD', key)
 *     if cnt < limit then
 *       redis.call('ZADD', key, now, now .. math.random())
 *       redis.call('EXPIRE', key, math.ceil(win / 1000))
 *       return {1, limit - cnt - 1}
 *     end
 *     return {0, 0}
 *
 *   One atomic round-trip; correct under horizontal scale.
 */

const RATE      = Number(process.env.RATE_LIMIT_PER_MIN || 5);
const WINDOW_MS = 60_000;

// userId → { windowStart: number, cnt: number }
const buckets = new Map();

// Sweep expired entries every window to prevent unbounded memory growth.
const sweepTimer = setInterval(() => {
  const cutoff = Date.now() - WINDOW_MS;
  for (const [k, v] of buckets) {
    if (v.windowStart < cutoff) buckets.delete(k);
  }
}, WINDOW_MS);

// Don't keep the process alive just for the sweep.
if (sweepTimer.unref) sweepTimer.unref();

/**
 * Check whether `userId` is within quota and, if so, consume one slot.
 *
 * @param {string} userId
 * @param {number} [nowMs] - injectable for testing
 * @returns {{ ok: boolean, remaining: number, resetMs: number }}
 */
export function checkAndConsume(userId, nowMs = Date.now()) {
  let ent = buckets.get(userId);

  // Start a fresh window when there is no entry or the current window expired.
  if (!ent || nowMs - ent.windowStart >= WINDOW_MS) {
    ent = { windowStart: nowMs, cnt: 0 };
  }

  ent.cnt += 1;
  buckets.set(userId, ent);

  const ok        = ent.cnt <= RATE;
  const resetMs   = ent.windowStart + WINDOW_MS;
  const remaining = Math.max(RATE - ent.cnt, 0);
  return { ok, remaining, resetMs };
}

// Exported so unit tests can read configuration without re-parsing env.
export { RATE, WINDOW_MS };
