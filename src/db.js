import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';

const dbPath = process.env.DATABASE_URL || './data/signals.db';

// For in-memory DBs (':memory:') dirname returns '.', which is always fine.
if (dbPath !== ':memory:') {
  fs.mkdirSync(path.dirname(dbPath), { recursive: true });
}

const db = new Database(dbPath);

// WAL journal gives much better concurrent-read throughput and avoids
// SQLITE_BUSY under burst writes on a single node.
db.pragma('journal_mode = WAL');
// Wait up to 3 s before giving up on a locked write rather than throwing immediately.
db.pragma('busy_timeout = 3000');

// schema
db.exec(`
CREATE TABLE IF NOT EXISTS signals (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id       TEXT    NOT NULL,
  type          TEXT    NOT NULL,
  payload       TEXT    NOT NULL,
  idempotency_key TEXT  UNIQUE,
  created_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_user_created ON signals(user_id, created_at);
`);

// ---------------------------------------------------------------------------
// Failure simulation (controlled by DB_FAIL_RATE env var, default 0)
// ---------------------------------------------------------------------------
function maybeFail() {
  const rate = Number(process.env.DB_FAIL_RATE || 0);
  if (rate > 0 && Math.random() < rate) {
    const err = new Error('simulated_db_failure');
    err.code = 'SQLITE_BUSY';
    throw err;
  }
}

// ---------------------------------------------------------------------------
// Atomic upsert — eliminates the check-then-insert race for idempotency.
//
// INSERT OR IGNORE inserts the row only if the idempotency_key is not already
// present (enforced by the UNIQUE constraint).  The subsequent SELECT always
// returns the canonical row whether the INSERT fired or was silently ignored.
// Two concurrent requests with the same key both get the same row back.
// ---------------------------------------------------------------------------
export function upsertSignal(userId, type, payload, idemKey, nowMs) {
  maybeFail();
  db.prepare(
    `INSERT OR IGNORE INTO signals
       (user_id, type, payload, idempotency_key, created_at)
     VALUES (?, ?, ?, ?, ?)`
  ).run(userId, type, String(payload), idemKey, nowMs);

  return db.prepare(
    `SELECT id,
            user_id          AS userId,
            type,
            payload,
            idempotency_key  AS idempotencyKey,
            created_at       AS createdAt
     FROM signals
     WHERE idempotency_key = ?`
  ).get(idemKey);
}

// ---------------------------------------------------------------------------
// Plain insert — used when no idempotency key is supplied.
// ---------------------------------------------------------------------------
export function insertSignal(userId, type, payload, idemKey, nowMs) {
  maybeFail();
  return db
    .prepare(
      `INSERT INTO signals (user_id, type, payload, idempotency_key, created_at)
       VALUES (?, ?, ?, ?, ?)`
    )
    .run(userId, type, String(payload), idemKey ?? null, nowMs);
}

export function listSignals(userId, limit) {
  maybeFail();
  return db
    .prepare(
      `SELECT id,
              user_id          AS userId,
              type,
              payload,
              idempotency_key  AS idempotencyKey,
              created_at       AS createdAt
       FROM signals
       WHERE user_id = ?
       ORDER BY created_at DESC
       LIMIT ?`
    )
    .all(userId, limit);
}
