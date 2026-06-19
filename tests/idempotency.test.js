import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { setTimeout as wait } from 'node:timers/promises';
import http from 'node:http';
import net from 'node:net';

/** Poll TCP until the port accepts connections (max 5 s). */
async function waitForPort(port, timeout = 5000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const ok = await new Promise((resolve) => {
      const s = net.createConnection({ port, host: '127.0.0.1' });
      s.once('connect', () => { s.destroy(); resolve(true); });
      s.once('error',   () => { s.destroy(); resolve(false); });
    });
    if (ok) return;
    await wait(50);
  }
  throw new Error(`Port ${port} did not open within ${timeout} ms`);
}

// ---------------------------------------------------------------------------
// Test 1: Same idempotency key → same resource returned
// ---------------------------------------------------------------------------
test('idempotency: same key returns same resource', async () => {
  const proc = spawn('node', ['src/server.js'], {
    env: {
      ...process.env,
      API_KEY:            'k',
      PORT:               '9091',
      RATE_LIMIT_PER_MIN: '100',
      DATABASE_URL:       ':memory:',
    },
  });
  await waitForPort(9091);

  const base = 'http://localhost:9091';
  const idem = 'idem-key-' + Date.now();

  const a = await postJson(`${base}/v1/signals`, {
    headers: { 'x-api-key': 'k', 'idempotency-key': idem },
    body: { userId: 'u1', type: 'note', payload: 'x' },
  });
  const b = await postJson(`${base}/v1/signals`, {
    headers: { 'x-api-key': 'k', 'idempotency-key': idem },
    body: { userId: 'u1', type: 'note', payload: 'x' },
  });

  assert.equal(a.id, b.id, 'id must match');
  assert.equal(a.idempotencyKey, b.idempotencyKey, 'idempotencyKey must match');
  assert.ok(a.id, 'response must contain an id');
  proc.kill();
});

// ---------------------------------------------------------------------------
// Test 2: Concurrent requests with the same idempotency key → no duplicates
// This is the race condition the README explicitly calls out.
// ---------------------------------------------------------------------------
test('idempotency: concurrent requests with same key create exactly one record', async () => {
  const proc = spawn('node', ['src/server.js'], {
    env: {
      ...process.env,
      API_KEY:            'k',
      PORT:               '9093',
      RATE_LIMIT_PER_MIN: '200',
      DATABASE_URL:       ':memory:',
    },
  });
  await waitForPort(9093);

  const base = 'http://localhost:9093';
  const idem = 'race-key-' + Date.now();

  // Fire 10 requests simultaneously — all must get back the same id.
  const results = await Promise.all(
    Array.from({ length: 10 }, () =>
      postJson(`${base}/v1/signals`, {
        headers: { 'x-api-key': 'k', 'idempotency-key': idem },
        body: { userId: 'u2', type: 'note', payload: 'race' },
      })
    )
  );

  const ids = results.map((r) => r.id);
  assert.ok(
    ids.every((id) => id === ids[0]),
    `All responses must share one id — got ${JSON.stringify(ids)}`
  );
  proc.kill();
});

// ---------------------------------------------------------------------------
// Test 3: No idempotency key → each call creates a distinct record
// ---------------------------------------------------------------------------
test('idempotency: without a key every call creates a new record', async () => {
  const proc = spawn('node', ['src/server.js'], {
    env: {
      ...process.env,
      API_KEY:            'k',
      PORT:               '9094',
      RATE_LIMIT_PER_MIN: '100',
      DATABASE_URL:       ':memory:',
    },
  });
  await waitForPort(9094);

  const base = 'http://localhost:9094';

  const a = await postJson(`${base}/v1/signals`, {
    headers: { 'x-api-key': 'k' },
    body: { userId: 'u3', type: 'note', payload: 'y' },
  });
  const b = await postJson(`${base}/v1/signals`, {
    headers: { 'x-api-key': 'k' },
    body: { userId: 'u3', type: 'note', payload: 'y' },
  });

  assert.notEqual(a.id, b.id, 'Without an idempotency key each call must produce a new record');
  proc.kill();
});

// ---------------------------------------------------------------------------
// Shared HTTP helper
// ---------------------------------------------------------------------------
async function postJson(url, { headers, body }) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req  = http.request(
      url,
      {
        method:  'POST',
        headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(data), ...headers },
      },
      (res) => {
        let chunks = '';
        res.on('data', (d) => (chunks += d));
        res.on('end', () => resolve(JSON.parse(chunks || '{}')));
      }
    );
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}
