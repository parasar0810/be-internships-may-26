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
// Test 1: Basic — 5 allowed, 6th is 429
// ---------------------------------------------------------------------------
test('rate limit: 5 allowed per minute, 6th is 429', async () => {
  const proc = spawn('node', ['src/server.js'], {
    env: {
      ...process.env,
      API_KEY:            'k',
      PORT:               '9092',
      RATE_LIMIT_PER_MIN: '5',
      DATABASE_URL:       ':memory:',
    },
  });
  await waitForPort(9092);

  const base = 'http://localhost:9092';
  const statuses = [];
  for (let i = 0; i < 6; i++) {
    const code = await postStatus(`${base}/v1/signals`, {
      headers: { 'x-api-key': 'k' },
      body: { userId: 'u1', type: 'note', payload: String(i) },
    });
    statuses.push(code);
  }

  const counts = statuses.reduce((acc, c) => ((acc[c] = (acc[c] || 0) + 1), acc), {});
  assert.ok(counts[200] >= 5, `Expected >= 5 × 200, got ${JSON.stringify(counts)}`);
  assert.ok(counts[429] >= 1, `Expected >= 1 × 429, got ${JSON.stringify(counts)}`);
  proc.kill();
});

// ---------------------------------------------------------------------------
// Test 2: Different users have independent buckets
// ---------------------------------------------------------------------------
test('rate limit: quotas are per-user and do not bleed across users', async () => {
  const proc = spawn('node', ['src/server.js'], {
    env: {
      ...process.env,
      API_KEY:            'k',
      PORT:               '9095',
      RATE_LIMIT_PER_MIN: '2',
      DATABASE_URL:       ':memory:',
    },
  });
  await waitForPort(9095);

  const base = 'http://localhost:9095';

  // Exhaust user-A quota
  await postStatus(`${base}/v1/signals`, { headers: { 'x-api-key': 'k' }, body: { userId: 'ua', type: 't', payload: '1' } });
  await postStatus(`${base}/v1/signals`, { headers: { 'x-api-key': 'k' }, body: { userId: 'ua', type: 't', payload: '2' } });
  const uaBlocked = await postStatus(`${base}/v1/signals`, { headers: { 'x-api-key': 'k' }, body: { userId: 'ua', type: 't', payload: '3' } });

  // User-B must still be allowed
  const ubAllowed = await postStatus(`${base}/v1/signals`, { headers: { 'x-api-key': 'k' }, body: { userId: 'ub', type: 't', payload: '1' } });

  assert.equal(uaBlocked, 429, 'user-A should be rate-limited');
  assert.equal(ubAllowed,  200, 'user-B should not be affected by user-A rate limit');
  proc.kill();
});

// ---------------------------------------------------------------------------
// Test 3: Burst — concurrent requests must not exceed the limit
// This catches naive counter implementations that race under concurrency.
// ---------------------------------------------------------------------------
test('rate limit: concurrent burst never allows more than the limit', async () => {
  const proc = spawn('node', ['src/server.js'], {
    env: {
      ...process.env,
      API_KEY:            'k',
      PORT:               '9096',
      RATE_LIMIT_PER_MIN: '5',
      DATABASE_URL:       ':memory:',
    },
  });
  await waitForPort(9096);

  const base = 'http://localhost:9096';

  // Fire 10 concurrent requests for the same user — at most 5 should succeed.
  const statuses = await Promise.all(
    Array.from({ length: 10 }, (_, i) =>
      postStatus(`${base}/v1/signals`, {
        headers: { 'x-api-key': 'k' },
        body: { userId: 'burst-user', type: 'note', payload: String(i) },
      })
    )
  );

  const counts = statuses.reduce((acc, c) => ((acc[c] = (acc[c] || 0) + 1), acc), {});
  assert.ok(
    counts[200] <= 5,
    `Should allow at most 5 requests, allowed ${counts[200]} — got ${JSON.stringify(counts)}`
  );
  assert.ok(
    (counts[429] || 0) >= 5,
    `Should reject at least 5 requests, rejected ${counts[429] || 0} — got ${JSON.stringify(counts)}`
  );
  proc.kill();
});

// ---------------------------------------------------------------------------
// Test 4: Authentication — wrong or missing API key is rejected
// ---------------------------------------------------------------------------
test('auth: requests with wrong API key are rejected with 401', async () => {
  const proc = spawn('node', ['src/server.js'], {
    env: {
      ...process.env,
      API_KEY:      'secret-key',
      PORT:         '9097',
      DATABASE_URL: ':memory:',
    },
  });
  await waitForPort(9097);

  const base = 'http://localhost:9097';

  const wrongKey = await postStatus(`${base}/v1/signals`, {
    headers: { 'x-api-key': 'wrong' },
    body: { userId: 'u1', type: 't', payload: 'x' },
  });
  const noKey = await postStatus(`${base}/v1/signals`, {
    headers: {},
    body: { userId: 'u1', type: 't', payload: 'x' },
  });

  assert.equal(wrongKey, 401, 'Wrong API key must return 401');
  assert.equal(noKey,    401, 'Missing API key must return 401');
  proc.kill();
});

// ---------------------------------------------------------------------------
// Test 5: Health endpoint is unauthenticated
// ---------------------------------------------------------------------------
test('healthz: responds 200 without an API key', async () => {
  const proc = spawn('node', ['src/server.js'], {
    env: {
      ...process.env,
      API_KEY:      'k',
      PORT:         '9098',
      DATABASE_URL: ':memory:',
    },
  });
  await waitForPort(9098);

  const code = await getStatus('http://localhost:9098/healthz');
  assert.equal(code, 200, '/healthz must be publicly accessible');
  proc.kill();
});

// ---------------------------------------------------------------------------
// Test 6: GET /v1/signals returns inserted records
// ---------------------------------------------------------------------------
test('GET /v1/signals returns records for the user', async () => {
  const proc = spawn('node', ['src/server.js'], {
    env: {
      ...process.env,
      API_KEY:            'k',
      PORT:               '9099',
      RATE_LIMIT_PER_MIN: '100',
      DATABASE_URL:       ':memory:',
    },
  });
  await waitForPort(9099);

  const base = 'http://localhost:9099';
  const hdrs = { 'x-api-key': 'k' };

  await postJson(`${base}/v1/signals`, { headers: hdrs, body: { userId: 'ug', type: 'a', payload: '1' } });
  await postJson(`${base}/v1/signals`, { headers: hdrs, body: { userId: 'ug', type: 'b', payload: '2' } });

  const res  = await getJson(`${base}/v1/signals?userId=ug`, hdrs);
  assert.ok(Array.isArray(res.items), 'items must be an array');
  assert.ok(res.items.length >= 2,    `Expected >= 2 items, got ${res.items.length}`);
  assert.ok(
    res.items.every((r) => r.userId === 'ug'),
    'All returned records must belong to the requested user'
  );
  proc.kill();
});

// ---------------------------------------------------------------------------
// Shared HTTP helpers
// ---------------------------------------------------------------------------
async function postStatus(url, { headers, body }) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req  = http.request(
      url,
      { method: 'POST', headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(data), ...headers } },
      (res) => { res.resume(); res.on('end', () => resolve(res.statusCode)); }
    );
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

async function postJson(url, { headers, body }) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req  = http.request(
      url,
      { method: 'POST', headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(data), ...headers } },
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

async function getStatus(url, headers = {}) {
  return new Promise((resolve, reject) => {
    const req = http.request(url, { method: 'GET', headers }, (res) => {
      res.resume();
      res.on('end', () => resolve(res.statusCode));
    });
    req.on('error', reject);
    req.end();
  });
}

async function getJson(url, headers = {}) {
  return new Promise((resolve, reject) => {
    const req = http.request(url, { method: 'GET', headers }, (res) => {
      let chunks = '';
      res.on('data', (d) => (chunks += d));
      res.on('end', () => resolve(JSON.parse(chunks || '{}')));
    });
    req.on('error', reject);
    req.end();
  });
}
