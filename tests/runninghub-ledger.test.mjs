import { test } from 'node:test';
import assert from 'node:assert/strict';
import { RunningHubLedger } from '../runninghub-ledger.mjs';
import { RunningHubScheduler } from '../runninghub-scheduler.mjs';

// Only browser storage is replaced; Web Locks and crypto use Node's real APIs.
function storage() {
  const data = new Map();
  return { get length() { return data.size; }, key: i => [...data.keys()][i],
    getItem: k => data.get(k) ?? null, setItem: (k, v) => data.set(k, v), removeItem: k => data.delete(k) };
}
const tick = () => new Promise(resolve => setImmediate(resolve));
function ledger(store, options = {}) { return new RunningHubLedger({ storage: store, keys: () => ['secret-A', 'secret-B'], ...options }); }

test('two tabs cannot claim the same last channel and persist no API key', async () => {
  const store = storage(); const a = ledger(store), b = ledger(store);
  try {
    const claims = await Promise.all([a.claim('secret-A', 1, 2, 'one'), b.claim('secret-A', 1, 2, 'two')]);
    assert.equal(claims.filter(Boolean).length, 1);
    assert.equal(JSON.stringify(a.records()).includes('secret-'), false);
    assert.ok(await b.claim('secret-B', 1, 2, 'three'));
    assert.equal(await a.claim('secret-B', 5, 2, 'four'), null);
  } finally { a.close(); b.close(); }
});

test('refresh adopts orphaned submitted task and saves recovered result without resubmitting', async () => {
  const store = storage(); const a = ledger(store); let b;
  try {
    const id = await a.claim('secret-A', 1, 1, 'old');
    a.transition(id, 'submitting'); a.transition(id, 'submitted', { remoteTaskId: 'remote-1' });
    a.close(); await tick();
    b = ledger(store, { fetcher: async (url, options) => {
      assert.ok(url.endsWith('/query')); assert.equal(JSON.parse(options.body).taskId, 'remote-1');
      return { json: async () => ({ status: 'SUCCESS', results: [{ url: 'https://example.org/image.png' }] }) };
    } });
    await b.ready;
    assert.equal(b.read(id).phase, 'completed');
    assert.equal(b.read(id).results[0].url, 'https://example.org/image.png');
    assert.ok(await b.claim('secret-A', 1, 1, 'new'));
  } finally { a.close(); b?.close(); }
});

test('refresh discards unsubmitted reservations but retains unknown submissions', async () => {
  const store = storage(); const a = ledger(store); let b;
  try {
    const reserved = await a.claim('secret-A', 2, 2);
    const uncertain = await a.claim('secret-A', 2, 2); a.transition(uncertain, 'submitting');
    a.close(); await tick(); b = ledger(store); await b.ready;
    assert.equal(b.read(reserved), null);
    assert.equal(b.read(uncertain).phase, 'uncertain');
    assert.equal(await b.claim('secret-A', 1, 1), null);
  } finally { a.close(); b?.close(); }
});

test('live tab ownership cannot be stolen by another tab recovery', async () => {
  const store = storage(); const a = ledger(store), b = ledger(store);
  try {
    const id = await a.claim('secret-A', 1, 1); a.transition(id, 'submitting');
    await b.ready; await b.recover();
    assert.equal(b.read(id).owner, a.owner);
    await assert.rejects(b.resolve(id), /所属标签页/);
  } finally { a.close(); b.close(); }
});

test('scheduler records success outputs so a failed download remains recoverable', async () => {
  const store = storage(); const journal = ledger(store);
  const p = new RunningHubScheduler({ keys: () => ['secret-A'], limit: () => 1,
    probe: async () => ({ queue: { isValid: true, limit: 1, runningCount: 0 }, balance: { isValid: true, hasBalance: true } }) });
  p.ledger = journal;
  try {
    const lease = await p.acquire(); lease.beginSubmit(); lease.submitted('remote');
    lease.terminal({ status: 'SUCCESS', results: [{ url: 'https://example.org/a.png' }] });
    assert.equal(p.totalActive(), 0);
    assert.equal(journal.records()[0].results[0].url, 'https://example.org/a.png');
  } finally { journal.close(); }
});

test('a key occupied in another tab does not block an available different key', async () => {
  const store = storage(); const a = ledger(store), b = ledger(store);
  const p = new RunningHubScheduler({ keys: () => ['secret-A', 'secret-B'], limit: () => 1,
    probe: async () => ({ queue: { isValid: true, limit: 1, runningCount: 0 }, balance: { isValid: true, hasBalance: true } }), refreshMs: 10 });
  p.ledger = b;
  try {
    await a.claim('secret-A', 1, 2);
    const lease = await p.acquire({ timeoutMs: 150 });
    assert.equal(lease.apiKey, 'secret-B'); lease.releaseKey();
  } finally { a.close(); b.close(); }
});

test('global policy update in one tab constrains another tab with stale settings', async () => {
  const store = storage(); const a = ledger(store), b = ledger(store);
  try {
    await a.ready; await b.ready;
    await a.configure({ globalLimit: 1 });
    assert.ok(await b.claim('secret-A', 3, 6));
    assert.equal(await b.claim('secret-B', 3, 6), null);
    await a.configure({ globalLimit: 2 });
    assert.ok(await b.claim('secret-B', 3, 6));
  } finally { a.close(); b.close(); }
});

test('configuration read failure rejects and removes the waiter instead of stranding it', async () => {
  const p = new RunningHubScheduler({ keys: () => ['A'], limit: () => { throw new Error('broken settings'); },
    probe: async () => ({ queue: { isValid: true, limit: 1 }, balance: { isValid: true, hasBalance: true } }) });
  p.state('A').info = { queue: { isValid: true, limit: 1 }, balance: { isValid: true, hasBalance: true } };
  p.state('A').checkedAt = Date.now();
  await assert.rejects(p.acquire({ timeoutMs: 30 }), /broken settings/);
  assert.equal(p.waiters.length, 0);
});

test('unsupported browser reports a readiness failure without throwing out the ledger object', async () => {
  let journal;
  assert.doesNotThrow(() => { journal = ledger(storage(), { crypto: {}, locks: null }); });
  await assert.rejects(journal.ready, /浏览器/);
  await assert.rejects(journal.claim('secret-A', 1, 1), /浏览器/);
});

test('production scheduler cannot bypass a failed ledger initialization', async () => {
  const p = new RunningHubScheduler({ requireLedger: true, keys: () => ['A'], limit: () => 1,
    probe: async () => ({ queue: { isValid: true, limit: 1 }, balance: { isValid: true, hasBalance: true } }) });
  await assert.rejects(p.acquire(), /初始化/);
  assert.equal(p.totalActive(), 0);
});

test('external platform work leaves only one shared slot across two tabs', async () => {
  const store = storage(); const a = ledger(store), b = ledger(store);
  const config = { keys: () => ['secret-A'], limit: () => 3, refreshMs: 10,
    probe: async () => ({ queue: { isValid: true, limit: 3, runningCount: 2, queuedCount: 0 }, balance: { isValid: true, hasBalance: true } }) };
  const p = new RunningHubScheduler(config), q = new RunningHubScheduler(config);
  p.ledger = a; q.ledger = b;
  const ac = new AbortController(); let first, second;
  try {
    first = await p.acquire();
    const work = q.acquire({ timeoutMs: 60, abortSignal: ac.signal });
    await assert.rejects(work, /超时/);
    assert.equal(a.records().filter(r => r.phase === 'reserved').length, 1);
  } finally { first?.releaseKey(); second?.releaseKey(); ac.abort(); a.close(); b.close(); }
});

test('slow orphan recovery does not delay admission on an unrelated key', async () => {
  const store = storage(); const a = ledger(store); let b, finish;
  try {
    const id = await a.claim('secret-A', 1, 2); a.transition(id, 'submitting'); a.transition(id, 'submitted', { remoteTaskId: 'remote' });
    a.close(); await tick();
    b = ledger(store, { fetcher: () => new Promise(resolve => { finish = resolve; }) });
    const work = b.claim('secret-B', 1, 2);
    const outcome = await Promise.race([work, new Promise(resolve => setTimeout(() => resolve('blocked'), 50))]);
    assert.notEqual(outcome, 'blocked');
  } finally {
    finish?.({ json: async () => ({ status: 'RUNNING' }) });
    if (b) { await b.ready; b.close(); } a.close();
  }
});
