import { test } from 'node:test';
import assert from 'node:assert/strict';
import { RunningHubScheduler, runningHubFetch, runningHubReadWithRetry, runningHubDelay } from '../runninghub-scheduler.mjs';

const tick = () => new Promise(resolve => setImmediate(resolve));

test('remote cancellation request never releases capacity before terminal confirmation', async () => {
  let requested = null;
  const p = new RunningHubScheduler({ keys: () => ['A'], limit: () => 1, probe: async () => info(1),
    cancelRemote: async (key, id) => { requested = [key, id]; } });
  const lease = await p.acquire({ taskId: 'local' }); lease.beginSubmit(); lease.submitted('remote');
  p.cancel('local'); await tick();
  assert.deepEqual(requested, ['A', 'remote']);
  assert.equal(p.totalActive(), 1); lease.terminal();
});

test('pause queues tasks and resume fills slots, queue cap rejects excess', async () => {
  const p = new RunningHubScheduler({ keys: () => ['A'], limit: () => 1, maxQueue: () => 1, probe: async () => info(1) });
  p.setPaused(true);
  const pending = p.acquire();
  await assert.rejects(p.acquire(), /队列已满/);
  assert.equal(p.totalActive(), 0);
  p.setPaused(false); const lease = await pending; lease.releaseKey();
});

test('read retries are bounded with backoff and do not retry authentication failures', async () => {
  let calls = 0;
  const delays = [];
  await assert.rejects(runningHubReadWithRetry(async () => { calls++; throw new Error('offline'); },
    { delay: async ms => { delays.push(ms); } }), /offline/);
  assert.equal(calls, 4); assert.deepEqual(delays, [1000, 2000, 4000]);
  calls = 0;
  await assert.rejects(runningHubReadWithRetry(async () => {
    calls++; throw Object.assign(new Error('denied'), { status: 401 });
  }), /denied/);
  assert.equal(calls, 1);
});

test('cancelling during backoff stops further reads immediately', async () => {
  const ac = new AbortController();
  let calls = 0;
  const work = runningHubReadWithRetry(async () => { calls++; throw new Error('offline'); }, { signal: ac.signal });
  const rejected = assert.rejects(work, /cancelled/);
  await tick(); ac.abort(new Error('cancelled')); await rejected;
  assert.equal(calls, 1);
  await assert.rejects(runningHubDelay(10000, ac.signal), /cancelled/);
});

test('released lease ignores a late submission callback', async () => {
  const p = pool();
  const lease = await p.acquire(); lease.beginSubmit(); lease.terminal();
  lease.submitted('late');
  assert.equal(lease.phase, 'terminal'); assert.equal(lease.remoteTaskId, null);
  assert.equal(p.totalActive(), 0);
});

test('late task ID starts reconciliation; duplicate recovery is coalesced and release aborts it', async () => {
  const originalFetch = globalThis.fetch;
  let calls = 0, aborted = false;
  globalThis.fetch = async (_url, options) => {
    calls++;
    return new Promise((_resolve, reject) => options.signal.addEventListener('abort', () => {
      aborted = true; reject(new Error('aborted'));
    }, { once: true }));
  };
  let lease;
  try {
    const p = pool(); lease = await p.acquire();
    lease.beginSubmit(); lease.releaseKey();
    lease.submitted('late-remote');
    p.reconcile(lease); p.reconcile(lease);
    assert.equal(calls, 1);
    p.resolveUncertain(lease.id);
    await tick();
    assert.equal(aborted, true); assert.equal(lease.reconciling, false);
    assert.equal(p.totalActive(), 0);
  } finally { lease?.terminal(); globalThis.fetch = originalFetch; }
});
const info = (limit = 3, runningCount = 0) => ({
  queue: { isValid: true, limit, runningCount, queuedCount: 0 },
  balance: { isValid: true, hasBalance: true }
});

test('mixed official capacities and per-key limits share one global ceiling', async () => {
  const official = { A: 1, B: 3, C: 5 };
  const local = { A: 9, B: 2, C: 4 };
  const p = new RunningHubScheduler({ keys: () => Object.keys(official),
    limit: key => local[key], globalLimit: () => 4, probe: async key => info(official[key]) });
  const first = await Promise.all(Array.from({ length: 4 }, () => p.acquire()));
  assert.equal(p.totalActive(), 4);
  assert.ok(p.count('A') <= 1 && p.count('B') <= 2 && p.count('C') <= 4);
  let allocated = false;
  const pending = p.acquire().then(l => { allocated = true; return l; });
  await tick(); assert.equal(allocated, false);
  first[0].releaseKey();
  const next = await pending;
  assert.equal(p.totalActive(), 4);
  first.forEach(l => l.releaseKey()); next.releaseKey();
});

test('no global cap uses summed mixed capacities; duplicate keys add no capacity', async () => {
  const official = { A: 1, B: 3, C: 5 };
  const p = new RunningHubScheduler({ keys: () => ['A', 'A', 'B', 'C'],
    limit: () => 0, probe: async key => info(official[key]) });
  const leases = await Promise.all(Array.from({ length: 9 }, () => p.acquire()));
  assert.deepEqual(['A', 'B', 'C'].map(key => p.count(key)), [1, 3, 5]);
  leases.forEach(l => l.releaseKey());
});

test('raising global limit fills immediately; lowering drains without cancellation', async () => {
  let ceiling = 3;
  const p = new RunningHubScheduler({ keys: () => ['A'], limit: () => 0,
    globalLimit: () => ceiling, probe: async () => info(5) });
  const first = await Promise.all([p.acquire(), p.acquire(), p.acquire()]);
  ceiling = 1; p.configurationChanged();
  assert.equal(p.totalActive(), 3);
  let allocated = false;
  const pending = p.acquire().then(l => { allocated = true; return l; });
  first[0].releaseKey(); first[1].releaseKey();
  await tick(); assert.equal(allocated, false);
  ceiling = 2; p.configurationChanged();
  const next = await pending;
  assert.equal(p.totalActive(), 2);
  first[2].releaseKey(); next.releaseKey();
});

test('changing per-key cap drains and refills without waiting for probe', async () => {
  let ceiling = 3;
  const p = new RunningHubScheduler({ keys: () => ['A'], limit: () => ceiling,
    probe: async () => info(5) });
  const first = await Promise.all([p.acquire(), p.acquire(), p.acquire()]);
  ceiling = 1; p.configurationChanged();
  let allocated = false;
  const pending = p.acquire().then(l => { allocated = true; return l; });
  first[0].releaseKey(); first[1].releaseKey();
  await tick(); assert.equal(allocated, false);
  ceiling = 2; p.configurationChanged();
  const next = await pending;
  assert.equal(p.snapshot()[0].effectiveLimit, 2);
  first[2].releaseKey(); next.releaseKey();
});

test('removed key still owns global capacity until its running task ends', async () => {
  let keys = ['A'];
  const p = new RunningHubScheduler({ keys: () => keys, limit: () => 1,
    globalLimit: () => 1, probe: async () => info(1) });
  const first = await p.acquire(); first.beginSubmit(); first.submitted('remote-A');
  keys = ['B']; p.configurationChanged();
  let allocated = false;
  const pending = p.acquire().then(l => { allocated = true; return l; });
  await tick(); assert.equal(allocated, false);
  first.terminal(); const next = await pending;
  assert.equal(next.apiKey, 'B'); next.releaseKey();
});

test('uncertain submission still counts toward global cap; duplicate submission blocked', async () => {
  const p = new RunningHubScheduler({ keys: () => ['A', 'B'], limit: () => 1,
    globalLimit: () => 1, probe: async () => info(1) });
  const first = await p.acquire(); first.beginSubmit();
  assert.throws(() => first.beginSubmit(), /重复提交/);
  first.releaseKey();
  let allocated = false;
  const pending = p.acquire().then(l => { allocated = true; return l; });
  await tick(); assert.equal(allocated, false);
  p.resolveUncertain(first.id);
  const next = await pending;
  first.releaseKey(); assert.equal(p.totalActive(), 1);
  next.releaseKey();
});
function pool(keys = ['A'], limit = 3, probe = async () => info(limit)) {
  return new RunningHubScheduler({ keys: () => keys, limit: () => limit, probe });
}

test('three slots: refill immediately after one of three finishes', async () => {
  const p = pool();
  const first = await Promise.all([1, 2, 3].map(taskId => p.acquire({ taskId })));
  const abort = new AbortController();
  const fourth = p.acquire({ taskId: 4 });
  const fifth = p.acquire({ taskId: 5, abortSignal: abort.signal });
  first[0].releaseKey();
  const next = await fourth;
  assert.equal(p.count('A'), 3);
  const cancelled = assert.rejects(fifth, /取消/);
  abort.abort(); await cancelled;
  first.slice(1).forEach(l => l.releaseKey()); next.releaseKey();
  assert.equal(p.count('A'), 0);
});

test('four one-slot accounts allocate before any completion', async () => {
  const p = pool(['A', 'B', 'C', 'D'], 1);
  const leases = await Promise.all([1, 2, 3, 4].map(taskId => p.acquire({ taskId })));
  assert.equal(new Set(leases.map(l => l.apiKey)).size, 4);
  leases.forEach(l => l.releaseKey());
});

test('slow key does not block another account', async () => {
  let resolveSlow;
  const p = pool(['A', 'B'], 1, key => key === 'A' ? new Promise(r => resolveSlow = r) : Promise.resolve(info(1)));
  const lease = await p.acquire();
  assert.equal(lease.apiKey, 'B');
  lease.releaseKey(); resolveSlow(info(1)); await tick();
});

test('cancel during probe cannot allocate or strand the promise', async () => {
  let resolveProbe;
  const p = pool(['A'], 3, () => new Promise(r => resolveProbe = r));
  const ac = new AbortController();
  const promise = p.acquire({ abortSignal: ac.signal });
  await tick();
  const rejected = assert.rejects(promise, /取消/);
  ac.abort(); await rejected;
  resolveProbe(info()); await tick();
  assert.equal(p.count('A'), 0);
  assert.equal(p.waiters.length, 0);
});

test('probe spanning release cannot resurrect an old count', async () => {
  const p = pool(); const first = await p.acquire();
  let resolveProbe;
  p.probe = () => new Promise(r => resolveProbe = r);
  p.state('A').checkedAt = 0;
  const nextPromise = p.acquire(); await tick();
  first.releaseKey(); resolveProbe(info());
  const next = await nextPromise; next.releaseKey();
  assert.equal(p.count('A'), 0);
});

test('refresh/reset preserves identity and release is idempotent', async () => {
  const p = pool(); const first = await p.acquire();
  p.reset(); const next = await p.acquire();
  first.releaseKey(); first.releaseKey();
  assert.equal(p.count('A'), 1);
  next.releaseKey(); assert.equal(p.count('A'), 0);
});

test('blocked restricted task cannot block a compatible later task', async () => {
  const p = pool(['A', 'B'], 1);
  const first = await p.acquire({ apiKeys: ['A'] });
  const ac = new AbortController();
  const waiting = p.acquire({ apiKeys: ['A'], abortSignal: ac.signal });
  const next = await p.acquire({ apiKeys: ['B'] });
  assert.equal(next.apiKey, 'B');
  const rejected = assert.rejects(waiting); ac.abort(); await rejected;
  first.releaseKey(); next.releaseKey();
});

test('local cancelled reservation cannot submit', async () => {
  const p = pool(); const l = await p.acquire({ taskId: 'x' });
  p.cancel('x'); assert.throws(() => l.beginSubmit(), /取消/);
  assert.equal(p.count('A'), 0);
});

test('submitted remote tasks survive UI cancellation until terminal', async () => {
  const p = pool(); const l = await p.acquire({ taskId: 'x' });
  l.beginSubmit(); l.submitted('remote'); p.cancel('x');
  assert.equal(p.count('A'), 1);
  l.terminal(); assert.equal(p.count('A'), 0);
});

test('unknown submission is retained, not retried or silently freed', async () => {
  const p = pool(); const l = await p.acquire();
  l.beginSubmit(); l.releaseKey(); l.releaseKey();
  assert.equal(l.phase, 'uncertain'); assert.equal(p.count('A'), 1);
  l.terminal(); assert.equal(p.count('A'), 0);
});

test('acquisition deadline works while probe never returns', async () => {
  const p = pool(['A'], 3, () => new Promise(() => {}));
  await assert.rejects(p.acquire({ timeoutMs: 15 }), /超时/);
  assert.equal(p.waiters.length, 0);
});

test('HTTP timeout includes response body', async () => {
  const original = globalThis.fetch;
  globalThis.fetch = async (_url, { signal }) => ({ ok: true,
    arrayBuffer: () => new Promise((_resolve, reject) => signal.addEventListener('abort', () => reject(signal.reason))) });
  try { await assert.rejects(runningHubFetch('https://example.test', {}, 15), /超时/); }
  finally { globalThis.fetch = original; }
});

test('remote count including our own jobs is not counted twice', async () => {
  let remote = 0;
  const p = pool(['A'], 3, async () => info(3, remote));
  const a = await p.acquire(); a.beginSubmit(); a.submitted('a'); remote++;
  p.reset(); await tick();
  const b = await p.acquire(); b.beginSubmit(); b.submitted('b'); remote++;
  p.reset(); await tick();
  const c = await p.acquire();
  assert.equal(p.count('A'), 3);
  a.terminal(); b.terminal(); c.releaseKey();
});

test('external work leaves only remaining capacity available', async () => {
  const p = pool(['A'], 3, async () => info(3, 2));
  const a = await p.acquire();
  await assert.rejects(p.acquire({ timeoutMs: 15 }), /超时/);
  assert.equal(p.count('A'), 1); a.releaseKey();
});

test('all zero-balance keys fail promptly', async () => {
  const p = pool(['A'], 1, async () => ({ ...info(1), balance: { isValid: true, hasBalance: false } }));
  await assert.rejects(p.acquire(), /余额/);
  assert.equal(p.waiters.length, 0);
});

test('lowering limit during a probe is respected', async () => {
  let cap = 3, resume;
  const p = new RunningHubScheduler({ keys: () => ['A'], limit: () => cap, probe: () => new Promise(r => resume = r) });
  const first = p.acquire(); await tick(); cap = 1; resume(info(3));
  const lease = await first;
  await assert.rejects(p.acquire({ timeoutMs: 15 }), /超时/);
  lease.releaseKey();
});

test('known uncertain task releases after remote reconciliation', async () => {
  const original = globalThis.fetch;
  globalThis.fetch = async () => new Response(JSON.stringify({ status: 'SUCCESS' }));
  try {
    const p = pool(); const lease = await p.acquire();
    lease.beginSubmit(); lease.submitted('remote'); lease.releaseKey();
    await tick(); assert.equal(p.count('A'), 0);
  } finally { globalThis.fetch = original; }
});

test('manual reconciliation only targets the selected uncertain lease', async () => {
  const p = pool(); const a = await p.acquire(), b = await p.acquire();
  a.beginSubmit(); a.releaseKey();
  assert.equal(p.resolveUncertain(b.id), false);
  assert.equal(p.resolveUncertain(a.id), true);
  assert.equal(p.count('A'), 1);
  assert.equal(p.resolveUncertain(a.id), false);
  b.releaseKey();
});
