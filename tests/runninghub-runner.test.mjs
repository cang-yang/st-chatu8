import { test } from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import fs from 'node:fs';
import { RunningHubScheduler, runningHubReadWithRetry, runningHubCreationRejected } from '../runninghub-scheduler.mjs';

const source = fs.readFileSync(new URL('../index.js', import.meta.url), 'utf8').replaceAll('\r\n', '\n');
const start = source.indexOf('async function generateRunningHubImage(');
const runner = source.slice(start, source.indexOf('\n}\n', start) + 2);
const tick = () => new Promise(resolve => setImmediate(resolve));
function setup(fetcher, settings = {}) {
  let disposed = 0;
  const controller = new AbortController();
  const tasks = new Map();
  const terminalResults = [];
  const config = { runninghub_apiKey: 'A', runninghub_workflowId: 'wf', ...settings };
  const pool = new RunningHubScheduler({ keys: () => ['A'], limit: () => 3,
    probe: async () => ({ queue: { isValid: true, limit: 3, runningCount: 0, queuedCount: 0 }, balance: { isValid: true, hasBalance: true } }) });
  const noop = () => {};
  const context = { console: { log: noop, warn: noop, error: noop }, Date, Math, JSON, String, Error, structuredClone,
    window: {}, extensionName: 'x', extension_settings50: { x: config },
    TaskType: { RUNNINGHUB_IMG: 'img' }, TaskStatus: { QUEUED: 'queued' },
    taskQueue: {
      addTask() { tasks.set('task', 'queued'); return 'task'; },
      isTaskInQueue(id) { return ['queued', 'running'].includes(tasks.get(id)); },
      updateStatus(id, status) { tasks.set(id, status); },
      completeTask(id, success) { tasks.set(id, success ? 'complete' : 'failed'); }
    },
    createRunningHubTaskScope() { return { signal: controller.signal, dispose() { disposed++; } }; },
    runningHubFetch: fetcher,
    runningHubCreationRejected,
    runningHubDownload: fetcher,
    runningHubReadWithRetry: (operation, options) => runningHubReadWithRetry(operation, { ...options, delay: async () => {} }),
    acquireRunningHubKey: async options => {
      const lease = await pool.acquire(options);
      const terminal = lease.terminal;
      lease.terminal = data => { terminalResults.push(data); terminal(data); };
      return lease;
    },
    clearLog: noop, addLog: noop, isPluginToastDisabled: () => true, toastr: { error: noop },
    processCharacterPrompt: x => x, stripChineseAnnotations: async x => x,
    deduplicateTags: x => x, prompt_replace: async x => ({ modifiedPrompt: x, insertions: [] }),
    getRandomYusheId: () => 'default', zhengmian: async (_a, b) => b, fumian: async () => '',
    buildRunningHubWorkflow: () => ({ promptObj: {}, seedUsed: 1 }), buildGenParams: () => ({}),
    extractNodeInfoListFromWorkflow: () => [], sleep: async () => {},
    formatRunningHubApiError: d => d.errorMessage, recordImageGeneration: noop,
    invalidateBalanceCache: noop, recordKeyConsumption: noop,
    FileReader: class { readAsDataURL() { this.result = 'data:image/png;base64,eA=='; this.onloadend(); } }
  };
  vm.createContext(context); vm.runInContext(runner, context);
  return { run: () => context.generateRunningHubImage({ prompt: 'cat' }), pool, tasks, terminalResults, controller, disposed: () => disposed };
}

test('production image runner releases before blocked media download', async () => {
  let download;
  const env = setup(async url => {
    if (url.includes('/run/')) return { json: async () => ({ taskId: 'remote' }) };
    if (url.includes('/query')) return { json: async () => ({ status: 'SUCCESS', results: [{ url: 'https://media.test/image.png' }] }) };
    return new Promise(resolve => download = () => resolve({ blob: async () => new Blob(['x'], { type: 'image/png' }) }));
  });
  const run = env.run();
  await tick();
  assert.equal(typeof download, 'function');
  assert.equal(env.pool.count('A'), 0);
  assert.equal(env.terminalResults[0]?.results?.[0]?.url, 'https://media.test/image.png');
  download(); const result = await run;
  assert.equal(result.image, 'data:image/png;base64,eA==');
  assert.equal(env.disposed(), 1);
});

test('production preparation failure closes task and scope', async () => {
  const env = setup(() => { throw new Error('must not request'); }, { runninghub_workflowId: '' });
  await assert.rejects(env.run());
  assert.equal(env.tasks.get('task'), 'failed');
  assert.equal(env.pool.count('A'), 0);
  assert.equal(env.disposed(), 1);
});

test('production remote CANCEL is terminal and frees lease', async () => {
  const env = setup(async url => ({ json: async () => url.includes('/run/') ? { taskId: 'remote' } : { status: 'CANCEL' } }));
  await assert.rejects(env.run());
  assert.equal(env.pool.count('A'), 0);
});

test('production definitive rejection frees lease', async () => {
  const env = setup(async () => ({ json: async () => ({ errorMessage: 'invalid workflow' }) }));
  await assert.rejects(env.run(), /invalid workflow/);
  assert.equal(env.pool.count('A'), 0);
});

test('production lost submit response retains uncertainty without retry', async () => {
  let calls = 0;
  const env = setup(async () => { calls++; throw new Error('network timeout'); });
  await assert.rejects(env.run(), /network timeout/);
  assert.equal(calls, 1);
  assert.equal(env.pool.snapshot()[0].tasks[0].phase, 'uncertain');
});

test('production malformed success without task ID retains unknown remote ownership', async () => {
  const env = setup(async () => ({ json: async () => ({ code: 0 }) }));
  await assert.rejects(env.run());
  assert.equal(env.pool.totalActive(), 1);
  assert.equal(env.pool.snapshot()[0].tasks[0].phase, 'uncertain');
});

test('production transient query failure recovers original task without resubmission', async () => {
  let creates = 0, queries = 0;
  const env = setup(async url => {
    if (url.includes('/run/')) { creates++; return { json: async () => ({ taskId: 'remote' }) }; }
    if (url.includes('/query')) {
      if (++queries < 3) throw new Error('temporary network failure');
      return { json: async () => ({ status: 'SUCCESS', results: [{ url: 'https://media.test/image.png' }] }) };
    }
    return { blob: async () => new Blob(['x'], { type: 'image/png' }) };
  });
  const result = await env.run();
  assert.ok(result.image);
  assert.equal(creates, 1); assert.equal(queries, 3);
  assert.equal(env.tasks.get('task'), 'complete');
  assert.equal(env.pool.totalActive(), 0);
});
