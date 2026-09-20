import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import * as rh from '../runninghub-scheduler.mjs';
const source = fs.readFileSync(new URL('../index.js', import.meta.url), 'utf8').replaceAll('\r\n', '\n');
const start = source.indexOf('async function executeRunningHubVideoDirectTest(');
const code = source.slice(start, source.indexOf('\n}\n', start) + 2);
function setup(overrides = {}) {
  const p = new rh.RunningHubScheduler({ keys: () => ['A'], limit: () => 1, probe: async () => ({ queue: { isValid: true, limit: 1 }, balance: { isValid: true, hasBalance: true } }) });
  const context = { AbortController, setTimeout, clearTimeout, JSON, String, Error, URL, console,
    extension_settings51: { x: {} }, extensionName: 'x', addLog() {},
    acquireRunningHubKey: options => p.acquire(options), stripChineseAnnotations: async x => x,
    isAssetManifestPrompt: () => false, buildRunningHubRefVideoWorkflow: () => ({ promptObj: {}, seedUsed: 1 }),
    extractNodeInfoListFromWorkflow: () => [], sleep: async () => {},
    runningHubReadWithRetry: (op, options) => rh.runningHubReadWithRetry(op, { ...options, delay: async () => {} }),
    runningHubAbortable: rh.runningHubAbortable,
    runningHubCreationRejected: rh.runningHubCreationRejected,
    runningHubFetch: async url => ({ json: async () => url.includes('/run/') ? { taskId: 'remote' } : { status: 'CANCEL' } }), ...overrides };
  vm.createContext(context); vm.runInContext(code, context);
  return { p, run: signal => context.executeRunningHubVideoDirectTest({ workflowId: 'wf', abortSignal: signal }) };
}
test('actual video test cancellation terminal releases its slot', async () => {
  const env = setup(); await assert.rejects(env.run()); assert.equal(env.p.totalActive(), 0);
});

test('cancelling video preparation cannot leave a reserved channel held forever', async () => {
  const ac = new AbortController();
  const env = setup({ stripChineseAnnotations: () => new Promise(() => {}) });
  const work = env.run(ac.signal);
  const observed = work.then(() => 'done', () => 'cancelled');
  await new Promise(resolve => setImmediate(resolve)); ac.abort(new Error('cancel'));
  const outcome = await Promise.race([observed, new Promise(resolve => setTimeout(() => resolve('hung'), 50))]);
  assert.equal(outcome, 'cancelled'); assert.equal(env.p.totalActive(), 0);
});
