import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as scheduler from '../runninghub-scheduler.mjs';
import fs from 'node:fs';
import vm from 'node:vm';

test('chat switch prevents an old generation from updating the current chat', () => {
  let context = { chatId: 'one', chat: [{ mes: 'original', swipe_id: 0 }] };
  const guard = scheduler.captureRunningHubTarget(() => context);
  assert.equal(guard(), true);
  context = { chatId: 'two', chat: [] };
  assert.equal(guard(), false);
});

test('deleted or swiped message rejects a late result while new appended messages do not', () => {
  const original = { mes: 'original', swipe_id: 0 };
  const context = { chatId: 'one', chat: [original] };
  const guard = scheduler.captureRunningHubTarget(() => context, 0);
  context.chat.push({ mes: 'next' }); assert.equal(guard(), true);
  original.swipe_id = 1; assert.equal(guard(), false);
  original.swipe_id = 0; context.chat[0] = { mes: 'replacement', swipe_id: 0 };
  assert.equal(guard(), false);
});

test('media transfers are independently limited and cancelled waiters do not run', async () => {
  const pool = new scheduler.RunningHubTransferPool(1);
  let done;
  const first = pool.run(() => new Promise(resolve => { done = resolve; }));
  const abort = new AbortController(); let ran = false;
  const second = pool.run(() => { ran = true; }, abort.signal);
  const rejected = assert.rejects(second, /cancel/);
  abort.abort(new Error('cancel')); await rejected;
  assert.equal(ran, false);
  done(); await first;
  assert.equal(await pool.run(() => 42), 42);
});

test('actual RunningHub response wrapper rejects late delivery after chat switch', async () => {
  const source = fs.readFileSync(new URL('../index.js', import.meta.url), 'utf8').replaceAll('\r\n', '\n');
  const start = source.indexOf('async function runninghubgenerate(');
  const code = source.slice(start, source.indexOf('\n}\n', start) + 2);
  let context = { chatId: 'one', chat: [] }, finish;
  const responses = [];
  const sandbox = { document: { querySelector: () => null }, CSS: { escape: x => x }, getContext: () => context,
    captureRunningHubTarget: scheduler.captureRunningHubTarget, addLog() {}, recordImageGeneration() {},
    generateRunningHubImage: () => new Promise(resolve => { finish = resolve; }),
    extension_settings50: { x: { cache: '0' } }, extensionName: 'x',
    eventSource23: { emit: (_type, data) => responses.push(data) }, EventType: { GENERATE_IMAGE_RESPONSE: 'response' },
    console: { error() {} } };
  vm.createContext(sandbox); vm.runInContext(code, sandbox);
  const work = sandbox.runninghubgenerate({ id: 'req', prompt: 'cat' });
  context = { chatId: 'two', chat: [] }; finish({ image: 'data:image/png;base64,AA==' }); await work;
  assert.equal(responses.length, 1); assert.equal(responses[0].success, false);
  assert.match(responses[0].error, /原聊天/);
});
