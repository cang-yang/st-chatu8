// RunningHub scheduling changes, 2026-09-20. Distributed under the repository AFPL.
// No network operation runs inside the synchronous allocation pass.
export function captureRunningHubTarget(getContext, messageIndex) {
  const initial = getContext();
  const chat = initial?.chat, chatId = initial?.chatId;
  const message = Number.isInteger(messageIndex) ? chat?.[messageIndex] : null;
  const text = message?.mes, swipe = message?.swipe_id;
  return () => {
    const current = getContext();
    if (current?.chat !== chat || current?.chatId !== chatId) return false;
    return !message || current.chat?.[messageIndex] === message && message.mes === text && message.swipe_id === swipe;
  };
}

export function runningHubAbortable(work, signal) {
  return new Promise((resolve, reject) => {
    const abort = () => reject(signal.reason || new Error('任务已取消'));
    if (signal?.aborted) abort();
    else signal?.addEventListener('abort', abort, { once: true });
    Promise.resolve(work).then(resolve, reject).finally(() => signal?.removeEventListener('abort', abort));
  });
}

export function runningHubCreationRejected(data) {
  return Boolean(data && (data.code != null && Number.isFinite(Number(data.code)) && Number(data.code) !== 0 ||
    data.errorCode != null && String(data.errorCode) !== '0' || data.errorMessage || /TASK_QUEUE_MAXED/i.test(data.msg || '')));
}

export async function runningHubFetch(url, options = {}, timeoutMs = 60000) {
  const controller = new AbortController();
  const abort = () => controller.abort(options.signal?.reason);
  if (options.signal?.aborted) abort();
  else options.signal?.addEventListener('abort', abort, { once: true });
  const timer = setTimeout(() => controller.abort(new Error('RunningHub 请求超时')), timeoutMs);
  try {
    const response = await fetch(url, { ...options, signal: controller.signal });
    // Include body transfer in the deadline, not just receipt of response headers.
    const body = await response.arrayBuffer();
    if (!response.ok) {
      let detail = '';
      try { const data = JSON.parse(new TextDecoder().decode(body)); detail = data.errorMessage || data.msg || ''; } catch (_) {}
      const error = new Error(`RunningHub HTTP ${response.status}${detail ? ': ' + detail : ''}`);
      error.status = response.status;
      throw error;
    }
    return new Response(body, { status: response.status, headers: response.headers });
  } finally {
    clearTimeout(timer);
    options.signal?.removeEventListener('abort', abort);
  }
}

export function runningHubDelay(ms, signal) {
  return new Promise((resolve, reject) => {
    const abort = () => { clearTimeout(timer); signal?.removeEventListener('abort', abort); reject(signal.reason || new Error('任务已取消')); };
    const timer = setTimeout(() => { signal?.removeEventListener('abort', abort); resolve(); }, ms);
    if (signal?.aborted) abort();
    else signal?.addEventListener('abort', abort, { once: true });
  });
}

export async function runningHubCancelRemote(apiKey, taskId) {
  const response = await runningHubFetch('https://www.runninghub.ai/task/openapi/cancel', {
    method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` },
    body: JSON.stringify({ apiKey, taskId })
  }, 15000);
  const data = await response.json();
  if (data.code !== 0) throw new Error(data.msg || '远端取消未确认');
  // Acceptance is not evidence of termination. Query still owns release.
}

export class RunningHubTransferPool {
  constructor(limit = 3) { this.limit = limit; this.active = 0; this.queue = []; }
  run(operation, signal) {
    return new Promise((resolve, reject) => {
      const entry = { operation, signal, resolve, reject };
      entry.abort = () => {
        this.queue = this.queue.filter(item => item !== entry);
        signal?.removeEventListener('abort', entry.abort);
        reject(signal.reason || new Error('任务已取消'));
      };
      if (signal?.aborted) { entry.abort(); return; }
      signal?.addEventListener('abort', entry.abort, { once: true });
      this.queue.push(entry); this.pump();
    });
  }
  pump() {
    while (this.active < this.limit && this.queue.length) {
      const entry = this.queue.shift();
      entry.signal?.removeEventListener('abort', entry.abort);
      this.active++;
      Promise.resolve().then(() => {
        if (entry.signal?.aborted) throw entry.signal.reason || new Error('任务已取消');
        return entry.operation();
      }).then(entry.resolve, entry.reject).finally(() => { this.active--; this.pump(); });
    }
  }
}
const runningHubTransfers = new RunningHubTransferPool();
export function runningHubDownload(url, options = {}) {
  return runningHubTransfers.run(() => runningHubReadWithRetry(() => runningHubFetch(url, options), { signal: options.signal }), options.signal);
}

// Only for repeatable reads. Never wrap task creation in this helper.
export async function runningHubReadWithRetry(operation, { signal, onRetry = () => {}, delay = runningHubDelay, attempts = 4 } = {}) {
  for (let attempt = 0; attempt < attempts; attempt++) {
    if (signal?.aborted) throw signal.reason || new Error('任务已取消');
    try { return await operation(); }
    catch (error) {
      const status = Number(error.status);
      if (signal?.aborted || attempt + 1 >= attempts || status >= 400 && status < 500 && ![408, 429].includes(status)) throw error;
      try { onRetry(attempt + 1); } catch (_) {}
      await delay(Math.min(1000 * 2 ** attempt, 8000), signal);
    }
  }
}

export class RunningHubScheduler {
  constructor({ keys, limit, globalLimit = () => 0, maxQueue = () => 100, pausedKey = () => false, cancelRemote, requireLedger = false, probe, log = () => {}, onChange = () => {}, refreshMs = 2000 }) {
    Object.assign(this, { keys, limit, globalLimit, maxQueue, pausedKey, cancelRemote, requireLedger, probe, refreshMs });
    this.log = message => { try { log(message); } catch (_) {} };
    this.onChange = onChange;
    this.states = new Map();
    this.waiters = [];
    this.sequence = 0;
    this.timer = null;
    this.paused = false;
  }
  state(key) {
    if (!this.states.has(key)) this.states.set(key, {
      leases: new Map(), checkedAt: 0, checking: false, info: null,
      external: 0, lastUsed: 0, cooldown: 0
    });
    return this.states.get(key);
  }
  count(key) { return this.state(key).leases.size; }
  totalActive() { return [...this.states.values()].reduce((total, s) => total + s.leases.size, 0); }
  configuredGlobalLimit() {
    const value = Number(this.globalLimit());
    return Number.isFinite(value) && value > 0 ? Math.floor(value) || 1 : Infinity;
  }
  effectiveLimit(key, officialLimit) {
    const value = Number(this.limit(key));
    return Math.min(Number.isFinite(value) && value > 0 ? Math.floor(value) || 1 : Infinity, officialLimit);
  }
  configurationChanged() { this.pump(); }
  setPaused(value) { this.paused = Boolean(value); this.pump(); }
  cancelWaiting() { for (const w of [...this.waiters]) w.abort(); }
  snapshot() {
    return [...this.states].map(([key, s]) => ({
      key: key.length > 10 ? `${key.slice(0, 4)}…${key.slice(-4)}` : '[key]',
      active: s.leases.size, external: s.external,
      limit: s.info?.queue?.limit ?? null,
      effectiveLimit: s.info?.queue?.limit == null ? null : this.effectiveLimit(key, s.info.queue.limit),
      checking: s.checking,
      tasks: [...s.leases.values()].map(l => ({ id: l.id, taskId: l.taskId,
        remoteTaskId: l.remoteTaskId, phase: l.phase }))
    }));
  }
  acquire({ apiKeys, abortSignal, taskId, isTaskCancelled, timeoutMs = 600000 } = {}) {
    return new Promise((resolve, reject) => {
      const queueLimit = Number(this.maxQueue());
      if (Number.isFinite(queueLimit) && queueLimit > 0 && this.waiters.length >= queueLimit) return reject(new Error('RunningHub 等待队列已满，请等待已有任务完成后再试'));
      const waiter = { apiKeys, abortSignal, taskId, isTaskCancelled, resolve, reject,
        deadline: Date.now() + timeoutMs, settled: false };
      waiter.abort = () => this.finish(waiter, new Error('任务已取消'));
      if (abortSignal?.aborted || isTaskCancelled?.()) return reject(new Error('任务已取消'));
      abortSignal?.addEventListener('abort', waiter.abort, { once: true });
      waiter.timeout = setTimeout(() => this.finish(waiter, new Error('等待 RunningHub 通道超时')), timeoutMs);
      this.waiters.push(waiter);
      this.pump();
    });
  }
  finish(w, error, lease) {
    if (w.settled) return;
    w.settled = true;
    clearTimeout(w.timeout);
    w.abortSignal?.removeEventListener('abort', w.abort);
    this.waiters = this.waiters.filter(item => item !== w);
    if (error) w.reject(error); else w.resolve(lease);
    this.schedule();
  }
  cancel(taskId) {
    for (const w of [...this.waiters]) if (w.taskId === taskId) w.abort();
    // A submitted task can still run remotely. Its owner must reconcile it.
    for (const s of this.states.values()) for (const l of [...s.leases.values()]) {
      if (l.taskId === taskId && l.phase === 'reserved') l.releaseKey();
      else if (l.taskId === taskId) { l.cancelRequested = true; this.requestRemoteCancel(l); }
    }
    this.pump();
  }
  async requestRemoteCancel(lease) {
    if (!this.cancelRemote || !lease.cancelRequested || !lease.remoteTaskId || lease.cancelling || lease.cancelAccepted) return;
    lease.cancelling = true;
    try {
      await this.cancelRemote(lease.apiKey, lease.remoteTaskId);
      lease.cancelAccepted = true;
      this.log('RunningHub 已受理取消请求，等待远端终态确认');
    } catch (error) { this.log(`RunningHub 取消尚未确认，将继续核对：${error.message}`); }
    finally { lease.cancelling = false; }
  }
  refresh(key, s) {
    if (s.checking || Date.now() - s.checkedAt < this.refreshMs) return;
    s.checking = true;
    const known = this.ledger ? this.ledger.knownRemoteCount(key) : [...s.leases.values()].filter(l => l.remoteTaskId).length;
    Promise.resolve().then(() => this.probe(key)).then(info => {
      s.info = info;
      // Aggregates include our submitted jobs. Never add them twice. Jobs
      // reserved while this probe runs remain included in leases.size.
      s.external = Math.max(0, (info.queue.runningCount || 0) + (info.queue.queuedCount || 0) - known);
    }, error => { s.info = { error: error.message }; }).finally(() => {
      s.checkedAt = Date.now(); s.checking = false; this.pump();
    });
  }
  pump() {
    try { this.pumpReady(); }
    catch (error) { for (const w of [...this.waiters]) this.finish(w, error); }
  }
  pumpReady() {
    if (this.requireLedger && !this.ledger) throw new Error('RunningHub 安全调度初始化失败，请检查浏览器存储及 HTTPS 设置');
    for (const w of [...this.waiters]) {
      if (w.settled) continue;
      if (w.abortSignal?.aborted || w.isTaskCancelled?.()) { w.abort(); continue; }
      if (Date.now() >= w.deadline) { this.finish(w, new Error('等待 RunningHub 通道超时')); continue; }
      if (w.admitting || this.paused) continue;
      const configured = this.keys();
      const keys = [...new Set(w.apiKeys?.length ? w.apiKeys : configured)];
      if (!keys.length) { this.finish(w, new Error('请配置 RunningHub API Key')); continue; }
      const candidates = [];
      for (const key of keys) {
        if (this.pausedKey(key)) continue;
        const s = this.state(key);
        this.refresh(key, s);
        const q = s.info?.queue, b = s.info?.balance;
        if (!q?.isValid || !b?.isValid || !b.hasBalance || Date.now() - s.checkedAt > this.refreshMs * 3) continue;
        const cap = this.effectiveLimit(key, q.limit);
        if (!Number.isFinite(cap) || cap <= 0 || s.cooldown > Date.now()) continue;
        if (s.leases.size >= cap || s.leases.size + s.external >= q.limit) continue;
        candidates.push({ key, s, cap });
      }
      const permanentlyUnavailable = keys.every(key => {
        const s = this.state(key), b = s.info?.balance, q = s.info?.queue;
        const denied = value => [401, 403].includes(Number(value?.errorCode));
        return !s.checking && (denied(q) || denied(b) || b?.isValid && !b.hasBalance);
      });
      if (!candidates.length && permanentlyUnavailable) {
        this.finish(w, new Error('RunningHub Key 不可用：请检查余额或鉴权')); continue;
      }
      candidates.sort((a, b) => a.s.leases.size / a.cap - b.s.leases.size / b.cap || a.s.lastUsed - b.s.lastUsed);
      // Include reservations and uncertain remote ownership across ALL keys,
      // including keys removed from configuration while their jobs still run.
      if (candidates.length && this.totalActive() < this.configuredGlobalLimit()) {
        const { key, s } = candidates[0];
        if (!this.ledger) this.finish(w, null, this.reserve(key, s, w));
        else {
          w.admitting = true;
          (async () => {
            for (const candidate of candidates) {
              if (w.settled) break;
              const availableCap = Math.min(candidate.cap, candidate.s.info.queue.limit - candidate.s.external);
              const id = await this.ledger.claim(candidate.key, availableCap, this.configuredGlobalLimit(), w.taskId);
              if (id) return { id, ...candidate };
            }
            return null;
          })().then(claim => {
            w.admitting = false;
            if (!claim) { this.schedule(); return; }
            const { id, key, s } = claim;
            if (w.settled || w.abortSignal?.aborted || w.isTaskCancelled?.() || this.paused || this.pausedKey(key) ||
                this.totalActive() >= this.configuredGlobalLimit() || s.leases.size >= this.effectiveLimit(key, s.info.queue.limit)) {
              this.ledger.finish(id); this.pump(); return;
            }
            this.finish(w, null, this.reserve(key, s, w, id)); this.pump();
          }).catch(error => { w.admitting = false; this.finish(w, error); });
        }
      }
    }
    this.schedule();
  }
  reserve(key, s, w, journalId) {
    const id = ++this.sequence;
    s.lastUsed = id;
    const lease = {
      id, journalId, taskId: w.taskId, apiKey: key, phase: 'reserved', remoteTaskId: null,
      remainCoins: s.info.balance.remainCoins, remainMoney: s.info.balance.remainMoney,
      queueInfo: s.info.queue,
      beginSubmit: () => {
        if (!s.leases.has(id) || w.abortSignal?.aborted || w.isTaskCancelled?.()) throw new Error('任务已取消');
        if (lease.phase !== 'reserved') throw new Error('任务已提交，不能重复提交');
        if (journalId) this.ledger.transition(journalId, 'submitting');
        lease.phase = 'submitting';
        this.schedule();
      },
      submitted: taskId => {
        if (!s.leases.has(id) || !['submitting', 'uncertain'].includes(lease.phase)) return;
        if (!taskId) throw new Error('缺少远程任务 ID');
        lease.remoteTaskId = taskId;
        if (journalId) this.ledger.transition(journalId, 'submitted', { remoteTaskId: taskId });
        if (lease.phase === 'uncertain') this.reconcile(lease);
        else lease.phase = 'submitted';
        this.schedule();
      },
      terminal: data => {
        if (!s.leases.has(id)) return;
        if (journalId) this.ledger.finish(journalId, data);
        lease.phase = 'terminal'; lease.releaseKey();
      },
      rejected: () => {
        if (!s.leases.has(id)) return;
        if (journalId) this.ledger.finish(journalId);
        s.cooldown = Date.now() + 3500;
        lease.phase = 'rejected'; lease.releaseKey();
      },
      releaseKey: () => {
        if (!s.leases.has(id)) return;
        if (lease.phase === 'submitted' || lease.phase === 'submitting') {
          if (w.abortSignal?.aborted) lease.cancelRequested = true;
          lease.phase = 'uncertain';
          if (journalId) {
            try { this.ledger.transition(journalId, 'uncertain', { remoteTaskId: lease.remoteTaskId }); }
            catch (error) { this.log(`恢复记录保存失败：${error.message}`); }
          }
          this.log(`[RunningHub] 任务 ${w.taskId || id} 远程状态待确认，保留占用，避免重复提交`);
          this.schedule();
          if (lease.remoteTaskId) this.reconcile(lease);
          return;
        }
        if (lease.phase === 'uncertain') return;
        if (journalId && lease.phase === 'reserved') this.ledger.finish(journalId);
        clearTimeout(lease.reconcileTimer);
        lease.reconcileController?.abort();
        s.leases.delete(id);
        this.log(`[RunningHub] 释放任务 ${w.taskId || id}，本地剩余 ${s.leases.size}`);
        this.pump();
      }
    };
    s.leases.set(id, lease);
    return lease;
  }
  async reconcile(lease) {
    if (lease.phase !== 'uncertain' || !lease.remoteTaskId || lease.reconciling) return;
    clearTimeout(lease.reconcileTimer);
    lease.reconciling = true;
    lease.reconcileController = new AbortController();
    this.requestRemoteCancel(lease);
    // Independent from the UI cancellation signal; never resubmit a lost job.
    try {
      const response = await runningHubFetch('https://www.runninghub.ai/openapi/v2/query', {
        method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${lease.apiKey}` },
        body: JSON.stringify({ taskId: lease.remoteTaskId }), signal: lease.reconcileController.signal
      }, 15000);
      const data = await response.json();
      if (['SUCCESS', 'FAILED', 'CANCEL', 'CANCELLED'].includes(String(data.status || '').toUpperCase())) { lease.terminal(data); return; }
    } catch (_) { /* Keep remote ownership when transport status is unknown. */ }
    finally { lease.reconciling = false; lease.reconcileController = null; }
    if (lease.phase === 'uncertain') lease.reconcileTimer = setTimeout(() => this.reconcile(lease), 15000);
  }
  schedule() {
    if (this.timer) clearTimeout(this.timer);
    this.timer = this.waiters.length ? setTimeout(() => { this.timer = null; this.pump(); }, this.refreshMs) : null;
    try { this.onChange(this.snapshot(), this.waiters.length); } catch (_) {}
  }
  reset() {
    // Refresh observations, never erase live ownership or invalidate old releases.
    for (const s of this.states.values()) s.checkedAt = 0;
    this.pump();
  }
  resolveUncertain(id) {
    for (const s of this.states.values()) {
      const lease = s.leases.get(id);
      if (lease?.phase === 'uncertain') { lease.terminal(); return true; }
    }
    return false;
  }
}
