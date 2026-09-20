// RunningHub cross-tab admission and recovery, 2026-09-20. Repository AFPL.
import { runningHubFetch } from './runninghub-scheduler.mjs';

const PREFIX = 'st-chatu8:rh:v1:';
const ACTIVE = new Set(['reserved', 'submitting', 'submitted', 'uncertain']);
export class RunningHubLedger {
  constructor({ keys, storage = globalThis.localStorage, locks = globalThis.navigator?.locks,
    crypto = globalThis.crypto, fetcher = runningHubFetch, initialPolicy = () => ({}), changed = () => {} }) {
    Object.assign(this, { keys, storage, locks, crypto, fetcher, initialPolicy, changed });
    this.changed = () => { try { changed(); } catch (_) {} };
    this.owner = null;
    this.keyIds = new Map(); this.busy = new Set(); this.error = '';
    this.ready = this.initialize();
    // Readiness errors are reported by claim/UI, not as unhandled rejections.
    this.ready.catch(error => { this.close(); this.error = error.message; this.changed(); });
  }
  async initialize() {
    if (!this.locks?.request || !this.locks?.query) throw new Error('浏览器不支持安全的多标签页协调，请使用 HTTPS 或 localhost 的现代浏览器');
    if (!this.crypto?.randomUUID || !this.crypto?.subtle) throw new Error('浏览器不支持安全的任务标识，请使用 HTTPS 或 localhost');
    this.owner = this.crypto.randomUUID();
    await new Promise((resolve, reject) => {
      this.ownerRequest = this.locks.request(PREFIX + 'owner:' + this.owner, async () => {
        resolve(); await new Promise(done => { this.releaseOwner = done; });
      }).catch(reject);
    });
    const check = PREFIX + 'check:' + this.owner;
    this.storage.setItem(check, '1'); this.storage.removeItem(check);
    await this.syncKeys();
    await this.locks.request(PREFIX + 'admission', () => {
      const policy = this.policy(), initial = this.initialPolicy();
      policy.globalLimit ??= initial.globalLimit ?? 0;
      policy.keys ||= {};
      for (const [key, keyId] of this.keyIds) policy.keys[keyId] ??= initial.keys?.[key] || {};
      this.storage.setItem(PREFIX + 'policy', JSON.stringify(policy));
    });
    await this.recover(false);
    this.timer = setInterval(() => { this.recover().catch(error => { this.error = error.message; this.changed(); }); }, 15000);
  }
  async syncKeys() {
    for (const key of this.keys()) if (!this.keyIds.has(key)) {
      const hash = await this.crypto.subtle.digest('SHA-256', new TextEncoder().encode(key));
      this.keyIds.set(key, [...new Uint8Array(hash)].map(x => x.toString(16).padStart(2, '0')).join(''));
    }
  }
  records() {
    const result = [];
    for (let i = 0; i < this.storage.length; i++) {
      const name = this.storage.key(i);
      if (!name?.startsWith(PREFIX + 'job:')) continue;
      const raw = this.storage.getItem(name);
      if (raw == null) continue;
      const value = JSON.parse(raw);
      if (value.version !== 1 || typeof value.id !== 'string' || typeof value.keyId !== 'string' || !['reserved', 'submitting', 'submitted', 'uncertain', 'completed'].includes(value.phase)) {
        throw new Error('RunningHub 恢复记录损坏，已停止新增提交；请先备份并核对平台任务');
      }
      result.push(value);
    }
    return result;
  }
  write(record) { this.storage.setItem(PREFIX + 'job:' + record.id, JSON.stringify(record)); this.changed(); }
  remove(id) { this.storage.removeItem(PREFIX + 'job:' + id); this.changed(); }
  read(id) { const raw = this.storage.getItem(PREFIX + 'job:' + id); return raw ? JSON.parse(raw) : null; }
  knownRemoteCount(key) {
    const keyId = this.keyIds.get(key);
    return this.records().filter(r => r.keyId === keyId && ACTIVE.has(r.phase) && r.remoteTaskId).length;
  }
  policy() { return JSON.parse(this.storage.getItem(PREFIX + 'policy') || '{}'); }
  async configure({ globalLimit, key, limit, paused }) {
    await this.ready; await this.syncKeys();
    await this.locks.request(PREFIX + 'admission', () => {
      const policy = this.policy();
      if (globalLimit !== undefined) policy.globalLimit = globalLimit;
      if (key && this.keyIds.has(key)) {
        policy.keys ||= {};
        const entry = policy.keys[this.keyIds.get(key)] ||= {};
        if (limit !== undefined) entry.limit = limit;
        if (paused !== undefined) entry.paused = paused;
      }
      this.storage.setItem(PREFIX + 'policy', JSON.stringify(policy));
    });
    this.changed();
  }
  async claim(key, cap, globalCap, taskId) {
    await this.ready; await this.syncKeys();
    return this.locks.request(PREFIX + 'admission', () => {
      const active = this.records().filter(r => ACTIVE.has(r.phase));
      const keyId = this.keyIds.get(key);
      if (!keyId) throw new Error('Key 未配置，无法登记任务');
      const policy = this.policy(), keyPolicy = policy.keys?.[keyId];
      if (keyPolicy?.paused) return null;
      if (Number(keyPolicy?.limit) > 0) cap = Math.min(cap, Number(keyPolicy.limit));
      if (Number(policy.globalLimit) > 0) globalCap = Math.min(globalCap, Number(policy.globalLimit));
      if (active.length >= globalCap || active.filter(r => r.keyId === keyId).length >= cap) return null;
      const record = { version: 1, id: this.crypto.randomUUID(), owner: this.owner, keyId,
        taskId: taskId || '', phase: 'reserved', remoteTaskId: null, createdAt: Date.now() };
      this.write(record);
      return record.id;
    });
  }
  transition(id, phase, extra = {}) {
    const r = this.read(id);
    if (!r || r.owner !== this.owner || !ACTIVE.has(r.phase)) throw new Error('任务通道凭证已失效');
    this.write({ ...r, ...extra, phase });
  }
  finish(id, data) {
    const r = this.read(id);
    if (!r || r.owner !== this.owner) return;
    if (data) this.write({ ...r, phase: 'completed', status: String(data.status || '').toUpperCase(),
      results: (data.results || []).map(item => ({ url: item.url, outputType: item.outputType })), finishedAt: Date.now() });
    else this.remove(id);
  }
  async recover(waitForQueries = true) {
    if (this.recovering) return;
    this.recovering = true;
    try {
      await this.syncKeys();
      const jobs = await this.locks.request(PREFIX + 'admission', async () => {
        const { held } = await this.locks.query();
        const owners = new Set(held.map(lock => lock.name));
        const result = [];
        for (const r of this.records()) {
          if (!ACTIVE.has(r.phase) || owners.has(PREFIX + 'owner:' + r.owner) && r.owner !== this.owner) continue;
          // An active local runner owns these; only adopted jobs are recovered here.
          if (r.owner === this.owner && !r.recovered) continue;
          if (r.phase === 'reserved') { this.remove(r.id); continue; }
          const key = [...this.keyIds].find(([, id]) => id === r.keyId)?.[0];
          if (!key) continue; // Missing credentials retain capacity and remain visible.
          const adopted = { ...r, owner: this.owner, recovered: true, phase: 'uncertain' };
          this.write(adopted);
          if (r.remoteTaskId && !this.busy.has(r.id)) result.push({ record: adopted, key });
        }
        return result;
      });
      const queries = Promise.all(jobs.map(({ record, key }) => this.queryRecovered(record, key)));
      if (waitForQueries) await queries;
      else queries.catch(error => { this.error = error.message; this.changed(); });
      this.error = '';
    } finally { this.recovering = false; this.changed(); }
  }
  async queryRecovered(record, key) {
    this.busy.add(record.id);
    try {
      const response = await this.fetcher('https://www.runninghub.ai/openapi/v2/query', {
        method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${key}` },
        body: JSON.stringify({ taskId: record.remoteTaskId })
      }, 15000);
      const data = await response.json();
      if (['SUCCESS', 'FAILED', 'CANCEL', 'CANCELLED'].includes(String(data.status || '').toUpperCase())) this.finish(record.id, data);
    } catch (_) { /* Never infer remote termination from a failed read. */ }
    finally { this.busy.delete(record.id); }
  }
  async resolve(id) {
    await this.ready;
    return this.locks.request(PREFIX + 'admission', async () => {
      const record = this.read(id);
      if (!record) return;
      const { held } = await this.locks.query();
      if (record.owner !== this.owner && held.some(l => l.name === PREFIX + 'owner:' + record.owner)) throw new Error('请在任务所属标签页操作');
      if (record.phase === 'completed' || record.phase === 'uncertain' || record.phase === 'submitting') this.remove(id);
      else throw new Error('任务仍由生成流程管理，不能移除');
    });
  }
  close() { clearInterval(this.timer); this.releaseOwner?.(); }
}
