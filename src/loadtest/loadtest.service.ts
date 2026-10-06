import { ConflictException, Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ChildProcess, spawn } from 'child_process';
import { randomUUID } from 'crypto';
import * as http from 'http';
import { join } from 'path';
import { createInterface } from 'readline';
import { RedisService } from '../redis/redis.service';

const STATE_KEY = 'mumin:loadtest';          // current/last run (JSON), readable from every instance
const CANCEL_KEY = 'mumin:loadtest:cancel';  // set by any instance, honoured by the one running it
const STATE_TTL_S = 7 * 24 * 3600;

export interface LoadTestState {
  id: string;
  status: 'running' | 'passed' | 'failed' | 'cancelled' | 'error';
  startedAt: string;
  finishedAt?: string;
  requests: number;
  concurrency: number;
  instances: string[];
  runner: string;
  heartbeatAt?: string;
  phase?: string;
  phases: Record<string, { title: string; total: number; done: number; elapsedMs: number; statuses: Record<string, number>; perInstance: Record<string, number>; metrics: Record<string, unknown> }>;
  checks: { phase: string; label: string; ok: boolean }[];
  log: string[];
  error?: string;
}

/**
 * Runs the distributed 1-lakh test (dist/cli/dtest.js) as a separate process so the load generator
 * does not compete with this API process, and mirrors its "@@{json}" progress events into Redis.
 */
@Injectable()
export class LoadTestService {
  private readonly logger = new Logger(LoadTestService.name);
  private child?: ChildProcess;
  private state?: LoadTestState;
  private saveTimer?: NodeJS.Timeout;

  constructor(private readonly redis: RedisService, private readonly config: ConfigService) {}

  async status(): Promise<LoadTestState | { status: 'none' }> {
    const raw = await this.redis.client.get(STATE_KEY);
    if (!raw) return { status: 'none' };
    const s: LoadTestState = JSON.parse(raw);
    // The instance running the test died: never report 'running' forever.
    if (s.status === 'running' && Date.now() - Date.parse(s.heartbeatAt ?? s.startedAt) > 10000) {
      s.status = 'error';
      s.error = `runner ${s.runner} stopped responding`;
    }
    return s;
  }

  async start(requests: number, concurrency: number) {
    const current = await this.status();
    if (current.status === 'running') throw new ConflictException('A load test is already running');
    const instances = await this.reachableInstances();
    const ports = instances.map((u) => u.split(':').pop()!);
    const runner = `${this.config.get('INSTANCE_NAME', 'app')}:${process.env.PORT ?? 3000}`;
    this.state = {
      id: randomUUID(), status: 'running', startedAt: new Date().toISOString(), requests, concurrency,
      instances, runner, phases: {}, checks: [], log: [],
    };
    await this.redis.client.del(CANCEL_KEY);
    await this.save();

    this.child = spawn(process.execPath, [join(__dirname, '..', 'cli', 'dtest.js'), String(requests), String(concurrency)], {
      cwd: process.cwd(),
      env: { ...process.env, API_PORTS: ports.join(','), PROGRESS_JSON: '1' },
      windowsHide: true,
    });
    createInterface({ input: this.child.stdout! }).on('line', (l) => this.onLine(l));
    createInterface({ input: this.child.stderr! }).on('line', (l) => this.addLog(l));
    this.child.on('exit', (code) => void this.onExit(code));
    // cancel requests can arrive at any instance -> poll the flag
    const poll = setInterval(async () => {
      if (!this.child) return clearInterval(poll);
      if (await this.redis.client.get(CANCEL_KEY)) {
        this.state!.status = 'cancelled';
        this.child.kill();
      }
    }, 500);
    this.saveTimer = setInterval(() => void this.save(), 500);
    this.logger.log(`Load test started: ${requests} requests over ${instances.join(', ')}`);
    return this.state;
  }

  async cancel() {
    await this.redis.client.set(CANCEL_KEY, '1', 'EX', 600);
    return { cancelling: true };
  }

  private onLine(line: string) {
    if (!line.startsWith('@@')) return this.addLog(line);
    const e = JSON.parse(line.slice(2));
    const s = this.state!;
    const ph = (id: string) => (s.phases[id] ??= { title: '', total: 0, done: 0, elapsedMs: 0, statuses: {}, perInstance: {}, metrics: {} });
    switch (e.type) {
      case 'phase': s.phase = e.phase; Object.assign(ph(e.phase), { title: e.title, total: e.total }); break;
      case 'progress': Object.assign(ph(e.phase), { done: e.done, total: e.total, elapsedMs: e.elapsedMs, statuses: e.statuses, perInstance: e.perInstance }); break;
      case 'metric': ph(e.phase).metrics[e.label] = e.value; break;
      case 'check': s.checks.push({ phase: e.phase, label: e.label, ok: e.ok }); break;
      case 'end': if (s.status === 'running') s.status = e.pass ? 'passed' : e.error ? 'error' : 'failed'; if (e.error) s.error = e.error; break;
    }
  }

  private addLog(line: string) {
    if (!this.state || !line.trim()) return;
    this.state.log.push(line);
    if (this.state.log.length > 200) this.state.log.shift();
  }

  private async onExit(code: number | null) {
    const s = this.state!;
    if (s.status === 'running') s.status = code === 0 ? 'passed' : 'error';
    s.finishedAt = new Date().toISOString();
    clearInterval(this.saveTimer);
    this.child = undefined;
    await this.save();
    await this.redis.client.del(CANCEL_KEY);
    this.logger.log(`Load test finished: ${s.status}`);
  }

  private async save() {
    if (this.state) this.state.heartbeatAt = new Date().toISOString();
    if (this.state) await this.redis.client.set(STATE_KEY, JSON.stringify(this.state), 'EX', STATE_TTL_S);
  }

  /** Instances from LOADTEST_PORTS (default 3001-3004) plus this one, keeping only those that answer. */
  private async reachableInstances(): Promise<string[]> {
    const own = Number(process.env.PORT ?? 3000);
    const ports = [...new Set([...String(this.config.get('LOADTEST_PORTS', '3001,3002,3003,3004')).split(',').map(Number), own])];
    const ok = await Promise.all(ports.map((p) => ping(p)));
    return ports.filter((_, i) => ok[i]).sort((a, b) => a - b).map((p) => `localhost:${p}`);
  }
}

function ping(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const req = http.get({ host: 'localhost', port, path: '/cache/queue', timeout: 1500 }, (res) => { res.resume(); resolve(res.statusCode === 200); });
    req.on('timeout', () => { req.destroy(); resolve(false); });
    req.on('error', () => resolve(false));
  });
}
