import { BadRequestException, ConflictException, Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import { ChildProcess, spawn } from 'child_process';
import { join } from 'path';
import { createInterface } from 'readline';
import { RedisService } from '../redis/redis.service';
import { TicketsNodes } from './tickets.nodes';
import { TicketsOpLog } from './tickets.oplog';

const STATE_KEY = 'tb:loadtest';            // current / last run (JSON), readable from every node
const CANCEL_KEY = 'tb:loadtest:cancel';

export interface TicketLoadState {
  id: string; status: 'running' | 'passed' | 'failed' | 'cancelled' | 'error';
  startedAt: number; finishedAt?: number; heartbeatAt?: number; runner: string; nodes: string[];
  params: Record<string, any>; phase?: string;
  phases: Record<string, { title: string; total?: number; done?: number; elapsedMs?: number; statuses?: Record<string, number>; outcomes?: Record<string, number>; perNode?: Record<string, number>; rounds?: unknown[] }>;
  metrics: Record<string, unknown>; checks: { label: string; ok: boolean }[]; end?: any; log: string[]; error?: string;
}

/**
 * Runs dist/cli/ticketload.js (1 lakh booking requests over every live node) as a separate process so the
 * load generator does not compete with this API, and mirrors its "@@{json}" progress into Redis.
 */
@Injectable()
export class TicketsLoadTest implements OnModuleDestroy {
  private readonly logger = new Logger('TicketsLoadTest');
  private child?: ChildProcess;
  private state?: TicketLoadState;
  private timer?: NodeJS.Timeout;

  constructor(private readonly redis: RedisService, private readonly nodes: TicketsNodes, private readonly oplog: TicketsOpLog) {}

  onModuleDestroy() {
    this.child?.kill();
  }

  async status(): Promise<TicketLoadState | { status: 'none' }> {
    const raw = await this.redis.client.get(STATE_KEY);
    if (!raw) return { status: 'none' };
    const s = JSON.parse(raw) as TicketLoadState;
    if (s.status === 'running' && Date.now() - (s.heartbeatAt ?? s.startedAt) > 10000) { s.status = 'error'; s.error = `runner ${s.runner} stopped responding`; }
    return s;
  }

  async start(params: Record<string, any>) {
    if ((await this.status()).status === 'running') throw new ConflictException('a load test is already running');
    const urls = await this.nodes.aliveUrls();
    if (!urls.length) throw new BadRequestException('no live nodes');
    const id = Date.now().toString(36);
    this.state = {
      id, status: 'running', startedAt: Date.now(), runner: this.nodes.instance, nodes: urls.map((u) => u.replace('http://', '')),
      params, phases: {}, metrics: {}, checks: [], log: [],
    };
    await this.redis.client.del(CANCEL_KEY);
    await this.save();
    this.child = spawn(process.execPath, [join(__dirname, '..', 'cli', 'ticketload.js')], {
      cwd: process.cwd(), windowsHide: true,
      env: { ...process.env, NODE_URLS: urls.join(','), TL_PARAMS: JSON.stringify(params), TL_RUN: id, PROGRESS_JSON: '1' },
    });
    createInterface({ input: this.child.stdout! }).on('line', (l) => this.onLine(l));
    createInterface({ input: this.child.stderr! }).on('line', (l) => this.addLog(l));
    this.child.on('exit', (code) => void this.onExit(code));
    this.timer = setInterval(async () => {
      await this.save();
      const child = this.child; // may exit (and be cleared) during the await below
      if (child && (await this.redis.client.get(CANCEL_KEY)) && this.child === child) { this.state!.status = 'cancelled'; child.kill(); }
    }, 500);
    this.logger.log(`Ticket load test ${id} started over ${urls.join(', ')}`);
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
    const ph = (k: string) => (s.phases[k] ??= { title: '' });
    switch (e.type) {
      case 'phase': s.phase = e.phase; Object.assign(ph(e.phase), { title: e.title, total: e.total }); break;
      case 'progress': { const { type: _t, phase, ...rest } = e; Object.assign(ph(phase), rest); break; }
      case 'metric': s.metrics[`${e.phase}.${e.label}`] = e.value; break;
      case 'check': s.checks.push({ label: e.label, ok: e.ok }); break;
      case 'end':
        s.end = e;
        if (s.status === 'running') s.status = e.error ? 'error' : e.pass ? 'passed' : 'failed';
        if (e.error) s.error = e.error;
        break;
    }
  }

  private addLog(line: string) {
    if (!this.state || !line.trim()) return;
    this.state.log.push(line);
    if (this.state.log.length > 100) this.state.log.shift();
  }

  private async onExit(code: number | null) {
    const s = this.state!;
    clearInterval(this.timer);
    if (s.status === 'running') s.status = code === 0 ? 'passed' : 'error';
    s.finishedAt = Date.now();
    this.child = undefined;
    await this.save();
    await this.redis.client.del(CANCEL_KEY);
    const p1 = s.metrics['P1.result'] as any;
    // one summary in the Redis key log (each load-test request is not logged one by one)
    this.oplog.note('load-test', {
      zone: s.params.zone || 'load-test-zone-1', ms: s.finishedAt - s.startedAt,
      result: `${s.status.toUpperCase()}: ${s.params.total} requests over ${s.nodes.length} node(s)${p1 ? `, ${p1.perSecond}/s, p95 ${p1.latencyMs.p95} ms` : ''}; ${s.checks.filter((c) => c.ok).length}/${s.checks.length} checks passed`,
      note: `nodes: ${s.nodes.join(', ')}. Per-request Redis commands are not listed (bulk test); each booking = 1 atomic script tbBook on the keys shown in a manual "book" entry.`,
    });
    this.logger.log(`Ticket load test ${s.id} finished: ${s.status}`);
  }

  private async save() {
    if (!this.state) return;
    this.state.heartbeatAt = Date.now();
    await this.redis.client.set(STATE_KEY, JSON.stringify(this.state), 'EX', 7 * 24 * 3600);
  }
}
