import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { AsyncLocalStorage } from 'async_hooks';
import { createHash } from 'crypto';
import { createWriteStream, mkdirSync, WriteStream } from 'fs';
import { join } from 'path';
import Redis from 'ioredis';
import { RedisService } from '../redis/redis.service';

export const OPLOG_KEY = 'tb:oplog';            // STREAM: one entry per action (shared by all instances, for the UI)
export const OPLOG_READS_KEY = 'tb:oplog:reads'; // '1' = also log read-only actions (UI polling); off by default
const OPLOG_MAX = 3000;
const FILE = join(process.cwd(), 'logs', 'tickets-redis.log');

/** One Redis command: c = command, k = key, a = other args, r = reply, lua = script it ran in (if any) */
export interface Op { c: string; k: string; a: string; r: string; lua?: string; ms?: number }
export interface OpEntry {
  ts: number; instance: string; action: string; kind: 'write' | 'read';
  zone?: string; rid?: string; its?: string; result?: string; ms: number; ops: Op[]; note?: string;
}
interface Ctx { ops: Op[] }

const short = (v: unknown): string => {
  if (v === null || v === undefined) return 'nil';
  if (Buffer.isBuffer(v)) v = v.toString();
  if (typeof v === 'number') return String(v);
  if (typeof v === 'string') return v.length > 80 ? `"${v.slice(0, 77)}..." (${v.length} bytes)` : `"${v}"`;
  if (Array.isArray(v)) return `[${v.length} items]`;
  if (v instanceof Error) return `ERR ${v.message}`;
  if (typeof v === 'object') return `{${Object.keys(v as object).length} fields}`;
  return String(v);
};

/**
 * Redis key log for the ticket module. Every action runs inside trace(); every command sent on the tickets
 * Redis connection inside it is recorded (command, key, args, reply), and the ops recorded INSIDE Lua scripts
 * (returned by withRedisLog) are added after the script call. The finished entry goes to the console,
 * logs/tickets-redis.log (JSON lines) and the Redis stream tb:oplog (UI "Redis log" tab).
 */
@Injectable()
export class TicketsOpLog implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger('TicketsRedisLog');
  private readonly als = new AsyncLocalStorage<Ctx>();
  private readonly scripts = new Map<string, string>(); // sha1 -> script name
  private file?: WriteStream;
  private logReads = false;
  private timer?: NodeJS.Timeout;
  readonly instance = `${process.env.INSTANCE_NAME ?? 'app'}:${process.env.PORT ?? 3000}`;
  /** Connection used by the ticket module; every command on it is recorded while inside trace(). */
  readonly client: Redis;

  constructor(private readonly redis: RedisService) {
    // No auto-pipelining on this connection: it batches commands from different callers into one flush,
    // so a command would be recorded under whichever action happened to trigger the flush.
    this.client = this.redis.client.duplicate({ enableAutoPipelining: false });
    const c = this.client as any;
    const send = c.sendCommand.bind(c);
    c.sendCommand = (cmd: any, ...rest: unknown[]) => {
      const ctx = this.als.getStore();
      if (ctx && cmd?.name) this.record(ctx, cmd);
      return send(cmd, ...rest);
    };
  }

  async onModuleInit() {
    try {
      mkdirSync(join(process.cwd(), 'logs'), { recursive: true });
      this.file = createWriteStream(FILE, { flags: 'a' });
    } catch (e) {
      this.logger.warn(`cannot open ${FILE}: ${(e as Error).message}`);
    }
    const poll = async () => { this.logReads = (await this.redis.client.get(OPLOG_READS_KEY).catch(() => null)) === '1'; };
    await poll();
    this.timer = setInterval(poll, 2000);
  }

  onModuleDestroy() {
    clearInterval(this.timer);
    this.file?.end();
    this.client.disconnect();
  }

  /** Define a Lua command on the tickets connection and remember its sha1 so the log can name it. */
  defineScript(name: string, numberOfKeys: number, lua: string) {
    (this.client as any).defineCommand(name, { numberOfKeys, lua });
    this.scripts.set(createHash('sha1').update(lua).digest('hex'), name);
  }

  /** Run a script defined with withRedisLog: returns its answer, records its inner ops. */
  async lua(name: string, ...keysAndArgs: (string | number)[]): Promise<any> {
    const reply = await (this.client as any)[name](...keysAndArgs);
    const [res, log] = reply as [unknown, string];
    const ctx = this.als.getStore();
    if (ctx) {
      let inner: string[][] = [];
      try { const p = JSON.parse(log); if (Array.isArray(p)) inner = p; } catch { /* empty log is "{}" */ }
      for (const [c, k, a, r] of inner) ctx.ops.push({ c, k, a, r, lua: name });
    }
    return res;
  }

  private record(ctx: Ctx, cmd: any) {
    const name = String(cmd.name).toUpperCase();
    const args: unknown[] = (cmd.args ?? []).map((x: unknown) => (Buffer.isBuffer(x) ? x.toString() : x));
    let op: Op;
    if (name === 'EVALSHA' || name === 'EVAL') {
      const sha = String(args[0]);
      const n = Number(args[1]);
      const script = name === 'EVALSHA' ? this.scripts.get(sha) ?? sha.slice(0, 10) : 'script';
      op = { c: `${name} ${script}`, k: args.slice(2, 2 + n).join(' '), a: args.slice(2 + n).map(short).join(' '), r: '' };
    } else if (['UNLINK', 'DEL', 'EXISTS'].includes(name)) {
      op = { c: name, k: args.slice(0, 12).map(String).join(' ') + (args.length > 12 ? ` +${args.length - 12} more` : ''), a: '', r: '' };
    } else if (['MULTI', 'EXEC', 'SCAN', 'TIME', 'PING'].includes(name)) {
      op = { c: name, k: '', a: args.slice(0, 8).map(short).join(' '), r: '' };
    } else {
      op = { c: name, k: args.length ? String(args[0]) : '', a: args.slice(1, 9).map(short).join(' ') + (args.length > 9 ? ` +${args.length - 9} more` : ''), r: '' };
    }
    const t = Date.now();
    ctx.ops.push(op);
    Promise.resolve(cmd.promise).then((r: unknown) => {
      // personal data stays out of the log: a Mumin_Mast record is logged by key and size only
      op.r = op.k.startsWith('mumin:rec:') && r ? `(Mumin_Mast record found, ${String(r).length} bytes)` : short(r);
      op.ms = Date.now() - t;
    }, (e: Error) => { op.r = `ERR ${e.message}`; op.ms = Date.now() - t; });
  }

  /**
   * Run `fn` as one logged action. `describe` turns its answer into the result text (and may add rid/its).
   * Read-only actions are only logged when switched on (UI polls them every 1.5 s).
   */
  async trace<T>(action: string, meta: { kind?: 'write' | 'read'; zone?: string; rid?: string; its?: string; note?: string },
    fn: () => Promise<T>, describe?: (r: T) => { result?: string; rid?: string; its?: string; note?: string; skip?: boolean }): Promise<T> {
    const kind = meta.kind ?? 'write';
    if (kind === 'read' && !this.logReads) return fn();
    const ctx: Ctx = { ops: [] };
    const t0 = Date.now();
    let res: T | undefined;
    let err: Error | undefined;
    try {
      res = await this.als.run(ctx, fn);
      return res;
    } catch (e) {
      err = e as Error;
      throw e;
    } finally {
      const d: { result?: string; rid?: string; its?: string; note?: string; skip?: boolean } = !err && describe ? describe(res as T) : {};
      if (!d.skip) { // skip = nothing happened (e.g. idle expiry sweep)
        this.emit({
          ts: t0, instance: this.instance, action, kind, zone: meta.zone, rid: d.rid ?? meta.rid, its: d.its ?? meta.its,
          result: err ? `ERROR ${err.message}` : d.result ?? 'OK', ms: Date.now() - t0, ops: ctx.ops, note: d.note ?? meta.note,
        });
      }
    }
  }

  /** A summary entry without per-command detail (used for bulk simulations). */
  note(action: string, meta: Partial<OpEntry>) {
    this.emit({ ts: Date.now(), instance: this.instance, action, kind: 'write', ms: 0, ops: [], ...meta });
  }

  private emit(e: OpEntry) {
    // replies of the last commands settle on the next tick
    setImmediate(() => {
      const head = `${e.action}${e.zone ? ` zone=${e.zone}` : ''}${e.its ? ` its=${e.its}` : ''}${e.rid ? ` rid=${e.rid}` : ''} -> ${e.result} (${e.ms} ms, ${e.ops.length} Redis ops)${e.note ? ` | ${e.note}` : ''}`;
      if (e.ops.length > 300) e.ops = [...e.ops.slice(0, 300), { c: '...', k: '', a: `${e.ops.length - 300} more Redis ops not listed`, r: '' } as Op];
      const lines = e.ops.map((o, i) => `    ${String(i + 1).padStart(3)}. ${o.lua ? '  [lua] ' : ''}${o.c} ${o.k}${o.a ? ` ${o.a}` : ''} -> ${o.r}`);
      this.logger.log([head, ...lines].join('\n'));
      this.file?.write(JSON.stringify(e) + '\n');
      this.redis.client.xadd(OPLOG_KEY, 'MAXLEN', '~', OPLOG_MAX, '*', 'e', JSON.stringify(e)).catch(() => undefined);
    });
  }

  // ---- for the UI ----
  async list(limit: number, zone?: string, action?: string, q?: string) {
    const raw = await this.redis.client.xrevrange(OPLOG_KEY, '+', '-', 'COUNT', zone || action || q ? OPLOG_MAX : limit);
    let out = raw.map(([id, f]) => ({ id, ...(JSON.parse(f[1]) as OpEntry) }));
    if (zone) out = out.filter((e) => !e.zone || e.zone === zone);
    if (action) out = out.filter((e) => e.action === action);
    if (q) {
      const s = q.toLowerCase();
      out = out.filter((e) => JSON.stringify(e).toLowerCase().includes(s));
    }
    return { logReads: this.logReads, total: await this.redis.client.xlen(OPLOG_KEY), file: FILE, entries: out.slice(0, limit) };
  }

  async setReads(on: boolean) {
    if (on) await this.redis.client.set(OPLOG_READS_KEY, '1'); else await this.redis.client.del(OPLOG_READS_KEY);
    this.logReads = on;
    return { logReads: on };
  }

  async clear() {
    await this.redis.client.del(OPLOG_KEY);
    return { cleared: true };
  }
}
