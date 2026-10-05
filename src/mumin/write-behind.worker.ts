import { Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import Redis from 'ioredis';
import { RedisService } from '../redis/redis.service';
import { KEY_PREFIX } from './mumin.cache';
import { MuminRepository } from './mumin.repository';
import { DIRTY_KEY, DLQ_KEY, MuminRedisWriter, STREAM_KEY } from './mumin.redis-writer';
import { MuminRecord } from './mumin.types';

const GROUP = 'mssql-writer';
const READ_COUNT = 2000;
/** An entry un-ACKed this long belongs to a dead consumer and is taken over. */
const CLAIM_IDLE_MS = 30000;
const CLAIM_EVERY_MS = 15000;

/**
 * Write-behind: drains the Redis Stream into MSSQL.
 *  - consumer group => at-least-once; un-ACKed entries are re-read after a crash/restart
 *  - entries carry the FULL row => upsert is idempotent, so re-processing is safe
 *  - a batch is coalesced by Mumin_ID (last write wins) and applied in ONE MSSQL transaction
 *  - rows MSSQL rejects go to a dead-letter stream instead of blocking the queue
 */
@Injectable()
export class WriteBehindWorker implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger(WriteBehindWorker.name);
  private readonly consumer = `app-${process.pid}`;
  private conn?: Redis;
  private running = false;
  private loop?: Promise<void>;

  constructor(
    private readonly redis: RedisService,
    private readonly repo: MuminRepository,
    private readonly writer: MuminRedisWriter,
  ) {}

  onApplicationBootstrap() {
    if (process.env.DISABLE_JOBS !== '1') void this.start();
  }

  async onModuleDestroy() {
    this.running = false;
    this.conn?.disconnect();
    await this.loop?.catch(() => undefined);
  }

  async start() {
    if (this.running) return;
    this.running = true;
    this.conn = this.redis.client.duplicate(); // blocking reads need their own connection
    await this.conn.xgroup('CREATE', STREAM_KEY, GROUP, '0', 'MKSTREAM').catch((e) => {
      if (!String(e.message).includes('BUSYGROUP')) throw e;
    });
    this.loop = this.run();
    this.logger.log('Write-behind worker started');
  }

  private async run() {
    let backlog = true; // first drain entries this consumer read but never ACKed
    let lastClaim = 0;
    while (this.running) {
      try {
        // Crash recovery across processes: take over entries a dead consumer read but never ACKed.
        if (Date.now() - lastClaim > CLAIM_EVERY_MS) {
          lastClaim = Date.now();
          const claimed = await this.claimStale();
          if (claimed) this.logger.warn(`Recovered ${claimed} un-persisted writes from dead consumers`);
        }
        const res: any = await this.conn!.xreadgroup(
          'GROUP', GROUP, this.consumer, 'COUNT', READ_COUNT, 'BLOCK', 1000, 'STREAMS', STREAM_KEY, backlog ? '0' : '>',
        );
        const entries: [string, string[]][] = res?.[0]?.[1] ?? [];
        if (backlog && entries.length === 0) { backlog = false; continue; }
        if (entries.length) await this.apply(entries);
      } catch (e) {
        if (!this.running) return;
        this.logger.error(`Write-behind batch failed, retrying: ${(e as Error).message}`);
        await new Promise((r) => setTimeout(r, 2000));
        backlog = true;
      }
    }
  }

  /** XAUTOCLAIM entries idle longer than CLAIM_IDLE_MS and apply them; returns how many. */
  private async claimStale(): Promise<number> {
    let cursor = '0-0';
    let total = 0;
    do {
      const res: any = await this.conn!.xautoclaim(STREAM_KEY, GROUP, this.consumer, CLAIM_IDLE_MS, cursor, 'COUNT', READ_COUNT);
      cursor = res[0];
      const entries: [string, string[]][] = (res[1] ?? []).filter((e: any) => e && e[1]);
      if (entries.length) {
        await this.apply(entries);
        total += entries.length;
      }
    } while (cursor !== '0-0');
    return total;
  }

  private async apply(entries: [string, string[]][]) {
    const latest = new Map<number, { sid: string; row: MuminRecord }>();
    for (const [sid, fields] of entries) {
      const f: Record<string, string> = {};
      for (let i = 0; i < fields.length; i += 2) f[fields[i]] = fields[i + 1];
      latest.set(Number(f.id), { sid, row: JSON.parse(f.row) });
    }
    const rows = [...latest.values()].map((v) => v.row);
    const rejected: number[] = [];
    try {
      await this.repo.upsertMany(rows);
    } catch (e) {
      // MSSQL unreachable / timeout: throw => nothing is ACKed, the whole batch is retried later.
      if (!isRejectedBySqlServer(e)) throw e;
      // A row MSSQL itself rejects: isolate it, dead-letter it, keep the queue moving.
      this.logger.warn(`Batch upsert rejected (${(e as Error).message}); applying row by row`);
      for (const r of rows) {
        try {
          await this.repo.upsertMany([r]);
        } catch (err) {
          if (!isRejectedBySqlServer(err)) throw err;
          rejected.push(r.Mumin_ID);
          await this.redis.client.xadd(DLQ_KEY, '*', 'id', String(r.Mumin_ID), 'row', JSON.stringify(r), 'error', String((err as Error).message));
        }
      }
    }
    const ids = entries.map(([sid]) => sid);
    await this.redis.client.multi().xack(STREAM_KEY, GROUP, ...ids).xdel(STREAM_KEY, ...ids).exec();
    await this.writer.markClean([...latest.entries()].map(([id, v]) => [String(id), v.sid]));
    if (rejected.length) {
      // Redis must not keep showing data MSSQL refused: drop the key, next read reloads the MSSQL row.
      const dirty = await this.writer.dirtyOf(rejected);
      const stale = rejected.filter((id) => !dirty.has(id)).map((id) => `${KEY_PREFIX}${id}`);
      if (stale.length) await this.redis.client.unlink(...stale);
    }
  }

  /** Resolves when every Redis write has reached MSSQL (used by the benchmark). */
  async drained(timeoutMs = 600000) {
    const until = Date.now() + timeoutMs;
    while (Date.now() < until) {
      if ((await this.redis.client.hlen(DIRTY_KEY)) === 0) return;
      await new Promise((r) => setTimeout(r, 20));
    }
    throw new Error('write-behind did not drain in time');
  }
}

/**
 * true  = SQL Server executed the statement and refused the data (constraint, conversion, truncation...):
 *         carries a SQL Server error number -> retrying will never succeed -> dead-letter.
 * false = connection / timeout / pool errors -> transient -> retry.
 */
function isRejectedBySqlServer(e: unknown): boolean {
  const err = e as { number?: number; originalError?: { number?: number; info?: { number?: number } } };
  const n = err?.number ?? err?.originalError?.number ?? err?.originalError?.info?.number;
  return typeof n === 'number' && n > 0;
}
