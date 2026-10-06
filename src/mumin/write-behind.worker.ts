import { Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import Redis from 'ioredis';
import { hostname } from 'os';
import { RedisService } from '../redis/redis.service';
import { KEY_PREFIX } from './mumin.cache';
import { MuminRepository } from './mumin.repository';
import { DIRTY_KEY, DLQ_KEY, LEADER_KEY, MuminRedisWriter, STREAM_KEY } from './mumin.redis-writer';
import { MuminRecord } from './mumin.types';

const GROUP = 'mssql-writer';
const READ_COUNT = 2000;
/** Leader lease: renewed every loop (<= ~1 s); if the leader dies another instance takes over after this. */
const LEADER_LEASE_MS = 15000;

/** Take the lease if free, renew it if ours. KEYS = [leaderKey]  ARGV = [me, leaseMs] */
const LEADER_LUA = `
local cur = redis.call('GET', KEYS[1])
if cur == ARGV[1] then redis.call('PEXPIRE', KEYS[1], ARGV[2]); return 1 end
if not cur then redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2]); return 1 end
return 0`;
const RELEASE_LUA = `if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0`;

/**
 * Write-behind: drains the Redis Stream into MSSQL.
 *  - ONE active writer across all instances (leader lease in Redis). Every instance runs this worker,
 *    only the leader consumes, so writes reach MSSQL strictly in stream order: an older value of a
 *    record can never overwrite a newer one, and concurrent writers cannot deadlock each other.
 *  - consumer group => at-least-once; a new leader first takes over ALL un-ACKed entries of the
 *    previous leader (XAUTOCLAIM, oldest first) before reading anything new
 *  - entries carry the FULL row => upsert is idempotent, so re-processing is safe
 *  - a batch is coalesced by Mumin_ID (last write wins) and applied in ONE MSSQL transaction
 *  - transient errors (connection, timeout, deadlock) => retried; rows MSSQL rejects => dead-letter
 */
@Injectable()
export class WriteBehindWorker implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger(WriteBehindWorker.name);
  private readonly consumer = `${hostname()}-${process.pid}`;
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
    await this.loop?.catch(() => undefined);
    // Graceful shutdown: hand leadership over immediately instead of waiting for the lease to expire.
    await this.redis.client.eval(RELEASE_LUA, 1, LEADER_KEY, this.consumer).catch(() => undefined);
    this.conn?.disconnect();
  }

  async start() {
    if (this.running) return;
    this.running = true;
    this.conn = this.redis.client.duplicate(); // blocking reads need their own connection
    await this.conn.xgroup('CREATE', STREAM_KEY, GROUP, '0', 'MKSTREAM').catch((e) => {
      if (!String(e.message).includes('BUSYGROUP')) throw e;
    });
    this.loop = this.run();
    this.logger.log(`Write-behind worker started (${this.consumer})`);
  }

  private async isLeader(): Promise<boolean> {
    return (await this.redis.client.eval(LEADER_LUA, 1, LEADER_KEY, this.consumer, LEADER_LEASE_MS)) === 1;
  }

  private async run() {
    let leader = false;
    while (this.running) {
      try {
        if (!(await this.isLeader())) {
          if (leader) this.logger.warn('Lost write-behind leadership');
          leader = false;
          await sleep(1000);
          continue;
        }
        if (!leader) {
          // New leader (or recovering from an error): finish every older un-ACKed entry first, in order.
          leader = true;
          this.logger.log('Became write-behind leader');
          const taken = await this.takeOverPending();
          if (taken) this.logger.warn(`Recovered ${taken} un-persisted writes left by a previous attempt/leader`);
        }
        const res: any = await this.conn!.xreadgroup(
          'GROUP', GROUP, this.consumer, 'COUNT', READ_COUNT, 'BLOCK', 1000, 'STREAMS', STREAM_KEY, '>',
        );
        const entries: [string, string[]][] = res?.[0]?.[1] ?? [];
        if (entries.length) await this.apply(entries);
      } catch (e) {
        if (!this.running) return;
        this.logger.error(`Write-behind batch failed, retrying: ${(e as Error).message}`);
        await sleep(2000);
        leader = false; // re-run takeOverPending: the failed batch is retried before anything newer
      }
    }
  }

  /** Claim ALL pending entries (any consumer, any idle time) and apply them oldest first. Leader only. */
  private async takeOverPending(): Promise<number> {
    let cursor = '0-0';
    let total = 0;
    do {
      const res: any = await this.conn!.xautoclaim(STREAM_KEY, GROUP, this.consumer, 0, cursor, 'COUNT', READ_COUNT);
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
    // Coalesce: only the latest write per Mumin_ID matters (upsert row, or delete).
    const latest = new Map<number, { sid: string; row?: MuminRecord }>();
    for (const [sid, fields] of entries) {
      const f: Record<string, string> = {};
      for (let i = 0; i < fields.length; i += 2) f[fields[i]] = fields[i + 1];
      latest.set(Number(f.id), { sid, row: f.op === 'delete' ? undefined : JSON.parse(f.row) });
    }
    const deletes = [...latest.entries()].filter(([, v]) => !v.row).map(([id]) => id);
    if (deletes.length) await this.repo.deleteByIds(deletes, true); // transient errors throw -> retried
    const rows = [...latest.values()].filter((v) => v.row).map((v) => v.row!);
    const rejected: number[] = [];
    try {
      if (rows.length) await this.repo.upsertMany(rows);
    } catch (e) {
      // MSSQL unreachable / timeout / deadlock: throw => nothing is ACKed, the batch is retried in order.
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
      await sleep(20);
    }
    throw new Error('write-behind did not drain in time');
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** SQL Server errors that are about load/locking, not about the data: always retry these. */
const TRANSIENT_SQL_ERRORS = new Set([
  1205,  // deadlock victim
  1222,  // lock request timeout
  -2,    // timeout
  233, 10053, 10054, 10060, // connection dropped
  40197, 40501, 40613, 49918, 49919, 49920, 4221, // service busy / throttled (Azure SQL)
]);

/**
 * true  = SQL Server refused the DATA (constraint, conversion, truncation...): retrying can never
 *         succeed -> dead-letter.
 * false = connection / timeout / deadlock / throttling -> transient -> retry.
 */
function isRejectedBySqlServer(e: unknown): boolean {
  const err = e as { number?: number; originalError?: { number?: number; info?: { number?: number } } };
  const n = err?.number ?? err?.originalError?.number ?? err?.originalError?.info?.number;
  return typeof n === 'number' && n > 0 && !TRANSIENT_SQL_ERRORS.has(n);
}
