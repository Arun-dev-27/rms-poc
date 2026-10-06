import { BadRequestException, HttpException, HttpStatus, Injectable, Logger, NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createHash } from 'crypto';
import { MuminCache } from './mumin.cache';
import { MAX_WAIT_MS, RecordQueue } from './mumin.queue';
import { LUA_CHUNK, MuminRedisWriter, WriteOp, WriteRejected, WriteResult } from './mumin.redis-writer';
import { chunk, MuminInsert, MuminRepository } from './mumin.repository';
import { MuminPatch, MuminRecord } from './mumin.types';

export interface WriteOptions {
  /** Idempotency-Key: the same request id is applied once; repeats get the stored result. */
  requestId?: string;
  /** If-Match: required current version per item (undefined = no check). */
  expected?: (number | undefined)[];
}

const REJECT_STATUS: Record<string, number> = {
  NOT_FOUND: HttpStatus.NOT_FOUND,
  ALREADY_EXISTS: HttpStatus.CONFLICT,
  VERSION_CONFLICT: HttpStatus.PRECONDITION_FAILED,
  DUPLICATE_ID_IN_REQUEST: HttpStatus.BAD_REQUEST,
  IDEMPOTENCY_KEY_REUSED: HttpStatus.UNPROCESSABLE_ENTITY,
  RECORD_CANCELLED: 423, // Locked
  RECORD_BUSY: HttpStatus.CONFLICT,
  LEASE_LOST: HttpStatus.CONFLICT,
  QUEUE_TIMEOUT: HttpStatus.SERVICE_UNAVAILABLE,
};

/**
 * Read path : cache-aside (Redis first, MSSQL on miss, then back-fill Redis).
 * Write path (WRITE_MODE):
 *  - redis-first (default): one atomic Lua script per request (dedupe by request id, version check,
 *    all-or-nothing), then WriteBehindWorker persists to MSSQL in batches.
 *  - db-first: write-through. Commit in MSSQL, then refresh Redis from OUTPUT inserted.*.
 */
@Injectable()
export class MuminService {
  private readonly logger = new Logger(MuminService.name);
  readonly redisFirst: boolean;

  constructor(
    private readonly repo: MuminRepository,
    private readonly cache: MuminCache,
    private readonly writer: MuminRedisWriter,
    private readonly queue: RecordQueue,
    config: ConfigService,
  ) {
    this.redisFirst = config.get('WRITE_MODE', 'redis-first') !== 'db-first';
  }

  // ---------------------------------------------------------------- reads

  async getOne(id: number) {
    const started = process.hrtime.bigint();
    const [cached, state] = await Promise.all([this.cache.get(id), this.writer.state(id)]);
    const { version, status } = state;
    if (cached) return { source: 'redis', version, status, ms: elapsed(started), data: cached };
    if ((await this.writer.dirtyOf([id])).size) throw new NotFoundException(`Mumin_ID ${id} not found`); // pending delete
    const row = await this.repo.findById(id);
    if (!row) throw new NotFoundException(`Mumin_ID ${id} not found`);
    await this.cache.set(row);
    return { source: 'mssql', version, status, ms: elapsed(started), data: row };
  }

  async getMany(ids: number[]) {
    const started = process.hrtime.bigint();
    const unique = [...new Set(ids)];
    const { hits, misses } = await this.cache.getMany(unique);
    let fromDb: MuminRecord[] = [];
    if (misses.length) {
      const pending = await this.writer.dirtyOf(misses); // pending deletes stay deleted
      fromDb = (await this.repo.findByIds(misses)).filter((r) => !pending.has(r.Mumin_ID));
      if (fromDb.length) await this.cache.setMany(fromDb);
      fromDb.forEach((r) => hits.set(r.Mumin_ID, r));
    }
    const found = unique.filter((id) => hits.has(id));
    const versions = await this.writer.versions(found);
    return {
      requested: unique.length,
      fromRedis: unique.length - misses.length,
      fromMssql: fromDb.length,
      notFound: unique.filter((id) => !hits.has(id)),
      ms: elapsed(started),
      versions: Object.fromEntries(found.map((id, i) => [id, versions[i]])),
      data: found.map((id) => hits.get(id)!),
    };
  }

  // ---------------------------------------------------------------- writes

  async create(rec: MuminInsert, opts: WriteOptions = {}) {
    const started = process.hrtime.bigint();
    if (!this.redisFirst) {
      const [row] = await this.repo.insertMany([rec]);
      await this.refreshCache([row]);
      return { mode: 'db-first', ms: elapsed(started), data: row };
    }
    return this.queued('insert', rec.Mumin_ID, rec, opts);
  }

  async createMany(recs: MuminInsert[], opts: WriteOptions = {}) {
    const started = process.hrtime.bigint();
    if (!this.redisFirst) {
      const rows = await this.repo.insertMany(recs);
      await this.refreshCache(rows);
      return { mode: 'db-first', inserted: rows.length, ms: elapsed(started) };
    }
    const r = await this.apply('insert', recs, opts);
    return { mode: 'redis-first', replayed: r.replayed, inserted: r.items.length, ms: elapsed(started) };
  }

  async updateOne(id: number, patch: MuminPatch, opts: WriteOptions = {}) {
    const started = process.hrtime.bigint();
    if (!this.redisFirst) {
      const row = await this.repo.updateOne(id, patch);
      if (!row) throw new NotFoundException(`Mumin_ID ${id} not found`);
      await this.refreshCache([row]);
      return { mode: 'db-first', ms: elapsed(started), data: row };
    }
    return this.queued('update', id, { ...patch, Mumin_ID: id }, opts);
  }

  async updateMany(items: MuminInsert[], opts: WriteOptions = {}) {
    const started = process.hrtime.bigint();
    if (!this.redisFirst) {
      const rows = await this.repo.updateMany(items);
      await this.refreshCache(rows);
      return { mode: 'db-first', requested: items.length, updated: rows.length, ms: elapsed(started) };
    }
    const r = await this.apply('update', items, opts);
    return { mode: 'redis-first', replayed: r.replayed, requested: items.length, updated: r.items.length, versions: r.items, ms: elapsed(started) };
  }

  async deleteOne(id: number, opts: WriteOptions = {}) {
    if (!this.redisFirst) return this.deleteMany([id], opts);
    return this.queued('delete', id, { Mumin_ID: id }, opts);
  }

  async deleteMany(ids: number[], opts: WriteOptions = {}) {
    const started = process.hrtime.bigint();
    if (!this.redisFirst) {
      const n = await this.repo.deleteByIds(ids);
      await this.cache.delMany(ids);
      return { mode: 'db-first', requested: ids.length, deleted: n, ms: elapsed(started) };
    }
    const r = await this.apply('delete', ids.map((Mumin_ID) => ({ Mumin_ID })), opts);
    return { mode: 'redis-first', replayed: r.replayed, requested: ids.length, deleted: r.items.length, ms: elapsed(started) };
  }

  // ---------------------------------------------------------------- record status (CANCEL)

  /**
   * CANCEL: the request being processed is refused at commit, everyone waiting in the record's line is
   * answered 423 at once, and every later request is refused (423) until RESUME.
   */
  async stop(ids: number[], reason = '') {
    const started = process.hrtime.bigint();
    const res = await this.writer.setState('stop', ids, reason);
    return { status: 'CANCELLED', ms: elapsed(started), records: res };
  }

  async resume(ids: number[]) {
    const started = process.hrtime.bigint();
    const res = await this.writer.setState('resume', ids);
    return { status: 'ACTIVE', ms: elapsed(started), records: res.map(({ id, previous }) => ({ id, previous })) };
  }

  async state(id: number) {
    return this.writer.state(id);
  }

  /** Business processing of one record that takes time (simulated by workMs): goes through the line. */
  async processRecord(id: number, patch: MuminPatch, workMs: number, opts: WriteOptions) {
    return this.queued('update', id, { ...patch, Mumin_ID: id }, opts, workMs);
  }

  /**
   * Single-record write, ticket-booking style:
   *   1. take a ticket in the record's FIFO line (duplicate request id -> answer of the original)
   *   2. wait for the turn (never "busy" errors; cancel answers waiters immediately with 423)
   *   3. work (workMs), then COMMIT with the same atomic Lua (cancelled meanwhile -> 423, nothing written)
   *   4. leave the line -> the next ticket is woken through pub/sub on whichever instance holds it
   */
  private async queued(op: WriteOp, id: number, item: MuminInsert, opts: WriteOptions, workMs = 0) {
    const started = process.hrtime.bigint();
    const requestHash = createHash('sha1')
      .update(`${op}|${JSON.stringify(item)}|${JSON.stringify(opts.expected ?? [])}|${workMs}`).digest('base64');
    try {
      if (op === 'insert') await this.assertNotInMssql([id]);
      else await this.warmMissing([id]); // cold cache: load the row before queueing for it
      const out = await this.queue.run(
        id, opts.requestId, requestHash,
        async (ticket) => {
          if (workMs > 0) await new Promise((r) => setTimeout(r, workMs));
          return this.writer.write(op, [item], { requestId: opts.requestId, requestHash, expected: opts.expected, wantRows: op !== 'delete', lease: ticket });
        },
        (stored): WriteResult => ({ replayed: true, rows: [], items: JSON.parse(stored).items.map(([i, v]: [number, number]) => ({ id: i, version: v })) }),
      );
      const r = out.result;
      return {
        mode: 'redis-first', status: op === 'delete' ? 'DELETED' : 'PROCESSED', replayed: out.replayed || r.replayed,
        version: r.items[0]?.version, queuePosition: out.position, waitedMs: out.waitedMs, workMs, ms: elapsed(started),
        data: op === 'delete' ? undefined : r.rows[0] ?? (await this.cache.get(id)),
      };
    } catch (e) {
      throw toHttp(e);
    }
  }

  /**
   * One request = one atomic Lua call (<= LUA_CHUNK items): all items succeed or nothing is written.
   * Larger lists are only allowed internally (benchmarks) and are applied chunk by chunk.
   */
  private async apply(op: WriteOp, items: MuminInsert[], opts: WriteOptions, wantRows = false): Promise<WriteResult> {
    if (items.length > LUA_CHUNK) {
      if (opts.requestId || opts.expected) throw new BadRequestException(`max ${LUA_CHUNK} items per request`);
      const merged: WriteResult = { replayed: false, items: [], rows: [] };
      for (const part of chunk(items, LUA_CHUNK)) merged.items.push(...(await this.apply(op, part, {})).items);
      return merged;
    }
    const requestHash = opts.requestId
      ? createHash('sha1').update(`${op}|${JSON.stringify(items)}|${JSON.stringify(opts.expected ?? [])}`).digest('base64')
      : '';
    // A record of the batch is being processed by a queued request right now: wait and retry instead
    // of failing (the batch is atomic, so nothing was written by the refused attempt).
    const run = async () => {
      const until = Date.now() + MAX_WAIT_MS;
      for (;;) {
        try {
          return await this.writer.write(op, items, { requestId: opts.requestId, requestHash, expected: opts.expected, wantRows });
        } catch (e) {
          if (!(e instanceof WriteRejected && e.code === 'RECORD_BUSY') || Date.now() > until) throw e;
          await new Promise((r) => setTimeout(r, 20 + Math.random() * 30));
        }
      }
    };
    try {
      if (op === 'insert') await this.assertNotInMssql(items.map((i) => i.Mumin_ID));
      return await run();
    } catch (e) {
      // Cold cache: rows that exist in MSSQL but not (yet) in Redis -> load them once and retry.
      if (e instanceof WriteRejected && e.code === 'NOT_FOUND' && (await this.warmMissing(items.map((i) => i.Mumin_ID)))) {
        try {
          return await run();
        } catch (e2) {
          throw toHttp(e2);
        }
      }
      throw toHttp(e);
    }
  }

  /** Insert must never overwrite a row that only MSSQL has (cold cache). Pending deletes are free to reuse. */
  private async assertNotInMssql(ids: number[]) {
    const { misses } = await this.cache.getMany(ids);
    if (!misses.length) return;
    const pending = await this.writer.dirtyOf(misses);
    const existing = await this.repo.findByIds(misses.filter((id) => !pending.has(id)));
    if (existing.length) throw new WriteRejected('ALREADY_EXISTS', String(existing[0].Mumin_ID), '');
  }

  private async warmMissing(ids: number[]): Promise<boolean> {
    const { misses } = await this.cache.getMany(ids);
    if (!misses.length) return false;
    const pending = await this.writer.dirtyOf(misses);
    const fromDb = await this.repo.findByIds(misses.filter((id) => !pending.has(id)));
    if (fromDb.length) await this.cache.setMany(fromDb);
    return fromDb.length > 0;
  }

  private async refreshCache(rows: MuminRecord[]) {
    try {
      await this.cache.setMany(rows);
    } catch (e) {
      this.logger.warn(`Redis write failed, invalidating ${rows.length} keys: ${(e as Error).message}`);
      await this.cache.delMany(rows.map((r) => r.Mumin_ID)).catch(() => undefined);
    }
  }
}

function toHttp(e: unknown): unknown {
  if (!(e instanceof WriteRejected)) return e;
  return new HttpException(
    { statusCode: REJECT_STATUS[e.code], error: e.code, message: e.message, Mumin_ID: e.id ? Number(e.id) : undefined, written: 0 },
    REJECT_STATUS[e.code],
  );
}

export function elapsed(start: bigint): number {
  return Math.round(Number(process.hrtime.bigint() - start) / 1e4) / 100;
}
