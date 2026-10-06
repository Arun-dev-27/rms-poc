import { BadRequestException, HttpException, HttpStatus, Injectable, Logger, NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createHash, randomUUID } from 'crypto';
import { MuminCache } from './mumin.cache';
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
  RECORD_STOPPED: 423, // Locked
  RECORD_BUSY: HttpStatus.CONFLICT,
  LEASE_LOST: HttpStatus.CONFLICT,
};
/** Processing lease auto-expires so a crashed request never blocks a record forever. */
const LEASE_MS = 30000;

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
    const r = await this.apply('insert', [rec], opts, true);
    return { mode: 'redis-first', replayed: r.replayed, version: r.items[0]?.version, ms: elapsed(started), data: r.rows[0] ?? (await this.cache.get(rec.Mumin_ID)) };
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
    const r = await this.apply('update', [{ ...patch, Mumin_ID: id }], opts, true);
    return { mode: 'redis-first', replayed: r.replayed, version: r.items[0]?.version, ms: elapsed(started), data: r.rows[0] ?? (await this.cache.get(id)) };
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
    return this.deleteMany([id], opts);
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

  // ---------------------------------------------------------------- record status (STOP / SKIP)

  /** STOP: every later request for these records is refused (423). In-flight 2-phase requests fail at commit. */
  async stop(ids: number[], reason = '') {
    const started = process.hrtime.bigint();
    const res = await this.writer.setState('stop', ids, reason);
    return { status: 'STOPPED', ms: elapsed(started), records: res };
  }

  async resume(ids: number[]) {
    const started = process.hrtime.bigint();
    const res = await this.writer.setState('resume', ids);
    return { status: 'ACTIVE', ms: elapsed(started), records: res.map(({ id, previous }) => ({ id, previous })) };
  }

  async state(id: number) {
    return this.writer.state(id);
  }

  /**
   * 2-phase processing of one record:
   *   BEGIN  (Lua) refuse if STOPPED (423) or already processing (409); else take a lease
   *   WORK   the business processing (simulated here by workMs)
   *   COMMIT (Lua) refuse if STOPPED meanwhile (423, nothing written) or lease lost (409); else write + release
   * Every path ends with a response; the lease is always released (and expires by itself after LEASE_MS).
   */
  async processRecord(id: number, patch: MuminPatch, workMs: number, opts: WriteOptions) {
    const started = process.hrtime.bigint();
    const item = { ...patch, Mumin_ID: id };
    const requestHash = createHash('sha1').update(`process|${JSON.stringify(item)}|${workMs}`).digest('base64');
    if (opts.requestId) {
      const prev = await this.writer.idempotencyResult(opts.requestId);
      if (prev) {
        if (JSON.parse(prev).h !== requestHash) throw toHttp(new WriteRejected('IDEMPOTENCY_KEY_REUSED', '', 'same request id, different request body'));
        return { replayed: true, ...this.summaryOf(prev), ms: elapsed(started) };
      }
    }
    const token = opts.requestId ?? `proc-${randomUUID()}`;
    try {
      await this.writer.begin(id, token, LEASE_MS);
    } catch (e) {
      if (e instanceof WriteRejected && e.code === 'NOT_FOUND' && (await this.warmMissing([id]))) {
        try { await this.writer.begin(id, token, LEASE_MS); } catch (e2) { throw toHttp(e2); }
      } else {
        throw toHttp(e);
      }
    }
    try {
      if (workMs > 0) await new Promise((r) => setTimeout(r, workMs)); // the "processing"
      const r = await this.writer.write('update', [item], { requestId: opts.requestId, requestHash, lease: token, wantRows: true });
      return { replayed: r.replayed, status: 'PROCESSED', version: r.items[0]?.version, workMs, ms: elapsed(started), data: r.rows[0] };
    } catch (e) {
      throw toHttp(e);
    } finally {
      await this.writer.release(id, token).catch(() => undefined);
    }
  }

  private summaryOf(stored: string) {
    const s = JSON.parse(stored);
    return { items: s.items.map(([id, version]: [number, number]) => ({ id, version })) };
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
    const run = () => this.writer.write(op, items, { requestId: opts.requestId, requestHash, expected: opts.expected, wantRows });
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
