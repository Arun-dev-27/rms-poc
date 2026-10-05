import { ConflictException, Injectable, Logger, NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { MuminCache } from './mumin.cache';
import { MuminRedisWriter } from './mumin.redis-writer';
import { MuminInsert, MuminRepository } from './mumin.repository';
import { MuminPatch, MuminRecord } from './mumin.types';

/**
 * Read path : cache-aside (Redis first, MSSQL on miss, then back-fill Redis).
 * Write path (WRITE_MODE):
 *  - redis-first (default): write-behind. The change is applied atomically in Redis (Lua) and queued
 *    on a Redis Stream; WriteBehindWorker persists it to MSSQL in batches (~ms later).
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

  async getOne(id: number) {
    const started = process.hrtime.bigint();
    const cached = await this.cache.get(id);
    if (cached) return { source: 'redis', ms: elapsed(started), data: cached };
    if ((await this.writer.dirtyOf([id])).size) throw new NotFoundException(`Mumin_ID ${id} not found`); // pending delete
    const row = await this.repo.findById(id);
    if (!row) throw new NotFoundException(`Mumin_ID ${id} not found`);
    await this.cache.set(row);
    return { source: 'mssql', ms: elapsed(started), data: row };
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
    return {
      requested: unique.length,
      fromRedis: unique.length - misses.length,
      fromMssql: fromDb.length,
      notFound: unique.filter((id) => !hits.has(id)),
      ms: elapsed(started),
      data: unique.filter((id) => hits.has(id)).map((id) => hits.get(id)!),
    };
  }

  async create(rec: MuminInsert) {
    const r = await this.createMany([rec]);
    if (r.conflicts.length) throw new ConflictException(`Mumin_ID ${rec.Mumin_ID} already exists`);
    return r;
  }

  async createMany(recs: MuminInsert[]) {
    const started = process.hrtime.bigint();
    if (!this.redisFirst) {
      const rows = await this.repo.insertMany(recs);
      await this.refreshCache(rows);
      return { mode: 'db-first', inserted: rows.length, conflicts: [] as number[], ms: elapsed(started) };
    }
    // Redis may not hold every row (cold cache) -> never let an "insert" overwrite an existing MSSQL row.
    const { misses } = await this.cache.getMany(recs.map((r) => r.Mumin_ID));
    const inDb = new Set((await this.repo.findByIds(misses)).map((r) => r.Mumin_ID));
    const candidates = recs.filter((r) => !inDb.has(r.Mumin_ID));
    const res = await this.writer.insert(candidates);
    const conflicts = [...inDb, ...candidates.filter((_, i) => !res[i]).map((r) => r.Mumin_ID)];
    return { mode: 'redis-first', inserted: res.filter(Boolean).length, conflicts, ms: elapsed(started) };
  }

  async updateOne(id: number, patch: MuminPatch) {
    const started = process.hrtime.bigint();
    if (!this.redisFirst) {
      const row = await this.repo.updateOne(id, patch);
      if (!row) throw new NotFoundException(`Mumin_ID ${id} not found`);
      await this.refreshCache([row]);
      return { mode: 'db-first', ms: elapsed(started), data: row };
    }
    const r = await this.updateRedisFirst([{ ...patch, Mumin_ID: id }]);
    if (!r.rows[0]) throw new NotFoundException(`Mumin_ID ${id} not found`);
    return { mode: 'redis-first', ms: elapsed(started), data: r.rows[0] };
  }

  async updateMany(items: MuminInsert[]) {
    const started = process.hrtime.bigint();
    if (!this.redisFirst) {
      const rows = await this.repo.updateMany(items);
      await this.refreshCache(rows);
      const updated = new Set(rows.map((r) => r.Mumin_ID));
      return { mode: 'db-first', requested: items.length, updated: rows.length, notFound: items.map((i) => i.Mumin_ID).filter((id) => !updated.has(id)), ms: elapsed(started) };
    }
    const r = await this.updateRedisFirst(items);
    return { mode: 'redis-first', requested: items.length, updated: r.rows.filter(Boolean).length, notFound: r.notFound, ms: elapsed(started) };
  }

  async deleteOne(id: number) {
    const r = await this.deleteMany([id]);
    if (r.deleted === 0) throw new NotFoundException(`Mumin_ID ${id} not found`);
    return r;
  }

  async deleteMany(ids: number[]) {
    const started = process.hrtime.bigint();
    const unique = [...new Set(ids)];
    if (!this.redisFirst) {
      const n = await this.repo.deleteByIds(unique);
      await this.cache.delMany(unique);
      return { mode: 'db-first', requested: unique.length, deleted: n, ms: elapsed(started) };
    }
    let res = await this.writer.delete(unique);
    const missing = unique.filter((_, i) => !res[i]);
    if (missing.length) {
      // Not in Redis (cold cache): pull from MSSQL so the delete goes through the same queue.
      const fromDb = await this.repo.findByIds(missing);
      if (fromDb.length) {
        await this.cache.setMany(fromDb);
        const retried = await this.writer.delete(fromDb.map((r) => r.Mumin_ID));
        res = res.concat(retried.filter(Boolean));
      }
    }
    const deleted = res.filter(Boolean).length;
    return { mode: 'redis-first', requested: unique.length, deleted, notFound: unique.length - deleted, ms: elapsed(started) };
  }

  /** Redis Lua update; ids missing from Redis are pulled from MSSQL once and retried. */
  private async updateRedisFirst(items: MuminInsert[]) {
    const rows = await this.writer.update(items);
    const missIdx = rows.map((r, i) => (r ? -1 : i)).filter((i) => i >= 0);
    if (missIdx.length) {
      const fromDb = await this.repo.findByIds(missIdx.map((i) => items[i].Mumin_ID));
      if (fromDb.length) {
        await this.cache.setMany(fromDb);
        const retried = await this.writer.update(missIdx.map((i) => items[i]));
        missIdx.forEach((i, j) => (rows[i] = retried[j]));
      }
    }
    return { rows, notFound: items.filter((_, i) => !rows[i]).map((i) => i.Mumin_ID) };
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

export function elapsed(start: bigint): number {
  return Math.round(Number(process.hrtime.bigint() - start) / 1e4) / 100;
}
