import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { RedisService } from '../redis/redis.service';
import { chunk } from './mumin.repository';
import { MuminRecord } from './mumin.types';

export const KEY_PREFIX = 'mumin:rec:';
export const META_KEY = 'mumin:meta';

export interface CacheMeta {
  source?: string;     // db | snapshot | import
  count?: string;
  loadedAt?: string;
  ctVersion?: string;  // MSSQL change-tracking version Redis is consistent with
  loadMs?: string;
}

/**
 * Redis layout: one STRING (JSON of the full row) per record  ->  mumin:rec:{Mumin_ID}
 *  - 79 columns, always read/written as a whole row  -> JSON string is ~2x smaller than a HASH
 *  - single read  : GET
 *  - batch read   : MGET (one command per chunk)
 *  - batch write  : MSET (one atomic command per chunk)
 */
@Injectable()
export class MuminCache {
  readonly batchSize: number;

  constructor(private readonly redis: RedisService, config: ConfigService) {
    this.batchSize = Number(config.get('REDIS_BATCH_SIZE', 5000));
  }

  key(id: number) {
    return `${KEY_PREFIX}${id}`;
  }

  async get(id: number): Promise<MuminRecord | null> {
    const v = await this.redis.client.get(this.key(id));
    return v ? JSON.parse(v) : null;
  }

  async getMany(ids: number[]): Promise<{ hits: Map<number, MuminRecord>; misses: number[] }> {
    const hits = new Map<number, MuminRecord>();
    const misses: number[] = [];
    for (const part of chunk(ids, this.batchSize)) {
      const vals = await this.redis.client.mget(part.map((id) => this.key(id)));
      vals.forEach((v, i) => (v ? hits.set(part[i], JSON.parse(v)) : misses.push(part[i])));
    }
    return { hits, misses };
  }

  async set(rec: MuminRecord): Promise<void> {
    await this.redis.client.set(this.key(rec.Mumin_ID), JSON.stringify(rec));
  }

  async setMany(records: MuminRecord[]): Promise<void> {
    for (const part of chunk(records, this.batchSize)) {
      if (!part.length) continue;
      const args: string[] = [];
      for (const r of part) args.push(this.key(r.Mumin_ID), JSON.stringify(r));
      await this.redis.client.mset(args);
    }
  }

  async delMany(ids: number[]): Promise<void> {
    for (const part of chunk(ids, this.batchSize)) {
      if (part.length) await this.redis.client.unlink(...part.map((id) => this.key(id)));
    }
  }

  /** Non-blocking iteration over every cached record (SCAN + MGET, never KEYS). */
  async *scanAll(): AsyncGenerator<string[]> {
    const stream = this.redis.client.scanStream({ match: `${KEY_PREFIX}*`, count: 2000 });
    for await (const keys of stream as AsyncIterable<string[]>) {
      if (!keys.length) continue;
      const vals = await this.redis.client.mget(keys);
      yield vals.filter((v): v is string => !!v);
    }
  }

  async clearAll(): Promise<number> {
    let n = 0;
    const stream = this.redis.client.scanStream({ match: `${KEY_PREFIX}*`, count: 2000 });
    for await (const keys of stream as AsyncIterable<string[]>) {
      if (keys.length) n += await this.redis.client.unlink(...keys);
    }
    await this.redis.client.unlink(META_KEY);
    return n;
  }

  async countKeys(): Promise<number> {
    return this.redis.client.dbsize();
  }

  async getMeta(): Promise<CacheMeta> {
    return this.redis.client.hgetall(META_KEY);
  }

  async setMeta(meta: CacheMeta): Promise<void> {
    await this.redis.client.hset(META_KEY, meta as Record<string, string>);
  }
}
