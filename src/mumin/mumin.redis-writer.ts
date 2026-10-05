import { Injectable } from '@nestjs/common';
import { RedisService } from '../redis/redis.service';
import { KEY_PREFIX } from './mumin.cache';
import { chunk } from './mumin.repository';
import { MuminPatch, MuminRecord } from './mumin.types';

export const STREAM_KEY = 'mumin:writes';      // pending MSSQL writes (Redis Stream)
export const DIRTY_KEY = 'mumin:dirty';        // Mumin_ID -> latest stream entry id not yet in MSSQL
export const DLQ_KEY = 'mumin:writes:dlq';     // rows MSSQL rejected
const LUA_CHUNK = 1000;

/**
 * One Lua script per chunk = atomic for every item in it:
 *   update: GET -> merge patch -> SET -> XADD stream -> HSET dirty     (0 if the key is missing)
 *   insert: (missing) SET -> XADD stream -> HSET dirty                 (0 if the key already exists)
 * KEYS = [stream, dirty, rec1, rec2, ...]   ARGV = [op, json1, json2, ...]
 */
const WRITE_LUA = `
local stream, dirty, op = KEYS[1], KEYS[2], ARGV[1]
local out = {}
for i = 3, #KEYS do
  local k = KEYS[i]
  local cur = redis.call('GET', k)
  local rec = nil
  if op == 'update' and cur then
    rec = cjson.decode(cur)
    for f, v in pairs(cjson.decode(ARGV[i - 1])) do rec[f] = v end
  elseif op == 'insert' and not cur then
    rec = cjson.decode(ARGV[i - 1])
  end
  if rec then
    local json = cjson.encode(rec)
    redis.call('SET', k, json)
    local id = tostring(rec['Mumin_ID'])
    local sid = redis.call('XADD', stream, '*', 'id', id, 'row', json)
    redis.call('HSET', dirty, id, sid)
    out[#out + 1] = json
  else
    out[#out + 1] = 0
  end
end
return out`;

/** Clears dirty flags only if no newer write for that id arrived meanwhile. ARGV = [id1, sid1, id2, sid2...] */
const CLEAN_LUA = `
local n = 0
for i = 1, #ARGV, 2 do
  if redis.call('HGET', KEYS[1], ARGV[i]) == ARGV[i + 1] then redis.call('HDEL', KEYS[1], ARGV[i]); n = n + 1 end
end
return n`;

@Injectable()
export class MuminRedisWriter {
  constructor(private readonly redis: RedisService) {
    this.redis.client.defineCommand('muminWrite', { lua: WRITE_LUA });
    this.redis.client.defineCommand('muminClean', { numberOfKeys: 1, lua: CLEAN_LUA });
  }

  /** Returns the merged row per item, or null when the record is not in Redis. */
  update(items: (MuminPatch & { Mumin_ID: number })[]) {
    return this.run('update', items);
  }

  /** Returns the row per item, or null when the key already exists. */
  insert(items: (MuminPatch & { Mumin_ID: number })[]) {
    return this.run('insert', items);
  }

  async markClean(pairs: [string, string][]) {
    if (!pairs.length) return 0;
    return (this.redis.client as any).muminClean(DIRTY_KEY, ...pairs.flat());
  }

  /** Ids that have a write not yet persisted to MSSQL (must not be overwritten from MSSQL). */
  async dirtyOf(ids: number[]): Promise<Set<number>> {
    const dirty = new Set<number>();
    for (const part of chunk(ids, 5000)) {
      if (!part.length) continue;
      const vals = await this.redis.client.hmget(DIRTY_KEY, ...part.map(String));
      vals.forEach((v, i) => v && dirty.add(part[i]));
    }
    return dirty;
  }

  async stats() {
    const c = this.redis.client;
    const [streamLength, dirty, dlq] = await Promise.all([c.xlen(STREAM_KEY), c.hlen(DIRTY_KEY), c.xlen(DLQ_KEY)]);
    return { streamLength, dirty, dlq };
  }

  private async run(op: 'update' | 'insert', items: (MuminPatch & { Mumin_ID: number })[]): Promise<(MuminRecord | null)[]> {
    const out: (MuminRecord | null)[] = [];
    for (const part of chunk(items, LUA_CHUNK)) {
      const keys = part.map((i) => `${KEY_PREFIX}${i.Mumin_ID}`);
      const res: (string | number)[] = await (this.redis.client as any).muminWrite(
        keys.length + 2, STREAM_KEY, DIRTY_KEY, ...keys, op, ...part.map((p) => JSON.stringify(p)),
      );
      res.forEach((r) => out.push(r === 0 ? null : JSON.parse(r as string)));
    }
    return out;
  }
}
