import { Injectable } from '@nestjs/common';
import { RedisService } from '../redis/redis.service';
import { KEY_PREFIX } from './mumin.cache';
import { chunk } from './mumin.repository';
import { MuminPatch, MuminRecord } from './mumin.types';

export const STREAM_KEY = 'mumin:writes';      // pending MSSQL writes (Redis Stream)
export const DIRTY_KEY = 'mumin:dirty';        // Mumin_ID -> latest stream entry id not yet in MSSQL
export const DLQ_KEY = 'mumin:writes:dlq';     // rows MSSQL rejected
export const VERSION_KEY = 'mumin:ver';        // Mumin_ID -> version (incremented on every write)
export const IDEM_KEY = 'mumin:idem';          // request id -> stored result (per-field TTL, HEXPIRE)
export const STATUS_KEY = 'mumin:status';      // Mumin_ID -> 'STOPPED' (absent = ACTIVE)
export const PROC_KEY = 'mumin:processing';    // Mumin_ID -> request id holding the processing lease (TTL)
export const LEADER_KEY = 'mumin:writer:leader'; // which instance currently writes to MSSQL
export const STOPINFO_KEY = 'mumin:stopinfo';  // Mumin_ID -> {"stoppedAt","reason"}
/** Max items in one atomic Lua call (= max items per write request). */
export const LUA_CHUNK = 1000;
const IDEM_TTL_SECONDS = 24 * 3600;

export type WriteOp = 'insert' | 'update' | 'delete';
export type RejectCode = 'NOT_FOUND' | 'ALREADY_EXISTS' | 'VERSION_CONFLICT' | 'DUPLICATE_ID_IN_REQUEST' | 'IDEMPOTENCY_KEY_REUSED'
  | 'RECORD_STOPPED' | 'RECORD_BUSY' | 'LEASE_LOST';

/** The Lua script refused the request; NOTHING was written. */
export class WriteRejected extends Error {
  constructor(readonly code: RejectCode, readonly id: string, readonly detail: string) {
    super(`${code}${id ? ` (Mumin_ID ${id})` : ''}${detail ? `: ${detail}` : ''}`);
  }
}

export interface WriteResult {
  replayed: boolean;                            // true = duplicate request, nothing applied again
  items: { id: number; version: number }[];     // new version per record
  rows: (MuminRecord | null)[];                 // new rows (only when requested, never on replay)
}

/**
 * ONE Lua script for insert / update / delete. Redis runs a script alone, start to finish,
 * so for every Mumin_ID the check and the write can never be interleaved by another request.
 *
 *   step 0  request id seen before?  same payload -> REPLAY stored result; different payload -> reject
 *   pass 1  validate EVERY item, write nothing:
 *             record STOPPED, record being processed by another request (lease), same id twice in request,
 *             record missing (update/delete), record exists (insert), expected version != current version
 *             -> return ERR  => response breaks, nothing written
 *   pass 2  apply: SET/DEL row, XADD to MSSQL queue, mark dirty, HINCRBY version
 *   store   request id -> result (24h TTL) so a retry/duplicate is answered without re-applying
 *
 * lease: '' for a plain write; for the COMMIT of a 2-phase request it is the token taken at BEGIN
 *        (commit only if this request still owns the processing lease, then release it).
 *
 * KEYS = [stream, dirty, versions, idemHash, status, processing, rec1..recN]
 * ARGV = [op, idemField('' = none), requestHash, ttl, wantRows('1'/'0'), lease, item1..itemN, expectedVer1..N ('' = any)]
 */
const WRITE_LUA = `
local stream, dirty, ver, idem, status, proc = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6]
local op, idemField, reqHash, ttl, wantRows, lease = ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5] == '1', ARGV[6]
local K, A = 6, 6
local n = #KEYS - K
local function release(id) if lease ~= '' and redis.call('HGET', proc, id) == lease then redis.call('HDEL', proc, id) end end

if idemField ~= '' then
  local prev = redis.call('HGET', idem, idemField)
  if prev then
    if cjson.decode(prev)['h'] ~= reqHash then return {'ERR', 'IDEMPOTENCY_KEY_REUSED', '', 'same request id, different request body'} end
    for i = 1, n do release(tostring(cjson.decode(ARGV[A + i])['Mumin_ID'])) end
    return {'REPLAY', prev}
  end
end

local plan, seen = {}, {}
for i = 1, n do
  local key = KEYS[K + i]
  local item = cjson.decode(ARGV[A + i])
  local id = tostring(item['Mumin_ID'])
  if redis.call('HGET', status, id) == 'STOPPED' then release(id); return {'ERR', 'RECORD_STOPPED', id, 'record is stopped; no further processing'} end
  local owner = redis.call('HGET', proc, id)
  if lease ~= '' then
    if owner ~= lease then return {'ERR', 'LEASE_LOST', id, 'processing lease expired or taken over'} end
  elseif owner then
    return {'ERR', 'RECORD_BUSY', id, 'record is being processed by request ' .. owner}
  end
  if seen[id] then return {'ERR', 'DUPLICATE_ID_IN_REQUEST', id, ''} end
  seen[id] = true
  local cur = redis.call('GET', key)
  if op == 'insert' then
    if cur then return {'ERR', 'ALREADY_EXISTS', id, ''} end
  elseif not cur then
    return {'ERR', 'NOT_FOUND', id, ''}
  end
  local current = redis.call('HGET', ver, id) or '0'
  local expected = ARGV[A + n + i]
  if expected ~= '' and expected ~= current then
    return {'ERR', 'VERSION_CONFLICT', id, 'expected version ' .. expected .. ', current version ' .. current}
  end
  local json = ARGV[A + i]
  if op == 'update' then
    local rec = cjson.decode(cur)
    for f, v in pairs(item) do rec[f] = v end
    json = cjson.encode(rec)
  end
  plan[i] = {key, id, json}
end

local items = {}
for i = 1, n do
  local key, id, json = plan[i][1], plan[i][2], plan[i][3]
  local sid
  if op == 'delete' then
    redis.call('DEL', key)
    sid = redis.call('XADD', stream, '*', 'id', id, 'op', 'delete')
  else
    redis.call('SET', key, json)
    sid = redis.call('XADD', stream, '*', 'id', id, 'row', json)
  end
  redis.call('HSET', dirty, id, sid)
  items[i] = {tonumber(id), redis.call('HINCRBY', ver, id, 1)}
  release(id)
end

local summary = cjson.encode({h = reqHash, op = op, items = items})
if idemField ~= '' then
  redis.call('HSET', idem, idemField, summary)
  redis.call('HEXPIRE', idem, tonumber(ttl), 'FIELDS', 1, idemField)
end
local out = {'OK', summary}
if wantRows and op ~= 'delete' then
  for i = 1, n do out[#out + 1] = plan[i][3] end
end
return out`;

/**
 * BEGIN of a 2-phase request: atomically refuse if STOPPED or already being processed, else take the
 * processing lease (auto-expires, so a crashed request can never block the record forever).
 * KEYS = [status, processing, recordKey]  ARGV = [id, token, leaseMs]
 */
const BEGIN_LUA = `
if redis.call('HGET', KEYS[1], ARGV[1]) == 'STOPPED' then return {'ERR', 'RECORD_STOPPED', 'record is stopped; no further processing'} end
if redis.call('EXISTS', KEYS[3]) == 0 then return {'ERR', 'NOT_FOUND', ''} end
if redis.call('HSETNX', KEYS[2], ARGV[1], ARGV[2]) == 0 then
  return {'ERR', 'RECORD_BUSY', 'record is being processed by request ' .. redis.call('HGET', KEYS[2], ARGV[1])}
end
redis.call('HPEXPIRE', KEYS[2], tonumber(ARGV[3]), 'FIELDS', 1, ARGV[1])
return {'OK', ''}`;

/** KEYS = [processing]  ARGV = [id, token]: release only if this request still owns the lease. */
const RELEASE_LUA = `
if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[2] then return redis.call('HDEL', KEYS[1], ARGV[1]) end
return 0`;

/**
 * STOP / RESUME many records atomically.
 * KEYS = [status, stopinfo, processing, versions]  ARGV = [action('stop'|'resume'), infoJson, id1..idN]
 * Returns per id: {previousStatus, inFlightRequestId, versionAtThisMoment}
 */
const STATE_LUA = `
local out = {}
for i = 3, #ARGV do
  local id = ARGV[i]
  local prev = redis.call('HGET', KEYS[1], id) or 'ACTIVE'
  if ARGV[1] == 'stop' then
    if prev ~= 'STOPPED' then
      redis.call('HSET', KEYS[1], id, 'STOPPED')
      redis.call('HSET', KEYS[2], id, ARGV[2])
    end
  else
    redis.call('HDEL', KEYS[1], id)
    redis.call('HDEL', KEYS[2], id)
  end
  out[#out + 1] = {prev, redis.call('HGET', KEYS[3], id) or '', redis.call('HGET', KEYS[4], id) or '0'}
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
    this.redis.client.defineCommand('muminBegin', { numberOfKeys: 3, lua: BEGIN_LUA });
    this.redis.client.defineCommand('muminRelease', { numberOfKeys: 1, lua: RELEASE_LUA });
    this.redis.client.defineCommand('muminState', { numberOfKeys: 4, lua: STATE_LUA });
  }

  /**
   * Atomic write of up to LUA_CHUNK items (all-or-nothing). Throws WriteRejected when the script
   * refuses the request. `expected[i]` = required current version of item i (undefined = any).
   */
  async write(
    op: WriteOp,
    items: (MuminPatch & { Mumin_ID: number })[],
    opts: { requestId?: string; requestHash?: string; expected?: (number | undefined)[]; wantRows?: boolean; lease?: string } = {},
  ): Promise<WriteResult> {
    if (items.length > LUA_CHUNK) throw new Error(`max ${LUA_CHUNK} items per atomic write`);
    const keys = items.map((i) => `${KEY_PREFIX}${i.Mumin_ID}`);
    const expected = items.map((_, i) => (opts.expected?.[i] === undefined ? '' : String(opts.expected[i])));
    const res: string[] = await (this.redis.client as any).muminWrite(
      keys.length + 6, STREAM_KEY, DIRTY_KEY, VERSION_KEY, IDEM_KEY, STATUS_KEY, PROC_KEY, ...keys,
      op, opts.requestId ?? '', opts.requestHash ?? '', IDEM_TTL_SECONDS, opts.wantRows ? '1' : '0', opts.lease ?? '',
      ...items.map((p) => JSON.stringify(p)), ...expected,
    );
    if (res[0] === 'ERR') throw new WriteRejected(res[1] as RejectCode, res[2], res[3]);
    const summary = JSON.parse(res[1]);
    return {
      replayed: res[0] === 'REPLAY',
      items: summary.items.map(([id, version]: [number, number]) => ({ id, version })),
      rows: res.slice(2).map((r) => JSON.parse(r)),
    };
  }

  /** BEGIN: take the processing lease for one record (throws WriteRejected if stopped / busy / missing). */
  async begin(id: number, token: string, leaseMs: number): Promise<void> {
    const res: string[] = await (this.redis.client as any).muminBegin(STATUS_KEY, PROC_KEY, `${KEY_PREFIX}${id}`, String(id), token, leaseMs);
    if (res[0] === 'ERR') throw new WriteRejected(res[1] as RejectCode, String(id), res[2]);
  }

  async release(id: number, token: string): Promise<void> {
    await (this.redis.client as any).muminRelease(PROC_KEY, String(id), token);
  }

  /** STOP or RESUME records atomically; returns the state each record was in at that exact moment. */
  async setState(action: 'stop' | 'resume', ids: number[], reason = '') {
    const info = JSON.stringify({ stoppedAt: new Date().toISOString(), reason });
    const out: { id: number; previous: string; inFlightRequest: string | null; versionAtStop: number }[] = [];
    for (const part of chunk(ids, LUA_CHUNK)) {
      const res: [string, string, string][] = await (this.redis.client as any).muminState(
        STATUS_KEY, STOPINFO_KEY, PROC_KEY, VERSION_KEY, action, info, ...part.map(String),
      );
      res.forEach(([previous, inFlight, v], i) => out.push({ id: part[i], previous, inFlightRequest: inFlight || null, versionAtStop: Number(v) }));
    }
    return out;
  }

  async state(id: number) {
    const c = this.redis.client;
    const [status, processing, info, version] = await Promise.all([
      c.hget(STATUS_KEY, String(id)), c.hget(PROC_KEY, String(id)), c.hget(STOPINFO_KEY, String(id)), c.hget(VERSION_KEY, String(id)),
    ]);
    return {
      Mumin_ID: id,
      status: status === 'STOPPED' ? 'STOPPED' : processing ? 'PROCESSING' : 'ACTIVE',
      processingRequest: processing,
      ...(status === 'STOPPED' && info ? JSON.parse(info) : {}),
      version: Number(version ?? 0),
    };
  }

  /** Stopped records with their stop info (saved in snapshots so STOP survives Redis data loss). */
  async stoppedAll(): Promise<Record<string, string>> {
    return this.redis.client.hgetall(STOPINFO_KEY);
  }

  async restoreStopped(entries: Record<string, string>) {
    const ids = Object.keys(entries);
    if (!ids.length) return;
    const m = this.redis.client.multi();
    m.hset(STATUS_KEY, Object.fromEntries(ids.map((id) => [id, 'STOPPED'])));
    m.hset(STOPINFO_KEY, entries);
    await m.exec();
  }

  async idempotencyResult(requestId: string): Promise<string | null> {
    return this.redis.client.hget(IDEM_KEY, requestId);
  }

  async versions(ids: number[]): Promise<number[]> {
    if (!ids.length) return [];
    const vals = await this.redis.client.hmget(VERSION_KEY, ...ids.map(String));
    return vals.map((v) => Number(v ?? 0));
  }

  /** Writes made outside the API (reconcile) must invalidate versions clients may hold. */
  async bumpVersions(ids: number[]) {
    for (const part of chunk(ids, 5000)) {
      if (!part.length) continue;
      const p = this.redis.client.pipeline();
      part.forEach((id) => p.hincrby(VERSION_KEY, String(id), 1));
      await p.exec();
    }
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
    const [streamLength, dirty, dlq, idempotencyKeys, stopped, processing, writerLeader] = await Promise.all([
      c.xlen(STREAM_KEY), c.hlen(DIRTY_KEY), c.xlen(DLQ_KEY), c.hlen(IDEM_KEY), c.hlen(STATUS_KEY), c.hlen(PROC_KEY), c.get(LEADER_KEY),
    ]);
    return { streamLength, dirty, dlq, idempotencyKeys, stopped, processing, writerLeader };
  }
}
