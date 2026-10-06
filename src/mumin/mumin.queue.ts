import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import Redis from 'ioredis';
import { randomUUID } from 'crypto';
import { RedisService } from '../redis/redis.service';
import { IDEM_KEY, PROC_KEY, STATUS_KEY, WriteRejected } from './mumin.redis-writer';

export const QUEUE_PREFIX = 'mumin:q:';          // LIST per record: tickets waiting, head = being processed
export const INFLIGHT_KEY = 'mumin:inflight';    // request id -> ticket currently working on it
export const EVENTS_CHANNEL = 'mumin:events';    // turn:<ticket> | cancel:<id> | done:<request id>
const LEASE_MS = 30000;      // head of the queue holds the record this long (renewed while working)
const CLAIM_MS = 5000;       // a promoted ticket must start within this, else it is skipped (dead instance)
const RENEW_EVERY_MS = 10000;
const POLL_MS = 500;         // safety net if a pub/sub message is missed
export const MAX_WAIT_MS = Number(process.env.QUEUE_MAX_WAIT_MS ?? 120000);

/**
 * JOIN: take a ticket (FIFO) for a record.
 *   request id already answered        -> REPLAY stored result (duplicate)
 *   same request id still in progress  -> DUPLICATE (caller waits for its result)
 *   record cancelled                   -> ERR RECORD_CANCELLED
 *   line empty                         -> TURN (process now)   else WAIT <position>
 * KEYS = [status, idem, inflight, queue, processing]  ARGV = [id, ticket, requestId, requestHash, leaseMs]
 */
const JOIN_LUA = `
local status, idem, inflight, queue, proc = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local id, ticket, rid, hash, lease = ARGV[1], ARGV[2], ARGV[3], ARGV[4], tonumber(ARGV[5])
if rid ~= '' then
  local prev = redis.call('HGET', idem, rid)
  if prev then
    if cjson.decode(prev)['h'] ~= hash then return {'ERR', 'IDEMPOTENCY_KEY_REUSED'} end
    return {'REPLAY', prev}
  end
  if redis.call('HGET', inflight, rid) then return {'DUPLICATE', ''} end
end
if redis.call('HGET', status, id) == 'CANCELLED' then return {'ERR', 'RECORD_CANCELLED'} end
if rid ~= '' then
  redis.call('HSET', inflight, rid, ticket)
  redis.call('HPEXPIRE', inflight, lease, 'FIELDS', 1, rid)
end
local n = redis.call('RPUSH', queue, ticket)
redis.call('PEXPIRE', queue, lease * 6) -- an abandoned line (its servers died) disappears by itself
if n == 1 then
  redis.call('HSET', proc, id, ticket)
  redis.call('HPEXPIRE', proc, lease, 'FIELDS', 1, id)
  return {'TURN', '0'}
end
return {'WAIT', tostring(n - 1)}`;

/**
 * CHECK (waiter woke up): is it my turn? Also removes a dead head (its lease/claim expired) and keeps
 * this request's in-progress marker alive while it waits.
 * KEYS = [status, queue, processing, inflight]  ARGV = [id, ticket, leaseMs, claimMs, channel, requestId]
 */
const CHECK_LUA = `
local status, queue, proc, inflight = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local id, ticket, lease, claim, ch, rid = ARGV[1], ARGV[2], tonumber(ARGV[3]), tonumber(ARGV[4]), ARGV[5], ARGV[6]
if rid ~= '' and redis.call('HGET', inflight, rid) == ticket then redis.call('HPEXPIRE', inflight, lease, 'FIELDS', 1, rid) end
redis.call('PEXPIRE', queue, lease * 6) -- someone is still waiting: keep the line
if redis.call('HGET', status, id) == 'CANCELLED' then
  redis.call('LREM', queue, 0, ticket)
  return {'ERR', 'RECORD_CANCELLED'}
end
local head = redis.call('LINDEX', queue, 0)
local holder = redis.call('HGET', proc, id)
if head and not holder and head ~= ticket then
  -- the head never started / stopped renewing (its instance died): skip it, promote the next
  redis.call('LPOP', queue)
  head = redis.call('LINDEX', queue, 0)
  if head and head ~= ticket then
    redis.call('HSET', proc, id, head)
    redis.call('HPEXPIRE', proc, claim, 'FIELDS', 1, id)
    redis.call('PUBLISH', ch, 'turn:' .. head)
  end
  holder = redis.call('HGET', proc, id)
end
if head == ticket and (not holder or holder == ticket) then
  redis.call('HSET', proc, id, ticket)
  redis.call('HPEXPIRE', proc, lease, 'FIELDS', 1, id)
  return {'TURN', '0'}
end
local pos = redis.call('LPOS', queue, ticket)
if not pos then return {'ERR', 'TICKET_LOST'} end
return {'WAIT', tostring(pos)}`;

/**
 * LEAVE (done, success or not): drop my ticket, free the record, hand it to the next ticket.
 * KEYS = [queue, processing, inflight]  ARGV = [id, ticket, requestId, claimMs, channel]
 */
const LEAVE_LUA = `
local queue, proc, inflight = KEYS[1], KEYS[2], KEYS[3]
local id, ticket, rid, claim, ch = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4]), ARGV[5]
redis.call('LREM', queue, 1, ticket)
if redis.call('HGET', proc, id) == ticket then redis.call('HDEL', proc, id) end
if rid ~= '' and redis.call('HGET', inflight, rid) == ticket then redis.call('HDEL', inflight, rid) end
if not redis.call('HGET', proc, id) then
  local nxt = redis.call('LINDEX', queue, 0)
  if nxt then
    redis.call('HSET', proc, id, nxt)
    redis.call('HPEXPIRE', proc, claim, 'FIELDS', 1, id)
    redis.call('PUBLISH', ch, 'turn:' .. nxt)
  end
end
if rid ~= '' then redis.call('PUBLISH', ch, 'done:' .. rid) end
return 1`;

/** KEYS = [processing, inflight, queue]  ARGV = [id, ticket, leaseMs, requestId]: keep my lease, marker and line alive */
const RENEW_LUA = `
redis.call('PEXPIRE', KEYS[3], tonumber(ARGV[3]) * 6)
if ARGV[4] ~= '' and redis.call('HGET', KEYS[2], ARGV[4]) == ARGV[2] then
  redis.call('HPEXPIRE', KEYS[2], tonumber(ARGV[3]), 'FIELDS', 1, ARGV[4])
end
if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[2] then
  redis.call('HPEXPIRE', KEYS[1], tonumber(ARGV[3]), 'FIELDS', 1, ARGV[1]); return 1
end
return 0`;

export interface QueueOutcome<T> { result: T; replayed: boolean; position: number; waitedMs: number }

/**
 * Ticket-booking style FIFO line per record (Mumin_ID), shared by every API instance through Redis.
 * Requests never fail because a record is busy: they wait their turn (woken by pub/sub, 500 ms poll as
 * a safety net). CANCEL removes everyone waiting at once; the one being processed is refused at commit.
 */
@Injectable()
export class RecordQueue implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RecordQueue.name);
  private sub?: Redis;
  private readonly wake = new Map<string, () => void>();           // ticket -> wake fn
  private readonly byRecord = new Map<string, Set<string>>();      // record id -> local waiting tickets
  private readonly dupWaiters = new Map<string, Set<() => void>>(); // request id -> wake fns

  constructor(private readonly redis: RedisService) {
    const c = this.redis.client;
    c.defineCommand('qJoin', { numberOfKeys: 5, lua: JOIN_LUA });
    c.defineCommand('qCheck', { numberOfKeys: 4, lua: CHECK_LUA });
    c.defineCommand('qLeave', { numberOfKeys: 3, lua: LEAVE_LUA });
    c.defineCommand('qRenew', { numberOfKeys: 3, lua: RENEW_LUA });
  }

  async onModuleInit() {
    this.sub = this.redis.client.duplicate();
    this.sub.on('message', (_ch, msg) => this.onEvent(msg));
    await this.sub.subscribe(EVENTS_CHANNEL);
  }

  async onModuleDestroy() {
    this.sub?.disconnect();
  }

  queueKey(id: number) {
    return `${QUEUE_PREFIX}${id}`;
  }

  /**
   * Wait for this request's turn on record `id`, run `work(ticket)`, always leave the line.
   * `replay(stored)` builds the answer for a duplicate of an already-answered request.
   */
  async run<T>(id: number, requestId: string | undefined, requestHash: string,
    work: (ticket: string) => Promise<T>, replay: (stored: string) => T): Promise<QueueOutcome<T>> {
    const started = Date.now();
    const rid = requestId ?? '';
    for (;;) {
      if (Date.now() - started > MAX_WAIT_MS) throw new WriteRejected('QUEUE_TIMEOUT' as any, String(id), `waited ${MAX_WAIT_MS} ms`);
      const ticket = `${rid || 't'}#${randomUUID()}`;
      const j: string[] = await (this.redis.client as any).qJoin(
        STATUS_KEY, IDEM_KEY, INFLIGHT_KEY, this.queueKey(id), PROC_KEY, String(id), ticket, rid, requestHash, LEASE_MS,
      );
      if (j[0] === 'REPLAY') return { result: replay(j[1]), replayed: true, position: 0, waitedMs: Date.now() - started };
      if (j[0] === 'ERR') throw new WriteRejected(j[1] as any, String(id), j[1] === 'RECORD_CANCELLED' ? 'record is cancelled; request not processed' : '');
      if (j[0] === 'DUPLICATE') { await this.waitDuplicate(rid); continue; } // same request is being processed: wait for its answer
      const position = Number(j[1]);
      if (j[0] === 'WAIT') {
        try {
          await this.waitTurn(id, ticket, rid, started);
        } catch (e) {
          // Left the line without processing (cancelled / timeout): same cleanup as a finished request,
          // so a retry of this request id is not left waiting for an answer that will never come.
          await this.leave(id, ticket, rid);
          throw e;
        }
      }
      return await this.process(id, ticket, rid, position, started, work);
    }
  }

  private async process<T>(id: number, ticket: string, rid: string, position: number, started: number, work: (t: string) => Promise<T>) {
    const waitedMs = Date.now() - started;
    const renew = setInterval(() => {
      (this.redis.client as any).qRenew(PROC_KEY, INFLIGHT_KEY, this.queueKey(id), String(id), ticket, LEASE_MS, rid).catch(() => undefined);
    }, RENEW_EVERY_MS);
    try {
      return { result: await work(ticket), replayed: false, position, waitedMs };
    } finally {
      clearInterval(renew);
      await this.leave(id, ticket, rid);
    }
  }

  private async leave(id: number, ticket: string, rid: string) {
    await (this.redis.client as any).qLeave(this.queueKey(id), PROC_KEY, INFLIGHT_KEY, String(id), ticket, rid, CLAIM_MS, EVENTS_CHANNEL)
      .catch((e: Error) => this.logger.warn(`leave failed for ${id}: ${e.message}`));
  }

  private async waitTurn(id: number, ticket: string, rid: string, started: number) {
    const key = String(id);
    for (;;) {
      await this.sleepUntilWoken(key, ticket);
      const c: string[] = await (this.redis.client as any).qCheck(
        STATUS_KEY, this.queueKey(id), PROC_KEY, INFLIGHT_KEY, key, ticket, LEASE_MS, CLAIM_MS, EVENTS_CHANNEL, rid,
      );
      if (c[0] === 'TURN') return;
      // TICKET_LOST = the line was cleared by a CANCEL (and the record resumed right after): still cancelled
      if (c[0] === 'ERR') throw new WriteRejected('RECORD_CANCELLED', key, 'record cancelled while waiting in line; request not processed');
      if (Date.now() - started > MAX_WAIT_MS) {
        throw new WriteRejected('QUEUE_TIMEOUT' as any, key, `waited ${MAX_WAIT_MS} ms`);
      }
    }
  }

  private sleepUntilWoken(id: string, ticket: string): Promise<void> {
    return new Promise((resolve) => {
      const done = () => { clearTimeout(t); this.wake.delete(ticket); this.byRecord.get(id)?.delete(ticket); resolve(); };
      const t = setTimeout(done, POLL_MS);
      this.wake.set(ticket, done);
      (this.byRecord.get(id) ?? this.byRecord.set(id, new Set()).get(id)!).add(ticket);
    });
  }

  private waitDuplicate(rid: string): Promise<void> {
    return new Promise((resolve) => {
      const set = this.dupWaiters.get(rid) ?? this.dupWaiters.set(rid, new Set()).get(rid)!;
      const done = () => { clearTimeout(t); set.delete(done); if (!set.size) this.dupWaiters.delete(rid); resolve(); };
      const t = setTimeout(done, POLL_MS);
      set.add(done);
    });
  }

  private onEvent(msg: string) {
    const i = msg.indexOf(':');
    const type = msg.slice(0, i);
    const value = msg.slice(i + 1);
    if (type === 'turn') this.wake.get(value)?.();
    else if (type === 'cancel') for (const t of [...(this.byRecord.get(value) ?? [])]) this.wake.get(t)?.();
    else if (type === 'done') for (const w of [...(this.dupWaiters.get(value) ?? [])]) w();
  }
}
