import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { randomUUID } from 'crypto';
import { KEY_PREFIX as MUMIN_PREFIX } from '../mumin/mumin.cache';
import { STATUS_KEY as MUMIN_STATUS } from '../mumin/mumin.redis-writer';
import Redis from 'ioredis';
import { BOOK_LUA, CONFIRM_LUA, EDIT_REQUEST_LUA, EXPIRE_LUA, RELEASE_LUA, UPDATE_ZONE_LUA, UPSERT_ZONE_LUA, withRedisLog, ZONE_FREEZE_LUA, ZONE_UNFREEZE_LUA } from './tickets.lua';
import { TicketsOpLog } from './tickets.oplog';

export const ZONES_KEY = 'tb:zones';   // SET of zone ids
const REQS_KEY = 'tb:reqs';
const SEQ_KEY = 'tb:seq';
const SWEEP_MS = 1000;                 // how often expired freezes are returned
const SEED_LOCK = 'tb:seeded';

export type Status = 'FROZEN' | 'WAITING' | 'BOOKED' | 'RELEASED' | 'EXPIRED' | 'CANCELLED' | 'BOOKING_CANCELLED';
export const STATUSES: Status[] = ['FROZEN', 'WAITING', 'BOOKED', 'RELEASED', 'EXPIRED', 'CANCELLED', 'BOOKING_CANCELLED'];

export interface TicketRequest {
  rid: string; seq: number; zone: string; user: string; name?: string; jamaat?: string; qty: number; kind: 'USER' | 'ADMIN'; note?: string;
  status: Status; requestedAt: number; frozenAt?: number; bookedAt?: number; endedAt?: number; expiresAt?: number;
  cause?: 'DIRECT' | 'WAITLIST'; freedBy?: string; joinPos?: number; leftLineAt?: number;
  position?: number; // live place in the waiting line (1 = next)
  edits?: number; editedAt?: number; lastEdit?: string;
  duringZoneFreeze?: string; // id of the zone freeze this request arrived in
}

export interface Zone {
  id: string; city: string; name: string; show: string; total: number; available: number; frozen: number; booked: number;
  holdMs: number; requests: number; duplicates: number; waiting: number; counts: Record<string, number>; createdAt: number;
  zoneFrozen: boolean; zoneFreeze?: { id: string; at: number; reason: string; allowPay: boolean; heldRequests: number };
}

export interface BookOutcome { outcome: 'FROZEN' | 'WAITING' | 'DUPLICATE' | 'ERR'; code?: string; request?: TicketRequest; ms: number }

export class TicketError extends Error {
  constructor(readonly code: string, readonly request?: TicketRequest) { super(code); }
}

const SEED: { city: string; name: string; total: number }[] = [
  { city: 'Chennai', name: 'Velachery', total: 120 },
  { city: 'Chennai', name: 'T Nagar', total: 80 },
  { city: 'Chennai', name: 'Anna Nagar', total: 60 },
  { city: 'Bengaluru', name: 'Koramangala', total: 100 },
  { city: 'Bengaluru', name: 'Whitefield', total: 50 },
  { city: 'Coimbatore', name: 'RS Puram', total: 40 },
];

const toEvent = ([id, f]: [string, string[]]) => {
  const o: Record<string, string> = { id };
  for (let i = 0; i < f.length; i += 2) o[f[i]] = f[i + 1];
  return o;
};

export const slug =(s: string) => s.toLowerCase().trim().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');

@Injectable()
export class TicketsService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(TicketsService.name);
  private sweeper?: NodeJS.Timeout;
  private sweeping = false;

  /** Redis connection of the ticket module: every command on it is recorded in the Redis key log. */
  private readonly r: Redis;

  constructor(private readonly oplog: TicketsOpLog) {
    this.r = oplog.client;
    oplog.defineScript('tbBook', 12, withRedisLog(BOOK_LUA));
    oplog.defineScript('tbConfirm', 8, withRedisLog(CONFIRM_LUA));
    oplog.defineScript('tbRelease', 8, withRedisLog(RELEASE_LUA));
    oplog.defineScript('tbExpire', 8, withRedisLog(EXPIRE_LUA));
    oplog.defineScript('tbUpsertZone', 8, withRedisLog(UPSERT_ZONE_LUA));
    oplog.defineScript('tbUpdateZone', 8, withRedisLog(UPDATE_ZONE_LUA));
    oplog.defineScript('tbEditRequest', 8, withRedisLog(EDIT_REQUEST_LUA));
    oplog.defineScript('tbZoneFreeze', 9, withRedisLog(ZONE_FREEZE_LUA));
    oplog.defineScript('tbZoneUnfreeze', 9, withRedisLog(ZONE_UNFREEZE_LUA));
  }

  async onModuleInit() {
    // first instance ever to start creates the demo cities/zones
    if (await this.r.set(SEED_LOCK, '1', 'NX')) {
      await this.oplog.trace('create-default-zones', {}, async () => {
        for (const z of SEED) await this.upsertZone({ ...z, show: 'Evening show 7:00 PM', holdSeconds: 60 });
      }, () => ({ result: `${SEED.length} zones created` }));
      this.logger.log(`Seeded ${SEED.length} ticket zones`);
    }
    this.sweeper = setInterval(() => this.sweep(), SWEEP_MS);
  }

  onModuleDestroy() {
    clearInterval(this.sweeper);
  }

  private keys(zone: string) {
    return [`tb:zone:${zone}`, REQS_KEY, `tb:wait:${zone}`, `tb:holds:${zone}`, `tb:active:${zone}`, `tb:ev:${zone}`, SEQ_KEY, `tb:zreqs:${zone}`];
  }

  /**
   * Return expired freezes on every zone (safe on all instances at once: each expiry is one atomic script).
   * Runs every second; logged in the Redis key log only when something actually expired.
   */
  private async sweep() {
    if (this.sweeping) return;
    this.sweeping = true;
    try {
      for (const z of await this.oplog.client.smembers(ZONES_KEY)) {
        await this.oplog.trace('expire-freezes', { zone: z }, async () => Number(await this.oplog.lua('tbExpire', ...this.keys(z), z, 500)),
          (n) => ({ skip: n === 0, result: `${n} freeze(s) expired, tickets returned (waiting line served first)` }));
      }
    } catch (e) {
      this.logger.warn(`sweep failed: ${(e as Error).message}`);
    } finally {
      this.sweeping = false;
    }
  }

  // ---- zones ------------------------------------------------------------------------------------

  async upsertZone(z: { city: string; name: string; show: string; total: number; holdSeconds: number }) {
    const id = `${slug(z.city)}-${slug(z.name)}`;
    const [res, extra] = await this.oplog.lua('tbUpsertZone', ...this.keys(id), id, z.city.trim(), z.name.trim(), z.show.trim(), z.total, z.holdSeconds * 1000);
    if (res === 'ERR') throw new TicketError(`TOTAL_BELOW_USED: ${extra} tickets are already frozen or booked`);
    await this.r.sadd(ZONES_KEY, id);
    return { id, result: res, movedFromLine: Number(extra) };
  }

  /** Edit a zone in place; any field may be left out. Tickets up -> waiting line served first. */
  async updateZone(id: string, f: { city?: string; name?: string; show?: string; total?: number; holdSeconds?: number }) {
    const [res, extra, changes] = await this.oplog.lua('tbUpdateZone', ...this.keys(id), id, f.city?.trim() ?? '', f.name?.trim() ?? '', f.show?.trim() ?? '', f.total ?? 0, (f.holdSeconds ?? 0) * 1000);
    // ERR replies: extra = code, changes = detail (tickets in use for TOTAL_BELOW_USED)
    if (res === 'ERR') throw new TicketError(extra === 'TOTAL_BELOW_USED' ? `TOTAL_BELOW_USED: ${changes} tickets are frozen or booked, so the total cannot go below that` : extra);
    return { id, result: res, movedFromLine: Number(extra), changes: changes || 'nothing changed', zone: await this.zone(id) };
  }

  /** Freeze everything except booked tickets in a zone (see ZONE_FREEZE_LUA). */
  async zoneFreeze(zone: string, reason: string, allowPay: boolean) {
    const [res, a, b] = await this.oplog.lua('tbZoneFreeze', ...this.keys(zone), `tb:pauses:${zone}`, zone, `zf-${Date.now().toString(36)}`, reason, allowPay ? '1' : '0');
    if (res === 'ERR') throw new TicketError(a === 'ALREADY_FROZEN' ? 'ALREADY_FROZEN' : a); // a = code, b = detail
    return { freeze: JSON.parse(a), zone: await this.zone(zone), detail: b || undefined };
  }

  /** Unfreeze: held timers resume, then the waiting line is served in request order. */
  async zoneUnfreeze(zone: string) {
    const [res, a, moved] = await this.oplog.lua('tbZoneUnfreeze', ...this.keys(zone), `tb:pauses:${zone}`, zone);
    if (res === 'ERR') throw new TicketError(a);
    return { freeze: JSON.parse(a), movedFromLine: Number(moved), zone: await this.zone(zone) };
  }

  /** Edit one request: tickets, freeze extension, admin reason. See EDIT_REQUEST_LUA for the rules. */
  async editRequest(zone: string, rid: string, f: { qty?: number; extendSeconds?: number; note?: string }) {
    const [res, body, extra] = await this.oplog.lua('tbEditRequest', ...this.keys(zone), zone, rid, f.qty ?? 0, (f.extendSeconds ?? 0) * 1000, f.note?.trim() ?? '');
    if (res === 'ERR') throw new TicketError(body);
    const request = JSON.parse(body) as TicketRequest;
    if (request.status === 'WAITING') request.position = await this.position(zone, rid);
    return { outcome: res, request, movedFromLine: Number(extra) };
  }

  async zones(): Promise<Zone[]> {
    const ids = await this.r.smembers(ZONES_KEY);
    const p = this.r.pipeline();
    for (const id of ids) p.hgetall(`tb:zone:${id}`).llen(`tb:wait:${id}`);
    const out = (await p.exec()) ?? [];
    const zones = ids.map((_, i) => this.toZone(out[i * 2][1] as Record<string, string>, out[i * 2 + 1][1] as number)).filter((z): z is Zone => !!z);
    return zones.sort((a, b) => a.city.localeCompare(b.city) || a.name.localeCompare(b.name));
  }

  async zone(id: string): Promise<Zone> {
    const [[, h], [, w]] = (await this.r.pipeline().hgetall(`tb:zone:${id}`).llen(`tb:wait:${id}`).exec()) as any;
    const z = this.toZone(h, w);
    if (!z) throw new TicketError('ZONE_NOT_FOUND');
    return z;
  }

  private toZone(h: Record<string, string>, waiting: number): Zone | null {
    if (!h || !h.id) return null;
    const n = (k: string) => Number(h[k] ?? 0);
    const counts: Record<string, number> = {};
    for (const s of STATUSES) counts[s] = n(`n_${s}`);
    return {
      id: h.id, city: h.city, name: h.name, show: h.show, total: n('total'), available: n('available'), frozen: n('frozen'),
      booked: n('booked'), holdMs: n('holdMs'), requests: n('requests'), duplicates: n('duplicates'), waiting, counts, createdAt: n('createdAt'),
      zoneFrozen: h.paused === '1',
      zoneFreeze: h.paused === '1' ? { id: h.pauseId, at: n('pausedAt'), reason: h.pauseReason ?? '', allowPay: h.pauseAllowPay === '1', heldRequests: n('n_FROZEN') } : undefined,
    };
  }

  /** Clear every request, freeze and event of a zone; tickets back to the full total. */
  async resetZone(id: string) {
    const z = await this.zone(id);
    const rids = await this.r.zrange(`tb:zreqs:${id}`, 0, -1);
    for (let i = 0; i < rids.length; i += 5000) await this.r.hdel(REQS_KEY, ...rids.slice(i, i + 5000));
    const counters: (string | number)[] = ['available', z.total, 'frozen', 0, 'booked', 0, 'requests', 0, 'duplicates', 0];
    for (const s of STATUSES) counters.push(`n_${s}`, 0);
    await this.r.multi()
      .del(`tb:wait:${id}`, `tb:holds:${id}`, `tb:active:${id}`, `tb:ev:${id}`, `tb:zreqs:${id}`, `tb:dups:${id}`, `tb:pauses:${id}`)
      .hdel(`tb:zone:${id}`, 'paused', 'pausedAt', 'pauseId', 'pauseReason', 'pauseAllowPay')
      .hset(`tb:zone:${id}`, ...counters)
      .xadd(`tb:ev:${id}`, '*', 'ts', String(Date.now()), 'type', 'ZONE_RESET', 'rid', '', 'seq', '', 'user', '', 'qty', '',
        'available', String(z.total), 'frozen', '0', 'booked', '0', 'waiting', '0', 'note', `report cleared (${rids.length} requests removed)`)
      .exec();
    return { id, removedRequests: rids.length };
  }

  /**
   * Clear ALL ticket data end to end: every request, freeze, waiting line, duplicate count, ITS history
   * and timeline, in every zone. Zones and their settings (city, name, total, freeze time) stay.
   * Mumin_Mast data is not touched.
   */
  async clearAll() {
    const ids = await this.r.smembers(ZONES_KEY);
    const requests = await this.r.hlen(REQS_KEY);
    let itsKeys = 0;
    let cursor = '0';
    do {
      const [next, keys] = await this.r.scan(cursor, 'MATCH', 'tb:its:*', 'COUNT', 5000);
      cursor = next;
      if (keys.length) { itsKeys += keys.length; await this.r.unlink(...keys); }
    } while (cursor !== '0');
    const m = this.r.multi().unlink(REQS_KEY, SEQ_KEY);
    for (const id of ids) {
      const total = Number((await this.r.hget(`tb:zone:${id}`, 'total')) ?? 0);
      const counters: (string | number)[] = ['available', total, 'frozen', 0, 'booked', 0, 'requests', 0, 'duplicates', 0];
      for (const s of STATUSES) counters.push(`n_${s}`, 0);
      m.unlink(`tb:wait:${id}`, `tb:holds:${id}`, `tb:active:${id}`, `tb:ev:${id}`, `tb:zreqs:${id}`, `tb:dups:${id}`, `tb:pauses:${id}`)
        .hdel(`tb:zone:${id}`, 'paused', 'pausedAt', 'pauseId', 'pauseReason', 'pauseAllowPay')
        .hset(`tb:zone:${id}`, ...counters)
        .xadd(`tb:ev:${id}`, '*', 'ts', String(Date.now()), 'type', 'ZONE_RESET', 'rid', '', 'seq', '', 'user', '', 'qty', '',
          'available', String(total), 'frozen', '0', 'booked', '0', 'waiting', '0', 'note', 'all ticket data cleared (every zone)');
    }
    await m.exec();
    this.logger.log(`Cleared all ticket data: ${requests} requests, ${ids.length} zones reset`);
    return { zones: ids.length, removedRequests: requests, removedItsHistories: itsKeys };
  }

  async deleteZone(id: string) {
    await this.resetZone(id);
    await this.r.multi().del(`tb:zone:${id}`, `tb:ev:${id}`).srem(ZONES_KEY, id).exec();
    return { id, deleted: true };
  }

  // ---- requests -----------------------------------------------------------------------------------

  /** user = Mumin_ID (ITS id) */
  async book(zone: string, muminId: number, qty: number, requestId?: string): Promise<BookOutcome> {
    return this.run(zone, requestId ?? randomUUID(), String(muminId), qty, 'USER', '');
  }

  async adminFreeze(zone: string, qty: number, reason: string): Promise<BookOutcome> {
    return this.run(zone, `admin-${randomUUID()}`, 'ADMIN', qty, 'ADMIN', reason);
  }

  private async run(zone: string, rid: string, user: string, qty: number, kind: 'USER' | 'ADMIN', note: string): Promise<BookOutcome> {
    const t = process.hrtime.bigint();
    const its = kind === 'USER' ? user : '0';
    const [outcome, body, code] = await this.oplog.lua('tbBook', ...this.keys(zone), `${MUMIN_PREFIX}${its}`, MUMIN_STATUS, `tb:dups:${zone}`, `tb:its:${its}`,
      zone, rid, user, qty, kind, note);
    const ms = Number(process.hrtime.bigint() - t) / 1e6;
    if (outcome === 'ERR') return { outcome, code: body, ms }; // ERR replies carry the code in the 2nd field
    const request = body && body !== '{}' ? JSON.parse(body) as TicketRequest : undefined;
    // A new WAITING request's place is exactly its join place (same atomic step) - no scan of the line,
    // which would cost O(line length) per request under load. Duplicates do not report a place.
    if (outcome === 'WAITING' && request) request.position = request.joinPos;
    return { outcome, code: code || undefined, request, ms };
  }

  async confirm(zone: string, rid: string) {
    const [res, a, b] = await this.oplog.lua('tbConfirm', ...this.keys(zone), zone, rid);
    if (res === 'ERR') throw new TicketError(a, b ? JSON.parse(b) : undefined); // a = code, b = request
    return { outcome: res, code: b || undefined, request: JSON.parse(a) as TicketRequest };
  }

  async release(zone: string, rid: string, reason: string) {
    const [res, body, extra] = await this.oplog.lua('tbRelease', ...this.keys(zone), zone, rid, reason);
    if (res === 'ERR') throw new TicketError(body);
    return { outcome: res, request: JSON.parse(body) as TicketRequest, movedFromLine: res === 'OK' ? Number(extra) : 0, code: res === 'OK' ? undefined : extra };
  }

  async request(rid: string): Promise<TicketRequest> {
    const s = await this.r.hget(REQS_KEY, rid);
    if (!s) throw new TicketError('REQUEST_NOT_FOUND');
    const r = JSON.parse(s) as TicketRequest;
    if (r.status === 'WAITING') r.position = await this.position(r.zone, rid);
    return r;
  }

  /**
   * One request with everything needed to explain it: the zone, who freed the tickets it got (ITS + name),
   * which waiting requests got ITS tickets when it ended, and the duplicates its ITS id sent in that zone.
   */
  async requestDetails(rid: string) {
    const r = await this.request(rid);
    const z = await this.zone(r.zone).catch(() => null);
    const brief = (x: TicketRequest) => ({ rid: x.rid, seq: x.seq, user: x.user, name: x.name ?? '', kind: x.kind, note: x.note ?? '', qty: x.qty, frozenAt: x.frozenAt });
    const freedBy = r.freedBy?.startsWith('zone-unfreeze:') ? { rid: r.freedBy, seq: 0, user: '', name: '', kind: 'ZONE', note: 'the zone was unfrozen', qty: 0, frozenAt: undefined }
      : r.freedBy && r.freedBy !== 'ZONE_UPDATED' ? await this.request(r.freedBy).then(brief).catch(() => null) : null;
    const gaveTo = r.endedAt !== undefined ? (await this.allRequests(r.zone)).filter((x) => x.freedBy === r.rid).sort((a, b) => a.seq - b.seq).map(brief) : [];
    const dups = r.kind === 'USER' ? Number((await this.r.hget(`tb:dups:${r.zone}`, r.user)) ?? 0) : 0;
    return {
      request: r,
      zone: z ? { id: z.id, city: z.city, name: z.name, show: z.show, holdMs: z.holdMs } : { id: r.zone, city: '', name: r.zone, show: '', holdMs: 0 },
      freedBy, gaveTo, itsDuplicatesInZone: dups,
    };
  }

  private async position(zone: string, rid: string) {
    const p = await this.r.lpos(`tb:wait:${zone}`, rid);
    return p === null ? undefined : p + 1;
  }

  private async requestsByIds(rids: string[]): Promise<TicketRequest[]> {
    const out: TicketRequest[] = [];
    for (let i = 0; i < rids.length; i += 5000) {
      const part = rids.slice(i, i + 5000);
      if (!part.length) break;
      for (const s of await this.r.hmget(REQS_KEY, ...part)) if (s) out.push(JSON.parse(s));
    }
    return out;
  }

  async allRequests(zone: string) {
    return this.requestsByIds(await this.r.zrange(`tb:zreqs:${zone}`, 0, -1));
  }

  // ---- report -------------------------------------------------------------------------------------

  async report(zone: string, limit: number) {
    const z = await this.zone(zone);
    const [line, recent, events, open] = await Promise.all([
      this.r.lrange(`tb:wait:${zone}`, 0, 999),
      this.r.zrevrange(`tb:zreqs:${zone}`, 0, limit - 1),
      this.r.xrevrange(`tb:ev:${zone}`, '+', '-', 'COUNT', limit),
      this.r.hvals(`tb:active:${zone}`),
    ]);
    const waiting = (await this.requestsByIds(line)).map((r, i) => ({ ...r, position: i + 1 }));
    const pos = new Map(waiting.map((r) => [r.rid, r.position]));
    const requests = (await this.requestsByIds(recent)).map((r) => (pos.has(r.rid) ? { ...r, position: pos.get(r.rid) } : r));
    // waiting requests further back than the first 1000 in line: look their place up one by one
    const far = requests.filter((r) => r.status === 'WAITING' && r.position === undefined);
    if (far.length) {
      const p = this.r.pipeline();
      for (const r of far) p.lpos(`tb:wait:${zone}`, r.rid);
      const res = (await p.exec()) ?? [];
      far.forEach((r, i) => { const v = res[i][1] as number | null; if (v !== null) r.position = v + 1; });
    }
    const frozen = (await this.requestsByIds(open)).filter((r) => r.status === 'FROZEN').sort((a, b) => (a.frozenAt ?? 0) - (b.frozenAt ?? 0));
    return {
      zone: z,
      now: Date.now(),
      waiting,
      frozen,
      requests,
      events: events.map(toEvent),
    };
  }

  /**
   * Admin freezes, newest first. For each: the zone at freeze / unfreeze time, every request that came
   * WHILE it was frozen (what happened on arrival and afterwards), duplicates blocked in that window,
   * the waiting requests that got the unfrozen tickets, and the requests that came AFTER the unfreeze.
   */
  async freezes(zone: string, limit: number) {
    await this.zone(zone);
    const all = (await this.allRequests(zone)).sort((a, b) => a.seq - b.seq);
    const admins = all.filter((r) => r.kind === 'ADMIN' && r.frozenAt !== undefined).reverse().slice(0, limit);
    const now = Date.now();
    const LIST = 300;
    const out = [];
    for (const a of admins) {
      const start = a.frozenAt!;
      const end = a.bookedAt ?? a.endedAt; // undefined = still frozen
      const inWindow = (t: number) => t >= start && (end === undefined || t < end);
      const during = all.filter((r) => r.rid !== a.rid && inWindow(r.requestedAt));
      const served = all.filter((r) => r.freedBy === a.rid);
      const after = end === undefined ? [] : all.filter((r) => r.requestedAt >= end);
      const evs = (await this.r.xrange(`tb:ev:${zone}`, `${start}-0`, end === undefined ? '+' : `${end}-18446744073709551615`, 'COUNT', 50000)).map(toEvent);
      const dups = evs.filter((e) => e.type === 'DUPLICATE_BLOCKED');
      const atFreeze = evs.find((e) => e.type === 'FROZEN' && e.rid === a.rid);
      // last event of the window = state after the unfreeze AND after the waiting line was served (same script)
      const atEnd = end === undefined ? undefined : evs[evs.length - 1];
      const lined = during.filter((r) => r.joinPos !== undefined);
      out.push({
        freeze: a,
        start, end, durationMs: (end ?? now) - start, active: end === undefined,
        atFreeze, atEnd,
        during: {
          total: during.length,
          frozenOnArrival: during.filter((r) => r.joinPos === undefined && r.frozenAt !== undefined).length,
          wentToLine: lined.length,
          servedLater: lined.filter((r) => r.frozenAt !== undefined).length,
          servedByThisUnfreeze: lined.filter((r) => r.freedBy === a.rid).length,
          stillWaiting: lined.filter((r) => r.status === 'WAITING').length,
          leftLine: lined.filter((r) => r.status === 'CANCELLED').length,
          duplicatesBlocked: dups.length,
          requests: during.slice(0, LIST),
        },
        duplicates: dups.slice(0, LIST),
        served,
        after: {
          total: after.length,
          frozenOnArrival: after.filter((r) => r.joinPos === undefined && r.frozenAt !== undefined).length,
          wentToLine: after.filter((r) => r.joinPos !== undefined).length,
          requests: after.slice(0, LIST),
        },
      });
    }
    const zoneFreezes = [];
    for (const raw of (await this.r.lrange(`tb:pauses:${zone}`, 0, -1)).reverse().slice(0, limit)) {
      const f = JSON.parse(raw) as { id: string; at: number; reason: string; allowPay: boolean; endedAt?: number; durationMs?: number; movedFromLine?: number; timersResumed?: number; atFreeze: any; atUnfreeze?: any };
      const end = f.endedAt;
      const during = all.filter((r) => r.kind === 'USER' && r.requestedAt >= f.at && (end === undefined || r.requestedAt < end));
      const served = all.filter((r) => r.freedBy === `zone-unfreeze:${f.id}`).sort((a, b) => a.seq - b.seq);
      const after = end === undefined ? [] : all.filter((r) => r.kind === 'USER' && r.requestedAt >= end);
      // anyone who got tickets while the zone was frozen (must be nobody)
      const gotTicketsWhileFrozen = all.filter((r) => r.kind === 'USER' && r.frozenAt !== undefined && r.frozenAt > f.at && (end === undefined || r.frozenAt < end));
      zoneFreezes.push({
        ...f, active: end === undefined, durationMs: (end ?? now) - f.at,
        during: { total: during.length, wentToLine: during.filter((r) => r.joinPos !== undefined).length, servedOnUnfreeze: during.filter((r) => r.freedBy === `zone-unfreeze:${f.id}`).length,
          stillWaiting: during.filter((r) => r.status === 'WAITING').length, requests: during.slice(0, 300) },
        served, gotTicketsWhileFrozen: gotTicketsWhileFrozen.length,
        after: { total: after.length, frozenOnArrival: after.filter((r) => r.joinPos === undefined && r.frozenAt !== undefined).length, wentToLine: after.filter((r) => r.joinPos !== undefined).length, requests: after.slice(0, 300) },
      });
    }
    return { zone, now, freezes: out, zoneFreezes };
  }

  /**
   * Full-scan checks for the report:
   *  - tickets: available + frozen + booked = total, and they match the requests' quantities
   *  - no duplicates: every request id once, at most one open request per user
   *  - FIFO: no user request got tickets while an earlier request was still waiting in line
   */
  async verify(zone: string) {
    const z = await this.zone(zone);
    const all = await this.allRequests(zone);
    const sum = (s: Status) => all.filter((r) => r.status === s).reduce((a, r) => a + r.qty, 0);
    const frozenQty = sum('FROZEN');
    const bookedQty = sum('BOOKED');
    const openPerUser = new Map<string, number>();
    for (const r of all) if (r.kind === 'USER' && (r.status === 'FROZEN' || r.status === 'WAITING')) openPerUser.set(r.user, (openPerUser.get(r.user) ?? 0) + 1);
    const usersWithTwoOpen = [...openPerUser.values()].filter((n) => n > 1).length;
    const ids = new Set(all.map((r) => r.rid));
    const line = await this.r.lrange(`tb:wait:${zone}`, 0, -1);
    const seqOf = new Map(all.map((r) => [r.rid, r.seq]));
    const lineInOrder = line.every((rid, i) => i === 0 || seqOf.get(rid)! > seqOf.get(line[i - 1])!);

    // FIFO: walk in request order; latest time an earlier line member left the line must be <= my freeze time
    const sorted = [...all].sort((a, b) => a.seq - b.seq);
    let leftMax = 0;
    let leftMaxBy = '';
    const violations: string[] = [];
    let servedFromLine = 0;
    for (const r of sorted) {
      if (r.kind === 'USER' && r.frozenAt !== undefined && r.frozenAt < leftMax) {
        violations.push(`#${r.seq} ${r.rid} got tickets at ${r.frozenAt} while earlier ${leftMaxBy} was still waiting`);
      }
      if (r.joinPos !== undefined) {
        if (r.cause === 'WAITLIST') servedFromLine++;
        const left = r.leftLineAt ?? Number.POSITIVE_INFINITY; // still waiting
        if (left > leftMax) { leftMax = left; leftMaxBy = `#${r.seq} ${r.rid}`; }
      }
    }
    const pauses = (await this.r.lrange(`tb:pauses:${zone}`, 0, -1)).map((x) => JSON.parse(x) as { at: number; endedAt?: number });
    const sneaked = all.filter((r) => r.kind === 'USER' && r.frozenAt !== undefined && pauses.some((f) => r.frozenAt! > f.at && (f.endedAt === undefined || r.frozenAt! < f.endedAt)));
    const checks = [
      { name: 'available + frozen + booked = total', ok: z.available + z.frozen + z.booked === z.total, detail: `${z.available} + ${z.frozen} + ${z.booked} = ${z.available + z.frozen + z.booked} (total ${z.total})` },
      { name: 'frozen tickets = sum of FROZEN requests', ok: frozenQty === z.frozen, detail: `${frozenQty} vs ${z.frozen}` },
      { name: 'booked tickets = sum of BOOKED requests', ok: bookedQty === z.booked, detail: `${bookedQty} vs ${z.booked}` },
      { name: 'no tickets oversold (available >= 0)', ok: z.available >= 0, detail: `${z.available} free` },
      { name: 'every request id processed once', ok: ids.size === all.length, detail: `${all.length} requests, ${ids.size} unique ids, ${z.duplicates} duplicates blocked` },
      { name: 'one open request per ITS id', ok: usersWithTwoOpen === 0, detail: `${openPerUser.size} ITS ids with an open request, ${usersWithTwoOpen} with more than one` },
      { name: 'waiting line in request order', ok: lineInOrder && line.length === z.counts.WAITING, detail: `${line.length} waiting` },
      { name: 'zone freeze: nobody got tickets while the zone was frozen', ok: sneaked.length === 0, detail: pauses.length ? `${pauses.length} zone freeze(s), ${sneaked.length} requests got tickets inside a freeze` : 'no zone freeze yet' },
      { name: 'FIFO: nobody overtook an earlier waiting request', ok: violations.length === 0, detail: violations.length ? violations.slice(0, 3).join('; ') : `${servedFromLine} served from the line in order` },
    ];
    return { zone: z.id, requests: all.length, passed: checks.every((c) => c.ok), checks };
  }

  // ---- ITS (Mumin_ID) view ----------------------------------------------------------------------------

  /** Every booking request of one ITS id across all zones, with its open request and duplicates per zone. */
  async its(muminId: number) {
    const rec = await this.r.get(`${MUMIN_PREFIX}${muminId}`);
    if (!rec) throw new TicketError('ITS_NOT_FOUND');
    const m = JSON.parse(rec);
    const requests = (await this.requestsByIds(await this.r.zrevrange(`tb:its:${muminId}`, 0, 499)))
      .filter((r) => r.user === String(muminId));
    for (const r of requests) if (r.status === 'WAITING') r.position = await this.position(r.zone, r.rid);
    const zones = await this.r.smembers(ZONES_KEY);
    const p = this.r.pipeline();
    for (const z of zones) p.hget(`tb:active:${z}`, String(muminId)).hget(`tb:dups:${z}`, String(muminId));
    const out = (await p.exec()) ?? [];
    const perZone = zones.map((z, i) => ({ zone: z, open: out[i * 2][1] as string | null, duplicatesBlocked: Number(out[i * 2 + 1][1] ?? 0) }))
      .filter((z) => z.open || z.duplicatesBlocked);
    return { muminId, name: m.Short_Name ?? '', jamaat: m.Jamaat ?? '', gender: m.Gender ?? '', cancelled: (await this.r.hget(MUMIN_STATUS, String(muminId))) === 'CANCELLED', perZone, requests };
  }

  /** `n` real ITS ids from the Mumin_Mast records in Redis (random spread over a scan). */
  async sampleIts(n: number): Promise<number[]> {
    const ids: number[] = [];
    let cursor = '0';
    do {
      const [next, keys] = await this.r.scan(cursor, 'MATCH', `${MUMIN_PREFIX}*`, 'COUNT', 5000);
      cursor = next;
      for (const k of keys) ids.push(Number(k.slice(MUMIN_PREFIX.length)));
    } while (cursor !== '0' && ids.length < n * 4);
    for (let i = ids.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [ids[i], ids[j]] = [ids[j], ids[i]]; }
    if (ids.length < n) throw new TicketError(`only ${ids.length} Mumin records in Redis`);
    return ids.slice(0, n);
  }

  // ---- ITS concurrency burst ------------------------------------------------------------------------

  /**
   * `its` real ITS ids each fire `perIts` booking requests AT THE SAME INSTANT (Promise.all), `concurrency`
   * ITS ids in parallel. `sameIdPct` % of an ITS id's parallel requests re-use the same request id (client
   * retry), the rest are different request ids (double click / two devices). Goal: per ITS id at most ONE
   * request is accepted (FROZEN or WAITING), every other one is DUPLICATE.
   */
  async simulate(zone: string, o: { its: number; perIts: number; sameIdPct: number; maxQty: number; concurrency: number }) {
    await this.zone(zone);
    const ids = await this.sampleIts(o.its);
    const run = Date.now().toString(36);
    const result: Record<string, number> = { FROZEN: 0, WAITING: 0, DUPLICATE_REQUEST_ID: 0, DUPLICATE_ITS: 0, ERR: 0 };
    const acceptedPerIts = new Map<number, number>();
    const lat: number[] = [];
    let next = 0;
    const started = Date.now();
    const worker = async () => {
      while (next < ids.length) {
        const its = ids[next++];
        const qty = 1 + Math.floor(Math.random() * o.maxQty);
        const first = `sim-${run}-${its}-0`;
        const burst = Array.from({ length: o.perIts }, (_, k) => (k > 0 && Math.random() * 100 < o.sameIdPct ? first : `sim-${run}-${its}-${k}`));
        const answers = await Promise.all(burst.map((rid) => this.book(zone, its, qty, rid).catch(() => null)));
        let accepted = 0;
        for (const r of answers) {
          if (!r || r.outcome === 'ERR') { result.ERR++; continue; }
          lat.push(r.ms);
          if (r.outcome === 'DUPLICATE') result[r.code === 'ITS_HAS_OPEN_REQUEST' ? 'DUPLICATE_ITS' : 'DUPLICATE_REQUEST_ID']++;
          else { result[r.outcome]++; accepted++; }
        }
        acceptedPerIts.set(its, accepted);
      }
    };
    await Promise.all(Array.from({ length: o.concurrency }, worker));
    const ms = Date.now() - started;
    const sent = o.its * o.perIts;
    lat.sort((a, b) => a - b);
    const pct = (p: number) => lat.length ? +lat[Math.min(lat.length - 1, Math.floor(lat.length * p))].toFixed(2) : 0;
    const counts = [...acceptedPerIts.values()];
    const sample = ids.slice(0, 5);
    // one summary entry: logging every command of tens of thousands of requests would bury everything else
    this.oplog.note('burst-test', {
      zone, ms,
      result: `${sent} requests from ${o.its} ITS ids: ${result.FROZEN} frozen, ${result.WAITING} waiting, ${result.DUPLICATE_REQUEST_ID + result.DUPLICATE_ITS} duplicates blocked, ${result.ERR} errors`,
      note: `per-request Redis commands not listed (bulk test). Each request = 1 atomic script tbBook on keys tb:zone:${zone}, tb:reqs, tb:wait:${zone}, tb:holds:${zone}, tb:active:${zone}, tb:ev:${zone}, tb:seq, tb:zreqs:${zone}, mumin:rec:<ITS>, mumin:status, tb:dups:${zone}, tb:its:<ITS>. Book one request by hand to see every command.`,
    });
    return {
      zone, its: o.its, perIts: o.perIts, sent, ms, perSecond: Math.round(sent / (ms / 1000)), result,
      perItsCheck: {
        exactlyOne: counts.filter((n) => n === 1).length,
        none: counts.filter((n) => n === 0).length,          // ITS already had an open request in this zone
        moreThanOne: counts.filter((n) => n > 1).length,     // must be 0
      },
      sampleIts: sample,
      latencyMs: { p50: pct(0.5), p95: pct(0.95), max: pct(1) },
      after: await this.zone(zone),
    };
  }

  /**
   * Finish the current freezes oldest-first: `payPct` % pay (BOOKED), the rest cancel (RELEASED).
   * Every release hands its tickets to the waiting line in request order.
   */
  async settle(zone: string, payPct: number) {
    const open = (await this.requestsByIds(await this.r.hvals(`tb:active:${zone}`)))
      .filter((r) => r.status === 'FROZEN' && r.kind === 'USER').sort((a, b) => (a.frozenAt ?? 0) - (b.frozenAt ?? 0) || a.seq - b.seq);
    let booked = 0, released = 0, moved = 0, expired = 0;
    for (const r of open) {
      try {
        if (Math.random() * 100 < payPct) { await this.confirm(zone, r.rid); booked++; }
        else { const x = await this.release(zone, r.rid, 'user cancelled before payment (settle)'); released++; moved += x.movedFromLine; }
      } catch {
        expired++;
      }
    }
    return { zone, processed: open.length, booked, released, expired, movedFromLine: moved, after: await this.zone(zone) };
  }
}
