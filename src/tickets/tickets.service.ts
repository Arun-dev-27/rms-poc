import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { randomUUID } from 'crypto';
import { KEY_PREFIX as MUMIN_PREFIX } from '../mumin/mumin.cache';
import { STATUS_KEY as MUMIN_STATUS } from '../mumin/mumin.redis-writer';
import Redis from 'ioredis';
import { BOOK_LUA, CONFIRM_LUA, EDIT_REQUEST_LUA, EXPIRE_LUA, RELEASE_LUA, UPDATE_ZONE_LUA, UPSERT_ZONE_LUA, withRedisLog, ZONE_FREEZE_LUA, ZONE_UNFREEZE_LUA } from './tickets.lua';
import { HTTP_LOG_KEY, HttpRecord } from './tickets.http-log';
import { TicketsOpLog } from './tickets.oplog';

export const ZONES_KEY = 'tb:zones';   // SET of zone ids
const REQS_KEY = 'tb:reqs';
const SEQ_KEY = 'tb:seq';
const SWEEP_MS = 1000;                 // how often expired freezes are returned
const SEED_LOCK = 'tb:seeded';
const CONFIG_KEY = 'tb:config';        // HASH: business + tech settings

export interface AppConfig {
  /** confirmMode: 'direct' = a free seat is CONFIRMED at once (no hold); 'hold' = held first, then paid / confirmed */
  business: { programName: string; maxPerIts: number; confirmMode: 'direct' | 'hold' };
  tech: { terminalLines: number };
}
const DEFAULT_CONFIG: AppConfig = { business: { programName: 'Raza Registration', maxPerIts: 1, confirmMode: 'direct' }, tech: { terminalLines: 100 } };

/** Redis keys an event touched, for the terminal log. */
function keysFor(type: string, zone: string): string[] {
  switch (type) {
    case 'FROZEN': return ['tb:reqs', `tb:holds:${zone}`, `tb:zone:${zone}`];
    case 'WAITING': case 'PAUSED': return ['tb:reqs', `tb:wait:${zone}`];
    case 'NO_SEATS': case 'REFUSED_ZONE_FROZEN': return ['tb:reqs', `tb:zreqs:${zone}`];
    case 'BOOKED': return ['tb:reqs', `tb:zone:${zone}`];
    case 'RELEASED': case 'EXPIRED': case 'CANCELLED': case 'BOOKING_CANCELLED': return ['tb:reqs', `tb:zone:${zone}`, `tb:wait:${zone}`];
    case 'DUPLICATE_BLOCKED': return [`tb:active:${zone}`, `tb:dups:${zone}`];
    case 'EDITED': return ['tb:reqs', `tb:zone:${zone}`];
    default: return [`tb:zone:${zone}`];
  }
}

/** "Chennai" -> "chennai", "Navi Mumbai" -> "navi-mumbai" */
export const cityId = (city: string) => city.trim().toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');

export type Status = 'FROZEN' | 'WAITING' | 'PAUSED' | 'NO_SEATS' | 'ZONE_FROZEN' | 'DROPPED' | 'BOOKED' | 'RELEASED' | 'EXPIRED' | 'CANCELLED' | 'BOOKING_CANCELLED';
export const STATUSES: Status[] = ['FROZEN', 'WAITING', 'PAUSED', 'NO_SEATS', 'ZONE_FROZEN', 'DROPPED', 'BOOKED', 'RELEASED', 'EXPIRED', 'CANCELLED', 'BOOKING_CANCELLED'];

export interface TicketRequest {
  rid: string; seq: number; zone: string; user: string; name?: string; jamaat?: string; qty: number; kind: 'USER' | 'ADMIN'; note?: string;
  status: Status; requestedAt: number; frozenAt?: number; bookedAt?: number; endedAt?: number; expiresAt?: number;
  cause?: 'DIRECT' | 'WAITLIST'; freedBy?: string; joinPos?: number; leftLineAt?: number;
  position?: number; // live place in the waiting line (1 = next)
  order?: number;    // arrival order inside the zone (1 = first request since the last reset)
  edits?: number; editedAt?: number; lastEdit?: string;
  duringZoneFreeze?: string; // id of the zone freeze this request arrived in
}

export interface Zone {
  id: string; city: string; name: string; show: string; total: number; available: number; frozen: number; booked: number;
  holdMs: number; requests: number; duplicates: number; waiting: number; counts: Record<string, number>; createdAt: number;
  zoneFrozen: boolean; zoneFreeze?: { id: string; at: number; reason: string; allowPay: boolean; heldRequests: number };
}

export interface BookOutcome { outcome: 'FROZEN' | 'BOOKED' | 'WAITING' | 'PAUSED' | 'NO_SEATS' | 'ZONE_FROZEN' | 'DUPLICATE' | 'ERR'; code?: string; request?: TicketRequest; ms: number }

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
    await this.syncConfirmMode();
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
    await this.r.hset(`tb:zone:${id}`, 'direct', (await this.config()).business.confirmMode === 'direct' ? '1' : '0');
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

  // ---- city level: a city = every zone with the same city name ----

  /** Cities with their zones added up: seats, available, confirmed, and how many zones are frozen. */
  async cities() {
    const by = new Map<string, { id: string; city: string; zones: { id: string; name: string; zoneFrozen: boolean }[]; total: number; available: number; booked: number; frozenZones: number }>();
    for (const z of await this.zones()) {
      const id = cityId(z.city);
      const c = by.get(id) ?? { id, city: z.city, zones: [], total: 0, available: 0, booked: 0, frozenZones: 0 };
      c.zones.push({ id: z.id, name: z.name, zoneFrozen: z.zoneFrozen });
      c.total += z.total; c.available += z.available; c.booked += z.booked;
      if (z.zoneFrozen) c.frozenZones++;
      by.set(id, c);
    }
    return [...by.values()].map((c) => ({ ...c, status: c.frozenZones === 0 ? 'OPEN' : c.frozenZones === c.zones.length ? 'FROZEN' : 'PARTLY_FROZEN' }));
  }

  private async cityZones(city: string) {
    const zs = (await this.zones()).filter((z) => cityId(z.city) === city);
    if (!zs.length) throw new TicketError('CITY_NOT_FOUND');
    return zs;
  }

  /**
   * Freeze every zone of a city in ONE Redis transaction (MULTI/EXEC): Redis runs the zone freezes back to back,
   * so no registration can land in one zone of the city after another zone was frozen. Each zone gets the same
   * freeze as a zone freeze (new requests answered Frozen). Zones already frozen stay as they are.
   */
  async cityFreeze(city: string, reason: string) {
    const zs = await this.cityZones(city);
    const id = `cf-${Date.now().toString(36)}`;
    const m = this.r.multi() as any;
    zs.forEach((z, i) => m.tbZoneFreeze(...this.keys(z.id), `tb:pauses:${z.id}`, z.id, `${id}-${i}`, `city frozen: ${reason}`, '0'));
    const out = ((await m.exec()) ?? []) as [Error | null, any][];
    const zones = zs.map((z, i) => {
      const [res, a] = (out[i]?.[1]?.[0] ?? ['ERR', out[i]?.[0]?.message ?? 'NO_REPLY']) as string[];
      const f = res === 'OK' ? JSON.parse(a) : undefined;
      return { zone: z.id, name: z.name, result: res === 'OK' ? 'FROZEN' : a, available: f?.atFreeze?.available ?? z.available, booked: f?.atFreeze?.booked ?? z.booked, lastBooked: f?.atFreeze?.lastBooked };
    });
    const frozen = zones.filter((z) => z.result === 'FROZEN').length;
    return { cityFreeze: { city, cityName: zs[0].city, id, reason, at: Date.now(), zones, frozen, alreadyFrozen: zones.filter((z) => z.result === 'ALREADY_FROZEN').length,
      booked: zones.reduce((n, z) => n + z.booked, 0), available: zones.reduce((n, z) => n + z.available, 0) } };
  }

  /** Unfreeze every frozen zone of a city in ONE Redis transaction. New requests are registered again if a seat is free. */
  async cityUnfreeze(city: string) {
    const zs = await this.cityZones(city);
    const m = this.r.multi() as any;
    zs.forEach((z) => m.tbZoneUnfreeze(...this.keys(z.id), `tb:pauses:${z.id}`, z.id));
    const out = ((await m.exec()) ?? []) as [Error | null, any][];
    const zones = zs.map((z, i) => {
      const [res, a] = (out[i]?.[1]?.[0] ?? ['ERR', out[i]?.[0]?.message ?? 'NO_REPLY']) as string[];
      return { zone: z.id, name: z.name, result: res === 'OK' ? 'UNFROZEN' : a };
    });
    const unfrozen = zones.filter((z) => z.result === 'UNFROZEN').length;
    return { cityUnfreeze: { city, cityName: zs[0].city, at: Date.now(), zones, unfrozen, notFrozen: zones.filter((z) => z.result === 'ZONE_NOT_FROZEN').length } };
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
    const m = this.r.multi().unlink(REQS_KEY, SEQ_KEY, HTTP_LOG_KEY); // the request/response log starts clean too
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
    if ((outcome === 'WAITING' || outcome === 'PAUSED') && request) request.position = request.joinPos;
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

  /** Every registration of a zone in arrival order, with its order number (1 = first) and live line place. */
  async registrations(zone: string) {
    const all = await this.allRequests(zone); // tb:zreqs is sorted by arrival -> already in order
    const line = await this.r.lrange(`tb:wait:${zone}`, 0, -1);
    const place = new Map(line.map((rid, i) => [rid, i + 1]));
    return all.map((r, i) => ({ ...r, order: i + 1, position: place.get(r.rid) }));
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
    // order number inside this zone (1 = first request since the last reset): the arrival rank in tb:zreqs:<zone>
    const ranked = [...new Map([...waiting, ...requests, ...frozen].map((r) => [r.rid, r])).values()];
    if (ranked.length) {
      const p = this.r.pipeline();
      for (const r of ranked) p.zrank(`tb:zreqs:${zone}`, r.rid);
      const res = (await p.exec()) ?? [];
      const order = new Map(ranked.map((r, i) => [r.rid, res[i][1] === null ? undefined : (res[i][1] as number) + 1]));
      for (const r of [...waiting, ...requests, ...frozen]) r.order = order.get(r.rid);
    }
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
    for (const r of all) if (r.kind === 'USER' && (r.status === 'FROZEN' || r.status === 'WAITING' || r.status === 'PAUSED')) openPerUser.set(r.user, (openPerUser.get(r.user) ?? 0) + 1);
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
      { name: 'waiting line in request order', ok: lineInOrder && line.length === z.counts.WAITING + (z.counts.PAUSED ?? 0), detail: `${line.length} waiting` },
      { name: 'zone freeze: nobody got tickets while the zone was frozen', ok: sneaked.length === 0, detail: pauses.length ? `${pauses.length} zone freeze(s), ${sneaked.length} requests got tickets inside a freeze` : 'no zone freeze yet' },
      { name: 'FIFO: nobody overtook an earlier waiting request', ok: violations.length === 0, detail: violations.length ? violations.slice(0, 3).join('; ') : `${servedFromLine} served from the line in order` },
    ];
    return { zone: z.id, requests: all.length, passed: checks.every((c) => c.ok), checks };
  }

  // ---- business + tech configuration ---------------------------------------------------------------

  private cfgCache = { at: 0, v: DEFAULT_CONFIG };

  /** Cached for 2 s: read on every booking (seats per ITS id) without an extra Redis call each time. */
  async config(): Promise<AppConfig> {
    if (Date.now() - this.cfgCache.at < 2000) return this.cfgCache.v;
    const h = await this.r.hgetall(CONFIG_KEY);
    const v: AppConfig = {
      business: { programName: h.programName || DEFAULT_CONFIG.business.programName, maxPerIts: Number(h.maxPerIts || DEFAULT_CONFIG.business.maxPerIts),
        confirmMode: h.confirmMode === 'hold' ? 'hold' : 'direct' },
      tech: { terminalLines: Math.max(100, Number(h.terminalLines || DEFAULT_CONFIG.tech.terminalLines)) }, // at least 100 lines
    };
    this.cfgCache = { at: Date.now(), v };
    return v;
  }

  async setConfig(f: { programName?: string; maxPerIts?: number; terminalLines?: number; confirmMode?: 'direct' | 'hold' }) {
    const set: (string | number)[] = [];
    if (f.confirmMode !== undefined) set.push('confirmMode', f.confirmMode);
    if (f.programName !== undefined) set.push('programName', f.programName);
    if (f.maxPerIts !== undefined) set.push('maxPerIts', f.maxPerIts);
    if (f.terminalLines !== undefined) set.push('terminalLines', f.terminalLines);
    if (set.length) await this.r.hset(CONFIG_KEY, ...set);
    this.cfgCache.at = 0;
    if (f.confirmMode !== undefined) await this.syncConfirmMode();
    return this.config();
  }

  /** The Lua scripts read the confirm mode from each zone hash (field "direct"): copy it to every zone. */
  async syncConfirmMode() {
    this.cfgCache.at = 0;
    const direct = (await this.config()).business.confirmMode === 'direct' ? '1' : '0';
    const ids = await this.r.smembers(ZONES_KEY);
    if (ids.length) { const p = this.r.pipeline(); for (const id of ids) p.hset(`tb:zone:${id}`, 'direct', direct); await p.exec(); }
  }

  /** What the tech configuration panel shows: Redis, nodes are separate (/tickets/nodes). */
  async techInfo() {
    const info = await this.r.info();
    const field = (k: string) => (info.match(new RegExp(`^${k}:(.*)$`, 'm'))?.[1] ?? '').trim();
    return {
      redis: { host: process.env.REDIS_HOST ?? 'localhost', port: Number(process.env.REDIS_PORT ?? 6379), version: field('redis_version'), usedMemory: field('used_memory_human'), keys: await this.r.dbsize(), aof: field('aof_enabled') === '1' },
      expiryCheckMs: SWEEP_MS,
      fifo: 'Redis LIST per zone (tb:wait:<zone>), served inside the atomic script that frees seats',
      atomic: 'every decision = one Lua script; Redis runs one script at a time',
      duplicateGuard: 'request id (tb:reqs) + one open request per ITS id per zone (tb:active:<zone>)',
    };
  }

  /**
   * Terminal log: the latest records across zones (or one zone), newest first, each with its Redis stream id,
   * ITS id, request id, status and the Redis keys it touched. Reads the per-zone event streams (tb:ev:<zone>),
   * which every Lua script writes in the same atomic step - so load-test traffic is included.
   */
  async terminal(zone: string | undefined, limit: number) {
    const ids = zone ? [zone] : await this.r.smembers(ZONES_KEY);
    const p = this.r.pipeline();
    // a request can cause several Redis events: read enough of them to cover the requests shown
    // (one zone: up to 1 lakh events; all zones: up to 20,000 per zone, to keep memory bounded)
    for (const z of ids) p.xrevrange(`tb:ev:${z}`, '+', '-', 'COUNT', Math.min(Math.max(limit * 5, 100), zone ? 100000 : 20000));
    const out = (await p.exec()) ?? [];
    const rows: Record<string, unknown>[] = [];
    ids.forEach((z, i) => {
      for (const [id, f] of (out[i]?.[1] as [string, string[]][]) ?? []) {
        const o: Record<string, string> = {};
        for (let k = 0; k < f.length; k += 2) o[f[k]] = f[k + 1];
        rows.push({ kind: 'event', redisId: id, zone: z, ts: Number(o.ts), status: o.type, its: o.user || '', rid: o.rid || '', seq: o.seq || '', qty: o.qty || '',
          available: o.available, booked: o.booked, waiting: o.waiting, keys: keysFor(o.type, z), note: o.note || '' });
      }
    });
    // ONE row per request: the request, its response and the Redis decision(s) it caused, together.
    const http = await this.r.xrevrange(HTTP_LOG_KEY, '+', '-', 'COUNT', zone ? Math.min(limit * 20, 20000) : limit);
    const calls: { row: Record<string, unknown>; h: HttpRecord; mine: Record<string, unknown>[] }[] = [];
    for (const [id, f] of http) {
      const h = JSON.parse(f[1]) as HttpRecord;
      if (zone && h.zone !== zone) continue;
      calls.push({ h, mine: [], row: { kind: 'call', redisId: id, ts: h.ts, zone: h.zone, its: h.its, rid: h.rid, instance: h.instance, method: h.method, path: h.path,
        request: h.request, httpStatus: h.status, response: h.response, ms: h.ms } });
    }
    // An event belongs to the request that was IN PROGRESS when it happened (between request and response, plus 25 ms
    // because Redis in Docker and the node clocks differ a little) and has the same request id (zone actions: none).
    const SKEW = 25;
    for (const e of rows) {
      let best: (typeof calls)[number] | undefined;
      for (const c of calls) {
        const h = c.h;
        if ((e.ts as number) < h.ts - SKEW || (e.ts as number) > h.endTs + SKEW) continue;
        if (h.zone && e.zone !== h.zone) continue;
        if (h.rid ? e.rid !== h.rid : !!e.rid) continue;
        if (!best || Math.abs((e.ts as number) - h.ts) < Math.abs((e.ts as number) - best.h.ts)) best = c;
      }
      if (best) best.mine.push(e); // events no request caused (expiry, moved up from the line) are not shown: one row = one request
    }
    // available BEFORE an event = available after the previous event of that zone (events in Redis order)
    // compare Redis ids as (milliseconds, sequence): ms x 1e6 + seq would exceed JavaScript's safe integers
    const cmpId = (x: unknown, y: unknown) => { const [a1, a2] = String(x).split('-').map(Number), [b1, b2] = String(y).split('-').map(Number); return a1 - b1 || a2 - b2; };
    const byZone = new Map<string, Record<string, unknown>[]>();
    for (const e of rows) (byZone.get(e.zone as string) ?? byZone.set(e.zone as string, []).get(e.zone as string)!).push(e);
    for (const list of byZone.values()) {
      list.sort((a, b) => cmpId(a.redisId, b.redisId));
      list.forEach((e, i) => { if (i > 0) e.availableBefore = list[i - 1].available; });
    }
    for (const c of calls) {
      const main = c.mine.find((e) => e.status !== 'NO_TICKETS_LEFT') ?? c.mine[0];
      // available before -> after this request. "After" includes everything the same Redis script did in that
      // millisecond, e.g. a cancel that also gave the seat to the next person in line.
      let availableBefore: unknown, availableAfter: unknown, bookedAfter: unknown;
      if (c.mine.length) {
        const own = [...c.mine].sort((a, b) => cmpId(a.redisId, b.redisId));
        const last = own[own.length - 1], ms = String(last.redisId).split('-')[0];
        const sameRun = (byZone.get(last.zone as string) ?? []).filter((e) => String(e.redisId).split('-')[0] === ms && cmpId(e.redisId, last.redisId) >= 0);
        availableBefore = own[0].availableBefore;
        const end = sameRun[sameRun.length - 1] ?? last;
        availableAfter = end.available;
        bookedAfter = end.booked;
      }
      Object.assign(c.row, {
        availableBefore, availableAfter, bookedAfter,
        status: main?.status ?? (c.h.status >= 400 ? 'ERROR' : 'OK'), available: main?.available, waiting: main?.waiting,
        redis: c.mine.map((e) => ({ redisId: e.redisId, status: e.status, available: e.available, waiting: e.waiting, zone: e.zone })),
        keys: [...new Set(c.mine.flatMap((e) => e.keys as string[]))],
      });
    }
    const merged: Record<string, unknown>[] = calls.map((c) => c.row);
    const num = (id: string) => { const [a, b] = id.split('-'); return [Number(a), Number(b)]; };
    merged.sort((a, b) => (b.ts as number) - (a.ts as number)
      || (() => { const [x1, x2] = num(a.redisId as string), [y1, y2] = num(b.redisId as string); return y1 - x1 || y2 - x2; })());
    const shown = merged.slice(0, limit);

    // Duplicates per ITS id (exact totals counted in Redis by the atomic script: tb:dups:<zone>).
    // Each row gets the count UP TO that row = total now - duplicates of that ITS that came after it.
    const pairs = [...new Set(shown.filter((r) => r.its && r.zone).map((r) => `${r.zone}|${r.its}`))];
    const tp = this.r.pipeline();
    for (const k of pairs) { const [z, its] = k.split('|'); tp.hget(`tb:dups:${z}`, its); }
    const totals = new Map(pairs.map((k, i) => [k, 0]));
    ((await tp.exec()) ?? []).forEach(([, v], i) => totals.set(pairs[i], Number(v ?? 0)));
    const dupEvents = rows.filter((e) => e.status === 'DUPLICATE_BLOCKED');
    for (const r of shown) {
      if (!r.its || !r.zone) continue;
      const anchor = (r.redis as { redisId: string }[] | undefined)?.[0]?.redisId;
      const later = dupEvents.filter((e) => e.zone === r.zone && e.its === r.its
        && (anchor ? cmpId(e.redisId, anchor) > 0 : (e.ts as number) > (r.ts as number) + (r.ms as number))).length;
      r.itsDuplicates = Math.max(0, (totals.get(`${r.zone}|${r.its}`) ?? 0) - later);
    }
    // top duplicate senders of the zone(s) shown, for the summary line
    const sp = this.r.pipeline();
    for (const z of ids) sp.hgetall(`tb:dups:${z}`);
    const dupTop: { zone: string; its: string; count: number }[] = [];
    ((await sp.exec()) ?? []).forEach(([, h], i) => { for (const [its, n] of Object.entries((h as Record<string, string>) ?? {})) dupTop.push({ zone: ids[i], its, count: Number(n) }); });
    dupTop.sort((a, b) => b.count - a.count);
    return { zone: zone ?? 'all', limit, records: shown, duplicates: { total: dupTop.reduce((s, d) => s + d.count, 0), itsIds: dupTop.length, top: dupTop.slice(0, 10) } };
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
    const result: Record<string, number> = { FROZEN: 0, WAITING: 0, NO_SEATS: 0, ZONE_FROZEN: 0, DUPLICATE_REQUEST_ID: 0, DUPLICATE_ITS: 0, ERR: 0 };
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
          if (r.outcome === 'NO_SEATS' || r.outcome === 'ZONE_FROZEN') { result[r.outcome]++; continue; }
          if (r.outcome === 'DUPLICATE') result[r.code === 'ITS_HAS_OPEN_REQUEST' ? 'DUPLICATE_ITS' : 'DUPLICATE_REQUEST_ID']++;
          else { result[r.outcome === 'BOOKED' ? 'FROZEN' : r.outcome === 'PAUSED' ? 'WAITING' : r.outcome]++; accepted++; } // direct mode: confirmed at once = got a seat
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
    // hold mode: finish the held seats (pay or cancel). direct mode: nothing is held, so confirmed registrations
    // are cancelled (payPct % kept) and their seats go to the waiting line in request order.
    const direct = (await this.config()).business.confirmMode === 'direct';
    const open = (await this.requestsByIds(await this.r.hvals(`tb:active:${zone}`)))
      .filter((r) => r.status === (direct ? 'BOOKED' : 'FROZEN') && r.kind === 'USER').sort((a, b) => (a.frozenAt ?? 0) - (b.frozenAt ?? 0) || a.seq - b.seq);
    let booked = 0, released = 0, moved = 0, expired = 0;
    for (const r of open) {
      try {
        if (Math.random() * 100 < payPct) { if (!direct) await this.confirm(zone, r.rid); booked++; }
        else { const x = await this.release(zone, r.rid, 'user cancelled before payment (settle)'); released++; moved += x.movedFromLine; }
      } catch {
        expired++;
      }
    }
    return { zone, processed: open.length, booked, released, expired, movedFromLine: moved, after: await this.zone(zone) };
  }
}
