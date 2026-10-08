import { BadRequestException, Body, Controller, Delete, Get, Header, Headers, HttpCode, HttpException, NotFoundException, Param, Patch, Post, Query, Res } from '@nestjs/common';
import { TicketsLoadTest } from './tickets.loadtest';
import { TicketsNodes } from './tickets.nodes';
import { TicketsOpLog } from './tickets.oplog';
import { BookOutcome, TicketError, TicketsService } from './tickets.service';

const ID_RE = /^[a-z0-9-]{1,80}$/;
const RID_RE = /^[A-Za-z0-9_.:-]{1,100}$/;
const MAX_QTY = 10;

const int = (v: unknown, name: string, min: number, max: number, def?: number) => {
  if ((v === undefined || v === null || v === '') && def !== undefined) return def;
  const n = typeof v === 'string' ? Number(v) : v;
  if (!Number.isInteger(n) || (n as number) < min || (n as number) > max) throw new BadRequestException(`${name} must be an integer ${min}..${max}`);
  return n as number;
};
const text = (v: unknown, name: string, max = 80) => {
  if (typeof v !== 'string' || !v.trim() || v.length > max) throw new BadRequestException(`${name} is required (max ${max} chars)`);
  return v.trim();
};
const zoneId = (id: string) => {
  if (!ID_RE.test(id)) throw new BadRequestException('bad zone id');
  return id;
};

/** Ticket errors -> HTTP: unknown -> 404, freeze over / wrong state -> 409, anything else -> 400 */
async function call<T>(fn: () => Promise<T>): Promise<T> {
  try {
    return await fn();
  } catch (e) {
    if (e instanceof TicketError) {
      const status = e.code.endsWith('NOT_FOUND') ? 404 : e.code === 'ZONE_FROZEN' ? 423 : e.code === 'FREEZE_EXPIRED' || e.code === 'NOT_FROZEN' || e.code === 'ALREADY_FROZEN' || e.code === 'ZONE_NOT_FROZEN' ? 409 : 400;
      throw new HttpException({ statusCode: status, code: e.code, request: e.request }, status);
    }
    throw e;
  }
}

const outcomeText = (r: BookOutcome) => ({
  rid: r.request?.rid,
  result: r.outcome === 'ERR' ? `refused: ${r.code}`
    : r.outcome === 'DUPLICATE' ? `DUPLICATE (${r.code}), original ${r.request?.rid ?? ''} is ${r.request?.status ?? ''}`
    : r.outcome === 'WAITING' ? `WAITING at place ${r.request?.position ?? '?'}` : `FROZEN ${r.request?.qty} ticket(s)`,
});

const csv = (rows: Record<string, unknown>[], cols: string[]) =>
  [cols.join(','), ...rows.map((r) => cols.map((c) => {
    const v = r[c] ?? '';
    const s = typeof v === 'number' && c.endsWith('At') ? new Date(v).toISOString() : String(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  }).join(','))].join('\n');

/**
 * Movie-ticket style booking per city zone.
 *   POST /tickets/zones/:id/book   { muminId (ITS id), qty }  header Idempotency-Key (request id; generated if absent)
 *        200 FROZEN  (tickets held for the zone's freeze time; pay with /confirm)
 *        202 WAITING (in the FIFO waiting line; poll GET /tickets/requests/:rid)
 *        409 DUPLICATE (same request id again, or the ITS id already has an open request) -> original request returned
 *        404 ITS_NOT_FOUND (not in Mumin_Mast) · 423 ITS_CANCELLED
 */
@Controller('tickets')
export class TicketsController {
  constructor(private readonly tickets: TicketsService, private readonly log: TicketsOpLog,
    private readonly nodes: TicketsNodes, private readonly load: TicketsLoadTest) {}

  /** Every route runs as one entry in the Redis key log (reads only when switched on). */
  private t<T>(action: string, meta: Parameters<TicketsOpLog['trace']>[1], fn: () => Promise<T>,
    d?: (r: T) => { result?: string; rid?: string; its?: string; note?: string }) {
    return call(() => this.log.trace(action, meta, fn, d));
  }
  private read<T>(action: string, zone: string | undefined, fn: () => Promise<T>) {
    return this.t(action, { kind: 'read', zone }, fn);
  }

  @Get('zones')
  zones() {
    return this.read('list-zones', undefined, () => this.tickets.zones());
  }

  /** { city, name, show, total, holdSeconds } create, or update the zone with the same city + name */
  @Post('zones')
  @HttpCode(200)
  upsert(@Body() b: any) {
    const z = {
      city: text(b?.city, 'city'), name: text(b?.name, 'name'), show: text(b?.show ?? 'Show', 'show'),
      total: int(b?.total, 'total', 1, 100000), holdSeconds: int(b?.holdSeconds, 'holdSeconds', 5, 3600, 60),
    };
    return this.t('create-zone', { note: `${z.city} · ${z.name}, ${z.total} tickets, freeze ${z.holdSeconds} s` }, () => this.tickets.upsertZone(z),
      (r) => ({ result: `zone ${r.id} ${String(r.result).toLowerCase()}` }));
  }

  /** Edit a zone in place: { city?, name?, show?, total?, holdSeconds? } (id stays the same) */
  @Patch('zones/:id')
  updateZone(@Param('id') id: string, @Body() b: any) {
    const opt = (v: unknown, n: string, max = 80) => (v === undefined || v === null || v === '' ? undefined : text(v, n, max));
    const f = {
      city: opt(b?.city, 'city'), name: opt(b?.name, 'name'), show: opt(b?.show, 'show'),
      total: b?.total === undefined ? undefined : int(b.total, 'total', 1, 100000),
      holdSeconds: b?.holdSeconds === undefined ? undefined : int(b.holdSeconds, 'holdSeconds', 5, 3600),
    };
    return this.t('edit-zone', { zone: id }, () => this.tickets.updateZone(zoneId(id), f),
      (r) => ({ result: `${r.changes}${r.movedFromLine ? `; ${r.movedFromLine} waiting request(s) got tickets` : ''}` }));
  }

  /** Edit a request: { qty?, extendSeconds?, note? (admin reason) } */
  @Patch('zones/:id/requests/:rid')
  editRequest(@Param('id') id: string, @Param('rid') rid: string, @Body() b: any) {
    const f = {
      qty: b?.qty === undefined ? undefined : int(b.qty, 'qty', 1, 100000),
      extendSeconds: b?.extendSeconds === undefined ? undefined : int(b.extendSeconds, 'extendSeconds', 1, 86400),
      note: typeof b?.note === 'string' ? b.note.slice(0, 200) : undefined,
    };
    return this.t('edit-request', { zone: id, rid }, () => this.tickets.editRequest(zoneId(id), rid, f),
      (r) => ({ its: r.request.user, result: `${r.outcome === 'UNCHANGED' ? 'nothing changed' : r.request.lastEdit}${r.movedFromLine ? `; ${r.movedFromLine} waiting request(s) got tickets` : ''}` }));
  }

  /** Freeze everything except booked tickets: { reason, allowPay? } */
  @Post('zones/:id/zone-freeze')
  @HttpCode(200)
  zoneFreeze(@Param('id') id: string, @Body() b: any) {
    const reason = typeof b?.reason === 'string' && b.reason.trim() ? b.reason.trim().slice(0, 200) : 'zone frozen by admin';
    const allowPay = b?.allowPay === true;
    return this.t('zone-freeze', { zone: id, note: `${reason}${allowPay ? ' (held tickets may still pay)' : ''}` }, () => this.tickets.zoneFreeze(zoneId(id), reason, allowPay),
      (r) => ({ result: `zone frozen: ${r.freeze.atFreeze.available} free tickets blocked, ${r.freeze.atFreeze.heldRequests} held requests paused, ${r.freeze.atFreeze.waiting} waiting` }));
  }

  /** Unfreeze: held timers resume, waiting line served in request order */
  @Post('zones/:id/zone-unfreeze')
  @HttpCode(200)
  zoneUnfreeze(@Param('id') id: string) {
    return this.t('zone-unfreeze', { zone: id }, () => this.tickets.zoneUnfreeze(zoneId(id)),
      (r) => ({ result: `zone unfrozen after ${Math.round(r.freeze.durationMs / 1000)} s: ${r.freeze.timersResumed} held timers resumed, ${r.movedFromLine} waiting requests got tickets` }));
  }

  /** Clear every request / freeze / line / report in ALL zones (zones and their settings stay) */
  @Post('clear-all')
  @HttpCode(200)
  clearAll() {
    return this.t('clear-all-data', {}, () => this.tickets.clearAll(),
      (r) => ({ result: `${r.removedRequests} requests removed, ${r.zones} zones reset, ${r.removedItsHistories} ITS histories removed` }));
  }

  @Delete('zones/:id')
  remove(@Param('id') id: string) {
    return this.t('delete-zone', { zone: id }, () => this.tickets.deleteZone(zoneId(id)), () => ({ result: 'zone deleted' }));
  }

  @Post('zones/:id/reset')
  @HttpCode(200)
  reset(@Param('id') id: string) {
    return this.t('reset-zone', { zone: id }, () => this.tickets.resetZone(zoneId(id)), (r) => ({ result: `${r.removedRequests} requests removed` }));
  }

  @Post('zones/:id/book')
  async book(@Param('id') id: string, @Body() b: any, @Res({ passthrough: true }) res: any, @Headers('idempotency-key') key?: string, @Headers('x-load-test') loadTest?: string) {
    const rid = key ?? b?.requestId;
    if (rid !== undefined && (typeof rid !== 'string' || !RID_RE.test(rid))) throw new BadRequestException('request id: 1..100 chars of A-Z a-z 0-9 _ . : -');
    const muminId = int(b?.muminId, 'muminId (ITS id)', 1, 99999999);
    const qty = int(b?.qty, 'qty', 1, MAX_QTY);
    if (loadTest) return this.answer(res, await call(() => this.tickets.book(zoneId(id), muminId, qty, rid)));
    return this.answer(res, await this.t('book', { zone: id, its: String(muminId), rid, note: `${qty} ticket(s)` }, () => this.tickets.book(zoneId(id), muminId, qty, rid), outcomeText));
  }

  /** Admin blocks free tickets (no expiry) until released: { qty, reason } */
  @Post('zones/:id/freeze')
  async freeze(@Param('id') id: string, @Body() b: any, @Res({ passthrough: true }) res: any) {
    const qty = int(b?.qty, 'qty', 1, 100000), reason = text(b?.reason ?? 'blocked by admin', 'reason', 200);
    return this.answer(res, await this.t('admin-freeze', { zone: id, note: `${qty} ticket(s), reason: ${reason}` }, () => this.tickets.adminFreeze(zoneId(id), qty, reason), outcomeText));
  }

  private answer(res: { status(code: number): void }, r: BookOutcome) {
    if (r.outcome === 'ERR') {
      const status = r.code === 'ZONE_NOT_FOUND' || r.code === 'ITS_NOT_FOUND' ? 404 : r.code === 'ITS_CANCELLED' ? 423 : 409;
      throw new HttpException({ statusCode: status, code: r.code }, status);
    }
    res.status(r.outcome === 'DUPLICATE' ? 409 : r.outcome === 'WAITING' ? 202 : 200); // 202 = accepted, not served yet
    return r;
  }

  /** A request with its full story: who freed its tickets, who got its tickets afterwards, duplicates */
  @Get('requests/:rid/details')
  requestDetails(@Param('rid') rid: string) {
    return this.t('request-details', { kind: 'read', rid }, () => this.tickets.requestDetails(rid));
  }

  @Get('requests/:rid')
  request(@Param('rid') rid: string) {
    return this.t('request', { kind: 'read', rid }, () => this.tickets.request(rid));
  }

  /** Pay: FROZEN -> BOOKED */
  @Post('zones/:id/requests/:rid/confirm')
  @HttpCode(200)
  confirm(@Param('id') id: string, @Param('rid') rid: string) {
    return this.t('pay', { zone: id, rid }, () => this.tickets.confirm(zoneId(id), rid), (r) => ({ its: r.request.user, result: `${r.request.status} ${r.request.qty} ticket(s)${r.code ? ` (${r.code})` : ''}` }));
  }

  /** Cancel / unfreeze: FROZEN -> RELEASED, WAITING -> CANCELLED, BOOKED -> BOOKING_CANCELLED. { reason } */
  @Post('zones/:id/requests/:rid/release')
  @HttpCode(200)
  release(@Param('id') id: string, @Param('rid') rid: string, @Body() b: any) {
    const reason = typeof b?.reason === 'string' && b.reason.trim() ? b.reason.trim().slice(0, 200) : 'cancelled';
    return this.t('cancel-or-unfreeze', { zone: id, rid, note: reason }, () => this.tickets.release(zoneId(id), rid, reason),
      (r) => ({ its: r.request.user, result: `${r.request.status}${r.code ? ` (${r.code})` : ''}${r.movedFromLine ? `; ${r.movedFromLine} waiting request(s) got the tickets` : ''}` }));
  }

  @Get('zones/:id/report')
  report(@Param('id') id: string, @Query('limit') limit?: string) {
    return this.read('zone-report', id, () => this.tickets.report(zoneId(id), int(limit, 'limit', 1, 5000, 1000)));
  }

  /** Admin freezes with the requests that came during each freeze and after its unfreeze */
  @Get('zones/:id/freezes')
  freezes(@Param('id') id: string, @Query('limit') limit?: string) {
    return this.read('admin-freeze-report', id, () => this.tickets.freezes(zoneId(id), int(limit, 'limit', 1, 100, 20)));
  }

  @Get('zones/:id/verify')
  verify(@Param('id') id: string) {
    return this.read('run-checks', id, () => this.tickets.verify(zoneId(id)));
  }

  @Get('zones/:id/requests.csv')
  @Header('Content-Type', 'text/csv; charset=utf-8')
  async requestsCsv(@Param('id') id: string) {
    const all = await this.read('csv-download', id, () => this.tickets.allRequests(zoneId(id)));
    return csv(all as any, ['seq', 'rid', 'user', 'name', 'jamaat', 'kind', 'qty', 'status', 'requestedAt', 'joinPos', 'frozenAt', 'cause', 'freedBy', 'expiresAt', 'bookedAt', 'endedAt', 'note']);
  }

  /** n real ITS ids from Mumin_Mast (for tests) */
  @Get('its-sample')
  itsSample(@Query('n') n?: string) {
    return this.read('its-sample', undefined, () => this.tickets.sampleIts(int(n, 'n', 1, 50000, 10)));
  }

  /** All booking requests of one ITS id (Mumin_ID) across zones */
  @Get('its/:muminId')
  its(@Param('muminId') muminId: string) {
    return this.t('its-lookup', { kind: 'read', its: muminId }, () => this.tickets.its(int(muminId, 'muminId', 1, 99999999)));
  }

  /** { its, perIts, sameIdPct, maxQty, concurrency }: each ITS id fires perIts requests at the same instant */
  @Post('zones/:id/simulate')
  @HttpCode(200)
  simulate(@Param('id') id: string, @Body() b: any) {
    return call(() => this.tickets.simulate(zoneId(id), {
      its: int(b?.its, 'its', 1, 50000, 1000),
      perIts: int(b?.perIts, 'perIts', 1, 100, 5),
      sameIdPct: int(b?.sameIdPct, 'sameIdPct', 0, 100, 30),
      maxQty: int(b?.maxQty, 'maxQty', 1, MAX_QTY, 4),
      concurrency: int(b?.concurrency, 'concurrency', 1, 1000, 100),
    }));
  }

  /** Finish open user freezes oldest-first: { payPct } pay, the rest cancel (their tickets go to the line) */
  @Post('zones/:id/settle')
  @HttpCode(200)
  settle(@Param('id') id: string, @Body() b: any, @Headers('x-load-test') loadTest?: string) {
    const payPct = int(b?.payPct, 'payPct', 0, 100, 50);
    if (loadTest) return call(() => this.tickets.settle(zoneId(id), payPct));
    return this.t('finish-freezes', { zone: id, note: `pay ${payPct}%` }, () => this.tickets.settle(zoneId(id), payPct),
      (r) => ({ result: `${r.processed} freezes: ${r.booked} paid, ${r.released} cancelled, ${r.expired} expired; ${r.movedFromLine} waiting request(s) got tickets` }));
  }

  // ---- dynamic nodes ----
  @Get('nodes')
  nodeList() {
    return this.nodes.list();
  }

  /** { count }: start more API nodes on the next free ports */
  @Post('nodes/start')
  @HttpCode(200)
  nodeStart(@Body() b: any) {
    const count = int(b?.count, 'count', 1, 8, 1);
    return this.log.trace('start-nodes', { note: `${count} node(s)` }, () => this.nodes.start(count), (r) => ({ result: `started ${r.started.map((x) => x.instance).join(', ')}` }));
  }

  @Post('nodes/:instance/stop')
  @HttpCode(200)
  nodeStop(@Param('instance') instance: string) {
    return this.log.trace('stop-node', { note: instance }, () => this.nodes.stop(instance), () => ({ result: `stop sent to ${instance}` }));
  }

  // ---- 1 lakh load test over every live node ----
  /** { total, concurrency, its, sameIdPct, maxQty, tickets, adminFreeze, unfreezeAt, rounds, payPct } */
  @Post('loadtest/start')
  @HttpCode(200)
  loadStart(@Body() b: any) {
    const total = int(b?.total, 'total', 1000, 500000, 100000);
    const its = int(b?.its, 'its', 100, 50000, 20000);
    if (its > total) throw new BadRequestException('ITS ids cannot be more than total requests');
    const tickets = int(b?.tickets, 'tickets', 10, 100000, 5000);
    return this.load.start({
      total, its, tickets,
      concurrency: int(b?.concurrency, 'concurrency', 10, 2000, 400),
      sameIdPct: int(b?.sameIdPct, 'sameIdPct', 0, 100, 30),
      maxQty: int(b?.maxQty, 'maxQty', 1, MAX_QTY, 4),
      adminFreeze: int(b?.adminFreeze, 'adminFreeze', 0, tickets, Math.floor(tickets / 5)),
      unfreezeAt: int(b?.unfreezeAt, 'unfreezeAt', 1, 99, 60),
      rounds: int(b?.rounds, 'rounds', 0, 20, 3),
      payPct: int(b?.payPct, 'payPct', 0, 100, 50),
      zoneFreezeAt: int(b?.zoneFreezeAt, 'zoneFreezeAt', 0, 98, 30),
      zoneUnfreezeAt: int(b?.zoneUnfreezeAt, 'zoneUnfreezeAt', 0, 99, 45),
      cancelDuringFreeze: int(b?.cancelDuringFreeze, 'cancelDuringFreeze', 0, 5000, 200),
    });
  }

  @Get('loadtest/status')
  loadStatus() {
    return this.load.status();
  }

  @Post('loadtest/cancel')
  @HttpCode(200)
  loadCancel() {
    return this.load.cancel();
  }

  /** Redis key log: ?limit=&zone=&action=&q= (newest first) */
  @Get('redis-log')
  redisLog(@Query('limit') limit?: string, @Query('zone') zone?: string, @Query('action') action?: string, @Query('q') q?: string) {
    return this.log.list(int(limit, 'limit', 1, 3000, 200), zone || undefined, action || undefined, q || undefined);
  }

  /** { reads: true|false } also log read-only calls (UI polling) */
  @Post('redis-log/settings')
  @HttpCode(200)
  redisLogSettings(@Body() b: any) {
    return this.log.setReads(b?.reads === true);
  }

  @Post('redis-log/clear')
  @HttpCode(200)
  redisLogClear() {
    return this.log.clear();
  }

  @Get('zones/:id')
  zone(@Param('id') id: string) {
    if (!ID_RE.test(id)) throw new NotFoundException();
    return this.read('zone', id, () => this.tickets.zone(id));
  }
}
