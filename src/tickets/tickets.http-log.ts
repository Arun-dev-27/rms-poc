import { CallHandler, ExecutionContext, HttpException, Injectable, Logger, NestInterceptor } from '@nestjs/common';
import { Observable, catchError, tap, throwError } from 'rxjs';
import { RedisService } from '../redis/redis.service';
import { TicketsNodes } from './tickets.nodes';

export const HTTP_LOG_KEY = 'tb:http';   // STREAM: one entry per write request (request + response), all nodes
const HTTP_LOG_MAX = 20000;

export interface HttpRecord {
  ts: number; endTs: number; ms: number; instance: string; method: string; path: string; zone: string;
  its: string; rid: string; request: string; status: number; response: string; loadTest: boolean;
}

const short = (v: unknown, n = 300) => { const s = typeof v === 'string' ? v : JSON.stringify(v ?? ''); return s.length > n ? s.slice(0, n - 1) + '…' : s; };

/** One line saying what the API answered, e.g. "WAITING · place 3" or "409 ITS_HAS_OPEN_REQUEST". */
const WORD: Record<string, string> = { FROZEN: 'HELD', BOOKED: 'CONFIRMED', RELEASED: 'RELEASED', CANCELLED: 'LEFT_LINE', WAITING: 'WAITING', EXPIRED: 'EXPIRED', PAUSED: 'FROZEN', NO_SEATS: 'NO SEATS AVAILABLE', ZONE_FROZEN: 'FROZEN', DROPPED: 'DROPPED' };

/** 16:57:30.123 */
const clock = (ms: number) => { const d = new Date(Number(ms)); const t = (n: number, w = 2) => String(n).padStart(w, '0'); return `${t(d.getHours())}:${t(d.getMinutes())}:${t(d.getSeconds())}.${t(d.getMilliseconds(), 3)}`; };

export function responseSummary(data: any): string {
  if (!data || typeof data !== 'object') return '';
  const r = data.request;
  const parts: string[] = [];
  if (data.code && !data.outcome) parts.push(String(data.code));
  else if (data.message && !data.outcome && typeof data.message !== 'object') parts.push(String(data.message));
  if (data.outcome) parts.push(data.outcome === 'DUPLICATE' ? `DUPLICATE ${data.code ?? ''}`.trim() : WORD[data.outcome] ?? String(data.outcome));
  if (r?.status && !data.outcome) parts.push(WORD[r.status] ?? String(r.status));
  if (r?.status === 'PAUSED') parts.push('zone is frozen');
  if (r?.status === 'ZONE_FROZEN') parts.push('zone is frozen, not booked');
  if ((r?.status === 'WAITING' || r?.status === 'PAUSED') && r.position) parts.push(`place ${r.position}`);
  if (r?.rid && data.outcome) parts.push(`request ${r.rid}`);
  if (data.changes) parts.push(String(data.changes));
  if (data.movedFromLine) parts.push(`${data.movedFromLine} moved up from the line`);
  if (data.cityFreeze) {
    const c = data.cityFreeze;
    parts.push(`city ${c.cityName} frozen: ${c.frozen} zone(s) frozen${c.alreadyFrozen ? `, ${c.alreadyFrozen} already frozen` : ''} · ${c.booked} booked · ${c.available} available`);
    for (const z of c.zones) if (z.lastBooked) parts.push(`${z.name}: last booked ITS ${z.lastBooked.its} (order #${z.lastBooked.seq}, ${clock(z.lastBooked.at)})`);
  }
  if (data.cityUnfreeze) parts.push(`city ${data.cityUnfreeze.cityName} unfrozen: ${data.cityUnfreeze.unfrozen} zone(s) unfrozen${data.cityUnfreeze.notFrozen ? `, ${data.cityUnfreeze.notFrozen} were not frozen` : ''}`);
  if (data.freeze) parts.push(data.freeze.endedAt ? 'zone unfrozen' : 'zone frozen');
  if (data.freeze && !data.freeze.endedAt) {
    const a = data.freeze.atFreeze ?? {}, lb = a.lastBooked;
    parts.push(`${a.booked ?? 0} booked`);
    parts.push(lb ? `last booked before freeze: ITS ${lb.its} request ${lb.rid} (order #${lb.seq}, ${clock(lb.at)})` : 'nothing booked before freeze');
  }
  if (data.freeze?.droppedRequests) parts.push(`${data.freeze.droppedRequests} frozen requests dropped`);
  if (data.removedRequests !== undefined) parts.push(`${data.removedRequests} removed`);
  if (data.started) parts.push(`started ${data.started.map((x: any) => x.instance).join(', ')}`);
  return parts.join(' · ') || 'OK';
}

/**
 * Logs every WRITE request of the ticket API (request + response) to the Redis stream tb:http and the console,
 * for the terminal log. GETs (page refreshes) are not logged. Load-test traffic (X-Load-Test) goes to the stream
 * only, not the console (1 lakh console lines would slow the node down).
 */
@Injectable()
export class TicketsHttpLog implements NestInterceptor {
  private readonly logger = new Logger('TicketsHTTP');

  constructor(private readonly redis: RedisService, private readonly nodes: TicketsNodes) {}

  intercept(ctx: ExecutionContext, next: CallHandler): Observable<unknown> {
    const req = ctx.switchToHttp().getRequest();
    const res = ctx.switchToHttp().getResponse();
    if (req.method === 'GET') return next.handle();
    const ts = Date.now();
    const done = (status: number, data: unknown) => this.write(req, ts, status, data);
    return next.handle().pipe(
      tap((data) => done(res.statusCode, data)),
      catchError((e) => {
        done(e instanceof HttpException ? e.getStatus() : 500, e instanceof HttpException ? e.getResponse() : { message: (e as Error).message });
        return throwError(() => e);
      }),
    );
  }

  private write(req: any, ts: number, status: number, data: unknown) {
    const path: string = (req.originalUrl ?? req.url ?? '').split('?')[0];
    const body = req.body && Object.keys(req.body).length ? req.body : undefined;
    const loadTest = !!req.headers['x-load-test'];
    const rec: HttpRecord = {
      ts, endTs: Date.now(), ms: Date.now() - ts, instance: this.nodes.instance, method: req.method, path,
      zone: path.match(/\/zones\/([^/]+)/)?.[1] ?? '',
      its: body?.muminId !== undefined ? String(body.muminId) : (data as any)?.request?.user ?? '',
      rid: req.headers['idempotency-key'] ?? body?.requestId ?? path.match(/\/requests\/([^/]+)/)?.[1] ?? (data as any)?.request?.rid ?? '',
      request: body ? short(body) : '', status, response: short(responseSummary(data), 200), loadTest,
    };
    this.redis.client.xadd(HTTP_LOG_KEY, 'MAXLEN', '~', HTTP_LOG_MAX, '*', 'j', JSON.stringify(rec)).catch(() => undefined);
    if (!loadTest) {
      this.logger.log(`→ ${rec.method} ${rec.path}${rec.its ? ` its=${rec.its}` : ''}${rec.rid ? ` rid=${rec.rid}` : ''}${rec.request ? ` ${rec.request}` : ''}  ← ${rec.status} ${rec.response} (${rec.ms} ms)`);
    }
  }
}
