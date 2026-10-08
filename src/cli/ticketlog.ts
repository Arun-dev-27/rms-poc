/**
 * Terminal log for registrations / ticket booking: every request (→), what Redis decided, and the response (←),
 * with ITS id, request id, status,
 * Redis stream id and the Redis keys touched, then follows new records live.
 *
 *   npm run tickets:log                         latest 100 (or the configured number, at least 100), all zones, follow
 *   npm run tickets:log -- --zone=chennai-velachery --lines=50
 *   npm run tickets:log -- --once               print and exit
 *   API_URL=http://localhost:3100 npm run tickets:log
 */
import * as http from 'http';

const arg = (k: string) => process.argv.find((a) => a.startsWith(`--${k}=`))?.split('=').slice(1).join('=');
const URL_ = new URL(arg('url') ?? process.env.API_URL ?? 'http://localhost:3000');
const ZONE = arg('zone') ?? '';
const LINES = arg('lines') ? Number(arg('lines')) : undefined;
const ONCE = process.argv.includes('--once');
const tty = process.stdout.isTTY;

const C: Record<string, string> = { reset: '\x1b[0m', dim: '\x1b[2m', bold: '\x1b[1m', green: '\x1b[32m', yellow: '\x1b[33m', red: '\x1b[31m', magenta: '\x1b[35m', cyan: '\x1b[36m', blue: '\x1b[34m' };
const c = (color: string, s: string) => (tty ? C[color] + s + C.reset : s);
const STATUS_COLOR: Record<string, string> = {
  NO_SEATS: 'red', REFUSED_ZONE_FROZEN: 'cyan', FROZEN: 'magenta', WAITING: 'yellow', BOOKED: 'green', DUPLICATE_BLOCKED: 'red', EXPIRED: 'red', BOOKING_CANCELLED: 'red',
  RELEASED: 'blue', CANCELLED: 'blue', EDITED: 'cyan', ZONE_FROZEN: 'cyan', ZONE_UNFROZEN: 'cyan', NO_TICKETS_LEFT: 'magenta',
};
const LABEL: Record<string, string> = { REFUSED_ZONE_FROZEN: 'FROZEN', NO_SEATS: 'NO SEATS', PAUSED: 'FROZEN', FROZEN: 'HELD', BOOKED: 'CONFIRMED', DUPLICATE_BLOCKED: 'DUPLICATE', CANCELLED: 'LEFT_LINE', RELEASED: 'RELEASED' };

function get(path: string): Promise<any> {
  return new Promise((resolve, reject) => {
    http.get({ host: URL_.hostname, port: URL_.port, path, timeout: 5000 }, (res) => {
      let b = '';
      res.on('data', (d) => (b += d));
      res.on('end', () => { try { resolve(JSON.parse(b)); } catch (e) { reject(e); } });
    }).on('error', reject).on('timeout', function (this: http.ClientRequest) { this.destroy(new Error('timeout')); });
  });
}

const pad = (s: unknown, n: number) => { const t = String(s ?? ''); return t.length > n ? t.slice(0, n - 1) + '…' : t.padEnd(n); };
const time = (ms: number) => { const d = new Date(ms); return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}:${String(d.getSeconds()).padStart(2, '0')}.${String(d.getMilliseconds()).padStart(3, '0')}`; };

/** "available 120 → 119", "available 0 (no change)" or "" when Redis did not change */
function avail(r: any) {
  const b = r.availableBefore, a = r.availableAfter;
  if (a === undefined || a === null) return '';
  if (b === undefined || b === null) return `available ${a}`;
  const bk = r.bookedAfter === undefined || r.bookedAfter === null || r.bookedAfter === '' ? '' : ` · booked ${r.bookedAfter}`;
  return (String(b) === String(a) ? `available ${a} (no change)` : `available ${b} → ${a}`) + bk;
}

function line(r: any) {
  const st = LABEL[r.status] ?? r.status;
  // one row per request: request -> response, and the Redis decision it caused
  if (r.kind === 'call') {
    const ok = r.httpStatus < 400;
    const path = (r.zone ? r.path.replace(`/tickets/zones/${r.zone}`, '') : r.path.replace('/tickets', '')) || '/';
    const main = (r.redis ?? []).find((e: any) => e.status !== 'NO_TICKETS_LEFT') ?? (r.redis ?? [])[0];
    const d = r.its ? Number(r.itsDuplicates ?? 0) : null;
    return [c('dim', time(r.ts)), c('dim', pad(r.instance, 15)), pad(r.zone || '—', 20), r.its ? 'ITS ' + c('bold', pad(r.its, 9)) : pad('', 13),
      d === null ? pad('', 6) : c(d > 0 ? 'red' : 'dim', pad(`dup ${d}`, 6)), pad(r.rid || '—', 22),
      c('blue', `${r.method} ${path}`) + (r.request ? ' ' + c('dim', r.request) : ''), '→', c(ok ? 'green' : 'red', `${r.httpStatus} ${r.response}`), c('dim', `(${r.ms} ms)`),
      avail(r) ? '│ ' + c(String(r.availableBefore) === String(r.availableAfter) ? 'dim' : 'yellow', avail(r)) : '',
      main ? c('dim', `│ ${main.redisId}`) + ' ' + c('cyan', (r.keys ?? []).join(' ')) : c('dim', '│ no Redis change')].filter(Boolean).join(' ');
  }
  // a Redis change no request here caused (hold expired, someone moved up from the line, zone reset)
  return [
    c('yellow', 'auto'),
    c('dim', time(r.ts)),
    c('dim', pad(r.redisId, 17)),
    pad(r.zone, 22),
    r.its ? 'ITS ' + c('bold', pad(r.its, 9)) : pad('', 13),
    pad(r.rid || '—', 30),
    c(STATUS_COLOR[r.status] ?? 'reset', pad(st, 14)),
    c('dim', `free ${pad(r.available, 6)} wait ${pad(r.waiting, 7)}`),
    c('cyan', (r.keys ?? []).join(' ')),
  ].join(' ');
}

// a request and its response share one Redis id: tell them apart by kind
const key = (r: any) => `${r.kind}:${r.redisId}`; // call rows use the HTTP log id, auto rows the Redis event id

async function main() {
  const qs = new URLSearchParams();
  if (ZONE) qs.set('zone', ZONE);
  if (LINES) qs.set('limit', String(LINES));
  const first = await get(`/tickets/terminal?${qs}`);
  const header = ['TIME        ', pad('NODE', 15), pad('ZONE', 20), pad('ITS ID', 13), pad('DUPS', 6), pad('REQUEST ID', 22), 'REQUEST → RESPONSE │ AVAILABLE BEFORE → AFTER · BOOKED │ REDIS ID · KEY'].join(' ');
  console.log(c('bold', `Latest ${first.records.length} records · ${first.zone === 'all' ? 'all zones' : first.zone} · ${URL_.host}${ONCE ? '' : ' · following (Ctrl+C to stop)'}`));
  const dd = first.duplicates;
  if (dd) console.log(dd.total ? c('red', `Duplicate requests blocked: ${dd.total} from ${dd.itsIds} ITS id(s)`) + c('dim', ' · most: ') + dd.top.map((x: any) => `${x.its} ×${x.count}${first.zone === 'all' ? ` (${x.zone})` : ''}`).join(' · ') : c('dim', 'Duplicate requests blocked: 0'));
  console.log(c('dim', header));
  const seen = new Set<string>();
  for (const r of [...first.records].reverse()) { seen.add(key(r)); console.log(line(r)); }
  if (ONCE) return;
  for (;;) {
    await new Promise((r) => setTimeout(r, 1000));
    try {
      const now = await get(`/tickets/terminal?${qs}`);
      for (const r of [...now.records].reverse()) if (!seen.has(key(r))) { seen.add(key(r)); console.log(line(r)); }
      if (seen.size > 5000) seen.clear();
    } catch (e) {
      console.log(c('red', `(API not reachable at ${URL_.host}: ${(e as Error).message}; retrying)`));
    }
  }
}

main().catch((e) => { console.error(`Cannot reach the API at ${URL_.host}: ${(e as Error).message}`); process.exit(1); });
