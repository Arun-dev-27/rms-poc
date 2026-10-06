/**
 * Distributed request-cancellation + concurrency-control test.
 * Several API instances share ONE Redis; requests and STOP commands are spread over all of them.
 *
 *   API_PORTS=3001,3002,3003,3004 node dist/cli/dtest.js [requests=100000] [concurrency=400]
 *
 * D1  1 lakh mixed requests (60% 2-phase /process, 40% plain update) on 10,000 records, round-robin
 *     over all instances, while 1,000 records are STOPPED via random instances during the run.
 * D2  duplicates: the same request (same Idempotency-Key) sent to TWO instances at the same moment.
 * D3  races: 1,000 records x one client per instance, all with the same If-Match version.
 * Then: everything resumed, MSSQL == Redis, original values restored (also on failure).
 */
import * as http from 'http';
import { randomUUID } from 'crypto';

const PORTS = (process.env.API_PORTS ?? '3001,3002,3003,3004').split(',').map(Number);
const HOST = process.env.API_HOST ?? 'localhost';
const TOTAL = Number(process.argv[2] ?? 100000);
const CONCURRENCY = Number(process.argv[3] ?? 400);
const CLIENT_TIMEOUT_MS = 30000; // slower than this = "hanging"
const agents = PORTS.map(() => new http.Agent({ keepAlive: true, maxSockets: Math.ceil(CONCURRENCY / PORTS.length) + 50 }));

interface Res { status: number | 'TIMEOUT' | 'ERROR'; body: any; ms: number; instance: string; id?: number; key?: string; kind?: string }

function call(node: number, method: string, path: string, body?: unknown, headers: Record<string, string> = {}): Promise<Res> {
  const n = ((node % PORTS.length) + PORTS.length) % PORTS.length;
  const payload = body === undefined ? undefined : JSON.stringify(body);
  const t0 = performance.now();
  return new Promise((resolve, reject) => {
    const req = http.request({
      host: HOST, port: PORTS[n], agent: agents[n], method, path, timeout: CLIENT_TIMEOUT_MS,
      headers: { ...(payload ? { 'content-type': 'application/json', 'content-length': Buffer.byteLength(payload) } : {}), ...headers },
    }, (res) => {
      const chunks: Buffer[] = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        const text = Buffer.concat(chunks).toString();
        let parsed: any = null;
        try { parsed = text ? JSON.parse(text) : null; } catch { parsed = { error: 'NON_JSON_BODY', text: text.slice(0, 200) }; }
        resolve({ status: res.statusCode ?? 0, body: parsed, ms: performance.now() - t0, instance: String(res.headers['x-instance'] ?? PORTS[n]) });
      });
    });
    req.on('timeout', () => { req.destroy(); resolve({ status: 'TIMEOUT', body: null, ms: performance.now() - t0, instance: String(PORTS[n]) }); });
    req.on('error', (e) => { if (!firstError) firstError = String(e); resolve({ status: 'ERROR', body: { error: (e as NodeJS.ErrnoException).code ?? 'ERROR' }, ms: performance.now() - t0, instance: String(PORTS[n]) }); });
    if (payload) req.write(payload);
    req.end();
  });
}

let completed = 0;
let firstError = '';

/** Live progress for the UI: the API starts this script with PROGRESS_JSON=1 and reads "@@{json}" lines. */
const PROGRESS = process.env.PROGRESS_JSON === '1';
let phase = '';
let phaseTotal = 0;
let phaseStart = performance.now();
let live: { statuses: Record<string, number>; perInstance: Record<string, number> } = { statuses: {}, perInstance: {} };
function emit(event: Record<string, unknown>) { if (PROGRESS) process.stdout.write('@@' + JSON.stringify(event) + '\n'); }
function flushProgress() {
  if (phase) emit({ type: 'progress', phase, done: completed, total: phaseTotal, elapsedMs: Math.round(performance.now() - phaseStart), ...live });
}
function startPhase(id: string, title: string, total: number) {
  flushProgress(); // final numbers of the phase that just ended
  phase = id; phaseTotal = total; phaseStart = performance.now(); completed = 0;
  live = { statuses: {}, perInstance: {} };
  console.log(`\n=== ${id}  ${title} ===`);
  emit({ type: 'phase', phase: id, title, total });
}
function track(r: Res | Res[]) {
  for (const x of Array.isArray(r) ? r : [r]) {
    const k = statusKey(x);
    live.statuses[k] = (live.statuses[k] ?? 0) + 1;
    live.perInstance[x.instance] = (live.perInstance[x.instance] ?? 0) + 1;
  }
}
const progressTimer = setInterval(() => {
  if (phase) emit({ type: 'progress', phase, done: completed, total: phaseTotal, elapsedMs: Math.round(performance.now() - phaseStart), ...live });
}, 500);
async function pool<T>(n: number, task: (i: number) => Promise<T>): Promise<T[]> {
  const out: T[] = new Array(n);
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(CONCURRENCY, n) }, async () => {
    while (next < n) { const i = next++; out[i] = await task(i); track(out[i] as any); completed++; }
  }));
  return out;
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const rnd = (n: number) => Math.floor(Math.random() * n);
function shuffle<T>(a: T[]): T[] { const b = [...a]; for (let i = b.length - 1; i > 0; i--) { const j = rnd(i + 1); [b[i], b[j]] = [b[j], b[i]]; } return b; }
function latency(rs: Res[]) {
  const s = rs.map((r) => r.ms).sort((a, b) => a - b);
  const p = (q: number) => +s[Math.min(s.length - 1, Math.floor(s.length * q))].toFixed(1);
  return { p50: p(0.5), p95: p(0.95), p99: p(0.99), max: p(1) };
}
function count<T>(rs: T[], key: (r: T) => string) { const m: Record<string, number> = {}; for (const r of rs) { const k = key(r); m[k] = (m[k] ?? 0) + 1; } return m; }
const statusKey = (r: Res) => String(r.status) + (r.body?.error ? ` ${r.body.error}` : r.body?.replayed ? ' replayed' : '');
const line = (label: string, v: unknown) => { console.log(`${label.padEnd(56)} ${typeof v === 'object' ? JSON.stringify(v) : v}`); emit({ type: 'metric', phase, label, value: v }); };
let pass = true;
const check = (label: string, ok: boolean) => { if (!ok) pass = false; console.log(`${label.padEnd(56)} ${ok ? 'PASS' : 'FAIL'}`); emit({ type: 'check', phase, label, ok }); };

async function batchGet(ids: number[]) {
  const versions = new Map<number, number>();
  const rows = new Map<number, any>();
  for (let i = 0; i < ids.length; i += 10000) {
    const r = await call(0, 'POST', '/mumin/batch/get', { ids: ids.slice(i, i + 10000) });
    for (const [id, v] of Object.entries(r.body.versions)) versions.set(Number(id), v as number);
    for (const row of r.body.data) rows.set(row.Mumin_ID, row);
  }
  return { versions, rows };
}
async function drained() {
  for (;;) {
    const q = (await call(0, 'GET', '/cache/queue')).body;
    if (q.dirty === 0 && q.streamLength === 0) return q;
    await sleep(100);
  }
}

async function main() {
  // all instances up?
  for (let n = 0; n < PORTS.length; n++) {
    const r = await call(n, 'GET', '/cache/queue');
    if (r.status !== 200) throw new Error(`instance on port ${PORTS[n]} not reachable`);
  }
  const all: number[] = (await call(0, 'GET', '/cache/sample-ids?n=12000')).body;
  const ids = all.slice(0, 10000);
  const raceIds = all.slice(10000, 11000);
  const { rows: originals } = await batchGet(all);
  console.log(`instances: ${PORTS.map((p) => 'localhost:' + p).join(', ')} · one shared Redis`);
  console.log(`records: ${ids.length} (D1/D2) + ${raceIds.length} (D3) · client concurrency ${CONCURRENCY}\n`);

  try {
    await d1(ids);
    await d2(ids);
    await d3(raceIds);
  } catch (e) {
    pass = false;
    console.error('TEST ABORTED:', e);
  } finally {
    if (firstError) line('first network error seen', firstError);
    startPhase('CLEANUP', 'resume stopped records, compare MSSQL with Redis, restore original values', 0);
    const stopped = (await call(0, 'GET', '/cache/queue')).body.stopped;
    for (let i = 0; i < all.length; i += 1000) await call(i / 1000, 'POST', '/mumin/batch/resume', { ids: all.slice(i, i + 1000) });
    line('stopped records resumed', stopped);
    await drained();
    const { rows: redisRows } = await batchGet(all);
    check(`MSSQL == Redis for ${all.length.toLocaleString()} records`, (await compareWithMssql(redisRows)) === 0);
    const restore = [...originals.values()];
    for (let i = 0; i < restore.length; i += 1000) {
      const items = restore.slice(i, i + 1000).map((r) => ({ Mumin_ID: r.Mumin_ID, Post: r.Post }));
      const r = await call(i / 1000, 'PATCH', '/mumin/batch', { items }, { 'Idempotency-Key': `dt-restore-${i}-${randomUUID()}` });
      if (r.status !== 200) throw new Error(`restore failed: ${JSON.stringify(r.body)}`);
    }
    await drained();
    line('original values restored', restore.length);
  }
  console.log(`\nRESULT: ${pass ? 'ALL CHECKS PASSED' : 'SOME CHECKS FAILED'}`);
  clearInterval(progressTimer);
  flushProgress();
  emit({ type: 'end', pass });
  agents.forEach((a) => a.destroy());
  process.exit(pass ? 0 : 1);
}

// ------------------------------------------------------------------ D1
async function d1(ids: number[]) {
  startPhase('D1', `${TOTAL.toLocaleString()} requests (60% /process, 40% update) + 1,000 STOPs over ${PORTS.length} instance(s)`, TOTAL);
  const { versions: v0 } = await batchGet(ids);
  const toStop = shuffle(ids).slice(0, 1000);
  const stops = new Map<number, { versionAtStop: number; inFlight: string | null; instance: string }>();
  const t0 = performance.now();
  let done = false;
  const canceller = (async () => {
    // 100 STOP commands of 10 records, each fired when the run reaches the next 1% of progress,
    // each sent to a random instance
    for (let k = 0; k < 100 && !done; k++) {
      while (!done && completed < ((k + 1) / 101) * TOTAL) await sleep(5);
      if (done) break;
      const r = await call(rnd(PORTS.length), 'POST', '/mumin/batch/stop', { ids: toStop.slice(k * 10, k * 10 + 10), reason: 'distributed test' });
      for (const s of r.body.records) stops.set(s.id, { versionAtStop: s.versionAtStop, inFlight: s.inFlightRequest, instance: r.instance });
    }
  })();
  const res = await pool(TOTAL, async (i) => {
    const id = ids[rnd(ids.length)];
    const key = `d1-${i}-${randomUUID()}`;
    const kind = Math.random() < 0.6 ? 'process' : 'update';
    const r = kind === 'process'
      ? await call(i, 'POST', `/mumin/${id}/process`, { workMs: rnd(31), patch: { Post: `D1-${i}` } }, { 'Idempotency-Key': key })
      : await call(i, 'PATCH', `/mumin/${id}`, { Post: `D1-${i}` }, { 'Idempotency-Key': key });
    r.id = id; r.key = key; r.kind = kind;
    return r;
  });
  done = true;
  await canceller;
  line('network errors', res.filter((r) => r.status === 'ERROR').length);
  const secs = (performance.now() - t0) / 1000;
  const { versions: v1 } = await batchGet(ids);
  const ok = res.filter((r) => r.status === 200);
  const byKey = new Map(res.map((r) => [r.key!, r]));

  line('time / throughput', `${secs.toFixed(1)} s · ${Math.round(TOTAL / secs).toLocaleString()} requests/s (all instances)`);
  line('latency ms', latency(res));
  line('requests per instance', count(res, (r) => r.instance));
  line('responses', count(res, statusKey));
  line('records STOPPED during run', `${stops.size} (STOP sent via ${Object.keys(count([...stops.values()], (s) => s.instance)).length} different instances)`);
  const inFlight = [...stops.values()].filter((s) => s.inFlight);
  const crossInstance = inFlight.filter((s) => byKey.get(s.inFlight!.split('#')[0])?.instance !== s.instance).length;
  line('requests IN FLIGHT when their record was STOPPED', `${inFlight.length} (${crossInstance} stopped from a different instance)`);

  console.log('  -- cancellation');
  check('every request answered (no hang / timeout / error)', res.length === TOTAL && res.every((r) => typeof r.status === 'number'));
  check('only 200 / 409 BUSY / 423 STOPPED returned', res.every((r) => r.status === 200 || r.status === 409 || r.status === 423));
  check('423 only for stopped records', res.filter((r) => r.status === 423).every((r) => stops.has(r.id!)));
  check('stopped records: NOTHING applied after STOP', [...stops].every(([id, s]) => v1.get(id) === s.versionAtStop));
  check('accepted writes on stopped records all BEFORE stop', ok.filter((r) => stops.has(r.id!)).every((r) => r.body.version <= stops.get(r.id!)!.versionAtStop));
  check('in-flight requests at STOP refused at commit (423)', inFlight.every((s) => byKey.get(s.inFlight!.split('#')[0])?.status === 423));

  console.log('  -- concurrency control');
  const perId = new Map<number, number[]>();
  for (const r of ok) { const a = perId.get(r.id!) ?? []; a.push(r.body.version); perId.set(r.id!, a); }
  check('no two accepted writes got the same version (no lost update)', [...perId.values()].every((vs) => new Set(vs).size === vs.length));
  check('accepted writes == version increase, per record (exactly once)', ids.every((id) => (perId.get(id)?.length ?? 0) === v1.get(id)! - v0.get(id)!));
  line('409 RECORD_BUSY (record already being processed elsewhere)', res.filter((r) => r.body?.error === 'RECORD_BUSY').length);
  const q = (await call(0, 'GET', '/cache/queue')).body;
  check('no leaked processing leases', q.processing === 0);
}

// ------------------------------------------------------------------ D2
async function d2(ids: number[]) {
  const N = 10000;
  startPhase('D2', `${(N + 2000).toLocaleString()} requests, each sent to two instances at the same moment (same request id)`, N + 2000);
  // The 1,000 records stopped in D1 are still stopped: both copies of those requests must get 423.
  const { versions: v0 } = await batchGet(ids);
  const pairs = await pool(N + 2000, async (i) => {
    const id = ids[i % ids.length];
    const key = `d2-${i}-${randomUUID()}`;
    const a = rnd(PORTS.length);
    const b = PORTS.length > 1 ? (a + 1 + rnd(PORTS.length - 1)) % PORTS.length : a; // a different instance when possible
    const send = (node: number) => i < N
      ? call(node, 'PATCH', `/mumin/${id}`, { Post: `D2-${i}` }, { 'Idempotency-Key': key })
      : call(node, 'POST', `/mumin/${id}/process`, { workMs: 10, patch: { Post: `D2P-${i}` } }, { 'Idempotency-Key': key });
    const [r1, r2] = await Promise.all([send(a), send(b)]);
    r1.id = r2.id = id;
    return [r1, r2];
  });
  const { versions: v1 } = await batchGet(ids);
  const flat = pairs.flat();
  const applied = flat.filter((r) => r.status === 200 && !r.body.replayed).length;
  line('responses', count(flat, statusKey));
  const stoppedPairs = pairs.filter(([r1, r2]) => r1.status === 423 && r2.status === 423).length;
  check('every pair: applied at most once', pairs.every(([r1, r2]) => [r1, r2].filter((r) => r.status === 200 && !r.body.replayed).length <= 1));
  check('every non-stopped pair: applied exactly once', pairs.every(([r1, r2]) => r1.status === 423 || [r1, r2].filter((r) => r.status === 200 && !r.body.replayed).length === 1));
  const added = [...v1].reduce((s, [id, v]) => s + v - (v0.get(id) ?? 0), 0);
  check('versions added == applied requests (no double apply)', added === applied);
  check('pairs on stopped records: both copies refused (423)', pairs.every(([r1, r2]) => (r1.status === 423) === (r2.status === 423)));
  line('applied / duplicates answered / pairs on stopped records', `${applied} / ${flat.length - applied - stoppedPairs * 2} / ${stoppedPairs}`);
}

// ------------------------------------------------------------------ D3
async function d3(raceIds: number[]) {
  startPhase('D3', `${raceIds.length} records x ${PORTS.length} clients racing with the same If-Match`, raceIds.length);
  const { versions: v0 } = await batchGet(raceIds);
  const groups = await pool(raceIds.length, async (i) => {
    const id = raceIds[i];
    return Promise.all(PORTS.map((_, n) =>
      call(n, 'PATCH', `/mumin/${id}`, { Post: `RACE-${n}` }, { 'Idempotency-Key': `d3-${randomUUID()}`, 'If-Match': String(v0.get(id)) })));
  });
  const { versions: v1 } = await batchGet(raceIds);
  line('responses', count(groups.flat(), statusKey));
  line('winners per instance', count(groups.flat().filter((r) => r.status === 200), (r) => r.instance));
  check('exactly 1 winner per record, others 412', groups.every((g) => g.filter((r) => r.status === 200).length === 1 && g.filter((r) => r.status === 412).length === PORTS.length - 1));
  check('each record changed exactly once', raceIds.every((id) => v1.get(id)! - v0.get(id)! === 1));
}

async function compareWithMssql(redisRows: Map<number, any>): Promise<number> {
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  require('dotenv').config({ quiet: true });
  const sql = await import('mssql');
  const p = await new sql.ConnectionPool({
    server: process.env.MSSQL_HOST ?? 'localhost', port: Number(process.env.MSSQL_PORT ?? 1433),
    user: process.env.MSSQL_USER, password: process.env.MSSQL_PASSWORD, database: process.env.MSSQL_DATABASE ?? 'MMS',
    options: { encrypt: false, trustServerCertificate: true },
  }).connect();
  const r = await p.request().input('ids', sql.NVarChar(sql.MAX), JSON.stringify([...redisRows.keys()]))
    .query('SELECT Mumin_ID, Post FROM dbo.Mumin_Mast WHERE Mumin_ID IN (SELECT CAST([value] AS INT) FROM OPENJSON(@ids))');
  await p.close();
  let mismatch = 0;
  for (const row of r.recordset) if ((redisRows.get(row.Mumin_ID)?.Post ?? null) !== row.Post) mismatch++;
  return mismatch + (redisRows.size - r.recordset.length);
}

main().catch((e) => { console.error(e); emit({ type: 'end', pass: false, error: String(e) }); process.exit(1); });
