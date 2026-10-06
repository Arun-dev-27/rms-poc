/**
 * Distributed "ticket booking" test: several API instances, one shared Redis.
 *
 *   API_PORTS=3001,3002,3003,3004 node dist/cli/dtest.js [requests=100000] [concurrency=400]
 *
 * T1  1 lakh requests (50% /process with work, 30% update, 20% read) on 2,000 records: heavy
 *     contention, NO cancels  -> every request must succeed (200), each write applied exactly once
 * T2  1 lakh requests on 10,000 records while 1,000 records are CANCELLED during the run
 *     -> only 200 or 423; nothing written after cancel; people waiting in line are answered at once
 * T3  duplicates: the same request (same Idempotency-Key) sent to TWO instances at the same moment
 *     -> both succeed, applied exactly once (the second gets the first's answer)
 * T4  "show cancelled": 100 records x 12 slow requests all waiting in line, then all 100 cancelled
 *     -> everyone waiting is answered 423 right away, the ones being processed are refused at commit
 * Then: everything resumed, no leftover lines/leases, MSSQL == Redis, original values restored.
 * PROGRESS_JSON=1 prints "@@{json}" progress lines for the UI.
 */
import * as http from 'http';
import { randomUUID } from 'crypto';
import { appendFileSync, mkdirSync, writeFileSync } from 'fs';

// Output also goes to logs/loadtest-last.log; if the parent API instance dies, its stdout pipe closes
// (EPIPE) -> ignore that and keep running so the cleanup still happens.
mkdirSync('logs', { recursive: true });
writeFileSync('logs/loadtest-last.log', '');
for (const stream of [process.stdout, process.stderr]) stream.on('error', () => undefined);
for (const m of ['log', 'error'] as const) {
  const orig = console[m].bind(console);
  console[m] = (...args: unknown[]) => {
    try { appendFileSync('logs/loadtest-last.log', args.map((x) => (typeof x === 'string' ? x : x instanceof Error ? x.stack : JSON.stringify(x))).join(' ') + '\n'); } catch { /* ignore */ }
    try { orig(...args); } catch { /* parent gone */ }
  };
}

const PORTS = (process.env.API_PORTS ?? '3001,3002,3003,3004').split(',').map(Number);
const HOST = process.env.API_HOST ?? 'localhost';
const TOTAL = Number(process.argv[2] ?? 100000);
const CONCURRENCY = Number(process.argv[3] ?? 400);
const CLIENT_TIMEOUT_MS = 150000; // > queue max wait: a response slower than this counts as "hanging"
const agents = PORTS.map(() => new http.Agent({ keepAlive: true, maxSockets: Math.ceil(CONCURRENCY / PORTS.length) + 50 }));

interface Res { status: number | 'TIMEOUT' | 'ERROR'; body: any; ms: number; instance: string; id?: number; key?: string; kind?: string }

// ------------------------------------------------------------------ progress (UI)
const PROGRESS = process.env.PROGRESS_JSON === '1';
let phase = '';
let phaseTotal = 0;
let phaseStart = performance.now();
let completed = 0;
let live: { statuses: Record<string, number>; perInstance: Record<string, number> } = { statuses: {}, perInstance: {} };
function emit(event: Record<string, unknown>) { if (PROGRESS) process.stdout.write('@@' + JSON.stringify(event) + '\n'); }
function flushProgress() {
  if (phase) emit({ type: 'progress', phase, done: completed, total: phaseTotal, elapsedMs: Math.round(performance.now() - phaseStart), ...live });
}
function startPhase(id: string, title: string, total: number) {
  flushProgress();
  phase = id; phaseTotal = total; phaseStart = performance.now(); completed = 0;
  live = { statuses: {}, perInstance: {} };
  console.log(`\n=== ${id}  ${title} ===`);
  emit({ type: 'phase', phase: id, title, total });
}
const progressTimer = setInterval(flushProgress, 500);

/** Started from the UI: a Cancel there stops new load, but cleanup (resume + restore) still runs. */
let aborted = false;
const parentPid = process.ppid;
const cancelWatch = PROGRESS ? setInterval(() => {
  // the API instance that started this test died (killed / restarted): stop the load, still clean up
  try { process.kill(parentPid, 0); } catch {
    if (!aborted) { aborted = true; console.log('\nParent API instance is gone: stopping load, running cleanup'); }
  }
  callLive('GET', '/loadtest/status').then((r) => {
    if (r.body?.cancelRequested && !aborted) { aborted = true; console.log('\nCANCEL requested: stopping load, running cleanup'); }
  });
}, 1000) : undefined;
const statusKey = (r: Res) => String(r.status) + (r.body?.error ? ` ${r.body.error}` : r.body?.replayed ? ' replayed' : '');
function track(r: Res | Res[]) {
  for (const x of Array.isArray(r) ? r : [r]) {
    const k = x.kind === 'read' && x.status === 200 ? '200 read' : statusKey(x);
    live.statuses[k] = (live.statuses[k] ?? 0) + 1;
    live.perInstance[x.instance] = (live.perInstance[x.instance] ?? 0) + 1;
  }
}
const line = (label: string, v: unknown) => { console.log(`${label.padEnd(58)} ${typeof v === 'object' ? JSON.stringify(v) : v}`); emit({ type: 'metric', phase, label, value: v }); };
let pass = true;
const check = (label: string, ok: boolean) => { if (!ok) pass = false; console.log(`${label.padEnd(58)} ${ok ? 'PASS' : 'FAIL'}`); emit({ type: 'check', phase, label, ok }); };

// ------------------------------------------------------------------ http
let firstError = '';
function call(node: number, method: string, path: string, body?: unknown, headers: Record<string, string> = {}): Promise<Res> {
  const n = ((node % PORTS.length) + PORTS.length) % PORTS.length;
  const payload = body === undefined ? undefined : JSON.stringify(body);
  const t0 = performance.now();
  return new Promise((resolve) => {
    const req = http.request({
      host: HOST, port: PORTS[n], agent: agents[n], method, path, timeout: CLIENT_TIMEOUT_MS,
      headers: { ...(payload ? { 'content-type': 'application/json', 'content-length': Buffer.byteLength(payload) } : {}), ...headers },
    }, (res) => {
      const chunks: Buffer[] = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        const text = Buffer.concat(chunks).toString();
        let parsed: any = null;
        try { parsed = text ? JSON.parse(text) : null; } catch { parsed = { error: 'NON_JSON_BODY' }; }
        resolve({ status: res.statusCode ?? 0, body: parsed, ms: performance.now() - t0, instance: String(res.headers['x-instance'] ?? PORTS[n]) });
      });
    });
    req.on('timeout', () => { req.destroy(); resolve({ status: 'TIMEOUT', body: null, ms: performance.now() - t0, instance: String(PORTS[n]) }); });
    req.on('error', (e) => { if (!firstError) firstError = String(e); resolve({ status: 'ERROR', body: { error: (e as NodeJS.ErrnoException).code ?? 'ERROR' }, ms: performance.now() - t0, instance: String(PORTS[n]) }); });
    if (payload) req.write(payload);
    req.end();
  });
}
/** Control / cleanup calls: use whichever instance answers (an instance may have died). */
async function callLive(method: string, path: string, body?: unknown, headers: Record<string, string> = {}): Promise<Res> {
  const start = rnd(PORTS.length);
  let last: Res | undefined;
  for (let k = 0; k < PORTS.length; k++) {
    last = await call(start + k, method, path, body, headers);
    if (typeof last.status === 'number') return last;
  }
  return last!;
}
async function pool<T>(n: number, task: (i: number) => Promise<T>): Promise<T[]> {
  const out: T[] = new Array(n);
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(CONCURRENCY, n) }, async () => {
    while (next < n && !aborted) { const i = next++; out[i] = await task(i); track(out[i] as any); completed++; }
  }));
  if (aborted) throw new Error('cancelled from the UI');
  return out;
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const rnd = (n: number) => Math.floor(Math.random() * n);
function shuffle<T>(a: T[]): T[] { const b = [...a]; for (let i = b.length - 1; i > 0; i--) { const j = rnd(i + 1); [b[i], b[j]] = [b[j], b[i]]; } return b; }
function pct(values: number[]) {
  const s = [...values].sort((a, b) => a - b);
  const p = (q: number) => (s.length ? +s[Math.min(s.length - 1, Math.floor(s.length * q))].toFixed(1) : 0);
  return { p50: p(0.5), p95: p(0.95), p99: p(0.99), max: p(1) };
}
const count = <T>(rs: T[], key: (r: T) => string) => { const m: Record<string, number> = {}; for (const r of rs) { const k = key(r); m[k] = (m[k] ?? 0) + 1; } return m; };

async function batchGet(ids: number[]) {
  const versions = new Map<number, number>();
  const rows = new Map<number, any>();
  for (let i = 0; i < ids.length; i += 10000) {
    const r = await callLive('POST', '/mumin/batch/get', { ids: ids.slice(i, i + 10000) });
    for (const [id, v] of Object.entries(r.body.versions)) versions.set(Number(id), v as number);
    for (const row of r.body.data) rows.set(row.Mumin_ID, row);
  }
  return { versions, rows };
}
async function queueState() { return (await callLive('GET', '/cache/queue')).body; }
async function drained() { for (;;) { const q = await queueState(); if (q.dirty === 0 && q.streamLength === 0) return q; await sleep(100); } }

/** One user request: 50% process (0-20 ms work), 30% update, 20% read. */
function userRequest(i: number, id: number, tag: string): Promise<Res> {
  const key = `${tag}-${i}-${randomUUID()}`;
  const x = Math.random();
  const kind = x < 0.5 ? 'process' : x < 0.8 ? 'update' : 'read';
  const p = kind === 'process'
    ? call(i, 'POST', `/mumin/${id}/process`, { workMs: rnd(21), patch: { Post: `${tag}-${i}` } }, { 'Idempotency-Key': key })
    : kind === 'update'
      ? call(i, 'PATCH', `/mumin/${id}`, { Post: `${tag}-${i}` }, { 'Idempotency-Key': key })
      : call(i, 'GET', `/mumin/${id}`);
  return p.then((r) => Object.assign(r, { id, key, kind }));
}

async function main() {
  for (let n = 0; n < PORTS.length; n++) {
    if ((await call(n, 'GET', '/cache/queue')).status !== 200) throw new Error(`instance on port ${PORTS[n]} not reachable`);
  }
  const all: number[] = (await call(0, 'GET', '/cache/sample-ids?n=12000')).body;
  const hot = all.slice(0, 2000);       // T1 + T3
  const t2Ids = all.slice(2000, 12000); // T2
  const { rows: originals } = await batchGet(all);
  console.log(`instances: ${PORTS.map((p) => 'localhost:' + p).join(', ')} · one shared Redis · client concurrency ${CONCURRENCY}`);
  try {
    await t1(hot);
    await t2(t2Ids);
    await t3(hot);
    await t4(all.slice(2000, 2100));
  } catch (e) {
    pass = false;
    console.error('TEST ABORTED:', e);
  } finally {
    startPhase('CLEANUP', 'resume cancelled records, check no leftovers, compare MSSQL with Redis, restore original values', 0);
    for (let i = 0; i < all.length; i += 1000) await callLive('POST', '/mumin/batch/resume', { ids: all.slice(i, i + 1000) });
    await drained();
    // hand-over marks of tickets that were cancelled expire by themselves within 5 s
    let q = await queueState();
    for (let i = 0; i < 100 && (q.processing || q.inflight || q.cancelled); i++) { await sleep(100); q = await queueState(); }
    check('no record left processing / waiting / in-progress / cancelled', q.processing === 0 && q.inflight === 0 && q.cancelled === 0);
    const { rows: redisRows } = await batchGet(all);
    check(`MSSQL == Redis for ${all.length.toLocaleString()} records`, (await compareWithMssql(redisRows)) === 0);
    const restore = [...originals.values()];
    for (let i = 0; i < restore.length; i += 1000) {
      const items = restore.slice(i, i + 1000).map((r) => ({ Mumin_ID: r.Mumin_ID, Post: r.Post }));
      const r = await callLive('PATCH', '/mumin/batch', { items }, { 'Idempotency-Key': `dt-restore-${i}-${randomUUID()}` });
      if (r.status !== 200) throw new Error(`restore failed: ${JSON.stringify(r.body)}`);
    }
    await drained();
    line('original values restored', restore.length);
    if (firstError) line('first network error', firstError);
  }
  console.log(`\nRESULT: ${aborted ? 'CANCELLED (cleanup done)' : pass ? 'ALL CHECKS PASSED' : 'SOME CHECKS FAILED'}`);
  clearInterval(progressTimer);
  if (cancelWatch) clearInterval(cancelWatch);
  flushProgress();
  emit({ type: 'end', pass });
  agents.forEach((a) => a.destroy());
  process.exit(pass ? 0 : 1);
}

// ------------------------------------------------------------------ T1: everything succeeds
async function t1(ids: number[]) {
  startPhase('T1', `${TOTAL.toLocaleString()} requests on ${ids.length.toLocaleString()} records (heavy contention), all instances, no cancel: all must succeed`, TOTAL);
  const { versions: v0 } = await batchGet(ids);
  const t0 = performance.now();
  const res = await pool(TOTAL, (i) => userRequest(i, ids[rnd(ids.length)], 'T1'));
  const secs = (performance.now() - t0) / 1000;
  const { versions: v1 } = await batchGet(ids);
  const writes = res.filter((r) => r.kind !== 'read');
  line('time / throughput', `${secs.toFixed(1)} s · ${Math.round(TOTAL / secs).toLocaleString()} requests/s`);
  line('latency ms', pct(res.map((r) => r.ms)));
  line('time waiting in line ms', pct(writes.filter((r) => r.status === 200).map((r) => r.body.waitedMs ?? 0)));
  line('requests', count(res, (r) => r.kind!));
  check('EVERY request succeeded (200) - zero failures', res.every((r) => r.status === 200));
  check('every read returned the record', res.filter((r) => r.kind === 'read').every((r) => r.status === 200 && r.body.data?.Mumin_ID === r.id));
  const perId = new Map<number, number[]>();
  for (const r of writes) { const a = perId.get(r.id!) ?? []; a.push(r.body.version); perId.set(r.id!, a); }
  check('each record processed one request at a time (unique versions)', [...perId.values()].every((vs) => new Set(vs).size === vs.length));
  check('every write applied exactly once (versions added == writes)', ids.every((id) => (perId.get(id)?.length ?? 0) === v1.get(id)! - v0.get(id)!));
  line('most requests in line at once for one record', Math.max(0, ...writes.map((r) => r.body.queuePosition ?? 0)));
}

// ------------------------------------------------------------------ T2: cancel cancels everything
async function t2(ids: number[]) {
  startPhase('T2', `${TOTAL.toLocaleString()} requests on ${ids.length.toLocaleString()} records while 1,000 records are CANCELLED during the run`, TOTAL);
  const { versions: v0 } = await batchGet(ids);
  const toCancel = shuffle(ids).slice(0, 1000);
  const cancels = new Map<number, { versionAtStop: number; inFlight: string | null; waiting: number; instance: string }>();
  let done = false;
  const t0 = performance.now();
  const canceller = (async () => {
    for (let k = 0; k < 100 && !done; k++) {
      while (!done && completed < ((k + 1) / 101) * TOTAL) await sleep(5);
      if (done) break;
      const r = await call(rnd(PORTS.length), 'POST', '/mumin/batch/cancel', { ids: toCancel.slice(k * 10, k * 10 + 10), reason: 'load test: show cancelled' });
      for (const s of r.body.records) cancels.set(s.id, { versionAtStop: s.versionAtStop, inFlight: s.inFlightRequest, waiting: s.waitingCancelled, instance: r.instance });
    }
  })();
  // extra pressure on the records that will be cancelled, so people are waiting in line when it happens
  const res = await pool(TOTAL, (i) => userRequest(i, Math.random() < 0.3 ? toCancel[rnd(toCancel.length)] : ids[rnd(ids.length)], 'T2'));
  done = true;
  await canceller;
  const secs = (performance.now() - t0) / 1000;
  const { versions: v1 } = await batchGet(ids);
  const writes = res.filter((r) => r.kind !== 'read');
  const byKey = new Map(writes.map((r) => [r.key!, r]));
  const inFlight = [...cancels.values()].filter((c) => c.inFlight);
  const waitingCancelled = [...cancels.values()].reduce((s, c) => s + c.waiting, 0);
  line('time / throughput', `${secs.toFixed(1)} s · ${Math.round(TOTAL / secs).toLocaleString()} requests/s`);
  line('latency ms', pct(res.map((r) => r.ms)));
  line('records cancelled', `${cancels.size} (cancel sent via ${Object.keys(count([...cancels.values()], (c) => c.instance)).length} instances)`);
  line('requests waiting in line when their record was cancelled', waitingCancelled);
  line('requests being processed when their record was cancelled', inFlight.length);
  check('every request answered (no hang / timeout / error)', res.every((r) => typeof r.status === 'number'));
  check('only 200 (done) or 423 (cancelled) - no other failure', res.every((r) => r.status === 200 || r.status === 423));
  check('423 only for cancelled records', res.filter((r) => r.status === 423).every((r) => cancels.has(r.id!)));
  check('cancelled records: NOTHING written after cancel', [...cancels].every(([id, c]) => v1.get(id) === c.versionAtStop));
  check('requests being processed at cancel were refused (423)', inFlight.every((c) => byKey.get(c.inFlight!.split('#')[0])?.status === 423));
  check('reads keep working on cancelled records', res.filter((r) => r.kind === 'read' && cancels.has(r.id!)).every((r) => r.status === 200));
  check('active records: never refused', res.filter((r) => !cancels.has(r.id!)).every((r) => r.status === 200));
  const perId = new Map<number, number>();
  for (const r of writes.filter((w) => w.status === 200)) perId.set(r.id!, (perId.get(r.id!) ?? 0) + 1);
  check('every accepted write applied exactly once', ids.every((id) => (perId.get(id) ?? 0) === v1.get(id)! - v0.get(id)!));
}

// ------------------------------------------------------------------ T3: no duplicates
async function t3(ids: number[]) {
  const N = 10000;
  startPhase('T3', `${N.toLocaleString()} requests, each sent to TWO instances at the same moment (same request id)`, N);
  const { versions: v0 } = await batchGet(ids);
  const pairs = await pool(N, async (i) => {
    const id = ids[i % ids.length];
    const key = `T3-${i}-${randomUUID()}`;
    const a = rnd(PORTS.length);
    const b = PORTS.length > 1 ? (a + 1 + rnd(PORTS.length - 1)) % PORTS.length : a;
    const send = (node: number) => (i % 2 === 0
      ? call(node, 'PATCH', `/mumin/${id}`, { Post: `T3-${i}` }, { 'Idempotency-Key': key })
      : call(node, 'POST', `/mumin/${id}/process`, { workMs: 10, patch: { Post: `T3P-${i}` } }, { 'Idempotency-Key': key }));
    const [r1, r2] = await Promise.all([send(a), send(b)]);
    r1.id = r2.id = id;
    return [r1, r2];
  });
  const { versions: v1 } = await batchGet(ids);
  const flat = pairs.flat();
  const applied = flat.filter((r) => r.status === 200 && !r.body.replayed).length;
  check('both copies of every request succeeded (200)', flat.every((r) => r.status === 200));
  check('applied exactly once, the other copy got the same answer', pairs.every(([r1, r2]) => [r1, r2].filter((r) => !r.body?.replayed).length === 1 && r1.body?.version === r2.body?.version));
  const added = [...v1].reduce((s, [id, v]) => s + v - (v0.get(id) ?? 0), 0);
  check('versions added == requests (no double apply)', added === N && applied === N);
}

// ------------------------------------------------------------------ T4: cancel clears the whole line at once
async function t4(ids: number[]) {
  const PER = 12;
  startPhase('T4', `show cancelled: ${ids.length} records x ${PER} slow requests (250 ms work) in line, then all ${ids.length} records cancelled`, ids.length * PER);
  await call(0, 'POST', '/mumin/batch/resume', { ids }); // these were T2 records
  const { versions: v0 } = await batchGet(ids);
  const bigAgents = PORTS.map(() => new http.Agent({ keepAlive: true, maxSockets: 500 }));
  const send = (i: number, id: number) => new Promise<Res>((resolve) => {
    const n = i % PORTS.length;
    const body = JSON.stringify({ workMs: 250, patch: { Post: `T4-${i}` } });
    const t0 = performance.now();
    const req = http.request({ host: HOST, port: PORTS[n], agent: bigAgents[n], method: 'POST', path: `/mumin/${id}/process`, timeout: CLIENT_TIMEOUT_MS,
      headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body), 'Idempotency-Key': `T4-${i}-${randomUUID()}` } }, (res) => {
      let text = ''; res.on('data', (c) => (text += c));
      res.on('end', () => resolve({ status: res.statusCode ?? 0, body: JSON.parse(text), ms: performance.now() - t0, instance: String(res.headers['x-instance']), id, key: String(performance.now()) }));
    });
    req.on('timeout', () => { req.destroy(); resolve({ status: 'TIMEOUT', body: null, ms: 0, instance: '', id }); });
    req.on('error', () => resolve({ status: 'ERROR', body: null, ms: 0, instance: '', id }));
    req.write(body); req.end();
  });
  const all = ids.flatMap((id, r) => Array.from({ length: PER }, (_, j) => send(r * PER + j, id).then((x) => { track(x); completed++; return x; })));
  // wait until the lines are really formed (most requests waiting), like a sold-out show
  const formUntil = performance.now() + 20000;
  let inLine = 0;
  while (performance.now() < formUntil) {
    const st = await Promise.all(ids.map((id, i) => call(i, 'GET', `/mumin/${id}/status`)));
    inLine = st.reduce((n, r) => n + (r.body?.waitingInLine ?? 0), 0);
    if (inLine >= ids.length * (PER - 4)) break;
    await sleep(100);
  }
  line('requests in line just before cancel', inLine);
  const cancelAt = performance.now();
  const c = await call(1, 'POST', '/mumin/batch/cancel', { ids, reason: 'show cancelled' });
  const waiting = c.body.records.reduce((n: number, r: any) => n + r.waitingCancelled, 0);
  const v = new Map<number, number>(c.body.records.map((r: any) => [r.id, r.versionAtStop]));
  const res = await Promise.all(all);
  // only requests that were WAITING in line when the cancel happened
  const answeredAfterCancel = res.filter((r) => r.status === 423 && String(r.body?.message).includes('waiting in line')).map((r) => Number(r.key) - cancelAt);
  line('cancelled while waiting in line', answeredAfterCancel.length);
  const { versions: v1 } = await batchGet(ids);
  line('requests waiting in line at cancel (cleared at once)', waiting);
  line('requests being processed at cancel', c.body.records.filter((r: any) => r.inFlightRequest).length);
  line('responses', count(res, statusKey));
  line('cancel -> answer time for cancelled requests ms', pct(answeredAfterCancel));
  check('every request answered (no hang / timeout / error)', res.every((r) => typeof r.status === 'number'));
  check('only 200 (done before cancel) or 423 (cancelled)', res.every((r) => r.status === 200 || r.status === 423));
  check('everyone waiting in line was answered within 1 s of the cancel', answeredAfterCancel.length >= waiting && answeredAfterCancel.every((ms) => ms < 1000));
  check('nothing written after cancel', ids.every((id) => v1.get(id) === v.get(id)));
  check('requests done before cancel == versions added', res.filter((r) => r.status === 200).length === ids.reduce((n, id) => n + v1.get(id)! - v0.get(id)!, 0));
  bigAgents.forEach((a) => a.destroy());
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
