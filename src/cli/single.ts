/**
 * ONE record, 1 lakh requests (ticket-booking "hot show").
 *
 *   API_PORTS=3001,3002,3003,3004 node dist/cli/single.js [requests=100000] [concurrency=400] [Mumin_ID]
 *
 * S1  1 lakh requests on the SAME record from all instances (50% update, 29.5% instant process,
 *     0.5% slow process (20 ms work, forms a line), 20% read) + every 500th request sent twice
 *     -> every request succeeds; writes get versions v+1 .. v+N with no gap and no repeat (one at a time)
 * S2  1 lakh requests on the same record, CANCELLED after 30%
 *     -> before: 200; waiting in line: 423 at once; after: 423 at once; nothing written after cancel
 * CLEANUP  resume, nothing left over, MSSQL row == Redis row (79 columns), original value restored.
 * Real request/response pairs are emitted as "sample" events for the UI.
 */
import * as http from 'http';
import { randomUUID } from 'crypto';
import { appendFileSync, mkdirSync, writeFileSync } from 'fs';

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
const FIXED_ID = process.argv[4] ? Number(process.argv[4]) : undefined;
const agents = PORTS.map(() => new http.Agent({ keepAlive: true, maxSockets: Math.ceil(CONCURRENCY / PORTS.length) + 60 }));
const SAMPLE_COLS = ['Mumin_ID', 'Full_Name', 'Gender', 'Age', 'Jamaat', 'Post'];

interface Req { method: string; path: string; port: number; headers: Record<string, string>; body?: unknown }
interface Res { status: number | 'TIMEOUT' | 'ERROR'; body: any; ms: number; instance: string; endAt: number; req: Req; kind?: string }

// ------------------------------------------------------------------ progress / samples
const PROGRESS = process.env.PROGRESS_JSON === '1';
let phase = '';
let phaseTotal = 0;
let phaseStart = performance.now();
let completed = 0;
let live: { statuses: Record<string, number>; perInstance: Record<string, number> } = { statuses: {}, perInstance: {} };
function emit(e: Record<string, unknown>) { if (PROGRESS) { try { process.stdout.write('@@' + JSON.stringify(e) + '\n'); } catch { /* parent gone */ } } }
function flushProgress() { if (phase) emit({ type: 'progress', phase, done: completed, total: phaseTotal, elapsedMs: Math.round(performance.now() - phaseStart), ...live }); }
function startPhase(id: string, title: string, total: number) {
  flushProgress();
  phase = id; phaseTotal = total; phaseStart = performance.now(); completed = 0;
  live = { statuses: {}, perInstance: {} };
  console.log(`\n=== ${id}  ${title} ===`);
  emit({ type: 'phase', phase: id, title, total });
}
const progressTimer = setInterval(flushProgress, 500);
const statusKey = (r: Res) => (r.kind === 'read' && r.status === 200 ? '200 read' : String(r.status) + (r.body?.error ? ` ${r.body.error}` : r.body?.replayed ? ' replayed' : r.body?.path === 'line' ? ' (waited in line)' : ''));
function track(r: Res) { const k = statusKey(r); live.statuses[k] = (live.statuses[k] ?? 0) + 1; live.perInstance[r.instance] = (live.perInstance[r.instance] ?? 0) + 1; }
const line = (label: string, v: unknown) => { console.log(`${label.padEnd(60)} ${typeof v === 'object' ? JSON.stringify(v) : v}`); emit({ type: 'metric', phase, label, value: v }); };
let pass = true;
const check = (label: string, ok: boolean) => { if (!ok) pass = false; console.log(`${label.padEnd(60)} ${ok ? 'PASS' : 'FAIL'}`); emit({ type: 'check', phase, label, ok }); };
function sample(title: string, r: Res | undefined) {
  if (!r) return;
  const body = r.body && typeof r.body === 'object' ? { ...r.body } : r.body;
  if (body?.data && typeof body.data === 'object') body.data = { ...Object.fromEntries(SAMPLE_COLS.filter((k) => k in body.data).map((k) => [k, body.data[k]])), '…': '+73 more columns' };
  const s = {
    title,
    request: { method: r.req.method, url: `http://${HOST}:${r.req.port}${r.req.path}`, headers: r.req.headers, body: r.req.body },
    response: { status: r.status, instance: r.instance, ms: +r.ms.toFixed(1), body },
  };
  console.log(`\n-- ${title}\n   ${s.request.method} ${s.request.url} ${JSON.stringify(s.request.headers)} ${s.request.body ? JSON.stringify(s.request.body) : ''}\n   -> ${r.status} (${r.instance}, ${s.response.ms} ms) ${JSON.stringify(body)}`);
  emit({ type: 'sample', phase, ...s });
}

// ------------------------------------------------------------------ http
function send(node: number, method: string, path: string, body?: unknown, headers: Record<string, string> = {}): Promise<Res> {
  const n = ((node % PORTS.length) + PORTS.length) % PORTS.length;
  const payload = body === undefined ? undefined : JSON.stringify(body);
  const req: Req = { method, path, port: PORTS[n], headers, body };
  const t0 = performance.now();
  return new Promise((resolve) => {
    const r = http.request({
      host: HOST, port: PORTS[n], agent: agents[n], method, path, timeout: 150000,
      headers: { ...(payload ? { 'content-type': 'application/json', 'content-length': Buffer.byteLength(payload) } : {}), ...headers },
    }, (res) => {
      const chunks: Buffer[] = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        const text = Buffer.concat(chunks).toString();
        let parsed: any = null;
        try { parsed = text ? JSON.parse(text) : null; } catch { parsed = { error: 'NON_JSON_BODY' }; }
        const end = performance.now();
        resolve({ status: res.statusCode ?? 0, body: parsed, ms: end - t0, endAt: end, instance: String(res.headers['x-instance'] ?? PORTS[n]), req });
      });
    });
    r.on('timeout', () => { r.destroy(); resolve({ status: 'TIMEOUT', body: null, ms: performance.now() - t0, endAt: performance.now(), instance: String(PORTS[n]), req }); });
    r.on('error', (e) => resolve({ status: 'ERROR', body: { error: (e as NodeJS.ErrnoException).code ?? 'ERROR' }, ms: performance.now() - t0, endAt: performance.now(), instance: String(PORTS[n]), req }));
    if (payload) r.write(payload);
    r.end();
  });
}
async function sendLive(method: string, path: string, body?: unknown, headers: Record<string, string> = {}): Promise<Res> {
  let last: Res | undefined;
  for (let k = 0; k < PORTS.length; k++) { last = await send(k, method, path, body, headers); if (typeof last.status === 'number') return last; }
  return last!;
}
let aborted = false;
const parentPid = process.ppid;
const watch = PROGRESS ? setInterval(() => {
  try { process.kill(parentPid, 0); } catch { if (!aborted) { aborted = true; console.log('\nParent API instance is gone: stopping load, running cleanup'); } }
  sendLive('GET', '/loadtest/status').then((r) => { if (r.body?.cancelRequested && !aborted) { aborted = true; console.log('\nCANCEL requested: stopping load, running cleanup'); } });
}, 1000) : undefined;
async function pool(n: number, task: (i: number) => Promise<Res | Res[]>): Promise<Res[]> {
  const out: Res[] = [];
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(CONCURRENCY, n) }, async () => {
    while (next < n && !aborted) {
      const i = next++;
      const r = await task(i);
      for (const x of Array.isArray(r) ? r : [r]) { out.push(x); track(x); }
      completed++;
    }
  }));
  if (aborted) throw new Error('cancelled');
  return out;
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const rnd = (n: number) => Math.floor(Math.random() * n);
function pct(values: number[]) {
  const s = [...values].sort((a, b) => a - b);
  const p = (q: number) => (s.length ? +s[Math.min(s.length - 1, Math.floor(s.length * q))].toFixed(1) : 0);
  return { p50: p(0.5), p95: p(0.95), p99: p(0.99), max: p(1) };
}

/** One user request on the record. kind: update / process (instant) / slow (20 ms work, forms a line) / read */
function userRequest(i: number, id: number, tag: string, node = i, key = `${tag}-${i}-${randomUUID()}`): Promise<Res> {
  const x = (i * 7919) % 1000; // deterministic mix
  const kind = x < 5 ? 'slow' : x < 500 ? 'update' : x < 800 ? 'process' : 'read';
  const h = { 'Idempotency-Key': key };
  const p = kind === 'read' ? send(node, 'GET', `/mumin/${id}`)
    : kind === 'update' ? send(node, 'PATCH', `/mumin/${id}`, { Post: `${tag}-${i}` }, h)
      : send(node, 'POST', `/mumin/${id}/process`, { workMs: kind === 'slow' ? 20 : 0, patch: { Post: `${tag}-${i}` } }, h);
  return p.then((r) => Object.assign(r, { kind }));
}

async function main() {
  for (let n = 0; n < PORTS.length; n++) if ((await send(n, 'GET', '/cache/queue')).status !== 200) throw new Error(`instance on port ${PORTS[n]} not reachable`);
  const id = FIXED_ID ?? (await sendLive('GET', '/cache/sample-ids?n=5000')).body[rnd(5000)];
  const first = await sendLive('GET', `/mumin/${id}`);
  if (first.status !== 200) throw new Error(`Mumin_ID ${id} not found`);
  const originalPost = first.body.data.Post;
  console.log(`record: Mumin_ID ${id} · instances ${PORTS.join(', ')} · client concurrency ${CONCURRENCY}`);
  emit({ type: 'metric', phase: '', label: 'record', value: id });
  try {
    await s1(id);
    await s2(id);
  } catch (e) {
    pass = false;
    console.error('TEST STOPPED:', (e as Error).message);
  } finally {
    startPhase('CLEANUP', 'resume the record, check nothing is left over, compare the MSSQL row with Redis, restore the original value', 0);
    await sendLive('POST', `/mumin/${id}/resume`, {});
    for (let i = 0; i < 300; i++) { const q = (await sendLive('GET', '/cache/queue')).body; if (q.dirty === 0 && q.streamLength === 0 && !q.processing && !q.inflight) break; await sleep(100); }
    const st = (await sendLive('GET', `/mumin/${id}/status`)).body;
    check('record ACTIVE again, nobody processing or waiting', st.status === 'ACTIVE' && st.waitingInLine === 0 && !st.processingRequest);
    check('MSSQL row == Redis row (all 79 columns)', await sameInMssql(id));
    const r = await sendLive('PATCH', `/mumin/${id}`, { Post: originalPost }, { 'Idempotency-Key': `single-restore-${randomUUID()}` });
    for (let i = 0; i < 100; i++) { const q = (await sendLive('GET', '/cache/queue')).body; if (q.dirty === 0) break; await sleep(100); }
    line('original value restored', `Post = ${JSON.stringify(originalPost)} (HTTP ${r.status})`);
  }
  console.log(`\nRESULT: ${aborted ? 'CANCELLED (cleanup done)' : pass ? 'ALL CHECKS PASSED' : 'SOME CHECKS FAILED'}`);
  clearInterval(progressTimer);
  if (watch) clearInterval(watch);
  flushProgress();
  emit({ type: 'end', pass });
  agents.forEach((a) => a.destroy());
  process.exit(pass ? 0 : 1);
}

// ------------------------------------------------------------------ S1
async function s1(id: number) {
  startPhase('S1', `${TOTAL.toLocaleString()} requests on ONE record (Mumin_ID ${id}) from all instances, no cancel: all must succeed`, TOTAL);
  const v0 = (await sendLive('GET', `/mumin/${id}/status`)).body.version;
  const t0 = performance.now();
  const dupPairs: Res[][] = [];
  const res = await pool(TOTAL, async (i) => {
    if (i % 500 === 250) { // the same request (same request id) sent to two instances at the same moment
      const key = `S1dup-${i}-${randomUUID()}`;
      const pair = await Promise.all([send(i, 'PATCH', `/mumin/${id}`, { Post: `S1-${i}` }, { 'Idempotency-Key': key }), send(i + 1, 'PATCH', `/mumin/${id}`, { Post: `S1-${i}` }, { 'Idempotency-Key': key })]);
      pair.forEach((p) => (p.kind = 'dup'));
      dupPairs.push(pair);
      return pair;
    }
    return userRequest(i, id, 'S1');
  });
  const secs = (performance.now() - t0) / 1000;
  const st = (await sendLive('GET', `/mumin/${id}`)).body;
  const writes = res.filter((r) => r.kind !== 'read' && r.status === 200 && !r.body.replayed);
  const versions = writes.map((r) => r.body.version).sort((a, b) => a - b);
  const top = writes.reduce((a, b) => (b.body.version > a.body.version ? b : a), writes[0]);

  line('time / throughput', `${secs.toFixed(1)} s · ${Math.round(res.length / secs).toLocaleString()} requests/s on ONE record`);
  line('latency ms', pct(res.map((r) => r.ms)));
  line('writes applied', `${writes.length.toLocaleString()} (direct ${writes.filter((r) => r.body.path === 'direct').length.toLocaleString()}, waited in line ${writes.filter((r) => r.body.path === 'line').length.toLocaleString()})`);
  line('version before -> after', `${v0} -> ${st.version}`);
  check('EVERY request succeeded (200) - zero failures', res.every((r) => r.status === 200));
  check('versions are v+1 ... v+N: no gap, no repeat (one at a time)', versions.length === st.version - v0 && versions.every((v, k) => v === v0 + k + 1));
  const topBody = top?.req.body as { Post?: string; patch?: { Post?: string } } | undefined;
  check('final value = the last write in that order', !!topBody && st.data.Post === (topBody.Post ?? topBody.patch?.Post));
  check('every read returned the record', res.filter((r) => r.kind === 'read').every((r) => r.status === 200 && r.body.data?.Mumin_ID === id));
  check(`duplicates (${dupPairs.length} pairs): applied once, both answered`, dupPairs.every(([a, b]) => a.status === 200 && b.status === 200 && [a, b].filter((x) => x.body.replayed).length === 1 && a.body.version === b.body.version));

  const byVersion = [...writes].sort((a, b) => a.body.version - b.body.version);
  sample('First request processed', byVersion[0]);
  sample('Second request (next version, same record)', byVersion[1]);
  const dp = dupPairs[Math.floor(dupPairs.length / 2)];
  if (dp) { sample('Duplicate: original', dp.find((x) => !x.body.replayed)); sample('Duplicate: same request id on another server (applied once)', dp.find((x) => x.body.replayed)); }
  sample('Slow request (20 ms work) that waited its turn in line', res.filter((r) => r.kind === 'slow' && r.body?.path === 'line').sort((a, b) => b.body.waitedMs - a.body.waitedMs)[0]);
  sample('Read during the load', res.find((r) => r.kind === 'read'));
  sample('Last request processed', byVersion[byVersion.length - 1]);
}

// ------------------------------------------------------------------ S2
async function s2(id: number) {
  startPhase('S2', `${TOTAL.toLocaleString()} requests on the same record, CANCELLED after 30%: the rest must be refused`, TOTAL);
  let cancel: Res | undefined;
  const cancelAfter = Math.floor(TOTAL * 0.3);
  const canceller = (async () => {
    while (completed < cancelAfter && !aborted) await sleep(5);
    if (aborted) return;
    cancel = await send(rnd(PORTS.length), 'POST', `/mumin/${id}/cancel`, { reason: 'show cancelled (load test)' });
  })();
  // a few more slow requests here so people are waiting in line at the moment of the cancel
  const res = await pool(TOTAL, (i) => (i % 50 === 0
    ? send(i, 'POST', `/mumin/${id}/process`, { workMs: 20, patch: { Post: `S2-${i}` } }, { 'Idempotency-Key': `S2-${i}-${randomUUID()}` }).then((r) => Object.assign(r, { kind: 'slow' }))
    : userRequest(i, id, 'S2')));
  await canceller;
  if (!cancel) throw new Error('cancel was not sent');
  const c = cancel.body.records[0];
  const cancelAt = cancel.endAt;
  const st = (await sendLive('GET', `/mumin/${id}`)).body;
  const writes = res.filter((r) => r.kind !== 'read');
  const ok = writes.filter((r) => r.status === 200);
  const waitingCancelled = writes.filter((r) => r.status === 423 && String(r.body?.message).includes('waiting in line'));
  const after = writes.filter((r) => r.status === 423 && r.endAt - r.ms > cancelAt);

  line('cancel', `sent via ${cancel.instance} after ${cancelAfter.toLocaleString()} requests · version at cancel ${c.versionAtStop} · ${c.waitingCancelled} waiting in line · ${c.inFlightRequest ? '1' : '0'} being processed`);
  line('writes done before cancel / refused (423)', `${ok.length.toLocaleString()} / ${writes.filter((r) => r.status === 423).length.toLocaleString()}`);
  line('cancel -> answer for requests waiting in line ms', pct(waitingCancelled.map((r) => r.endAt - cancelAt)));
  line('response time of requests sent after the cancel ms', pct(after.map((r) => r.ms)));
  check('every request answered (no hang / timeout / error)', res.every((r) => typeof r.status === 'number'));
  check('only 200 (before cancel) or 423 (cancelled) - no other failure', res.every((r) => r.status === 200 || r.status === 423));
  check('NOTHING written after the cancel (version unchanged)', st.version === c.versionAtStop);
  check('every write that got 200 happened before the cancel', ok.every((r) => r.body.version <= c.versionAtStop));
  check('requests waiting in line were answered within 1 s of the cancel', waitingCancelled.every((r) => r.endAt - cancelAt < 1000));
  check('reads kept working (also on the cancelled record)', res.filter((r) => r.kind === 'read').every((r) => r.status === 200));
  check('status shows CANCELLED', st.status === 'CANCELLED');

  sample('Last request processed before the cancel', ok.sort((a, b) => b.body.version - a.body.version)[0]);
  sample('CANCEL request', cancel);
  sample('Request that was waiting in line when the cancel came', waitingCancelled[0]);
  sample('New request after the cancel (refused at once)', after[after.length - 1]);
  sample('Read on the cancelled record (still works)', res.filter((r) => r.kind === 'read').pop());
}

async function sameInMssql(id: number): Promise<boolean> {
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  require('dotenv').config({ quiet: true });
  const sql = await import('mssql');
  const p = await new sql.ConnectionPool({
    server: process.env.MSSQL_HOST ?? 'localhost', port: Number(process.env.MSSQL_PORT ?? 1433),
    user: process.env.MSSQL_USER, password: process.env.MSSQL_PASSWORD, database: process.env.MSSQL_DATABASE ?? 'MMS',
    options: { encrypt: false, trustServerCertificate: true },
  }).connect();
  const row = (await p.request().input('id', sql.Int, id).query('SELECT * FROM dbo.Mumin_Mast WHERE Mumin_ID = @id')).recordset[0];
  await p.close();
  const redis = (await sendLive('GET', `/mumin/${id}`)).body.data;
  return !!row && Object.keys(row).every((k) => JSON.stringify(row[k] instanceof Date ? row[k].toISOString() : row[k]) === JSON.stringify(redis[k]));
}

main().catch((e) => { console.error(e); emit({ type: 'end', pass: false, error: String(e) }); process.exit(1); });
