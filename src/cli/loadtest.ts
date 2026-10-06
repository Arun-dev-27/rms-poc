/**
 * HTTP load test against the running API (npm start first).
 *   node dist/cli/loadtest.js [requests=100000] [concurrency=200]
 *
 * P1  1 lakh single updates, unique request ids, 10,000 records  -> throughput + no lost update
 * P2  10,000 requests each sent TWICE at the same moment          -> applied exactly once
 * P3  1,000 records x 10 clients racing with the same If-Match    -> exactly 1 winner per record
 * P4  1,000 batch requests (100 items), every 10th has a bad id   -> bad ones break, write nothing
 * Then: MSSQL == Redis for every touched record, and all test changes are reverted.
 */
import * as http from 'http';
import { randomUUID } from 'crypto';

const BASE = { host: process.env.API_HOST ?? 'localhost', port: Number(process.env.API_PORT ?? 3000) };
const TOTAL = Number(process.argv[2] ?? 100000);
const CONCURRENCY = Number(process.argv[3] ?? 200);
const agent = new http.Agent({ keepAlive: true, maxSockets: CONCURRENCY });

interface Res { status: number; body: any; ms: number }

function call(method: string, path: string, body?: unknown, headers: Record<string, string> = {}): Promise<Res> {
  const payload = body === undefined ? undefined : JSON.stringify(body);
  const t0 = performance.now();
  return new Promise((resolve, reject) => {
    const req = http.request({ ...BASE, agent, method, path, headers: { ...(payload ? { 'content-type': 'application/json', 'content-length': Buffer.byteLength(payload) } : {}), ...headers } }, (res) => {
      const chunks: Buffer[] = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        const text = Buffer.concat(chunks).toString();
        resolve({ status: res.statusCode ?? 0, body: text ? JSON.parse(text) : null, ms: performance.now() - t0 });
      });
    });
    req.on('error', reject);
    if (payload) req.write(payload);
    req.end();
  });
}

/** Runs `n` tasks with at most CONCURRENCY in flight. */
async function pool<T>(n: number, task: (i: number) => Promise<T>): Promise<T[]> {
  const out: T[] = new Array(n);
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(CONCURRENCY, n) }, async () => {
    while (next < n) { const i = next++; out[i] = await task(i); }
  }));
  return out;
}

function pct(sorted: number[], p: number) { return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))]; }
function latency(results: Res[]) {
  const s = results.map((r) => r.ms).sort((a, b) => a - b);
  return { p50: +pct(s, 0.5).toFixed(1), p95: +pct(s, 0.95).toFixed(1), p99: +pct(s, 0.99).toFixed(1), max: +s[s.length - 1].toFixed(1) };
}
function byStatus(results: Res[]) {
  const m: Record<string, number> = {};
  for (const r of results) { const k = r.status === 200 && r.body?.replayed ? '200-replayed' : String(r.status); m[k] = (m[k] ?? 0) + 1; }
  return m;
}
async function versionsOf(ids: number[]): Promise<Map<number, number>> {
  const m = new Map<number, number>();
  for (let i = 0; i < ids.length; i += 10000) {
    const r = await call('POST', '/mumin/batch/get', { ids: ids.slice(i, i + 10000) });
    for (const [id, v] of Object.entries(r.body.versions)) m.set(Number(id), v as number);
  }
  return m;
}
async function rowsOf(ids: number[]): Promise<Map<number, any>> {
  const m = new Map<number, any>();
  for (let i = 0; i < ids.length; i += 10000) {
    const r = await call('POST', '/mumin/batch/get', { ids: ids.slice(i, i + 10000) });
    for (const row of r.body.data) m.set(row.Mumin_ID, row);
  }
  return m;
}
const sumDiff = (a: Map<number, number>, b: Map<number, number>) => [...b].reduce((s, [id, v]) => s + v - (a.get(id) ?? 0), 0);
const line = (label: string, v: unknown) => console.log(`${label.padEnd(44)} ${typeof v === 'object' ? JSON.stringify(v) : v}`);
let pass = true;
const check = (label: string, ok: boolean) => { if (!ok) pass = false; line(label, ok ? 'PASS' : 'FAIL'); };

async function drained() {
  const t0 = performance.now();
  for (;;) {
    const q = (await call('GET', '/cache/queue')).body;
    if (q.dirty === 0 && q.streamLength === 0) return performance.now() - t0;
    await new Promise((r) => setTimeout(r, 100));
  }
}

async function main() {
  const ids: number[] = (await call('GET', '/cache/sample-ids?n=10000')).body;
  const originals = await rowsOf(ids);
  console.log(`records under test: ${ids.length}, concurrency: ${CONCURRENCY}\n`);

  // ---------------- P1: 1 lakh unique requests
  console.log(`=== P1  ${TOTAL.toLocaleString()} single updates, each with a unique request id ===`);
  let before = await versionsOf(ids);
  let t0 = performance.now();
  const p1 = await pool(TOTAL, (i) => call('PATCH', `/mumin/${ids[i % ids.length]}`, { Post: `LT-${i}` }, { 'Idempotency-Key': `lt-p1-${i}-${randomUUID()}` }));
  let secs = (performance.now() - t0) / 1000;
  let after = await versionsOf(ids);
  line('time', `${secs.toFixed(1)} s`);
  line('throughput', `${Math.round(TOTAL / secs).toLocaleString()} requests/s`);
  line('latency ms', latency(p1));
  line('status', byStatus(p1));
  check('every request applied exactly once', p1.filter((r) => r.status === 200).length === TOTAL && sumDiff(before, after) === TOTAL);
  line('versions added (must equal requests)', sumDiff(before, after));
  line('time until all in MSSQL', `${(await drained() / 1000).toFixed(1)} s`);

  // ---------------- P2: duplicates sent at the same moment
  const DUP = 10000;
  console.log(`\n=== P2  ${DUP.toLocaleString()} requests, each sent TWICE at the same time (same request id) ===`);
  before = await versionsOf(ids);
  t0 = performance.now();
  const p2 = (await pool(DUP, async (i) => {
    const key = `lt-p2-${i}-${randomUUID()}`;
    const id = ids[i % ids.length];
    return Promise.all([0, 1].map(() => call('PATCH', `/mumin/${id}`, { Post: `DUP-${i}` }, { 'Idempotency-Key': key })));
  })).flat();
  after = await versionsOf(ids);
  line('time', `${((performance.now() - t0) / 1000).toFixed(1)} s for ${(DUP * 2).toLocaleString()} requests`);
  line('status', byStatus(p2));
  check('applied once, duplicate got stored answer', p2.filter((r) => r.status === 200 && !r.body.replayed).length === DUP && p2.filter((r) => r.body?.replayed).length === DUP);
  line('versions added (must equal 10,000)', sumDiff(before, after));
  check('no double apply', sumDiff(before, after) === DUP);

  // ---------------- P3: race on the same record with If-Match
  const RACE_IDS = ids.slice(0, 1000);
  const CLIENTS = 10;
  console.log(`\n=== P3  ${RACE_IDS.length} records x ${CLIENTS} clients updating the SAME record at once (If-Match) ===`);
  const v0 = await versionsOf(RACE_IDS);
  const p3 = (await pool(RACE_IDS.length, (i) => {
    const id = RACE_IDS[i];
    return Promise.all(Array.from({ length: CLIENTS }, (_, c) =>
      call('PATCH', `/mumin/${id}`, { Post: `RACE-${c}` }, { 'Idempotency-Key': `lt-p3-${randomUUID()}`, 'If-Match': String(v0.get(id)) })));
  }));
  const winners = p3.map((group) => group.filter((r) => r.status === 200).length);
  line('status', byStatus(p3.flat()));
  check('exactly 1 winner per record, rest 412', winners.every((w) => w === 1));
  const v1 = await versionsOf(RACE_IDS);
  check('each record changed exactly once', RACE_IDS.every((id) => v1.get(id)! - v0.get(id)! === 1));

  // ---------------- P4: batch with a bad id breaks, writes nothing
  const BATCHES = 1000;
  console.log(`\n=== P4  ${BATCHES} batch requests x 100 items, every 10th contains a non-existent id ===`);
  before = await versionsOf(ids);
  const p4 = await pool(BATCHES, (b) => {
    const items = Array.from({ length: 100 }, (_, j) => ({ Mumin_ID: ids[(b * 100 + j) % ids.length], Post: `B-${b}` }));
    if (b % 10 === 0) items[50] = { Mumin_ID: 1, Post: 'bad' };
    return call('PATCH', '/mumin/batch', { items }, { 'Idempotency-Key': `lt-p4-${b}-${randomUUID()}` });
  });
  after = await versionsOf(ids);
  line('status', byStatus(p4));
  line('batch latency ms', latency(p4));
  check('bad batches rejected (404, written 0)', p4.filter((_, b) => b % 10 === 0).every((r) => r.status === 404 && r.body.written === 0));
  check('only good batches applied (900 x 100)', sumDiff(before, after) === 900 * 100);

  // ---------------- consistency + cleanup
  console.log('\n=== MSSQL vs Redis ===');
  line('time until all in MSSQL', `${(await drained() / 1000).toFixed(1)} s`);
  const redisRows = await rowsOf(ids);
  const status = (await call('GET', '/cache/status')).body;
  line('queue / dirty / errors', `${status.writeBehind.streamLength} / ${status.writeBehind.dirty} / ${status.writeBehind.dlq}`);
  // MSSQL check through a cold read: drop nothing, ask the API's DB path via reconcile-free compare
  const mismatch = await compareWithMssql(redisRows);
  check(`MSSQL == Redis for ${ids.length.toLocaleString()} records`, mismatch === 0);

  console.log('\n=== cleanup: restore original Post values ===');
  const restore = [...originals.values()];
  for (let i = 0; i < restore.length; i += 1000) {
    const items = restore.slice(i, i + 1000).map((r) => ({ Mumin_ID: r.Mumin_ID, Post: r.Post }));
    const r = await call('PATCH', '/mumin/batch', { items }, { 'Idempotency-Key': `lt-restore-${i}-${randomUUID()}` });
    if (r.status !== 200) throw new Error(`restore failed: ${JSON.stringify(r.body)}`);
  }
  line('restored', `${restore.length} records, in MSSQL after ${(await drained() / 1000).toFixed(1)} s`);
  console.log(`\nRESULT: ${pass ? 'ALL CHECKS PASSED' : 'SOME CHECKS FAILED'}`);
  agent.destroy();
  process.exit(pass ? 0 : 1);
}

/** Reads the same records straight from MSSQL (env from .env) and compares the Post column. */
async function compareWithMssql(redisRows: Map<number, any>): Promise<number> {
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  require('dotenv').config({ quiet: true });
  const sql = await import('mssql');
  const pool = await new sql.ConnectionPool({
    server: process.env.MSSQL_HOST ?? 'localhost', port: Number(process.env.MSSQL_PORT ?? 1433),
    user: process.env.MSSQL_USER, password: process.env.MSSQL_PASSWORD, database: process.env.MSSQL_DATABASE ?? 'MMS',
    options: { encrypt: false, trustServerCertificate: true },
  }).connect();
  const r = await pool.request().input('ids', sql.NVarChar(sql.MAX), JSON.stringify([...redisRows.keys()]))
    .query('SELECT Mumin_ID, Post FROM dbo.Mumin_Mast WHERE Mumin_ID IN (SELECT CAST([value] AS INT) FROM OPENJSON(@ids))');
  await pool.close();
  let mismatch = 0;
  for (const row of r.recordset) if ((redisRows.get(row.Mumin_ID)?.Post ?? null) !== row.Post) mismatch++;
  return mismatch + (redisRows.size - r.recordset.length);
}

main().catch((e) => { console.error(e); process.exit(1); });
