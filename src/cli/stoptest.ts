/**
 * STOP / SKIP load test against the running API (npm start first).
 *   node dist/cli/stoptest.js [requests=100000] [concurrency=200]
 *
 * S1  1 lakh updates on 10,000 records while 1,000 of them are STOPPED at random moments mid-run
 *       -> nothing applied to a record after its STOP, blocked requests get 423, none hang
 * S2  10,000 two-phase /process requests (0-100 ms work) on 2,000 records while 500 are STOPPED
 *       -> requests in flight at STOP fail at commit (423, nothing written), no leaked leases
 * Then: all records resumed, MSSQL == Redis, original values restored.
 */
import * as http from 'http';
import { randomUUID } from 'crypto';

const BASE = { host: process.env.API_HOST ?? 'localhost', port: Number(process.env.API_PORT ?? 3000) };
const TOTAL = Number(process.argv[2] ?? 100000);
const CONCURRENCY = Number(process.argv[3] ?? 200);
const CLIENT_TIMEOUT_MS = 30000; // a response slower than this counts as "hanging"
const agent = new http.Agent({ keepAlive: true, maxSockets: CONCURRENCY + 20 });

interface Res { status: number | 'TIMEOUT'; body: any; ms: number; key?: string; id?: number }

function call(method: string, path: string, body?: unknown, headers: Record<string, string> = {}): Promise<Res> {
  const payload = body === undefined ? undefined : JSON.stringify(body);
  const t0 = performance.now();
  return new Promise((resolve, reject) => {
    const req = http.request({ ...BASE, agent, method, path, timeout: CLIENT_TIMEOUT_MS, headers: { ...(payload ? { 'content-type': 'application/json', 'content-length': Buffer.byteLength(payload) } : {}), ...headers } }, (res) => {
      const chunks: Buffer[] = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        const text = Buffer.concat(chunks).toString();
        resolve({ status: res.statusCode ?? 0, body: text ? JSON.parse(text) : null, ms: performance.now() - t0 });
      });
    });
    req.on('timeout', () => { req.destroy(); resolve({ status: 'TIMEOUT', body: null, ms: performance.now() - t0 }); });
    req.on('error', (e) => (req.destroyed ? undefined : reject(e)));
    if (payload) req.write(payload);
    req.end();
  });
}

async function pool<T>(n: number, task: (i: number) => Promise<T>): Promise<T[]> {
  const out: T[] = new Array(n);
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(CONCURRENCY, n) }, async () => {
    while (next < n) { const i = next++; out[i] = await task(i); }
  }));
  return out;
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
function shuffle<T>(a: T[]): T[] { const b = [...a]; for (let i = b.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [b[i], b[j]] = [b[j], b[i]]; } return b; }
function latency(results: Res[]) {
  const s = results.map((r) => r.ms).sort((a, b) => a - b);
  const p = (q: number) => +s[Math.min(s.length - 1, Math.floor(s.length * q))].toFixed(1);
  return { p50: p(0.5), p95: p(0.95), p99: p(0.99), max: p(1) };
}
function byStatus(results: Res[]) {
  const m: Record<string, number> = {};
  for (const r of results) { const k = String(r.status) + (r.body?.error ? ` ${r.body.error}` : ''); m[k] = (m[k] ?? 0) + 1; }
  return m;
}
async function batchGet(ids: number[]) {
  const versions = new Map<number, number>();
  const rows = new Map<number, any>();
  for (let i = 0; i < ids.length; i += 10000) {
    const r = await call('POST', '/mumin/batch/get', { ids: ids.slice(i, i + 10000) });
    for (const [id, v] of Object.entries(r.body.versions)) versions.set(Number(id), v as number);
    for (const row of r.body.data) rows.set(row.Mumin_ID, row);
  }
  return { versions, rows };
}
async function stopIds(ids: number[], reason: string) {
  const r = await call('POST', '/mumin/batch/stop', { ids, reason });
  return r.body.records as { id: number; versionAtStop: number; inFlightRequest: string | null }[];
}
async function drained() {
  for (;;) {
    const q = (await call('GET', '/cache/queue')).body;
    if (q.dirty === 0 && q.streamLength === 0) return q;
    await sleep(100);
  }
}
const line = (label: string, v: unknown) => console.log(`${label.padEnd(50)} ${typeof v === 'object' ? JSON.stringify(v) : v}`);
let pass = true;
const check = (label: string, ok: boolean) => { if (!ok) pass = false; line(label, ok ? 'PASS' : 'FAIL'); };

async function main() {
  const all: number[] = (await call('GET', '/cache/sample-ids?n=12000')).body;
  const s1Ids = all.slice(0, 10000);
  const s2Ids = all.slice(10000, 12000);
  const { rows: originals } = await batchGet(all);
  console.log(`records: S1 ${s1Ids.length}, S2 ${s2Ids.length} · concurrency ${CONCURRENCY}\n`);

  // ======================= S1
  console.log(`=== S1  ${TOTAL.toLocaleString()} updates, 1,000 records STOPPED at random moments during the run ===`);
  const toStop = shuffle(s1Ids).slice(0, 1000);
  const stopVersion = new Map<number, number>();
  const stopAt = new Map<number, number>();
  let running = true;
  const t0 = performance.now();
  const stopper = (async () => {
    // 100 STOP commands of 10 records, spread over the run
    for (let i = 0; i < 100 && running; i++) {
      await sleep(250 + Math.random() * 250);
      for (const r of await stopIds(toStop.slice(i * 10, i * 10 + 10), 'load test S1')) {
        stopVersion.set(r.id, r.versionAtStop);
        stopAt.set(r.id, performance.now() - t0);
      }
    }
  })();
  const s1 = await pool(TOTAL, async (i) => {
    const id = s1Ids[i % s1Ids.length];
    const r = await call('PATCH', `/mumin/${id}`, { Post: `S1-${i}` }, { 'Idempotency-Key': `s1-${i}-${randomUUID()}` });
    r.id = id;
    return r;
  });
  running = false;
  await stopper;
  const secs = (performance.now() - t0) / 1000;
  const { versions: s1After } = await batchGet(s1Ids);
  line('time / throughput', `${secs.toFixed(1)} s · ${Math.round(TOTAL / secs).toLocaleString()} requests/s`);
  line('latency ms', latency(s1));
  line('responses', byStatus(s1));
  line('records stopped during run', `${stopVersion.size} (STOP commands spread over ${(Math.max(...stopAt.values()) / 1000).toFixed(1)} s)`);
  check('every request got a response (no hang/timeout)', s1.every((r) => r.status !== 'TIMEOUT') && s1.length === TOTAL);
  check('only 200 or 423 returned', s1.every((r) => r.status === 200 || r.status === 423));
  check('423 only for stopped records', s1.filter((r) => r.status === 423).every((r) => stopVersion.has(r.id!)));
  check('stopped records: NOTHING applied after STOP', [...stopVersion].every(([id, v]) => s1After.get(id) === v));
  check('accepted updates of stopped records were all BEFORE stop', s1.filter((r) => r.status === 200 && stopVersion.has(r.id!)).every((r) => r.body.version <= stopVersion.get(r.id!)!));
  check('active records: never blocked', s1.filter((r) => !stopVersion.has(r.id!)).every((r) => r.status === 200));

  // ======================= S2
  const S2N = 10000;
  console.log(`\n=== S2  ${S2N.toLocaleString()} two-phase /process requests (0-100 ms work), 500 records STOPPED mid-run ===`);
  const toStop2 = shuffle(s2Ids).slice(0, 500);
  const stop2 = new Map<number, { versionAtStop: number; inFlightRequest: string | null }>();
  running = true;
  const t2 = performance.now();
  const stopper2 = (async () => {
    for (let i = 0; i < 50 && running; i++) {
      await sleep(80 + Math.random() * 80);
      for (const r of await stopIds(toStop2.slice(i * 10, i * 10 + 10), 'load test S2')) stop2.set(r.id, r);
    }
  })();
  const s2 = await pool(S2N, async (i) => {
    const id = s2Ids[i % s2Ids.length];
    const key = `s2-${i}-${randomUUID()}`;
    const r = await call('POST', `/mumin/${id}/process`, { workMs: Math.floor(Math.random() * 100), patch: { Post: `S2-${i}` } }, { 'Idempotency-Key': key });
    r.id = id; r.key = key;
    return r;
  });
  running = false;
  await stopper2;
  const { versions: s2After } = await batchGet(s2Ids);
  const byKey = new Map(s2.map((r) => [r.key!, r]));
  const inFlight = [...stop2.values()].filter((s) => s.inFlightRequest);
  line('time', `${((performance.now() - t2) / 1000).toFixed(1)} s`);
  line('latency ms', latency(s2));
  line('responses', byStatus(s2));
  line('requests caught IN FLIGHT by a STOP', inFlight.length);
  check('every request got a response (no hang/timeout)', s2.every((r) => r.status !== 'TIMEOUT'));
  check('only 200 / 409 BUSY / 423 STOPPED returned', s2.every((r) => [200, 409, 423].includes(r.status as number)));
  check('in-flight requests at STOP were refused at commit (423)', inFlight.every((s) => byKey.get(s.inFlightRequest!)?.status === 423));
  check('stopped records: NOTHING applied after STOP', [...stop2].every(([id, s]) => s2After.get(id) === s.versionAtStop));
  const q = (await call('GET', '/cache/queue')).body;
  check('no leaked processing leases', q.processing === 0);

  // ======================= consistency + cleanup
  console.log('\n=== resume, MSSQL vs Redis, cleanup ===');
  await call('POST', '/mumin/batch/resume', { ids: toStop });
  await call('POST', '/mumin/batch/resume', { ids: toStop2 });
  line('stopped records after resume', (await call('GET', '/cache/queue')).body.stopped);
  await drained();
  const { rows: redisRows } = await batchGet(all);
  check(`MSSQL == Redis for ${all.length.toLocaleString()} records`, (await compareWithMssql(redisRows)) === 0);
  const restore = [...originals.values()];
  for (let i = 0; i < restore.length; i += 1000) {
    const items = restore.slice(i, i + 1000).map((r) => ({ Mumin_ID: r.Mumin_ID, Post: r.Post }));
    const r = await call('PATCH', '/mumin/batch', { items }, { 'Idempotency-Key': `st-restore-${i}-${randomUUID()}` });
    if (r.status !== 200) throw new Error(`restore failed: ${JSON.stringify(r.body)}`);
  }
  await drained();
  line('original values restored', restore.length);
  console.log(`\nRESULT: ${pass ? 'ALL CHECKS PASSED' : 'SOME CHECKS FAILED'}`);
  agent.destroy();
  process.exit(pass ? 0 : 1);
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

main().catch((e) => { console.error(e); process.exit(1); });
