/**
 * Ticket booking load test: 1 lakh real HTTP booking requests spread over every live API node.
 *
 *   NODE_URLS=http://localhost:3100,http://localhost:3101 TL_PARAMS='{"total":100000}' node dist/cli/ticketload.js
 *
 * P0  setup     test zone (reset, N tickets, long freeze time so nothing expires mid-test), real ITS ids from
 *               Mumin_Mast, admin freezes part of the tickets
 * P1  bookings  `total` requests. Each ITS id fires its requests AT THE SAME INSTANT, each to a different node;
 *               `sameIdPct` % of them re-use the same request id. At `unfreezeAt` % the admin unfreezes under load.
 *      At `zoneFreezeAt` % the whole zone is frozen (everything except booked): held requests are cancelled and one
 *      payment tried while frozen; at `zoneUnfreezeAt` % it is unfrozen and the freed tickets go to the line in order.
 * P2  line      `rounds` x "finish freezes" (pay `payPct` %, cancel the rest) -> freed tickets go to the line in order
 * P3  checks    exactly one accepted request per ITS id, no errors, tickets add up, no oversell, FIFO, all nodes used
 * PROGRESS_JSON=1 prints "@@{json}" progress lines for the API that started it.
 */
import * as http from 'http';

interface Params {
  total: number; concurrency: number; its: number; sameIdPct: number; maxQty: number;
  tickets: number; adminFreeze: number; unfreezeAt: number; rounds: number; payPct: number;
  zoneFreezeAt: number; zoneUnfreezeAt: number; cancelDuringFreeze: number;
}
const P: Params = {
  total: 100000, concurrency: 400, its: 20000, sameIdPct: 30, maxQty: 4,
  tickets: 5000, adminFreeze: 1000, unfreezeAt: 60, rounds: 3, payPct: 50,
  zoneFreezeAt: 30, zoneUnfreezeAt: 45, cancelDuringFreeze: 200,
  ...JSON.parse(process.env.TL_PARAMS ?? '{}'),
};
const NODES = (process.env.NODE_URLS ?? 'http://localhost:3100').split(',').map((u) => new URL(u));
const RUN = process.env.TL_RUN ?? Date.now().toString(36);
const ZONE = 'load-test-zone-1';
const TIMEOUT_MS = 60000;
const agents = NODES.map(() => new http.Agent({ keepAlive: true, maxSockets: Math.ceil(P.concurrency / NODES.length) + 50 }));
for (const s of [process.stdout, process.stderr]) s.on('error', () => undefined);

const emit = (e: Record<string, unknown>) => { if (process.env.PROGRESS_JSON === '1') process.stdout.write('@@' + JSON.stringify(e) + '\n'); else console.log(JSON.stringify(e)); };
const log = (m: string) => console.log(m);

interface Res { status: number; body: any; ms: number; node: number }
function req(node: number, method: string, path: string, body?: unknown, headers: Record<string, string> = {}): Promise<Res> {
  const u = NODES[node];
  const data = body === undefined ? undefined : JSON.stringify(body);
  const t = performance.now();
  return new Promise((resolve) => {
    const r = http.request({
      host: u.hostname, port: u.port, path, method, agent: agents[node], timeout: TIMEOUT_MS,
      headers: { 'Content-Type': 'application/json', 'X-Load-Test': RUN, ...(data ? { 'Content-Length': Buffer.byteLength(data) } : {}), ...headers },
    }, (res) => {
      const chunks: Buffer[] = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        let b: any = null;
        try { b = JSON.parse(Buffer.concat(chunks).toString()); } catch { /* not json */ }
        resolve({ status: res.statusCode ?? 0, body: b, ms: performance.now() - t, node });
      });
    });
    r.on('timeout', () => { r.destroy(); resolve({ status: -1, body: { message: 'TIMEOUT' }, ms: performance.now() - t, node }); });
    r.on('error', (e) => resolve({ status: -2, body: { message: e.message }, ms: performance.now() - t, node }));
    if (data) r.write(data);
    r.end();
  });
}
const must = async (p: Promise<Res>, what: string) => {
  const r = await p;
  if (r.status < 200 || r.status >= 300) throw new Error(`${what}: HTTP ${r.status} ${JSON.stringify(r.body)}`);
  return r.body;
};
const pct = (a: number[], p: number) => (a.length ? +a[Math.min(a.length - 1, Math.floor(a.length * p))].toFixed(1) : 0);

async function main() {
  const t0 = performance.now();
  emit({ type: 'start', nodes: NODES.map((u) => u.host), params: P, zone: ZONE });

  // ---------------- P0 setup
  emit({ type: 'phase', phase: 'P0', title: 'Setup: test zone, real ITS ids, admin freeze' });
  await must(req(0, 'POST', '/tickets/zones', { city: 'Load Test', name: 'Zone 1', show: 'Load test', total: P.tickets, holdSeconds: 3600 }), 'create zone');
  await must(req(0, 'POST', `/tickets/zones/${ZONE}/reset`), 'reset zone');
  await must(req(0, 'PATCH', `/tickets/zones/${ZONE}`, { total: P.tickets, holdSeconds: 3600 }), 'set zone');
  const itsIds: number[] = await must(req(0, 'GET', `/tickets/its-sample?n=${P.its}`), 'ITS sample');
  let adminRid = '';
  if (P.adminFreeze > 0) adminRid = (await must(req(1 % NODES.length, 'POST', `/tickets/zones/${ZONE}/freeze`, { qty: P.adminFreeze, reason: 'load test: blocked until unfreeze under load' }), 'admin freeze')).request.rid;
  emit({ type: 'metric', phase: 'P0', label: 'setup', value: { its: itsIds.length, tickets: P.tickets, adminFrozen: P.adminFreeze, freezeTimeS: 3600 } });

  // ---------------- P1 booking storm
  emit({ type: 'phase', phase: 'P1', title: `${P.total.toLocaleString('en-IN')} booking requests over ${NODES.length} node(s)`, total: P.total });
  const perIts = new Map<number, number>();
  const base = Math.floor(P.total / itsIds.length), extra = P.total % itsIds.length;
  itsIds.forEach((id, i) => perIts.set(id, base + (i < extra ? 1 : 0)));
  const groups = itsIds.filter((id) => perIts.get(id)! > 0);
  const statuses: Record<string, number> = {};
  const outcomes: Record<string, number> = { FROZEN: 0, WAITING: 0, DUP_REQUEST_ID: 0, DUP_ITS: 0, OTHER: 0 };
  const perNode = NODES.map(() => 0);
  const accepted = new Map<number, number>();
  const lat: number[] = [];
  const errors: string[] = [];
  let done = 0, next = 0, rr = 0, unfrozen: any = null;
  const heldRids: string[] = [];
  // zone freeze under load: okAt = freeze confirmed, offAt = unfreeze sent
  const zf: any = { on: false, okAt: 0, offAt: 0, answeredWhileFrozen: 0, frozenWhileFrozen: 0, cancels: 0, cancelsMoved: 0, payStatus: 0, payCode: '', unfreeze: null };
  const p1 = performance.now();
  const progress = () => emit({ type: 'progress', phase: 'P1', done, total: P.total, elapsedMs: Math.round(performance.now() - p1), statuses, outcomes, perNode: Object.fromEntries(NODES.map((u, i) => [u.host, perNode[i]])) });
  const tick = setInterval(progress, 300);
  const groupSize = Math.max(1, base);
  const workers = Math.max(1, Math.floor(P.concurrency / groupSize));

  const worker = async () => {
    while (next < groups.length) {
      const its = groups[next++];
      const n = perIts.get(its)!;
      const qty = 1 + ((its * 7) % P.maxQty);
      const first = `lt-${RUN}-${its}-0`;
      const ids = Array.from({ length: n }, (_, k) => (k > 0 && Math.random() * 100 < P.sameIdPct ? first : `lt-${RUN}-${its}-${k}`));
      // same ITS id, all requests at the same instant, each to a different node
      const sentAt = performance.now();
      const answers = await Promise.all(ids.map((rid) => req(rr++ % NODES.length, 'POST', `/tickets/zones/${ZONE}/book`, { muminId: its, qty }, { 'Idempotency-Key': rid })));
      const answeredAt = performance.now();
      // sent after the freeze was confirmed and answered before the unfreeze was sent -> must never get tickets
      const insideFreeze = zf.okAt && sentAt > zf.okAt && (!zf.offAt || answeredAt < zf.offAt);
      for (const a of answers) {
        if (insideFreeze) { zf.answeredWhileFrozen++; if (a.status === 200 && a.body?.outcome === 'FROZEN') zf.frozenWhileFrozen++; }
        if (a.status === 200 && a.body?.outcome === 'FROZEN' && heldRids.length < 5000) heldRids.push(a.body.request.rid);
        done++;
        perNode[a.node]++;
        lat.push(a.ms);
        statuses[a.status] = (statuses[a.status] ?? 0) + 1;
        if (a.status === 200 && a.body?.outcome === 'FROZEN') { outcomes.FROZEN++; accepted.set(its, (accepted.get(its) ?? 0) + 1); }
        else if (a.status === 202) { outcomes.WAITING++; accepted.set(its, (accepted.get(its) ?? 0) + 1); }
        else if (a.status === 409 && a.body?.code === 'REQUEST_ID_ALREADY_USED') outcomes.DUP_REQUEST_ID++;
        else if (a.status === 409 && a.body?.code === 'ITS_HAS_OPEN_REQUEST') outcomes.DUP_ITS++;
        else { outcomes.OTHER++; if (errors.length < 20) errors.push(`${a.status} ${JSON.stringify(a.body).slice(0, 160)}`); }
      }
      // whole zone frozen under load (everything except booked), once
      if (P.zoneFreezeAt && !zf.on && !zf.unfreeze && done >= (P.total * P.zoneFreezeAt) / 100) {
        zf.on = true;
        const fr = await req(3 % NODES.length, 'POST', `/tickets/zones/${ZONE}/zone-freeze`, { reason: 'load test: zone frozen under load' });
        zf.freezeStatus = fr.status; zf.atFreeze = fr.body?.freeze?.atFreeze; zf.okAt = performance.now(); zf.freezeNode = NODES[fr.node].host;
        // while frozen: cancel held requests -> their tickets must NOT go to anyone yet
        for (const rid of heldRids.splice(0, P.cancelDuringFreeze)) {
          const c = await req(zf.cancels++ % NODES.length, 'POST', `/tickets/zones/${ZONE}/requests/${rid}/release`, { reason: 'load test: cancelled while the zone is frozen' });
          if (c.status === 200) zf.cancelsMoved += c.body?.movedFromLine ?? 0;
        }
        // while frozen: paying must be refused
        const payRid = heldRids.shift();
        if (payRid) { const pr = await req(0, 'POST', `/tickets/zones/${ZONE}/requests/${payRid}/confirm`); zf.payStatus = pr.status; zf.payCode = pr.body?.code ?? ''; }
        zf.duringDone = true; // the unfreeze waits for these in-freeze checks
        emit({ type: 'metric', phase: 'P1', label: 'zoneFreeze', value: { ...zf } });
      }
      // exactly one worker unfreezes (flag set before the await), and only after the in-freeze checks
      if (zf.on && zf.duringDone && !zf.unfreezing && done >= (P.total * P.zoneUnfreezeAt) / 100) {
        zf.unfreezing = true;
        zf.offAt = performance.now();
        const ur = await req(4 % NODES.length, 'POST', `/tickets/zones/${ZONE}/zone-unfreeze`);
        zf.on = false;
        zf.unfreeze = { status: ur.status, movedFromLine: ur.body?.movedFromLine ?? 0, timersResumed: ur.body?.freeze?.timersResumed ?? 0, node: NODES[ur.node].host };
        emit({ type: 'metric', phase: 'P1', label: 'zoneFreeze', value: { ...zf } });
      }
      // admin unfreezes under load, once, from a different node
      if (adminRid && !unfrozen && done >= (P.total * P.unfreezeAt) / 100) {
        unfrozen = { at: done };
        const r = await req(2 % NODES.length, 'POST', `/tickets/zones/${ZONE}/requests/${adminRid}/release`, { reason: 'load test: unfreeze under load' });
        unfrozen = { at: done, status: r.status, movedFromLine: r.body?.movedFromLine ?? 0, node: NODES[r.node].host };
        emit({ type: 'metric', phase: 'P1', label: 'unfreeze', value: unfrozen });
      }
    }
  };
  await Promise.all(Array.from({ length: workers }, worker));
  clearInterval(tick);
  progress();
  const p1ms = performance.now() - p1;
  lat.sort((a, b) => a - b);
  const p1metrics = {
    seconds: +(p1ms / 1000).toFixed(1), perSecond: Math.round(P.total / (p1ms / 1000)),
    latencyMs: { p50: pct(lat, 0.5), p95: pct(lat, 0.95), p99: pct(lat, 0.99), max: pct(lat, 1) },
    outcomes, statuses, unfreeze: unfrozen, errors, zoneFreeze: zf,
  };
  emit({ type: 'metric', phase: 'P1', label: 'result', value: p1metrics });
  const afterP1 = await must(req(0, 'GET', `/tickets/zones/${ZONE}`), 'zone after P1');

  // ---------------- P2 the line moves
  emit({ type: 'phase', phase: 'P2', title: `${P.rounds} round(s): pay ${P.payPct} %, cancel the rest → line served in order`, total: P.rounds });
  const rounds: any[] = [];
  for (let i = 0; i < P.rounds; i++) {
    const r = await req(i % NODES.length, 'POST', `/tickets/zones/${ZONE}/settle`, { payPct: P.payPct });
    rounds.push({ round: i + 1, node: NODES[r.node].host, status: r.status, processed: r.body?.processed, booked: r.body?.booked, released: r.body?.released, movedFromLine: r.body?.movedFromLine, ms: Math.round(r.ms) });
    emit({ type: 'progress', phase: 'P2', done: i + 1, total: P.rounds, rounds });
  }

  // ---------------- P3 checks
  emit({ type: 'phase', phase: 'P3', title: 'Checks' });
  const verify = await must(req(NODES.length - 1, 'GET', `/tickets/zones/${ZONE}/verify`), 'verify');
  const zone = await must(req(0, 'GET', `/tickets/zones/${ZONE}`), 'zone');
  const acc = [...accepted.values()];
  const exactlyOne = itsIds.filter((id) => accepted.get(id) === 1).length;
  const checks = [
    { label: `all ${P.total.toLocaleString('en-IN')} requests answered (no timeout / network error / 5xx)`, ok: done === P.total && !Object.keys(statuses).some((s) => Number(s) < 0 || Number(s) >= 500) },
    { label: `exactly ONE accepted request per ITS id (${exactlyOne.toLocaleString('en-IN')} / ${itsIds.length.toLocaleString('en-IN')})`, ok: exactlyOne === itsIds.length && acc.every((n) => n === 1) },
    { label: `every other request answered DUPLICATE (${(outcomes.DUP_REQUEST_ID + outcomes.DUP_ITS).toLocaleString('en-IN')})`, ok: outcomes.DUP_REQUEST_ID + outcomes.DUP_ITS === P.total - itsIds.length && outcomes.OTHER === 0 },
    { label: `server counted the same: ${afterP1.requests} requests (incl. 1 admin freeze), ${afterP1.duplicates} duplicates`, ok: afterP1.requests === itsIds.length + (adminRid ? 1 : 0) && afterP1.duplicates === P.total - itsIds.length },
    { label: `admin unfreeze under load served the waiting line (${unfrozen?.movedFromLine ?? 0} requests)`, ok: !adminRid || (unfrozen?.status === 200 && unfrozen.movedFromLine > 0) },
    ...(P.zoneFreezeAt ? [
      { label: `zone freeze under load: ${zf.answeredWhileFrozen.toLocaleString('en-IN')} bookings answered while frozen, ${zf.frozenWhileFrozen} got tickets (must be 0)`, ok: zf.freezeStatus === 200 && zf.answeredWhileFrozen > 0 && zf.frozenWhileFrozen === 0 },
      { label: `while frozen: ${zf.cancels} held requests cancelled → ${zf.cancelsMoved} waiting requests got their tickets (must be 0)`, ok: zf.cancels > 0 && zf.cancelsMoved === 0 },
      { label: `while frozen: paying a held request refused (HTTP ${zf.payStatus} ${zf.payCode})`, ok: zf.payStatus === 423 && zf.payCode === 'ZONE_FROZEN' },
      { label: `zone unfreeze: ${zf.unfreeze?.timersResumed ?? 0} held timers resumed, ${zf.unfreeze?.movedFromLine ?? 0} waiting requests got tickets in order`, ok: zf.unfreeze?.status === 200 && zf.unfreeze.movedFromLine > 0 },
    ] : []),
    { label: 'line moved in every round (freed tickets → waiting requests)', ok: rounds.every((r) => r.status === 200) && (zone.waiting === 0 || rounds.some((r) => r.movedFromLine > 0)) },
    ...verify.checks.map((c: any) => ({ label: `${c.name} (${c.detail})`, ok: c.ok })),
    { label: `every node took load (${NODES.map((u, i) => `${u.port}: ${perNode[i]}`).join(', ')})`, ok: perNode.every((n) => n > 0) },
  ];
  checks.forEach((c) => emit({ type: 'check', phase: 'P3', label: c.label, ok: c.ok }));
  const pass = checks.every((c) => c.ok);
  emit({ type: 'end', pass, totalSeconds: +((performance.now() - t0) / 1000).toFixed(1), zone: { total: zone.total, available: zone.available, frozen: zone.frozen, booked: zone.booked, waiting: zone.waiting, requests: zone.requests, duplicates: zone.duplicates }, p1: p1metrics, rounds });
  log(pass ? 'PASSED' : 'FAILED');
  process.exit(pass ? 0 : 1);
}

main().catch((e) => { emit({ type: 'end', pass: false, error: (e as Error).message }); process.exit(2); });
