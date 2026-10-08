/**
 * Terminal log page on its own port (default 3300): one row per request, latest 100+ records, live.
 * It only reads: GET /tickets/terminal, /tickets/zones and the terminal downloads are passed through to
 * an API node (the next one is tried when a node is down).
 *
 *   npm run tickets:terminal                                   http://localhost:3300
 *   TERM_PORT=3400 API_URL=http://localhost:3000,http://localhost:3100 npm run tickets:terminal
 */
import * as http from 'http';

const PORT = Number(process.env.TERM_PORT ?? 3300);
const APIS = (process.env.API_URL ?? 'http://localhost:3000,http://localhost:3100,http://localhost:3101,http://localhost:3102').split(',').map((u) => u.trim()).filter(Boolean);
const ALLOWED = [/^\/tickets\/terminal(\?|$)/, /^\/tickets\/zones(\?|$)/, /^\/tickets\/config(\?|$)/, /^\/tickets\/download\/terminal(\?|$)/];
let good = 0; // index of the API node that answered last

async function pass(path: string, res: http.ServerResponse) {
  for (let i = 0; i < APIS.length; i++) {
    const k = (good + i) % APIS.length;
    try {
      const r = await fetch(APIS[k] + path, { signal: AbortSignal.timeout(path.includes('download') ? 120000 : 8000) });
      good = k;
      const head: Record<string, string> = { 'Content-Type': r.headers.get('content-type') ?? 'application/json', 'X-Api-Node': APIS[k], 'Cache-Control': 'no-store' };
      const cd = r.headers.get('content-disposition');
      if (cd) head['Content-Disposition'] = cd;
      res.writeHead(r.status, head);
      res.end(Buffer.from(await r.arrayBuffer()));
      return;
    } catch { /* node down or slow: try the next one */ }
  }
  res.writeHead(502, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ code: 'NO_API_NODE', message: `no API node answered (${APIS.join(', ')})` }));
}

http.createServer((req, res) => {
  const url = req.url ?? '/';
  if (req.method === 'GET' && (url === '/' || url.startsWith('/?'))) {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
    res.end(PAGE);
    return;
  }
  if (req.method === 'GET' && ALLOWED.some((r) => r.test(url))) { void pass(url, res); return; }
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('not found');
}).listen(PORT, () => console.log(`Terminal log: http://localhost:${PORT}  (reads from ${APIS.join(', ')})`));

const PAGE = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Registration Terminal</title>
<style>
:root {
  --bg: #0f1419; --panel: #161c23; --border: #29313b; --text: #d6dde6; --muted: #7f8b99; --accent: #61afef;
  --mono: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
}
:root[data-theme="light"] { --bg: #f5f6f8; --panel: #ffffff; --border: #dde1e6; --text: #1b1f24; --muted: #5d6673; --accent: #2457d6; }
@media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) { --bg: #0f1419; --panel: #161c23; --border: #29313b; --text: #d6dde6; --muted: #7f8b99; --accent: #61afef; } }
:root[data-theme="dark"] { --bg: #0f1419; --panel: #161c23; --border: #29313b; --text: #d6dde6; --muted: #7f8b99; --accent: #61afef; }
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--text); font: 14px/1.45 system-ui, -apple-system, "Segoe UI", Roboto, sans-serif; height: 100vh; display: flex; flex-direction: column; }
header { display: flex; flex-wrap: wrap; gap: 10px 16px; align-items: flex-end; padding: 10px 16px; background: var(--panel); border-bottom: 1px solid var(--border); }
header h1 { font-size: 16px; margin: 0 12px 4px 0; }
label.f { display: flex; flex-direction: column; gap: 3px; font-size: 12px; color: var(--muted); }
select, button { font: inherit; color: var(--text); background: var(--bg); border: 1px solid var(--border); border-radius: 7px; padding: 5px 9px; }
button { cursor: pointer; font-weight: 600; color: var(--accent); }
.chk { font-size: 13px; color: var(--muted); margin-bottom: 5px; }
.right { margin-left: auto; font-size: 12.5px; color: var(--muted); margin-bottom: 5px; text-align: right; }
.nav { font-size: 13px; margin-bottom: 6px; } .nav a { color: var(--accent); text-decoration: none; font-weight: 600; }
.dl { display: flex; gap: 6px; flex-wrap: wrap; }
#sum { padding: 6px 16px; font: 12.5px/1.5 var(--mono); color: var(--muted); border-bottom: 1px solid var(--border); white-space: pre-wrap; }
.term { flex: 1; overflow: auto; background: #0f1419; color: #d6dde6; padding: 10px 16px; font: 12.5px/1.6 var(--mono); white-space: pre; }
.term .h { color: #7f8b99; font-weight: 700; } .term .t, .term .cnt { color: #7f8b99; } .term .id { color: #5f6b78; } .term .z { color: #9fb3c8; }
.term .its { color: #ffffff; font-weight: 700; } .term .k { color: #56b6c2; } .term .REQ { color: #61afef; }
.term .RES { color: #98c379; } .term .RESBAD { color: #e06c75; } .term .DUP { color: #e06c75; font-weight: 700; } .term .DUPHEAD { color: #e06c75; }
.term .AV { color: #e5c07b; font-weight: 700; } .term .ZN { color: #56b6c2; font-weight: 700; }
.term .row.new { animation: flash 1.6s ease-out; }
@keyframes flash { from { background: rgba(229, 192, 123, .28); } to { background: transparent; } }
@media (max-width: 700px) { header { padding: 10px 16px; } .right { margin-left: 0; text-align: left; } }
</style>
</head>
<body>
<header>
  <h1>Terminal log</h1>
  <nav class="nav"><a id="lnReg" href="http://localhost:3000/registration.html">Registration</a> · <a id="lnMumin" href="http://localhost:3000/">Mumin console</a></nav>
  <label class="f">Zone<select id="zone"><option value="">All zones</option></select></label>
  <label class="f">Lines<select id="lines"><option>100</option><option>200</option><option>500</option><option>1000</option></select></label>
  <label class="chk"><input type="checkbox" id="pause"> pause</label>
  <div class="dl">
    <button data-dl="csv">⬇ Shown (CSV)</button><button data-dl="txt">⬇ Shown (TXT)</button><button data-dl="full">⬇ Full log (CSV)</button>
  </div>
  <div class="right" id="st">connecting…</div>
</header>
<div id="sum"></div>
<div class="term" id="term">Loading…</div>
<script>
const $ = (q) => document.querySelector(q);
const esc = (s) => String(s ?? '').replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const fmtN = (n) => Number(n || 0).toLocaleString('en-IN');
const p2 = (n, w = 2) => String(n).padStart(w, '0');
const tm = (ms) => { const d = new Date(+ms); return p2(d.getHours()) + ':' + p2(d.getMinutes()) + ':' + p2(d.getSeconds()) + '.' + p2(d.getMilliseconds(), 3); };
const P = (s, n) => { const t = String(s ?? ''); return esc(t.length > n ? t.slice(0, n - 1) + '…' : t.padEnd(n)); };
try { const q = new URLSearchParams(location.search); if (q.get('zone')) $('#zone').dataset.want = q.get('zone'); } catch {}

const avail = (x) => {
  const b = x.availableBefore, a = x.availableAfter;
  if (a === undefined || a === null) return '';
  const bk = x.bookedAfter === undefined || x.bookedAfter === null || x.bookedAfter === '' ? '' : ' · booked ' + x.bookedAfter;
  if (b === undefined || b === null) return 'available ' + a + bk;
  return (String(b) === String(a) ? 'available ' + a + ' (no change)' : 'available ' + b + ' → ' + a) + bk;
};
const shortPath = (x) => (x.zone ? x.path.replace('/tickets/zones/' + x.zone, '') : x.path.replace('/tickets', '')) || '/';
const zoneAction = (x) => /zone-(un)?freeze|PATCH/.test(x.method + ' ' + x.path) && !x.its;
const keyOf = (x) => x.ts + '|' + x.instance + '|' + x.rid + '|' + x.path;
const lineOf = (x, isNew) => {
  const ok = x.httpStatus < 400, main = (x.redis || []).find((e) => e.status !== 'NO_TICKETS_LEFT') || (x.redis || [])[0];
  return '<span class="row' + (isNew ? ' new' : '') + '"><span class="t">' + P(tm(x.ts), 12) + '</span> <span class="id">' + P(x.instance, 15) + '</span> <span class="z">' + P(x.zone || '—', 20) + '</span> '
    + '<span class="its">' + P(x.its || '—', 9) + '</span> ' + (x.its ? '<span class="' + (x.itsDuplicates > 0 ? 'DUP' : 'cnt') + '">' + P(x.itsDuplicates ?? 0, 5) + '</span>' : P('', 5)) + ' ' + P(x.rid || '—', 22) + ' '
    + '<span class="REQ">' + esc(x.method) + ' ' + esc(shortPath(x)) + '</span>' + (x.request ? ' <span class="cnt">' + esc(x.request) + '</span>' : '') + ' '
    + '<span class="cnt">→</span> <span class="' + (ok ? (zoneAction(x) ? 'ZN' : 'RES') : 'RESBAD') + '">' + esc(x.httpStatus) + ' ' + esc(x.response) + '</span> <span class="cnt">(' + esc(x.ms) + ' ms)</span> '
    + (avail(x) ? '<span class="cnt">│</span> <span class="' + (String(x.availableBefore) === String(x.availableAfter) ? 'cnt' : 'AV') + '">' + esc(avail(x)) + '</span> ' : '')
    + (main ? '<span class="cnt">│</span> <span class="id">' + esc(main.redisId) + '</span> <span class="k">' + esc((x.keys || []).join(' ')) + '</span>' : '<span class="cnt">│ no Redis change</span>') + '</span>';
};

let seen = new Set(), first = true, busy = false;
async function loadZones() {
  try {
    const r = await fetch('/tickets/zones'); if (!r.ok) return;
    const d = await r.json(), zones = d.zones || d, s = $('#zone'), keep = s.value || s.dataset.want || '';
    const html = '<option value="">All zones</option>' + zones.map((z) => '<option value="' + esc(z.id) + '">' + esc(z.city) + ' · ' + esc(z.name) + ' (' + fmtN(z.available) + ' of ' + fmtN(z.total) + ' available, ' + fmtN(z.booked) + ' booked' + (z.zoneFrozen ? ', FROZEN' : '') + ')</option>').join('');
    if (s.dataset.sig !== html && document.activeElement !== s) { s.innerHTML = html; s.value = keep; s.dataset.sig = html; delete s.dataset.want; }
  } catch {}
}
async function load(force) {
  if (busy || ($('#pause').checked && !force)) return;
  busy = true;
  try {
    const z = $('#zone').value, n = $('#lines').value;
    const r = await fetch('/tickets/terminal?limit=' + n + (z ? '&zone=' + encodeURIComponent(z) : ''));
    const d = await r.json();
    if (!r.ok) { $('#st').textContent = 'API: ' + (d.message || r.status); return; }
    const recs = [...d.records].reverse();
    const head = '<span class="h">' + P('TIME', 12) + ' ' + P('NODE', 15) + ' ' + P('ZONE', 20) + ' ' + P('ITS ID', 9) + ' ' + P('DUPS', 5) + ' ' + P('REQUEST ID', 22) + ' REQUEST  →  RESPONSE  │ AVAILABLE BEFORE → AFTER · BOOKED │ REDIS ID · KEY</span>';
    const el = $('#term'), atBottom = el.scrollTop + el.clientHeight >= el.scrollHeight - 30;
    const now = new Set(recs.map(keyOf));
    el.innerHTML = head + '\\n' + (recs.map((x) => lineOf(x, !first && !seen.has(keyOf(x)))).join('\\n') || '<span class="h">(no records yet)</span>');
    seen = now; first = false;
    if (atBottom || force) el.scrollTop = el.scrollHeight;
    const dd = d.duplicates;
    $('#sum').innerHTML = dd && dd.total ? '<span style="color:#e06c75">Duplicate requests blocked: ' + fmtN(dd.total) + ' from ' + fmtN(dd.itsIds) + ' ITS id(s)</span> · most: ' + dd.top.map((t) => esc(t.its) + ' ×' + fmtN(t.count) + (z ? '' : ' (' + esc(t.zone) + ')')).join(' · ') : 'Duplicate requests blocked: 0';
    $('#st').textContent = 'latest ' + recs.length + ' records · live · API ' + (r.headers.get('X-Api-Node') || '') + ' · ' + tm(Date.now());
  } catch (e) { $('#st').textContent = 'no API node answered, retrying…'; }
  finally { busy = false; }
}
function save(url) { const a = document.createElement('a'); a.href = url; a.download = ''; document.body.append(a); a.click(); a.remove(); }
document.addEventListener('click', (e) => {
  const b = e.target.closest('[data-dl]'); if (!b) return;
  const z = $('#zone').value, q = z ? 'zone=' + encodeURIComponent(z) + '&' : '';
  if (b.dataset.dl === 'full') save('/tickets/download/terminal?' + q + 'limit=100000&format=csv');
  else save('/tickets/download/terminal?' + q + 'limit=' + $('#lines').value + '&format=' + b.dataset.dl);
});
$('#zone').onchange = () => { first = true; load(true); };
$('#lines').onchange = () => { first = true; load(true); };
const apiBase = location.protocol + '//' + location.hostname + ':3000';
$('#lnReg').href = apiBase + '/registration.html'; $('#lnMumin').href = apiBase + '/';
loadZones().then(() => load(true));
setInterval(() => { if (!document.hidden) load(); }, 1000);
setInterval(() => { if (!document.hidden) loadZones(); }, 3000);
</script>
</body>
</html>`;
