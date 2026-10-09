# Miqat Registration System (RMS POC)

Registration by **city and zone** on top of **Redis**, with the Mumin master data (`MMS.dbo.Mumin_Mast`, ~2,64,615 ITS records) served from Redis and written back to MSSQL in the background.

- Correct under heavy concurrency: the same ITS id can hit any number of API nodes at the same moment and still gets **at most one seat**.
- First come, first served (request order), no duplicates, city / zone freeze, live seat changes.
- Every request is visible in a live terminal log (ITS id, request id, Redis id, key, available before → after, booked).

---

## 1. Pages

| Page | URL | What it is for |
|---|---|---|
| **Registration** | http://localhost:3000/registration.html | One page: business config (cities + zones), tech config (nodes), register, load test |
| **Terminal log** | http://localhost:3300 | Live log, latest 100+ requests, one row per request, CSV/TXT download |
| **Mumin console** | http://localhost:3000/ | Mumin_Mast records in Redis (get / insert / update / delete, write-behind status) |

---

## 2. Registration rules

Every decision is **one atomic Lua script** in Redis. Redis runs one script at a time, so requests from all nodes are decided one after another — no locks, no race conditions.

| Situation | Answer |
|---|---|
| Seat free | **200 CONFIRMED** — registered at once, in request order |
| All seats confirmed | **409 NO SEATS AVAILABLE** — not registered, no waiting line |
| Zone (or city) frozen | **423 FROZEN** — not registered, nothing stored; the unfreeze registers nobody |
| Same request id sent again | **409 DUPLICATE** `REQUEST_ID_ALREADY_USED` (idempotency) |
| Same ITS id, new request | **409 DUPLICATE** `ITS_HAS_OPEN_REQUEST` (one registration per ITS id per zone) |
| ITS id not in Mumin_Mast | **404** `ITS_NOT_FOUND` |
| ITS id cancelled | **423** `ITS_CANCELLED` |

- **FIFO** = the order requests reach Redis; every request gets an order number (`INCR tb:seq`).
- **Idempotency** = a request id is processed once (header `Idempotency-Key`).
- **Cancel** a confirmed registration → the seat is available again.

## 3. City and zone controls

| Control | Level | Notes |
|---|---|---|
| Freeze / unfreeze | **City** | All zones of the city in one Redis transaction (MULTI/EXEC) |
| Freeze / unfreeze | **Zone** | New requests answered Frozen until unfrozen |
| Seats increase / reduce | **Zone** | Live; cannot go below the confirmed count |
| City status | **City** | Open / Frozen / Partly frozen, totals added up from its zones |

A city is the group of zones with the same city name (`cityId("Navi Mumbai") = "navi-mumbai"`).
A freeze log row names the **last request booked before the freeze** (ITS id, request id, order #, time).

---

## 4. Architecture

```
 Browser / clients
        │  HTTP
 ┌──────┴──────┬─────────────┬─────────────┐
 node 3000     node 3100     node 3101     node 3102      NestJS API nodes (stateless, any number)
 └──────┬──────┴─────────────┴─────────────┘
        │  one Lua script per decision
     Redis 7.4  (AOF on)  ── all state: zones, registrations, event log, Mumin records
        │  write-behind stream (leader node only)
     MSSQL  (Mumin_Mast)
```

- **API**: NestJS 10 (Node.js), ioredis.
- **Redis**: 7.4 (Docker, host port 6395), AOF persistence.
- **MSSQL**: 2022 (Docker, host port 14330).
- **Nodes** register themselves in Redis (`tb:nodes`, heartbeat every 2 s) and can be started / stopped from the Registration page.

## 5. Redis patterns

| Pattern | Where | Why |
|---|---|---|
| Atomic server-side scripting (Lua) | Every register / cancel / freeze / seat change | Check and update in one step |
| Single-writer serialization | Redis runs one script at a time | Concurrency without distributed locks |
| Idempotency key | `tb:reqs` (request id → request) | Same request processed once |
| Uniqueness guard | `tb:active:<zone>` (ITS id → open registration) | One registration per ITS id |
| Atomic counters | `tb:zone:<id>` HASH: total / available / booked | available + booked = total, always |
| Global sequence | `tb:seq`, `tb:zreqs:<zone>` (ZSET) | Provable request order |
| Event log (Streams) | `tb:ev:<zone>`, `tb:http`, `tb:oplog` | Terminal log, reports, audit |
| MULTI/EXEC transaction | City freeze / unfreeze | All zones of a city together |
| Write-behind cache | `mumin:writes` stream → MSSQL, `mumin:writes:dlq` | Fast Redis-first writes, MSSQL in order |
| Leader election (lease) | `mumin:writer:leader` | One MSSQL writer; failover ~15 s |
| Consumer group (at-least-once) | XREADGROUP / XAUTOCLAIM | No write lost if a node dies |
| Snapshot + delta | Snapshot file + MSSQL change tracking | Rebuild Redis in ~12–19 s |
| Heartbeat registry + Pub/Sub | `tb:nodes`, `tb:nodes:cmd` | Live node list, start/stop nodes |

### Main Redis keys

| Key | Type | Content |
|---|---|---|
| `tb:zones` | SET | zone ids |
| `tb:zone:<id>` | HASH | city, name, total, available, booked, paused (frozen), last booked, counters |
| `tb:reqs` | HASH | request id → registration (JSON) |
| `tb:active:<zone>` | HASH | ITS id → open registration |
| `tb:zreqs:<zone>` | ZSET | order # → request id |
| `tb:seq` | STRING | global request counter |
| `tb:ev:<zone>` | STREAM | every decision (type, ITS, request, available, booked) |
| `tb:http` | STREAM | every write request + response (terminal log) |
| `tb:dups:<zone>` | HASH | ITS id → duplicates blocked |
| `tb:pauses:<zone>` | LIST | freeze history |
| `tb:config` | HASH | business + tech configuration |
| `tb:nodes` | HASH | live API nodes |
| `mumin:rec:<ITS>` | STRING | Mumin_Mast record (JSON) |
| `mumin:status` | HASH | ITS id → status (e.g. CANCELLED) |
| `mumin:meta` | HASH | last load: source, rows, time taken |

---

## 6. Run it

**Needs:** Node.js 20+, Docker.

```bash
cp .env.example .env            # fill in the values
docker compose up -d mssql redis
npm install
npm run build

PORT=3000 INSTANCE_NAME=node1 npm start   # API + pages on :3000
npm run tickets:terminal                  # terminal log on :3300
```

More nodes: Registration page → Tech configuration → **+ Add nodes** (ports 3100, 3101, …).

Mumin data into Redis: loaded at startup (from snapshot if present, else from MSSQL).

### Scripts

| Command | What it does |
|---|---|
| `npm start` | API node (`PORT`, `INSTANCE_NAME`) |
| `npm run tickets:terminal` | Terminal log page (`TERM_PORT`, default 3300; `API_URL` = comma-separated nodes) |
| `npm run tickets:log` | Terminal log in the console (`--zone=<id>`, `--lines=100`, `--once`) |
| `npm run bench:db-to-redis` | Time a full MSSQL → Redis load |
| `npm run bench:snapshot-to-redis` | Time snapshot → Redis restore |
| `npm run bench:writes` | Time Redis-first writes vs DB-first |

---

## 7. API (registration)

Base: `http://localhost:3000/tickets`

| Method | Path | Body | |
|---|---|---|---|
| GET | `/zones` | | All zones |
| POST | `/zones` | `{ city, name, total }` | Create a zone |
| PATCH | `/zones/:id` | `{ total }` | Increase / reduce seats |
| POST | `/zones/:id/book` | `{ muminId, qty: 1 }` + header `Idempotency-Key` | Register |
| POST | `/zones/:id/requests/:rid/release` | `{ reason }` | Cancel |
| POST | `/zones/:id/zone-freeze` | `{ reason }` | Freeze a zone |
| POST | `/zones/:id/zone-unfreeze` | | Unfreeze a zone |
| GET | `/cities` | | Cities with totals and status |
| POST | `/cities/:city/freeze` | `{ reason }` | Freeze every zone of a city |
| POST | `/cities/:city/unfreeze` | | Unfreeze every zone of a city |
| GET | `/zones/:id/verify` | | Redis consistency checks |
| GET | `/terminal?zone=&limit=` | | Terminal log (≥ 100 rows) |
| GET | `/download/terminal?zone=&limit=&format=csv\|txt` | | Terminal log download |
| GET | `/download/zones/:id/report` | | Zone registrations (CSV) |
| GET | `/download/summary` | | All zones (CSV) |
| GET / PATCH | `/config` | | Business + tech configuration |
| GET | `/nodes` · POST `/nodes/start` · POST `/nodes/:instance/stop` | | Nodes |
| POST | `/loadtest/start` · GET `/loadtest/status` · POST `/loadtest/cancel` | | Load test |

Mumin API: `GET/POST/PATCH/DELETE /mumin/:id`, batch routes under `/mumin/batch/*`, cache control under `/cache/*`.

---

## 8. Test results

| Test | Result |
|---|---|
| End-to-end (pages, config, zones, rules, cancel, zone + city freeze, concurrency, terminal, downloads) | **54 / 54 passed** |
| 1,00,000 requests on **1 seat**, 4 nodes | **exactly 1 confirmed**, 0 errors (16/16 checks) |
| 1,00,000 requests on **5,000 seats**, zone frozen in the middle | nobody registered while frozen, 0 waiting, 0 errors (19/19 checks) |
| 200 ITS ids × 4 nodes at the same moment | exactly 1 seat each, 600 duplicates blocked |
| Mumin_Mast MSSQL → Redis full load | ~53 s (2,64,614 rows, ~5,000 rows/s, 476 MB) |
| Snapshot → Redis restore | ~12–19 s |
| Throughput (4 nodes, one laptop) | ~1,000–2,300 requests/s |

## 9. Project structure

```
public/
  index.html            Mumin console
  registration.html     Registration (one page)
src/
  tickets/              Registration module
    tickets.lua.ts        Lua scripts (register, cancel, freeze, seats)
    tickets.service.ts    zones, cities, terminal, reports, verify
    tickets.controller.ts REST API + downloads
    tickets.http-log.ts   request/response log (tb:http)
    tickets.oplog.ts      Redis key log
    tickets.nodes.ts      node registry, start/stop
    tickets.loadtest.ts   load test runner
  cli/
    ticketterm.ts         terminal log page (port 3300)
    ticketlog.ts          terminal log in the console
    ticketload.ts         load generator (1 lakh requests)
  mumin/                Mumin_Mast in Redis + write-behind to MSSQL
  snapshot/             snapshot / restore / reconcile
  redis/, database/     connections
docker-compose.yml      MSSQL + Redis + app
```

## 10. Not done yet

- Single Redis instance (no Redis Cluster / replica).
- One registration per ITS id is **per zone**, not per city or per programme.
- Seats are set per zone; a city has no seat limit of its own.
- Tested on a laptop, not production hardware.
