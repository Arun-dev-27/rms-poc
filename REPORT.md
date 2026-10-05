# RMS: `dbo.Mumin_Mast` on Redis (NestJS + MSSQL + Redis, Docker)

**Scope:** only `MMS` → schema `dbo` → table `Mumin_Mast`.
**Key:** `Mumin_ID` (= ITS id). **Columns:** 79. **Rows:** 264,615.
**Report date:** 2026-10-05. **Commit:** `73638a0`. **Environment:** Windows 11 + Docker Desktop.

---

## 0. Requirement checklist

| # | Requirement | Status | Where |
|---|---|---|---|
| 1 | `git init` | ✅ Done (branch `main`) | repo root |
| 2 | Implement in a NestJS project | ✅ Done | `src/` |
| 3 | Put only `dbo.Mumin_Mast` in Docker | ✅ Done: MSSQL 2022 + Redis 7 containers | `docker-compose.yml`, `docker/` |
| 4 | Store ~2 lakh+ rows in Redis, with timing | ✅ 264,615 rows, **35–53 s** | §4 |
| 5 | Redis read/update best practice, single and batch | ✅ GET/MGET, Lua update, batched | §5, §6 |
| 6 | Insert / update **Redis-based** (Redis first) | ✅ Write-behind to MSSQL | §6 |
| 7 | How a snapshot is taken | ✅ Cron every 15 min + API | §7 |
| 8 | Insert into Redis from a snapshot, with timing | ✅ **11–19 s** | §8 |
| 9 | Redis crashed / data deleted → restore from snapshot | ✅ Tested end-to-end | §9 |
| 10 | Update a particular record | ✅ `PATCH /mumin/:id` | §6.2 |
| 11 | Update records in batch | ✅ `PATCH /mumin/batch` | §6.3 |
| 12 | One-time script for the timings | ✅ `npm run bench` | §10 |

---

## 1. Git

```bash
git init -b main          # done
git log --oneline         # 73638a0 Mumin_Mast on Redis: NestJS + MSSQL mirror + snapshots + write-behind
```

The stage password lives only in `.env`, which is git-ignored. It was checked and is not in any committed file.

---

## 2. NestJS project structure

```
src/
  main.ts                       HTTP bootstrap (port 3000)
  app.module.ts
  database/mssql.service.ts     2 pools: docker MSSQL (read/write) + stage (read-only, import only)
  redis/redis.service.ts        ioredis client
  mumin/
    mumin.columns.ts            79 columns, AUTO-GENERATED from stage INFORMATION_SCHEMA
    mumin.validation.ts         request validation driven by the column metadata
    mumin.repository.ts         MSSQL: read, batch update/insert (OPENJSON), upsert, change tracking, bulk copy
    mumin.cache.ts              Redis: GET / MGET / MSET / SCAN
    mumin.redis-writer.ts       Redis-first insert/update (Lua script)
    write-behind.worker.ts      Redis Stream -> MSSQL background writer
    mumin.service.ts            business logic (redis-first / db-first)
    mumin.controller.ts         REST API
  snapshot/
    snapshot.service.ts         import, full load, snapshot, restore, reconcile, startup recovery
    snapshot.controller.ts      /cache/* admin API
  cli/bench.ts                  ONE-TIME TIMING SCRIPT
scripts/gen-schema.js           regenerates columns + DDL from stage
docker/mssql/init.sql           table DDL (generated) + change tracking
docker/redis/redis.conf         AOF + RDB persistence
```

---

## 3. Docker: only `dbo.Mumin_Mast`

The stage DB (`MMS`, user `siam_read`) is **read-only**, so writes cannot go there. The table is copied into a Docker MSSQL, and all reads, writes and snapshots run against that copy.

```
stage MMS.dbo.Mumin_Mast (read-only)
        │  import: keyset pages of 10,000 rows (WHERE Mumin_ID > @last ORDER BY Mumin_ID), retry per page
        ▼
docker MSSQL  rms-mssql  :14330   DB MMS, dbo.Mumin_Mast (same 79 columns, PK Mumin_ID, Change Tracking ON)
        ▲  write-behind worker (batched upsert)
        │
docker Redis  rms-redis  :6395    mumin:rec:{Mumin_ID} = full row JSON   (AOF everysec + RDB)
        ▲
NestJS API  :3000
```

| Container | Image | Port | Volume |
|---|---|---|---|
| `rms-mssql` | mcr.microsoft.com/mssql/server:2022-latest | 14330 | `mssql-data` |
| `rms-mssql-init` | same; runs `init.sql` once | — | — |
| `rms-redis` | redis:7-alpine | 6395 | `redis-data` |
| `rms-app` (optional, `--profile app`) | built from `Dockerfile` | 3000 | `./snapshots` |

**Schema facts read from stage:**
- `Mumin_ID` has a unique clustered index on stage. It is the PK in the Docker copy.
- There is no `Updated_At` column and no change tracking on stage. That is why **SQL Server Change Tracking** is enabled in the Docker copy: it tracks inserts, updates and deletes without changing the table's schema.

**Import timing (stage → Docker MSSQL):** 264,615 rows in **150–167 s**, limited by the network to stage.
A single long query to stage is cut off at about 14 s / ~30k rows every time, so the import uses short paged queries instead.

---

## 4. Storing all data in Redis, with timing

### How
- **Layout:** 1 Redis STRING per record, key `mumin:rec:{Mumin_ID}`, value = JSON of all 79 columns.
  A row is always read and written as a whole, so a JSON string is smaller and faster than a HASH.
- **Load:** stream rows from MSSQL, write to Redis with `MSET` in batches of 5,000 (one command per batch).
  Reading from MSSQL and writing to Redis overlap: up to 2 batches are in flight at a time.
- The change-tracking version is captured **before** the load starts. Any change made during the load is picked up by the next reconcile.
- Memory: **~476 MB** for 264,615 rows. Redis `maxmemory 2gb`, `noeviction` (rows are never silently dropped).

### Timing (measured)

| Step | Time |
|---|---|
| MSSQL → Redis full load, 264,615 rows | **35–53 s** (5,000–7,600 rows/s), ranges over runs |
| Of which: MSSQL driver read (79 cols × 264k) | ~24 s ← the bottleneck |
| Of which: Redis `MSET` | ~9 s |
| Tried: SQL Server builds the JSON (`FOR JSON PATH`) | 54 s, slower, rejected |

### Verified now
| | Rows |
|---|---|
| Stage | 264,615 |
| Docker MSSQL | 264,615 |
| Redis records | 264,615 |
| 2,000 random rows compared field by field (Redis vs MSSQL) | **0 missing, 0 mismatched** |

---

## 5. Reading from Redis: best practice

| Case | How | Code |
|---|---|---|
| **Single read** | `GET mumin:rec:{id}`. On a miss, read MSSQL and write it back to Redis (cache-aside) | `MuminService.getOne` |
| **Batch read** | `MGET`, 5,000 keys per command; all misses fetched with **one** `OPENJSON` query, then cached | `MuminService.getMany` |
| **Full scan** (snapshots) | `SCAN` + `MGET`, **never `KEYS`** (KEYS blocks Redis) | `MuminCache.scanAll` |

```http
GET  /mumin/20178652
POST /mumin/batch/get        {"ids":[20178652, 20240152, ...]}        (max 10,000)
```
Response includes `source: "redis" | "mssql"` and `ms`.
Measured: batch read of 1,000 records ≈ **200 ms** including HTTP.

---

## 6. Insert and update: Redis-first

### 6.1 How it works (write-behind)

```
API ──► Lua script in Redis (ONE atomic step per 1,000 items):
          GET row → merge patch → SET row → XADD to stream "mumin:writes" → HSET "mumin:dirty" id
     ◄── response immediately (MSSQL not touched)

WriteBehindWorker (background):
          XREADGROUP 2,000 entries → keep latest per Mumin_ID → ONE MSSQL transaction (UPDATE + INSERT)
          → XACK + XDEL → clear dirty flag (only if no newer write arrived meanwhile)
```

| Best practice | Implemented |
|---|---|
| Atomic in Redis | Lua script; nothing can interleave between read-merge-write-queue |
| Few round trips | 1 Lua call per 1,000 items; MSSQL gets 1 statement per 1,000 rows |
| Idempotent persistence | Stream carries the **full row** → upsert can safely run twice |
| No lost writes | Consumer group; ACK only **after** MSSQL commit |
| Worker / app crash | Un-ACKed entries re-read; entries from dead processes taken over with `XAUTOCLAIM` (idle > 30 s) |
| MSSQL down | Writes still accepted in Redis; batch retried until MSSQL is back |
| Row MSSQL rejects | Moved to `mumin:writes:dlq` with the SQL error; Redis key dropped so the next read shows MSSQL's value |
| Insert never overwrites | Before inserting, ids missing in Redis are checked in MSSQL → `409 Conflict` |
| Reconcile never overwrites newer data | Rows with a pending Redis write (dirty) are skipped |
| Validation | Unknown column, wrong type, too long, NOT NULL → `400` (from the 79-column metadata) |

`WRITE_MODE=db-first` switches to write-through (MSSQL first, then Redis) if ever needed.

### 6.2 Particular record update / insert
```http
PATCH /mumin/20178652          {"Mobile_No":"9999999999","Jamaat":"Chennai"}     only these columns change
POST  /mumin                   {"Mumin_ID":..., "Person_ID":..., ...}            Person_ID is NOT NULL
```

### 6.3 Batch update / insert
```http
PATCH /mumin/batch   {"items":[{"Mumin_ID":1,"Post":"X"},{"Mumin_ID":2,"Age":40,"Misaq":true}]}
POST  /mumin/batch   {"items":[{...},{...}]}                                     max 10,000 per request
```
Each item can update **different columns**. A missing field is left unchanged; `null` sets the column to NULL.
The response shows `updated`, `notFound`, `conflicts` and `ms`.

### 6.4 Timing (measured, `npm run bench:writes`)

| Operation | **Redis-first** (API response) | Data in MSSQL after | DB-first (for comparison) |
|---|---|---|---|
| Single update | **avg 1.2–2.0 ms**, p95 1.8–3.5 ms | 0.15–0.24 s (200 updates) | avg 5.5–12 ms |
| Batch update 1,000 | **0.12–0.20 s** | 0.46–0.84 s | 0.49–0.72 s |
| Batch update 10,000 | **0.95–2.1 s** | 3.7–6.2 s | 4.6–6.2 s |
| Single insert | avg 4.8–10.9 ms | 0.11–0.13 s (100 inserts) | — |
| Batch insert 5,000 | **0.85–1.6 s** | 1.7–2.4 s | 2.1–2.7 s |

Checked in MSSQL after each run: 10,000 updated and 5,100 inserted, as expected. The queue was empty afterwards and the DLQ was 0.
The script reverts its updates and deletes its test rows at the end.

---

## 7. How a snapshot is taken

| | |
|---|---|
| When | Cron `SNAPSHOT_CRON="*/15 * * * *"` (every 15 min) **and** `POST /cache/snapshot` |
| What | Every Redis record → `snapshots/mumin-<timestamp>.ndjson.gz` (gzip, 1 JSON row per line) |
| Header line | `{"meta":{"table","takenAt","ctVersion"}}`, where `ctVersion` is the MSSQL change-tracking version captured **before** scanning |
| Last line | `{"end":true,"count":N}`, a completeness marker. A truncated or corrupt file is **rejected** on restore |
| Safety | Written to `.tmp`, then renamed (atomic); also triggers Redis `BGSAVE` |
| Retention | Last 5 kept (`SNAPSHOT_KEEP`) |
| Size | **44 MB** for 264,615 rows |
| **Time** | **8–14 s** |

---

## 8. Insert into Redis from a snapshot (restore), with timing

```
1. wipe mumin:rec:* keys
2. read snapshot line by line → MSET batches of 5,000
3. verify row count == end marker (else reject → fall back to MSSQL)
4. if snapshot ctVersion < MSSQL min valid version (older than 7-day CT retention) → full MSSQL reload
5. CHANGETABLE(CHANGES dbo.Mumin_Mast, ctVersion): replay every insert / update / DELETE made after the snapshot
```

| Step | Time |
|---|---|
| **Snapshot → Redis, 264,615 rows** | **11–19 s** (≈ 2–3× faster than the MSSQL load) |
| Change-tracking delta after it | under 1 s (tested: 1 update + 1 delete in 0.87 s) |

`POST /cache/restore` (latest) or `{"file":"mumin-....ndjson.gz"}`.

---

## 9. Crash / data-deleted scenarios (all tested)

**Startup decision:** the app does this by itself on every start (`SnapshotService.ensureWarm`).
```
MSSQL table empty?                → import from stage → load Redis
Redis has data (came back by AOF)?→ reconcile changes since its CT version
Redis empty?                      → latest snapshot + CT delta
   snapshot missing/corrupt/old?  → full MSSQL → Redis load
```

| # | Scenario | Result | Time |
|---|---|---|---|
| A | Snapshot taken → row updated **and** row deleted directly in MSSQL → Redis `FLUSHALL` → app restart | Restored from snapshot; delta applied the 1 update and 1 delete; Redis = MSSQL | 11.2 s + 0.87 s |
| B | `docker kill` Redis (SIGKILL, no graceful save) → start | Redis reloaded itself from AOF; last write present | **1.9 s** |
| C | MSSQL paused → write via API → app killed → MSSQL resumed → app restart | Write accepted in 4 ms; reads still served from Redis; on restart worker recovered both orphaned writes into MSSQL | — |
| D | MSSQL paused 25 s with a write queued | Retried, persisted after resume, nothing dead-lettered | — |
| E | Row SQL Server rejects (forced conversion error) | Dead-lettered with the real SQL error; queue kept moving; Redis key reloaded from MSSQL | — |

**Redis persistence settings (`docker/redis/redis.conf`):**
- `appendonly yes`, `appendfsync everysec`: at most about 1 s of writes at risk on a crash
- `save 900 1` / `300 100` / `60 10000` (RDB)
- `maxmemory 2gb`, `noeviction`

**Recommended timings:**

| Setting | Value | Why |
|---|---|---|
| Snapshot interval | 15 min | Restore needs a snapshot plus at most 15 min of delta |
| Change-tracking retention | 7 days | Must be longer than the oldest snapshot you would restore |
| Reconcile | every 1 min | Catches writes made directly in MSSQL (outside the API) |
| Snapshots kept | 5 | ~220 MB disk |

---

## 10. One-time timing script

The script runs without the HTTP server (Nest application context). **Stop the API first** so its background jobs don't affect the numbers.

```bash
npm run build
npm run bench:db-to-redis        # wipe Redis, MSSQL -> Redis full load
npm run bench:snapshot-to-redis  # take snapshot, wipe Redis, snapshot -> Redis (+ delta)
npm run bench:writes             # Redis-first single/batch insert/update + time until in MSSQL, vs DB-first; then reverts
npm run bench                    # all of the above
```
Results are also written to `bench-results-<mode>.json`.

Sample output:
```
db->redis rows                                   264615
db->redis ms                                     34732
snapshot -> redis ms (load only)                 12349
single update x200 (redis)                       {"n":200,"avgMs":1.17,"p50Ms":0.75,"p95Ms":1.83}
batch update 10000 (redis) ms                    952.19
  ...time until all 10000 in MSSQL ms            3655.43
```

Numbers vary by about ±40% between runs on this machine, because Docker Desktop shares the PC with other containers. That is why the tables give ranges.

---

## 11. API reference

| Method | Path | Purpose |
|---|---|---|
| GET | `/mumin/:id` | single read |
| POST | `/mumin/batch/get` | batch read `{ids}` |
| POST | `/mumin` | insert |
| POST | `/mumin/batch` | batch insert `{items}` |
| PATCH | `/mumin/:id` | partial update |
| PATCH | `/mumin/batch` | batch partial update `{items}` |
| GET | `/cache/status` | Redis keys/memory, MSSQL rows, CT version, write queue/dirty/DLQ, persistence |
| POST | `/cache/import-source` | stage → Docker MSSQL → Redis |
| POST | `/cache/load` | MSSQL → Redis (`{"fresh":true}` wipes first) |
| POST | `/cache/snapshot` | take snapshot |
| GET | `/cache/snapshots` | list snapshots |
| POST | `/cache/restore` | snapshot → Redis + delta |
| POST | `/cache/reconcile` | apply MSSQL changes now |

---

## 12. Bugs found during testing (all fixed and re-tested)

| Bug | Fix |
|---|---|
| A single long query to stage was cut off at ~14 s / 30k rows | Keyset paging, 10k rows per page, retry per page; a failed import truncates the table so it is retried cleanly |
| A partial import left half a table, so startup skipped the import | Truncate on failure |
| Snapshot restore lost 68 rows (an `await` between creating the reader and reading from it) | Read immediately; the end-marker check is what caught this |
| Writes from a crashed process were never persisted (pending under the dead consumer's name) | `XAUTOCLAIM` takeover |
| MSSQL outage dead-lettered writes instead of retrying | Only SQL Server rejections (error number) go to the DLQ; connection errors are retried |
| A failed rollback hid the real SQL error | Rollback errors are ignored; the original error is kept |
| `#` in the `.env` password was read as a comment | Value quoted |

---

## 13. Limits and recommendations

1. **MSSQL lags Redis by ~0.1–6 s** (write-behind). Anything reading MSSQL directly sees new writes after that delay. Use `WRITE_MODE=db-first` if a consumer needs MSSQL to be immediate.
2. Redis is now the write path: keep **AOF on** and use a stable volume. For production, add a Redis replica + Sentinel.
3. The Lua script touches many keys in one call, so it needs a single Redis server (or Sentinel). Redis Cluster would need hash tags.
4. Add an alert when `writeBehind.dlq > 0` (from `GET /cache/status`).
5. A full MSSQL → Redis load is CPU-bound in Node (~24 s of driver parsing). Use the snapshot (11–19 s) for recovery; use the full load only as a fallback.
6. If the stage table's columns change: run `npm run gen:schema`, rebuild, then `POST /cache/import-source`.

---

## 14. How to run

```bash
cp .env.example .env                         # fill SOURCE_MSSQL_* (stage read-only account)
docker compose up -d mssql redis mssql-init  # MSSQL + Redis in docker
npm ci && npm run build
npm start                                    # empty table -> import from stage -> load Redis -> jobs start
# optional: API inside docker too
docker compose --profile app up -d --build
```

Key `.env` settings: `REDIS_BATCH_SIZE=5000`, `SNAPSHOT_CRON="*/15 * * * *"`, `SNAPSHOT_KEEP=5`, `RECONCILE_CRON="*/1 * * * *"`, `WRITE_MODE=redis-first`.
