# RMS – `dbo.Mumin_Mast` on Redis (NestJS + MSSQL + Redis, Docker)

Scope: **only `MMS.dbo.Mumin_Mast`** (79 columns, key = `Mumin_ID` = ITS id, 264,615 rows).
Stage DB is read-only, so the table is copied into a docker MSSQL mirror; all reads/writes/snapshots run there.

## Architecture

```
 stage MMS (read-only) --import (keyset pages)--> docker MSSQL  dbo.Mumin_Mast  (PK Mumin_ID, Change Tracking ON)
                                                     ^  write-behind worker (batched upsert)
                                                     |
 API (NestJS) --- reads: GET / MGET ------------> Redis  mumin:rec:{Mumin_ID} = JSON row
              --- writes: Lua (merge+SET+XADD) -> Redis  mumin:writes (Stream), mumin:dirty (Hash)
                                                     |
                                     snapshots/  mumin-<ts>.ndjson.gz  (+ CT version)
```

| Concern | Practice used |
|---|---|
| Redis layout | 1 STRING (full-row JSON) per record. ~476 MB for 264,615 rows |
| Single read | `GET`; on a miss, read MSSQL and back-fill Redis (cache-aside) |
| Batch read | `MGET`, 5,000 keys per command; misses are fetched with one `OPENJSON` query |
| Single/batch update | **Redis-first**: a Lua script merges the patch, `SET`s the row, `XADD`s it to the stream and marks it dirty. Atomic, one round trip per 1,000 items |
| Insert | Same Lua script with an "exists" check; MSSQL is checked first so an insert can never overwrite an existing row |
| Persist to MSSQL | Write-behind worker: consumer group, batch coalesced by id, one transaction (UPDATE + INSERT), ACK only after commit |
| Worker crash | Un-ACKed entries are re-read; entries from dead consumers are taken over with `XAUTOCLAIM` |
| MSSQL down | Writes are still accepted in Redis; the batch retries until MSSQL is back (nothing lost, nothing dead-lettered) |
| Row MSSQL rejects | Goes to the `mumin:writes:dlq` stream with the SQL error; its Redis key is dropped so the next read shows MSSQL's value |
| Writes outside the API | Change-tracking reconcile every minute (`RECONCILE_CRON`); skips rows with pending Redis writes |
| `WRITE_MODE=db-first` | Optional write-through mode: commit in MSSQL first, then update Redis |

## Recovery – what happens when Redis data is lost

| Failure | Recovery | Measured |
|---|---|---|
| Redis process crash / container kill | Redis reloads itself from **AOF** (`appendfsync everysec`, RDB base) | **1.9 s** for 264,615 keys, at most ~1 s of writes at risk |
| Redis data deleted (FLUSHALL / volume lost) | App start: **latest snapshot → Redis**, then the **change-tracking delta** replays every insert/update/**delete** made since the snapshot | 11–19 s plus a delta under 1 s |
| Snapshot missing, corrupt, or older than CT retention (7 days) | Full **MSSQL → Redis** load | 35–53 s |
| Docker MSSQL empty | Re-import from stage (keyset paging, 10k rows/page, retry per page) then load Redis | ~150–167 s (stage network bound) |

Snapshots: cron `*/15 * * * *` (`SNAPSHOT_CRON`), keeps 5, gzip NDJSON (44 MB). Written to a `.tmp` file and renamed, with an end-marker so a truncated file is rejected; a Redis `BGSAVE` is also triggered.
Timing guidance: snapshot interval ≤ 15 min; CT retention (7 days) must be longer than the oldest snapshot you would restore.

## Timings (one-time script `npm run bench`, Windows + Docker Desktop, ranges over runs)

| Operation | Redis-first | DB-first (for comparison) |
|---|---|---|
| **MSSQL → Redis full load (264,615 rows)** | **35–53 s** (5–7.6k rows/s) | |
| **Snapshot → Redis (264,615 rows)** | **11–19 s** | |
| Take snapshot (Redis → file) | 8–14 s | |
| Single update | **avg 1.2–2.0 ms**, p95 1.8–3.5 ms | avg 5.5–12 ms |
| Batch update 1,000 | **120–200 ms** (in MSSQL after 0.5–0.8 s) | 490–720 ms |
| Batch update 10,000 | **0.95–2.1 s** (in MSSQL after 3.7–6.2 s) | 4.6–6.2 s |
| Single insert | avg 5–11 ms | |
| Batch insert 5,000 | **0.85–1.6 s** (in MSSQL after 1.7–2.4 s) | 2.1–2.7 s |
| Batch read 1,000 (`MGET`) | ~200 ms incl. HTTP | |

The MSSQL → Redis load is limited by the Node MSSQL driver parsing 79 columns × 264k rows (~24 s of the total).
Having SQL Server build the JSON (`FOR JSON`) was tested and was slower (54 s).

## Verified scenarios

1. Redis `FLUSHALL` after making a direct DB update and delete → restored from snapshot + delta. Both changes were reflected.
2. `docker kill` on Redis → AOF reload in 1.9 s, last write present.
3. MSSQL paused → the API write was accepted (4 ms) and reads were served from Redis. The app was killed and MSSQL resumed; on restart the worker recovered both orphaned writes.
4. MSSQL paused for 25 s with a write queued → retried, persisted after resume, DLQ = 0.
5. A row SQL Server rejects (conversion error) → dead-lettered with the real SQL error, queue kept moving, Redis key reloaded from MSSQL.

## Run

```bash
cp .env.example .env            # fill SOURCE_MSSQL_* (stage read-only account)
docker compose up -d mssql redis mssql-init
npm ci && npm run build
npm start                        # empty table -> imports from stage -> loads Redis
npm run bench                    # one-time timing script (stop the API first)
#   bench:db-to-redis | bench:snapshot-to-redis | bench:writes
npm run gen:schema               # regenerate columns + DDL if the stage table changes
docker compose --profile app up -d --build   # optional: API in docker too
```

| Endpoint | |
|---|---|
| `GET /mumin/:id` | single read |
| `POST /mumin/batch/get` `{ids:[...]}` | batch read (≤10,000) |
| `POST /mumin` / `POST /mumin/batch` `{items:[...]}` | insert / batch insert |
| `PATCH /mumin/:id` / `PATCH /mumin/batch` `{items:[{Mumin_ID,...}]}` | partial update / batch update (each item can update different columns) |
| `GET /cache/status` | keys, memory, CT version, queue/dirty/DLQ, persistence |
| `POST /cache/snapshot` · `/cache/restore` · `/cache/load` · `/cache/reconcile` · `/cache/import-source` | admin |

## Known limits / next steps

- Redis-first means MSSQL lags Redis by ~0.1–6 s. Anything else that reads MSSQL directly sees writes after that lag.
- With Redis-first writes, Redis is the write path. Keep AOF on and `maxmemory-policy noeviction` (already configured); Redis is ~476 MB of a 2 GB cap.
- The Lua script works on many keys at once, so this design assumes a single Redis (or Sentinel). Redis Cluster would need hash tags.
- Nothing monitors the DLQ yet; add an alert on `writeBehind.dlq > 0` from `/cache/status`.
