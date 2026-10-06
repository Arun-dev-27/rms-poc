import { Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { SchedulerRegistry } from '@nestjs/schedule';
import { CronJob } from 'cron';
import { createReadStream, createWriteStream, promises as fs } from 'fs';
import * as path from 'path';
import * as readline from 'readline';
import { Readable } from 'stream';
import { pipeline } from 'stream/promises';
import { createGunzip, createGzip } from 'zlib';
import { MssqlService } from '../database/mssql.service';
import { TABLE } from '../mumin/mumin.columns';
import { META_KEY, MuminCache } from '../mumin/mumin.cache';
import * as sql from 'mssql';
import { DIRTY_KEY, DLQ_KEY, IDEM_KEY, LEADER_KEY, MuminRedisWriter, PROC_KEY, STATUS_KEY, STOPINFO_KEY, STREAM_KEY, VERSION_KEY } from '../mumin/mumin.redis-writer';
import { MuminRepository } from '../mumin/mumin.repository';
import { ALL_COLUMNS, KEY_COLUMN, MuminRecord } from '../mumin/mumin.types';
import { RedisService } from '../redis/redis.service';

const LOCK_KEY = 'mumin:lock:load';
const IMPORT_BATCH = 10000;

export interface SnapshotInfo { file: string; bytes: number; modifiedAt: string }

/**
 * Recovery layers, in order:
 *  1. Redis' own AOF (fsync everysec) + RDB -> a Redis restart restores itself (lose <= ~1s of writes).
 *  2. App snapshot (gzip NDJSON of every cached row + MSSQL change-tracking version) -> used when Redis
 *     data is gone (volume lost / FLUSHALL / new node). After loading it, every change MSSQL recorded
 *     after that version (insert / update / DELETE) is re-applied, so Redis ends exactly in sync.
 *  3. Full reload from MSSQL -> no valid snapshot, or the snapshot is older than change-tracking retention.
 */
@Injectable()
export class SnapshotService implements OnApplicationBootstrap {
  private readonly logger = new Logger(SnapshotService.name);
  private readonly dir: string;
  private readonly keep: number;

  constructor(
    private readonly cache: MuminCache,
    private readonly repo: MuminRepository,
    private readonly writer: MuminRedisWriter,
    private readonly db: MssqlService,
    private readonly redis: RedisService,
    private readonly config: ConfigService,
    private readonly scheduler: SchedulerRegistry,
  ) {
    this.dir = path.resolve(config.get('SNAPSHOT_DIR', './snapshots'));
    this.keep = Number(config.get('SNAPSHOT_KEEP', 5));
  }

  async onApplicationBootstrap() {
    await fs.mkdir(this.dir, { recursive: true });
    // A snapshot interrupted by a crash leaves a .tmp file; it is never restored from, so just remove it.
    for (const f of await fs.readdir(this.dir)) {
      if (f.endsWith('.tmp')) await fs.unlink(path.join(this.dir, f)).catch(() => undefined);
    }
    if (process.env.DISABLE_JOBS === '1') return; // one-time scripts drive everything themselves
    // Every instance has the timer; the Redis lock makes sure only one of them takes the snapshot.
    this.schedule('mumin-snapshot', this.config.get('SNAPSHOT_CRON', '*/15 * * * *'), () => this.takeSnapshotLocked());
    this.schedule('mumin-reconcile', this.config.get('RECONCILE_CRON', '*/1 * * * *'), () => this.reconcile());
    // Runs in the background; reads still work meanwhile through cache-aside.
    this.ensureWarm().catch((e) => this.logger.error(`Startup warm-up failed: ${e.message}`));
  }

  /** Startup decision tree: empty table -> import from stage; empty Redis -> snapshot or DB; else reconcile. */
  async ensureWarm() {
    if ((await this.repo.count()) === 0 && this.config.get('SOURCE_MSSQL_HOST')) {
      this.logger.warn(`${TABLE} is empty in docker MSSQL -> importing from stage`);
      await this.importFromSource();
      return;
    }
    const meta = await this.cache.getMeta();
    if (meta.ctVersion && (await this.cache.countKeys()) > 1) {
      const r = await this.reconcile();
      this.logger.log(`Redis already warm (AOF/RDB). Reconciled ${r.upserts} upserts / ${r.deletes} deletes`);
      return;
    }
    this.logger.warn('Redis cache is empty -> restoring');
    if ((await this.listSnapshots()).length) {
      try {
        await this.restoreFromSnapshot();
        return;
      } catch (e) {
        this.logger.error(`Snapshot restore failed (${(e as Error).message}); falling back to MSSQL`);
      }
    }
    await this.loadFromDb(true);
  }

  /**
   * Stage (read-only) -> docker MSSQL (TRUNCATE + bulk copy) -> Redis full load.
   * Keyset pagination (WHERE Mumin_ID > @last ORDER BY Mumin_ID) instead of one long stream: the stage
   * link drops long-running transfers, so every page is a short query that is retried on its own.
   */
  async importFromSource() {
    return this.withLock(async () => {
      const started = Date.now();
      const cols = ALL_COLUMNS.map((c) => `[${c}]`).join(', ');
      await this.repo.truncate();
      let last = 0;
      let rows = 0;
      try {
        for (;;) {
          const page = await retry(3, async () => {
            const r = await (await this.db.sourceRequest())
              .input('last', sql.Int, last)
              .query(`SELECT TOP (${IMPORT_BATCH}) ${cols} FROM ${TABLE} WHERE ${KEY_COLUMN} > @last ORDER BY ${KEY_COLUMN}`);
            return r.recordset;
          });
          if (!page.length) break;
          await this.repo.bulkInsert(page);
          rows += page.length;
          last = page[page.length - 1][KEY_COLUMN];
          if (rows % 50000 < IMPORT_BATCH) this.logger.log(`Import progress: ${rows} rows`);
        }
      } catch (e) {
        // Never leave a half-imported table behind: empty table => next startup retries the import.
        await this.repo.truncate().catch(() => undefined);
        throw e;
      }
      const importMs = Date.now() - started;
      this.logger.log(`Imported ${rows} rows stage -> docker MSSQL in ${importMs} ms`);
      const redis = await this.loadFromDbUnlocked(true);
      return { importedRows: rows, importMs, redis };
    });
  }

  /** Full load MSSQL -> Redis (streamed, MSET batches). */
  loadFromDb(fresh = false) {
    return this.withLock(() => this.loadFromDbUnlocked(fresh));
  }

  private async loadFromDbUnlocked(fresh: boolean) {
    const started = Date.now();
    if (fresh) await this.cache.clearAll();
    // Version BEFORE reading: anything changed during the load is re-applied by the next reconcile.
    const { current } = await this.repo.ctVersions();
    const rows = await this.repo.streamAll(this.cache.batchSize, (batch) => this.setClean(batch));
    const ms = Date.now() - started;
    await this.cache.setMeta({ source: 'db', count: String(rows), loadedAt: new Date().toISOString(), ctVersion: String(current), loadMs: String(ms) });
    const memory = await this.usedMemory();
    this.logger.log(`Loaded ${rows} rows MSSQL -> Redis in ${ms} ms (${memory})`);
    return { source: 'mssql', rows, ms, rowsPerSec: Math.round((rows / ms) * 1000), redisMemory: memory };
  }

  /** Snapshot under the cluster-wide lock (skipped if another instance is already snapshotting/loading). */
  takeSnapshotLocked() {
    return this.withLock(() => this.takeSnapshot(), { skipped: true, reason: 'another instance holds the lock' });
  }

  /** Dump every cached row to snapshots/mumin-<ts>.ndjson.gz (atomic: write .tmp then rename). */
  async takeSnapshot() {
    const started = Date.now();
    // Change-tracking version captured BEFORE scanning: writes during the scan are re-applied on restore.
    const { current: ctVersion } = await this.repo.ctVersions();
    const takenAt = new Date().toISOString();
    const file = path.join(this.dir, `mumin-${takenAt.replace(/[:.]/g, '-')}.ndjson.gz`);
    const tmp = `${file}.tmp`;
    let count = 0;
    const cache = this.cache;
    const stopped = await this.writer.stoppedAll(); // STOP status must survive Redis data loss too
    async function* lines() {
      yield JSON.stringify({ meta: { table: TABLE, takenAt, ctVersion, version: 1 } }) + '\n';
      for await (const batch of cache.scanAll()) {
        count += batch.length;
        yield batch.join('\n') + '\n';
      }
      yield JSON.stringify({ state: { stopped } }) + '\n';
      yield JSON.stringify({ end: true, count }) + '\n'; // completeness marker
    }
    await pipeline(Readable.from(lines()), createGzip({ level: 6 }), createWriteStream(tmp));
    await fs.rename(tmp, file);
    await this.redis.client.bgsave().catch(() => undefined); // also refresh Redis' own dump.rdb
    await this.prune();
    const info = { file: path.basename(file), takenAt, ctVersion, rows: count, bytes: (await fs.stat(file)).size, ms: Date.now() - started };
    this.logger.log(`Snapshot ${info.file}: ${count} rows, ${(info.bytes / 1048576).toFixed(1)} MB, ${info.ms} ms`);
    return info;
  }

  /** Snapshot -> Redis, then change-tracking delta since the snapshot's version. */
  async restoreFromSnapshot(fileName?: string) {
    return this.withLock(async () => {
      const file = fileName ? path.basename(fileName) : (await this.listSnapshots())[0]?.file;
      if (!file) throw new Error('No snapshot available');
      const started = Date.now();
      await this.cache.clearAll();
      // Iterate immediately after creating the reader: readline emits lines right away, and any
      // `await` in between would silently drop them.
      const rl = readline.createInterface({ input: createReadStream(path.join(this.dir, file)).pipe(createGunzip()), crlfDelay: Infinity });
      let meta: { ctVersion: number; takenAt: string } | undefined;
      let endCount: number | undefined;
      let rows = 0;
      let buf: MuminRecord[] = [];
      for await (const line of rl) {
        if (!line) continue;
        const obj = JSON.parse(line);
        if (obj.meta) { meta = obj.meta; continue; }
        if (obj.end) { endCount = obj.count; continue; }
        if (obj.state) { await this.writer.restoreStopped(obj.state.stopped ?? {}); continue; }
        buf.push(obj);
        if (buf.length >= this.cache.batchSize) { await this.cache.setMany(buf); rows += buf.length; buf = []; }
      }
      if (buf.length) { await this.cache.setMany(buf); rows += buf.length; }
      if (!meta || endCount !== rows) throw new Error(`Snapshot ${file} is incomplete/corrupt (rows=${rows}, expected=${endCount})`);
      const snapshotMs = Date.now() - started;

      const { minValid } = await this.repo.ctVersions();
      if (meta.ctVersion < minValid) {
        this.logger.warn(`Snapshot version ${meta.ctVersion} < min valid ${minValid} (older than CT retention) -> full reload`);
        return this.loadFromDbUnlocked(true);
      }
      await this.cache.setMeta({ ctVersion: String(meta.ctVersion) });
      const delta = await this.reconcileUnlocked();
      const totalMs = Date.now() - started;
      await this.cache.setMeta({ source: 'snapshot', count: String(rows), loadedAt: new Date().toISOString(), loadMs: String(totalMs) });
      this.logger.log(`Restored ${rows} rows from ${file} in ${snapshotMs} ms + delta ${delta.upserts}/${delta.deletes} in ${delta.ms} ms`);
      return { source: 'snapshot', file, snapshotTakenAt: meta.takenAt, rows, snapshotMs, delta, totalMs };
    });
  }

  /** Apply every MSSQL change since the version Redis is at (catches writes made outside this API). */
  async reconcile() {
    const meta = await this.cache.getMeta();
    if (!meta.ctVersion) return { upserts: 0, deletes: 0, ms: 0, skipped: true };
    // Skip (don't wait) when a load/restore/another reconcile holds the lock.
    return this.withLock(() => this.reconcileUnlocked(), { upserts: 0, deletes: 0, ms: 0, skipped: true });
  }

  private async reconcileUnlocked() {
    const started = Date.now();
    const meta = await this.cache.getMeta();
    const from = Number(meta.ctVersion);
    const { current, minValid } = await this.repo.ctVersions();
    if (from < minValid) {
      this.logger.warn('Redis version is older than change-tracking retention -> full reload');
      await this.loadFromDbUnlocked(true);
      return { upserts: -1, deletes: -1, ms: Date.now() - started, fullReload: true };
    }
    let upserts = 0;
    let deletes = 0;
    if (current > from) {
      await this.repo.streamChanges(from, this.cache.batchSize, async (b) => {
        await this.setClean(b.upserts);
        const dirty = await this.writer.dirtyOf(b.deletes);
        await this.cache.delMany(b.deletes.filter((id) => !dirty.has(id)));
        // Changed outside the API: any version a client holds for these rows is now stale.
        await this.writer.bumpVersions([...b.upserts.map((r) => r.Mumin_ID), ...b.deletes]);
        upserts += b.upserts.length;
        deletes += b.deletes.length;
      });
    }
    await this.cache.setMeta({ ctVersion: String(current) });
    if (upserts || deletes) this.logger.log(`Reconcile v${from}->v${current}: ${upserts} upserts, ${deletes} deletes`);
    return { fromVersion: from, toVersion: current, upserts, deletes, ms: Date.now() - started };
  }

  /** Deletes every cached record (simulates "Redis data lost"). Pending write queue is kept. */
  async clearRedis() {
    return this.withLock(async () => {
      const started = Date.now();
      const deletedKeys = await this.cache.clearAll();
      return { deletedKeys, ms: Date.now() - started };
    });
  }

  /** Up to n record ids currently in Redis (SCAN, non-blocking). */
  async sampleIds(n: number): Promise<number[]> {
    const ids: number[] = [];
    const stream = this.redis.client.scanStream({ match: 'mumin:rec:*', count: 1000 });
    for await (const keys of stream as AsyncIterable<string[]>) {
      for (const k of keys) ids.push(Number(k.slice('mumin:rec:'.length)));
      if (ids.length >= n) { stream.destroy(); break; }
    }
    return ids.slice(0, n);
  }

  async listSnapshots(): Promise<SnapshotInfo[]> {
    const files = (await fs.readdir(this.dir).catch(() => [] as string[]))
      .filter((f) => f.startsWith('mumin-') && f.endsWith('.ndjson.gz'))
      .sort()
      .reverse();
    return Promise.all(files.map(async (f) => {
      const st = await fs.stat(path.join(this.dir, f));
      return { file: f, bytes: st.size, modifiedAt: st.mtime.toISOString() };
    }));
  }

  async status() {
    const persistence = await this.redis.client.info('persistence');
    const pick = (k: string) => persistence.match(new RegExp(`${k}:(.*)`))?.[1]?.trim();
    const keys = await this.cache.countKeys();
    // dbsize minus the bookkeeping keys = exact record count, without scanning 264k keys
    const bookkeeping = await this.redis.client.exists(META_KEY, STREAM_KEY, DLQ_KEY, DIRTY_KEY, LOCK_KEY, VERSION_KEY, IDEM_KEY, STATUS_KEY, PROC_KEY, STOPINFO_KEY, LEADER_KEY);
    return {
      redisKeys: keys,
      redisRecords: keys - bookkeeping,
      redisMemory: await this.usedMemory(),
      mssqlRows: await this.repo.count(),
      changeTracking: await this.repo.ctVersions(),
      meta: await this.cache.getMeta(),
      writeBehind: await this.writer.stats(),
      redisPersistence: {
        aof_enabled: pick('aof_enabled'),
        rdb_last_save_time: new Date(Number(pick('rdb_last_save_time')) * 1000).toISOString(),
        rdb_changes_since_last_save: pick('rdb_changes_since_last_save'),
        rdb_last_bgsave_status: pick('rdb_last_bgsave_status'),
      },
      snapshots: await this.listSnapshots(),
    };
  }

  /** Never overwrite a row whose Redis-first write is still on its way to MSSQL. */
  private async setClean(rows: MuminRecord[]) {
    const dirty = await this.writer.dirtyOf(rows.map((r) => r.Mumin_ID));
    await this.cache.setMany(dirty.size ? rows.filter((r) => !dirty.has(r.Mumin_ID)) : rows);
  }

  private async usedMemory() {
    const info = await this.redis.client.info('memory');
    return info.match(/used_memory_human:(.*)/)?.[1]?.trim() ?? '?';
  }

  private schedule(name: string, cron: string, task: () => Promise<unknown>) {
    const job = new CronJob(cron, () => {
      void task().catch((e) => this.logger.error(`${name} failed: ${e.message}`));
    });
    this.scheduler.addCronJob(name, job);
    job.start();
  }

  private async prune() {
    const old = (await this.listSnapshots()).slice(this.keep);
    await Promise.all(old.map((s) => fs.unlink(path.join(this.dir, s.file))));
  }

  /** Distributed lock so two app instances never bulk-load at the same time. */
  private async withLock<T, F = never>(work: () => Promise<T>, ifBusy?: F): Promise<T | F> {
    const token = `${process.pid}-${Date.now()}`;
    const ok = await this.redis.client.set(LOCK_KEY, token, 'EX', 900, 'NX');
    if (!ok) {
      if (ifBusy !== undefined) return ifBusy;
      throw new Error('Another cache load/restore is already running');
    }
    try {
      return await work();
    } finally {
      await this.redis.client.eval(
        "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
        1, LOCK_KEY, token,
      );
    }
  }
}

async function retry<T>(attempts: number, fn: () => Promise<T>): Promise<T> {
  for (let i = 1; ; i++) {
    try {
      return await fn();
    } catch (e) {
      if (i >= attempts) throw e;
      await new Promise((r) => setTimeout(r, 1000 * i));
    }
  }
}
