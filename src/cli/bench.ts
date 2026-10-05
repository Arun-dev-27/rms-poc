/**
 * One-time timing script.
 *   node dist/cli/bench.js db-to-redis        MSSQL -> Redis full load
 *   node dist/cli/bench.js snapshot-to-redis  Redis -> snapshot file, wipe Redis, snapshot -> Redis
 *   node dist/cli/bench.js writes             Redis-first insert/update (+ time until persisted in MSSQL)
 *                                             vs DB-first, then reverts every change it made
 *   node dist/cli/bench.js all
 * Stop the API server first so its worker/cron jobs do not touch the numbers.
 */
process.env.DISABLE_JOBS = '1';

import { NestFactory } from '@nestjs/core';
import { promises as fs } from 'fs';
import { AppModule } from '../app.module';
import { MssqlService } from '../database/mssql.service';
import { MuminCache } from '../mumin/mumin.cache';
import { MuminRedisWriter } from '../mumin/mumin.redis-writer';
import { MuminRepository } from '../mumin/mumin.repository';
import { elapsed, MuminService } from '../mumin/mumin.service';
import { WriteBehindWorker } from '../mumin/write-behind.worker';
import { SnapshotService } from '../snapshot/snapshot.service';

const results: Record<string, unknown> = {};
const log = (label: string, value: unknown) => {
  results[label] = value;
  console.log(`${label.padEnd(48)} ${typeof value === 'object' ? JSON.stringify(value) : value}`);
};

async function main() {
  const mode = process.argv[2] ?? 'all';
  const app = await NestFactory.createApplicationContext(AppModule, { logger: ['error', 'warn'] });
  const snap = app.get(SnapshotService);
  const cache = app.get(MuminCache);
  const repo = app.get(MuminRepository);
  const svc = app.get(MuminService);
  const writer = app.get(MuminRedisWriter);
  const worker = app.get(WriteBehindWorker);
  const db = app.get(MssqlService);

  if (mode === 'db-to-redis' || mode === 'all') {
    console.log('\n=== MSSQL -> Redis (full load, Redis wiped first) ===');
    const r = await snap.loadFromDb(true);
    log('db->redis rows', r.rows);
    log('db->redis ms', r.ms);
    log('db->redis rows/sec', r.rowsPerSec);
    log('redis memory after load', r.redisMemory);
  }

  if (mode === 'snapshot-to-redis' || mode === 'all') {
    console.log('\n=== Snapshot ===');
    const s = await snap.takeSnapshot();
    log('snapshot take ms (Redis -> gzip file)', s.ms);
    log('snapshot rows', s.rows);
    log('snapshot file MB', +(s.bytes / 1048576).toFixed(1));
    const t = process.hrtime.bigint();
    const wiped = await cache.clearAll();
    log('simulated crash: keys wiped', wiped);
    log('wipe ms', elapsed(t));
    const r: any = await snap.restoreFromSnapshot(s.file);
    log('snapshot -> redis ms (load only)', r.snapshotMs);
    log('change-tracking delta after restore', r.delta);
    log('snapshot -> redis total ms', r.totalMs);
    log('redis keys after restore', await cache.countKeys());
  }

  if (mode === 'writes' || mode === 'all') {
    await worker.start();
    const ids: number[] = (await db.request().query('SELECT TOP 10000 Mumin_ID FROM dbo.Mumin_Mast ORDER BY Mumin_ID'))
      .recordset.map((r: any) => r.Mumin_ID);
    const originals = await repo.findByIds(ids);
    const stats = (arr: number[]) => {
      const s = [...arr].sort((a, b) => a - b);
      return { n: s.length, avgMs: +(s.reduce((a, b) => a + b, 0) / s.length).toFixed(2), p50Ms: s[Math.floor(s.length * 0.5)], p95Ms: s[Math.floor(s.length * 0.95)] };
    };
    const drain = async () => { const t = process.hrtime.bigint(); await worker.drained(); return elapsed(t); };

    console.log('\n=== Redis-first (write-behind) ===');
    let lat: number[] = [];
    for (const id of ids.slice(0, 200)) {
      const t = process.hrtime.bigint();
      await svc.updateOne(id, { Post: 'BENCH-1' });
      lat.push(elapsed(t));
    }
    log('single update x200 (redis)', stats(lat));
    log('  ...time until all 200 in MSSQL ms', await drain());

    for (const n of [1000, 10000]) {
      const t = process.hrtime.bigint();
      const r = await svc.updateMany(ids.slice(0, n).map((Mumin_ID) => ({ Mumin_ID, Post: `BENCH-${n}` })));
      log(`batch update ${n} (redis) ms`, elapsed(t));
      log(`  ...updated`, r.updated);
      log(`  ...time until all ${n} in MSSQL ms`, await drain());
    }

    const newRow = (id: number) => ({ ...originals[0], Mumin_ID: id, Person_ID: id, Full_Name: `Bench ${id}`, Post: 'BENCH-INS' });
    lat = [];
    for (let i = 0; i < 100; i++) {
      const t = process.hrtime.bigint();
      await svc.create(newRow(99000001 + i) as any);
      lat.push(elapsed(t));
    }
    log('single insert x100 (redis)', stats(lat));
    log('  ...time until all 100 in MSSQL ms', await drain());
    let t = process.hrtime.bigint();
    const ins = await svc.createMany(Array.from({ length: 5000 }, (_, i) => newRow(99100001 + i)) as any);
    log('batch insert 5000 (redis) ms', elapsed(t));
    log('  ...inserted', ins.inserted);
    log('  ...time until all 5000 in MSSQL ms', await drain());

    const check = (await db.request().query(`SELECT
        (SELECT COUNT(*) FROM dbo.Mumin_Mast WHERE Post = 'BENCH-10000') AS updated10k,
        (SELECT COUNT(*) FROM dbo.Mumin_Mast WHERE Mumin_ID BETWEEN 99000001 AND 99199999) AS inserted`)).recordset[0];
    log('MSSQL verify (expect 10000 / 5100)', check);
    log('write-behind queue after drain', await writer.stats());

    console.log('\n=== DB-first (write-through) for comparison ===');
    lat = [];
    for (const id of ids.slice(0, 200)) {
      const t2 = process.hrtime.bigint();
      const row = await repo.updateOne(id, { Post: 'BENCH-DB' });
      await cache.setMany([row!]);
      lat.push(elapsed(t2));
    }
    log('single update x200 (mssql+redis)', stats(lat));
    for (const n of [1000, 10000]) {
      t = process.hrtime.bigint();
      const rows = await repo.updateMany(ids.slice(0, n).map((Mumin_ID) => ({ Mumin_ID, Post: `BENCH-DB-${n}` })));
      await cache.setMany(rows);
      log(`batch update ${n} (mssql+redis) ms`, elapsed(t));
    }
    t = process.hrtime.bigint();
    const dbRows = await repo.insertMany(Array.from({ length: 5000 }, (_, i) => newRow(99200001 + i)) as any);
    await cache.setMany(dbRows);
    log('batch insert 5000 (mssql+redis) ms', elapsed(t));

    console.log('\n=== Cleanup (revert updates, delete bench rows) ===');
    t = process.hrtime.bigint();
    await svc.updateMany(originals.map((o) => ({ Mumin_ID: o.Mumin_ID, Post: o.Post })));
    await worker.drained();
    const benchIds = [
      ...Array.from({ length: 100 }, (_, i) => 99000001 + i),
      ...Array.from({ length: 5000 }, (_, i) => 99100001 + i),
      ...Array.from({ length: 5000 }, (_, i) => 99200001 + i),
    ];
    const deleted = await repo.deleteByIds(benchIds);
    await cache.delMany(benchIds);
    log('cleanup: reverted rows / deleted bench rows', { reverted: originals.length, deleted, ms: elapsed(t) });
  }

  await fs.writeFile(`bench-results-${mode}.json`, JSON.stringify(results, null, 2));
  await app.close();
}

main().then(() => process.exit(0), (e) => { console.error(e); process.exit(1); });
