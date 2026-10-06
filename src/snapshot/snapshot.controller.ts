import { Body, Controller, Get, Post, Query } from '@nestjs/common';
import { COLUMNS } from '../mumin/mumin.columns';
import { MuminRedisWriter } from '../mumin/mumin.redis-writer';
import { SnapshotService } from './snapshot.service';

@Controller('cache')
export class SnapshotController {
  constructor(private readonly snapshots: SnapshotService, private readonly writer: MuminRedisWriter) {}

  /** Column metadata (used by the UI to build forms). */
  @Get('columns')
  columns() {
    return COLUMNS;
  }

  /** Write-behind queue only (cheap; the UI polls it to time "saved in MSSQL"). */
  @Get('queue')
  queue() {
    return this.writer.stats();
  }

  @Get('sample-ids')
  sampleIds(@Query('n') n?: string) {
    return this.snapshots.sampleIds(Math.min(Math.max(Number(n) || 100, 1), 50000));
  }

  /** Delete ALL cached records from Redis. */
  @Post('clear')
  clear() {
    return this.snapshots.clearRedis();
  }

  @Get('status')
  status() {
    return this.snapshots.status();
  }

  /** Stage MMS (read-only) -> docker MSSQL -> Redis. */
  @Post('import-source')
  importSource() {
    return this.snapshots.importFromSource();
  }

  /** Full MSSQL -> Redis load. { "fresh": true } clears the cache first. */
  @Post('load')
  load(@Body() body: { fresh?: boolean }) {
    return this.snapshots.loadFromDb(!!body?.fresh);
  }

  @Post('snapshot')
  snapshot() {
    return this.snapshots.takeSnapshot();
  }

  /** Restore latest (or { "file": "..." }) snapshot into Redis + change-tracking delta. */
  @Post('restore')
  restore(@Body() body: { file?: string }) {
    return this.snapshots.restoreFromSnapshot(body?.file);
  }

  /** Apply MSSQL changes made since Redis' version (also runs on RECONCILE_CRON). */
  @Post('reconcile')
  reconcile() {
    return this.snapshots.reconcile();
  }

  @Get('snapshots')
  list() {
    return this.snapshots.listSnapshots();
  }
}
