import { Body, Controller, Get, Post } from '@nestjs/common';
import { SnapshotService } from './snapshot.service';

@Controller('cache')
export class SnapshotController {
  constructor(private readonly snapshots: SnapshotService) {}

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
