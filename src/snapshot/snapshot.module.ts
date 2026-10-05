import { Module } from '@nestjs/common';
import { MuminModule } from '../mumin/mumin.module';
import { SnapshotController } from './snapshot.controller';
import { SnapshotService } from './snapshot.service';

@Module({
  imports: [MuminModule],
  controllers: [SnapshotController],
  providers: [SnapshotService],
})
export class SnapshotModule {}
