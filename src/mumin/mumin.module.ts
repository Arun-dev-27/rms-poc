import { Module } from '@nestjs/common';
import { MuminCache } from './mumin.cache';
import { MuminController } from './mumin.controller';
import { RecordQueue } from './mumin.queue';
import { MuminRedisWriter } from './mumin.redis-writer';
import { MuminRepository } from './mumin.repository';
import { MuminService } from './mumin.service';
import { WriteBehindWorker } from './write-behind.worker';

@Module({
  controllers: [MuminController],
  providers: [MuminRepository, MuminCache, MuminRedisWriter, RecordQueue, MuminService, WriteBehindWorker],
  exports: [MuminRepository, MuminCache, MuminRedisWriter, MuminService, WriteBehindWorker],
})
export class MuminModule {}
