import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ScheduleModule } from '@nestjs/schedule';
import { DatabaseModule } from './database/database.module';
import { RedisModule } from './redis/redis.module';
import { MuminModule } from './mumin/mumin.module';
import { SnapshotModule } from './snapshot/snapshot.module';
import { LoadTestModule } from './loadtest/loadtest.module';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    ScheduleModule.forRoot(),
    DatabaseModule,
    RedisModule,
    MuminModule,
    SnapshotModule,
    LoadTestModule,
  ],
})
export class AppModule {}
