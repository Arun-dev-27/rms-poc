import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ScheduleModule } from '@nestjs/schedule';
import { DatabaseModule } from './database/database.module';
import { RedisModule } from './redis/redis.module';
import { MuminModule } from './mumin/mumin.module';
import { SnapshotModule } from './snapshot/snapshot.module';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    ScheduleModule.forRoot(),
    DatabaseModule,
    RedisModule,
    MuminModule,
    SnapshotModule,
  ],
})
export class AppModule {}
