import { Module } from '@nestjs/common';
import { LoadTestController } from './loadtest.controller';
import { LoadTestService } from './loadtest.service';

@Module({
  controllers: [LoadTestController],
  providers: [LoadTestService],
})
export class LoadTestModule {}
