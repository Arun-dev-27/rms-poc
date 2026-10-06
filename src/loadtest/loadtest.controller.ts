import { BadRequestException, Body, Controller, Get, HttpCode, Post } from '@nestjs/common';
import { LoadTestService } from './loadtest.service';

@Controller('loadtest')
export class LoadTestController {
  constructor(private readonly loadTest: LoadTestService) {}

  /** { "requests": 100000, "concurrency": 400 } */
  @Post('start')
  @HttpCode(200)
  start(@Body() body: any) {
    const requests = body?.requests ?? 100000;
    const concurrency = body?.concurrency ?? 400;
    if (!Number.isInteger(requests) || requests < 1000 || requests > 500000) throw new BadRequestException('requests must be 1,000..5,00,000');
    if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 2000) throw new BadRequestException('concurrency must be 1..2,000');
    const mode = body?.mode ?? 'many';
    if (mode !== 'many' && mode !== 'single') throw new BadRequestException("mode must be 'many' or 'single'");
    const recordId = body?.recordId;
    if (recordId !== undefined && recordId !== null && (!Number.isInteger(recordId) || recordId <= 0)) throw new BadRequestException('recordId must be a Mumin_ID');
    return this.loadTest.start(requests, concurrency, mode, recordId ?? undefined);
  }

  @Get('status')
  status() {
    return this.loadTest.status();
  }

  @Post('cancel')
  @HttpCode(200)
  cancel() {
    return this.loadTest.cancel();
  }
}
