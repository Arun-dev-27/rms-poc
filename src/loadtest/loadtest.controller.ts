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
    return this.loadTest.start(requests, concurrency);
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
