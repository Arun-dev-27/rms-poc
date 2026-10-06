import { BadRequestException, Body, Controller, Delete, Get, Headers, HttpCode, Param, ParseIntPipe, Patch, Post } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { LUA_CHUNK } from './mumin.redis-writer';
import { MuminService, WriteOptions } from './mumin.service';
import { validateBatch, validateId, validateInsert, validatePatch } from './mumin.validation';

const KEY_RE = /^[A-Za-z0-9_.:-]{8,100}$/;

/**
 * Write headers:
 *   Idempotency-Key: <unique request id>   same id sent again -> applied once, stored result returned
 *   If-Match: <version>                    (single record) reject with 412 if the record changed meanwhile
 * Batch items may carry "_version" for the same check per record.
 */
@Controller('mumin')
export class MuminController {
  private readonly requireKey: boolean;

  constructor(private readonly service: MuminService, config: ConfigService) {
    this.requireKey = config.get('IDEMPOTENCY_REQUIRED', 'true') !== 'false';
  }

  // Batch routes are declared before the ':id' routes so 'batch' is never parsed as an id.
  /** { "ids": [20178652, ...] } */
  @Post('batch/get')
  @HttpCode(200)
  getMany(@Body() body: any) {
    return this.service.getMany(validateBatch(body, 'ids', validateId));
  }

  /** { "ids": [20178652, ...], "versions": [3, 7, ...] (optional) } */
  @Post('batch/delete')
  @HttpCode(200)
  deleteMany(@Body() body: any, @Headers('idempotency-key') key?: string) {
    const ids = validateBatch(body, 'ids', validateId, LUA_CHUNK);
    const versions = body?.versions === undefined ? undefined : validateBatch(body, 'versions', (v, w) => version(v, w), LUA_CHUNK);
    if (versions && versions.length !== ids.length) throw new BadRequestException("'versions' must match 'ids'");
    return this.service.deleteMany(ids, { ...this.opts(key), expected: versions });
  }

  /** { "items": [{ "Mumin_ID": 1, "Person_ID": 1, ...columns }] } */
  @Post('batch')
  createMany(@Body() body: any, @Headers('idempotency-key') key?: string) {
    return this.service.createMany(validateBatch(body, 'items', validateInsert, LUA_CHUNK), this.opts(key));
  }

  /** { "items": [{ "Mumin_ID": 1, "Mobile_No": "...", "_version": 3 }, { "Mumin_ID": 2, "Jamaat": "..." }] } */
  @Patch('batch')
  updateMany(@Body() body: any, @Headers('idempotency-key') key?: string) {
    const expected: (number | undefined)[] = [];
    const items = validateBatch(body, 'items', (item, where) => {
      const { _version, ...rest } = item ?? {};
      expected.push(_version === undefined ? undefined : version(_version, where));
      return { ...validatePatch(rest, where), Mumin_ID: validateId(rest.Mumin_ID, where) };
    }, LUA_CHUNK);
    return this.service.updateMany(items, { ...this.opts(key), expected: expected.some((v) => v !== undefined) ? expected : undefined });
  }

  // ---- record status: STOP / SKIP ------------------------------------------------------------

  /** { "ids": [...], "reason": "..." }  every later request for these records gets 423 Locked */
  @Post(['batch/cancel', 'batch/stop'])
  @HttpCode(200)
  stopMany(@Body() body: any) {
    return this.service.stop(validateBatch(body, 'ids', validateId, LUA_CHUNK), reason(body));
  }

  @Post('batch/resume')
  @HttpCode(200)
  resumeMany(@Body() body: any) {
    return this.service.resume(validateBatch(body, 'ids', validateId, LUA_CHUNK));
  }

  @Get(':id/status')
  status(@Param('id', ParseIntPipe) id: number) {
    return this.service.state(id);
  }

  @Post([':id/cancel', ':id/stop'])
  @HttpCode(200)
  stopOne(@Param('id', ParseIntPipe) id: number, @Body() body: any) {
    return this.service.stop([id], reason(body));
  }

  @Post(':id/resume')
  @HttpCode(200)
  resumeOne(@Param('id', ParseIntPipe) id: number) {
    return this.service.resume([id]);
  }

  /**
   * 2-phase processing POC: { "workMs": 200, "patch": { "Post": "..." } }
   * 200 processed · 423 record STOPPED (before or during processing; nothing written)
   * Waits its turn in the record's line (never 'busy'); 423 also when cancelled while waiting. 404 unknown record
   */
  @Post(':id/process')
  @HttpCode(200)
  process(@Param('id', ParseIntPipe) id: number, @Body() body: any, @Headers('idempotency-key') key?: string) {
    const workMs = body?.workMs ?? 0;
    if (!Number.isInteger(workMs) || workMs < 0 || workMs > 60000) throw new BadRequestException('workMs must be 0..60000');
    return this.service.processRecord(id, validatePatch(body?.patch ?? {}, 'patch'), workMs, this.opts(key));
  }

  @Get(':id')
  getOne(@Param('id', ParseIntPipe) id: number) {
    return this.service.getOne(id);
  }

  @Post()
  create(@Body() body: any, @Headers('idempotency-key') key?: string) {
    return this.service.create(validateInsert(body), this.opts(key));
  }

  @Delete(':id')
  deleteOne(@Param('id', ParseIntPipe) id: number, @Headers('idempotency-key') key?: string, @Headers('if-match') ifMatch?: string) {
    return this.service.deleteOne(id, { ...this.opts(key), expected: ifMatchVersion(ifMatch) });
  }

  /** Partial update: only the columns present in the body are changed. */
  @Patch(':id')
  updateOne(@Param('id', ParseIntPipe) id: number, @Body() body: any, @Headers('idempotency-key') key?: string, @Headers('if-match') ifMatch?: string) {
    return this.service.updateOne(id, validatePatch(body), { ...this.opts(key), expected: ifMatchVersion(ifMatch) });
  }

  private opts(key?: string): WriteOptions {
    if (!key) {
      if (this.requireKey) throw new BadRequestException('Idempotency-Key header is required for writes (a unique id per request)');
      return {};
    }
    if (!KEY_RE.test(key)) throw new BadRequestException('Idempotency-Key must be 8-100 chars: letters, digits, _ . : -');
    return { requestId: key };
  }
}

function version(v: unknown, where: string): number {
  if (typeof v !== 'number' || !Number.isInteger(v) || v < 0) throw new BadRequestException(`${where}: version must be a non-negative integer`);
  return v;
}

function reason(body: any): string {
  const r = body?.reason ?? '';
  if (typeof r !== 'string' || r.length > 200) throw new BadRequestException('reason must be a string (max 200)');
  return r;
}

function ifMatchVersion(h?: string): number[] | undefined {
  if (h === undefined || h === '' || h === '*') return undefined;
  const v = Number(h.replace(/^W\//, '').replace(/"/g, ''));
  if (!Number.isInteger(v) || v < 0) throw new BadRequestException('If-Match must be a record version number');
  return [v];
}
