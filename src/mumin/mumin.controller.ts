import { Body, Controller, Delete, Get, Param, ParseIntPipe, Patch, Post } from '@nestjs/common';
import { MuminService } from './mumin.service';
import { validateBatch, validateId, validateInsert, validatePatch } from './mumin.validation';

@Controller('mumin')
export class MuminController {
  constructor(private readonly service: MuminService) {}

  // Batch routes are declared before the ':id' routes so 'batch' is never parsed as an id.
  /** { "ids": [20178652, ...] } */
  @Post('batch/get')
  getMany(@Body() body: any) {
    return this.service.getMany(validateBatch(body, 'ids', validateId));
  }

  /** { "ids": [20178652, ...] } */
  @Post('batch/delete')
  deleteMany(@Body() body: any) {
    return this.service.deleteMany(validateBatch(body, 'ids', validateId));
  }

  /** { "items": [{ "Mumin_ID": 1, "Person_ID": 1, ...columns }] } */
  @Post('batch')
  createMany(@Body() body: any) {
    return this.service.createMany(validateBatch(body, 'items', validateInsert));
  }

  /** { "items": [{ "Mumin_ID": 1, "Mobile_No": "..." }, { "Mumin_ID": 2, "Jamaat": "..." }] } */
  @Patch('batch')
  updateMany(@Body() body: any) {
    return this.service.updateMany(
      validateBatch(body, 'items', (item, where) => ({ ...validatePatch(item, where), Mumin_ID: validateId(item?.Mumin_ID, where) })),
    );
  }

  @Get(':id')
  getOne(@Param('id', ParseIntPipe) id: number) {
    return this.service.getOne(id);
  }

  @Post()
  create(@Body() body: any) {
    return this.service.create(validateInsert(body));
  }

  @Delete(':id')
  deleteOne(@Param('id', ParseIntPipe) id: number) {
    return this.service.deleteOne(id);
  }

  /** Partial update: only the columns present in the body are changed. */
  @Patch(':id')
  updateOne(@Param('id', ParseIntPipe) id: number, @Body() body: any) {
    return this.service.updateOne(id, validatePatch(body));
  }
}
