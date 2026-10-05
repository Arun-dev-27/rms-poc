import { BadRequestException } from '@nestjs/common';
import { COLUMNS } from './mumin.columns';
import { COLUMN_BY_NAME, KEY_COLUMN, MuminPatch } from './mumin.types';

export const MAX_BATCH = 10000;

/**
 * Validation is driven by the generated column metadata (79 columns), so it always matches
 * the real table: unknown columns, wrong types, over-long strings and NOT NULL violations are rejected.
 */
export function validatePatch(body: unknown, where = 'body'): MuminPatch {
  if (!body || typeof body !== 'object' || Array.isArray(body)) throw new BadRequestException(`${where} must be an object`);
  const out: MuminPatch = {};
  for (const [k, v] of Object.entries(body)) {
    if (k === KEY_COLUMN) continue;
    const col = COLUMN_BY_NAME.get(k);
    if (!col) throw new BadRequestException(`${where}: unknown column '${k}'`);
    if (v === null) {
      if (!col.nullable) throw new BadRequestException(`${where}: '${k}' cannot be null`);
      out[k] = null;
      continue;
    }
    switch (col.type) {
      case 'int':
      case 'numeric':
        if (typeof v !== 'number' || !Number.isInteger(v)) throw new BadRequestException(`${where}: '${k}' must be an integer`);
        if (col.type === 'int' && (v < -2147483648 || v > 2147483647)) throw new BadRequestException(`${where}: '${k}' out of INT range`);
        break;
      case 'bit':
        if (typeof v !== 'boolean') throw new BadRequestException(`${where}: '${k}' must be boolean`);
        break;
      case 'datetime':
        if (typeof v !== 'string' || Number.isNaN(Date.parse(v))) throw new BadRequestException(`${where}: '${k}' must be an ISO date`);
        break;
      default:
        if (typeof v !== 'string') throw new BadRequestException(`${where}: '${k}' must be a string`);
        if (col.length && col.length > 0 && v.length > col.length) {
          throw new BadRequestException(`${where}: '${k}' exceeds ${col.length} characters`);
        }
    }
    out[k] = v as any;
  }
  return out;
}

export function validateId(v: unknown, where: string): number {
  if (typeof v !== 'number' || !Number.isInteger(v) || v <= 0) throw new BadRequestException(`${where}: ${KEY_COLUMN} must be a positive integer`);
  return v;
}

export function validateInsert(body: any, where = 'body') {
  const id = validateId(body?.[KEY_COLUMN], where);
  const patch = validatePatch(body, where);
  for (const c of COLUMNS) {
    if (!c.nullable && c.name !== KEY_COLUMN && (patch[c.name] === undefined || patch[c.name] === null)) {
      throw new BadRequestException(`${where}: '${c.name}' is required`);
    }
  }
  return { [KEY_COLUMN]: id, ...patch } as MuminPatch & { Mumin_ID: number };
}

export function validateBatch<T>(body: any, field: string, each: (item: any, where: string) => T): T[] {
  const arr = body?.[field];
  if (!Array.isArray(arr) || arr.length === 0) throw new BadRequestException(`'${field}' must be a non-empty array`);
  if (arr.length > MAX_BATCH) throw new BadRequestException(`'${field}' max ${MAX_BATCH} items per request`);
  return arr.map((item, i) => each(item, `${field}[${i}]`));
}
