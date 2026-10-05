import { COLUMNS, ColumnMeta, KEY_COLUMN } from './mumin.columns';

/** One dbo.Mumin_Mast row. Dates are ISO-8601 strings, bits are booleans, nullable columns may be null. */
export type MuminRecord = Record<string, string | number | boolean | null> & { Mumin_ID: number };
export type MuminPatch = Record<string, string | number | boolean | null>;

export const COLUMN_BY_NAME = new Map<string, ColumnMeta>(COLUMNS.map((c) => [c.name, c]));
export const ALL_COLUMNS = COLUMNS.map((c) => c.name);
/** Mumin_ID (= ITS id) is the key and is never updated. */
export const WRITABLE_COLUMNS = ALL_COLUMNS.filter((c) => c !== KEY_COLUMN);
export { KEY_COLUMN };
