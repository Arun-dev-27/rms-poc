import { Injectable } from '@nestjs/common';
import * as sql from 'mssql';
import { MssqlService } from '../database/mssql.service';
import { COLUMNS, ColumnMeta, TABLE } from './mumin.columns';
import { ALL_COLUMNS, COLUMN_BY_NAME, KEY_COLUMN, MuminPatch, MuminRecord, WRITABLE_COLUMNS } from './mumin.types';

const q = (c: string) => `[${c}]`;
const SELECT_COLS = ALL_COLUMNS.map((c) => `m.${q(c)}`).join(', ');
const OUTPUT_COLS = ALL_COLUMNS.map((c) => `inserted.${q(c)}`).join(', ');
const OPENJSON_WITH = COLUMNS.map((c) => `${q(c.name)} ${c.sql} '$.${c.name}'`).join(', ');
/** Rows per SQL statement inside a batch (keeps JSON parameter & lock footprint small). */
const SQL_CHUNK = 1000;

export type MuminInsert = MuminPatch & { Mumin_ID: number };
export interface ChangeBatch { upserts: MuminRecord[]; deletes: number[] }

export function mapRow(row: any): MuminRecord {
  for (const k of Object.keys(row)) if (row[k] instanceof Date) row[k] = row[k].toISOString();
  return row;
}

export function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

export function driverType(c: ColumnMeta): sql.ISqlType | (() => sql.ISqlType) {
  switch (c.type) {
    case 'int': return sql.Int();
    case 'numeric': return sql.Numeric(c.precision ?? 18, c.scale ?? 0);
    case 'bit': return sql.Bit();
    case 'datetime': return sql.DateTime();
    case 'nvarchar': return sql.NVarChar(c.length && c.length > 0 ? c.length : sql.MAX);
    default: return sql.VarChar(c.length && c.length > 0 ? c.length : sql.MAX);
  }
}

/** JSON for OPENJSON: datetimes as ISO without 'Z' so the implicit DATETIME conversion is unambiguous. */
function toSqlJson(rows: MuminPatch[]): string {
  return JSON.stringify(rows.map((r) => {
    const o: MuminPatch = {};
    for (const [k, v] of Object.entries(r)) {
      o[k] = COLUMN_BY_NAME.get(k)?.type === 'datetime' && typeof v === 'string' ? new Date(v).toISOString().slice(0, 23) : v;
    }
    return o;
  }));
}

@Injectable()
export class MuminRepository {
  constructor(private readonly db: MssqlService) {}

  async count(): Promise<number> {
    const r = await this.db.request().query(`SELECT COUNT_BIG(*) AS c FROM ${TABLE}`);
    return Number(r.recordset[0].c);
  }

  async findById(id: number): Promise<MuminRecord | null> {
    const r = await this.db.request()
      .input('id', sql.Int, id)
      .query(`SELECT ${SELECT_COLS} FROM ${TABLE} m WHERE m.${KEY_COLUMN} = @id`);
    return r.recordset[0] ? mapRow(r.recordset[0]) : null;
  }

  async findByIds(ids: number[]): Promise<MuminRecord[]> {
    const out: MuminRecord[] = [];
    for (const part of chunk(ids, SQL_CHUNK)) {
      const r = await this.db.request()
        .input('ids', sql.NVarChar(sql.MAX), JSON.stringify(part))
        .query(`SELECT ${SELECT_COLS} FROM ${TABLE} m
                WHERE m.${KEY_COLUMN} IN (SELECT CAST([value] AS INT) FROM OPENJSON(@ids))`);
      out.push(...r.recordset.map(mapRow));
    }
    return out;
  }

  /** Single-row partial update; returns the full row after the update (OUTPUT inserted.*). */
  async updateOne(id: number, patch: MuminPatch): Promise<MuminRecord | null> {
    const cols = WRITABLE_COLUMNS.filter((c) => c in patch);
    if (!cols.length) return this.findById(id);
    const req = this.db.request().input('id', sql.Int, id);
    cols.forEach((c, i) => {
      const meta = COLUMN_BY_NAME.get(c)!;
      const v = patch[c];
      req.input(`p${i}`, driverType(meta), meta.type === 'datetime' && v !== null ? new Date(v as string) : v);
    });
    const set = cols.map((c, i) => `${q(c)} = @p${i}`).join(', ');
    const r = await req.query(`UPDATE ${TABLE} SET ${set} OUTPUT ${OUTPUT_COLS} WHERE ${KEY_COLUMN} = @id`);
    return r.recordset[0] ? mapRow(r.recordset[0]) : null;
  }

  /**
   * Batch partial update in ONE transaction. Each chunk is one set-based UPDATE ... FROM OPENJSON;
   * JSON_PATH_EXISTS lets every row carry its own set of fields (absent field = unchanged, null = set NULL).
   */
  async updateMany(items: MuminInsert[]): Promise<MuminRecord[]> {
    const set = WRITABLE_COLUMNS.map(
      (c) => `m.${q(c)} = CASE WHEN JSON_PATH_EXISTS(j.[__raw], '$.${c}') = 1 THEN j.${q(c)} ELSE m.${q(c)} END`,
    ).join(', ');
    return this.inTransaction(async (tx) => {
      const out: MuminRecord[] = [];
      for (const part of chunk(items, SQL_CHUNK)) {
        const r = await new sql.Request(tx)
          .input('json', sql.NVarChar(sql.MAX), toSqlJson(part))
          .query(`UPDATE m SET ${set}
                  OUTPUT ${OUTPUT_COLS}
                  FROM ${TABLE} m
                  JOIN OPENJSON(@json) WITH (${OPENJSON_WITH}, [__raw] NVARCHAR(MAX) '$' AS JSON) j
                    ON j.${KEY_COLUMN} = m.${KEY_COLUMN}`);
        out.push(...r.recordset.map(mapRow));
      }
      return out;
    });
  }

  /** Batch insert in ONE transaction via INSERT ... SELECT FROM OPENJSON (chunked). */
  async insertMany(items: MuminInsert[]): Promise<MuminRecord[]> {
    const cols = ALL_COLUMNS.map(q).join(', ');
    return this.inTransaction(async (tx) => {
      const out: MuminRecord[] = [];
      for (const part of chunk(items, SQL_CHUNK)) {
        const r = await new sql.Request(tx)
          .input('json', sql.NVarChar(sql.MAX), toSqlJson(part))
          .query(`INSERT INTO ${TABLE} (${cols}) OUTPUT ${OUTPUT_COLS}
                  SELECT ${cols} FROM OPENJSON(@json) WITH (${OPENJSON_WITH})`);
        out.push(...r.recordset.map(mapRow));
      }
      return out;
    });
  }

  /**
   * Idempotent full-row upsert used by the write-behind worker (UPDATE existing + INSERT missing,
   * one transaction; two statements instead of MERGE to avoid MERGE's concurrency pitfalls).
   */
  async upsertMany(rows: MuminRecord[]): Promise<void> {
    const cols = ALL_COLUMNS.map(q).join(', ');
    const set = WRITABLE_COLUMNS.map((c) => `m.${q(c)} = j.${q(c)}`).join(', ');
    await this.inTransaction(async (tx) => {
      for (const part of chunk(rows, SQL_CHUNK)) {
        await new sql.Request(tx)
          .input('json', sql.NVarChar(sql.MAX), toSqlJson(part))
          .query(`SELECT * INTO #j FROM OPENJSON(@json) WITH (${OPENJSON_WITH});
                  UPDATE m SET ${set} FROM ${TABLE} m WITH (UPDLOCK) JOIN #j j ON j.${KEY_COLUMN} = m.${KEY_COLUMN};
                  INSERT INTO ${TABLE} (${cols}) SELECT ${cols} FROM #j j
                    WHERE NOT EXISTS (SELECT 1 FROM ${TABLE} m WITH (UPDLOCK, HOLDLOCK) WHERE m.${KEY_COLUMN} = j.${KEY_COLUMN});
                  DROP TABLE #j;`);
      }
    });
  }

  async deleteByIds(ids: number[]): Promise<number> {
    let n = 0;
    for (const part of chunk(ids, SQL_CHUNK)) {
      const r = await this.db.request()
        .input('ids', sql.NVarChar(sql.MAX), JSON.stringify(part))
        .query(`DELETE FROM ${TABLE} WHERE ${KEY_COLUMN} IN (SELECT CAST([value] AS INT) FROM OPENJSON(@ids))`);
      n += r.rowsAffected[0];
    }
    return n;
  }

  // ---------- bulk / streaming (cache load, snapshot delta, stage import) ----------

  /** Streams every row in batches with back-pressure (SQL stream paused while the consumer writes). */
  streamAll(batchSize: number, onBatch: (rows: MuminRecord[]) => Promise<void>): Promise<number> {
    return streamQuery(this.db.request(), `SELECT ${SELECT_COLS} FROM ${TABLE} m`, batchSize, onBatch);
  }

  /** SQL Server Change Tracking version: captured with a snapshot, used for the delta on restore. */
  async ctVersions(): Promise<{ current: number; minValid: number }> {
    const r = await this.db.request().query(
      `SELECT CHANGE_TRACKING_CURRENT_VERSION() AS cur, CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('${TABLE}')) AS minv`,
    );
    return { current: Number(r.recordset[0].cur ?? 0), minValid: Number(r.recordset[0].minv ?? 0) };
  }

  /** Every insert/update/delete after `sinceVersion`, current row values joined in. */
  async streamChanges(sinceVersion: number, batchSize: number, onBatch: (b: ChangeBatch) => Promise<void>): Promise<number> {
    const req = this.db.request().input('v', sql.BigInt, sinceVersion);
    return streamQuery(
      req,
      `SELECT ct.SYS_CHANGE_OPERATION AS [__op], ct.${KEY_COLUMN} AS [__id], ${SELECT_COLS}
       FROM CHANGETABLE(CHANGES ${TABLE}, @v) ct
       LEFT JOIN ${TABLE} m ON m.${KEY_COLUMN} = ct.${KEY_COLUMN}`,
      batchSize,
      async (rows: any[]) => {
        const b: ChangeBatch = { upserts: [], deletes: [] };
        for (const r of rows) {
          const { __op, __id, ...rec } = r;
          if (__op === 'D' || rec[KEY_COLUMN] == null) b.deletes.push(__id);
          else b.upserts.push(rec);
        }
        await onBatch(b);
      },
    );
  }

  async truncate(): Promise<void> {
    await this.db.request().query(`TRUNCATE TABLE ${TABLE}`);
  }

  /** Fastest insert path (TDS bulk copy) - used by the stage import. */
  async bulkInsert(rows: any[]): Promise<void> {
    const table = new sql.Table(TABLE);
    table.create = false;
    COLUMNS.forEach((c) => table.columns.add(c.name, driverType(c), { nullable: c.nullable }));
    for (const r of rows) {
      table.rows.add(...COLUMNS.map((c) => (c.type === 'datetime' && r[c.name] != null ? new Date(r[c.name]) : r[c.name])));
    }
    await this.db.request().bulk(table, { tableLock: true } as any);
  }

  private async inTransaction<T>(work: (tx: sql.Transaction) => Promise<T>): Promise<T> {
    const tx = this.db.transaction();
    await tx.begin(sql.ISOLATION_LEVEL.READ_COMMITTED);
    try {
      const result = await work(tx);
      await tx.commit();
      return result;
    } catch (e) {
      // SQL Server may already have aborted the transaction (e.g. conversion error); a failing
      // rollback must not hide the original error, which callers inspect (error number).
      await tx.rollback().catch(() => undefined);
      throw e;
    }
  }
}

/** Batches the consumer may be writing while SQL keeps streaming (overlaps DB read with Redis write). */
const MAX_IN_FLIGHT = 2;

/**
 * Generic streamed query with bounded back-pressure; raw rows are passed through mapRow.
 * Batches are handed to onBatch in order (serially); the SQL stream is paused only when
 * MAX_IN_FLIGHT batches are already queued, so reading and writing overlap.
 */
export function streamQuery(req: sql.Request, query: string, batchSize: number, onBatch: (rows: any[]) => Promise<void>): Promise<number> {
  return new Promise((resolve, reject) => {
    req.stream = true;
    let buf: any[] = [];
    let total = 0;
    let failed = false;
    let inFlight = 0;
    let paused = false;
    let pending: Promise<void> = Promise.resolve();
    const fail = (e: unknown) => {
      if (failed) return;
      failed = true;
      req.cancel();
      reject(e);
    };
    const flush = () => {
      const rows = buf;
      buf = [];
      inFlight++;
      if (inFlight >= MAX_IN_FLIGHT && !paused) { paused = true; req.pause(); }
      pending = pending.then(() => onBatch(rows)).then(() => {
        inFlight--;
        if (paused && inFlight < MAX_IN_FLIGHT) { paused = false; req.resume(); }
      });
      pending.catch(fail);
    };
    req.on('row', (row) => {
      buf.push(mapRow(row));
      total++;
      if (buf.length >= batchSize) flush();
    });
    req.on('error', fail);
    req.on('done', () => {
      if (buf.length) flush();
      pending.then(() => { if (!failed) resolve(total); }, () => undefined);
    });
    req.query(query);
  });
}
