import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as sql from 'mssql';

/**
 * Two pools:
 *  - local  : docker MSSQL mirror of MMS.dbo.Mumin_Mast (read/write, change tracking ON)
 *  - source : stage MMS (READ-ONLY account) used only to import the table; connected lazily
 */
@Injectable()
export class MssqlService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(MssqlService.name);
  private pool: sql.ConnectionPool;
  private sourcePool?: sql.ConnectionPool;

  constructor(private readonly config: ConfigService) {}

  async onModuleInit() {
    this.pool = await this.connect('MSSQL', 20);
    this.logger.log('Local MSSQL pool connected');
  }

  async onModuleDestroy() {
    await this.pool?.close();
    await this.sourcePool?.close();
  }

  request(): sql.Request {
    return this.pool.request();
  }

  transaction(): sql.Transaction {
    return new sql.Transaction(this.pool);
  }

  async sourceRequest(): Promise<sql.Request> {
    if (!this.config.get('SOURCE_MSSQL_HOST')) throw new Error('SOURCE_MSSQL_* is not configured');
    this.sourcePool ??= await this.connect('SOURCE_MSSQL', 2);
    return this.sourcePool.request();
  }

  private connect(prefix: string, max: number): Promise<sql.ConnectionPool> {
    return new sql.ConnectionPool({
      server: this.config.get(`${prefix}_HOST`, 'localhost'),
      port: Number(this.config.get(`${prefix}_PORT`, 1433)),
      user: this.config.get(`${prefix}_USER`),
      password: this.config.get(`${prefix}_PASSWORD`),
      database: this.config.get(`${prefix}_DATABASE`, 'MMS'),
      pool: { max, min: 1, idleTimeoutMillis: 30000 },
      connectionTimeout: 15000,
      requestTimeout: 300000,
      options: { encrypt: false, trustServerCertificate: true },
    }).connect();
  }
}
