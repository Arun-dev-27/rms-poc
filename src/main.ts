import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { NestExpressApplication } from '@nestjs/platform-express';
import { join } from 'path';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create<NestExpressApplication>(AppModule);
  app.useBodyParser('json', { limit: '50mb' });
  app.useStaticAssets(join(__dirname, '..', 'public')); // UI at http://localhost:3000/
  const instance = `${process.env.INSTANCE_NAME ?? 'app'}:${process.env.PORT ?? 3000}`;
  app.use((_req: unknown, res: { setHeader(k: string, v: string): void }, next: () => void) => { res.setHeader('X-Instance', instance); next(); });
  app.enableShutdownHooks();
  const port = Number(process.env.PORT ?? 3000);
  await app.listen(port);
  // Keep idle keep-alive connections longer than clients/load balancers do (default 5 s causes
  // ECONNRESET when a client reuses a socket the server is closing at that exact moment).
  const server = app.getHttpServer();
  server.keepAliveTimeout = 65000;
  server.headersTimeout = 66000;
  Logger.log(`RMS API listening on :${port}`, 'Bootstrap');
}
bootstrap();
