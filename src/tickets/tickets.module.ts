import { MiddlewareConsumer, Module, NestModule } from '@nestjs/common';
import { TicketsController } from './tickets.controller';
import { TicketsLoadTest } from './tickets.loadtest';
import { TicketsNodes } from './tickets.nodes';
import { TicketsOpLog } from './tickets.oplog';
import { TicketsService } from './tickets.service';

@Module({
  controllers: [TicketsController],
  providers: [TicketsOpLog, TicketsService, TicketsNodes, TicketsLoadTest],
})
export class TicketsModule implements NestModule {
  constructor(private readonly nodes: TicketsNodes) {}

  /** Count every /tickets HTTP request this node serves (shown per node in the UI and the load test). */
  configure(consumer: MiddlewareConsumer) {
    consumer.apply((_req: unknown, _res: unknown, next: () => void) => { this.nodes.count(); next(); }).forRoutes('tickets');
  }
}
