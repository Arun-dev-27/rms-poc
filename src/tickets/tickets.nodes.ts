import { BadRequestException, Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { spawn } from 'child_process';
import { mkdirSync, openSync } from 'fs';
import * as http from 'http';
import { join } from 'path';
import Redis from 'ioredis';
import { RedisService } from '../redis/redis.service';

export const NODES_KEY = 'tb:nodes';           // HASH instance -> JSON heartbeat (every live API node)
const PENDING_KEY = 'tb:nodes:pending';        // HASH port -> JSON (started, not answering yet)
const CMD_CHANNEL = 'tb:nodes:cmd';            // "stop:<instance>"
const BEAT_MS = 2000;
const DEAD_MS = 7000;                          // no heartbeat for this long = gone
const FIRST_PORT = 3100;
const MAX_NODES = 16;

export interface NodeInfo {
  instance: string; port: number; pid: number; url: string; startedAt: number; lastSeen: number;
  served: number; rssMb: number; startedBy?: string; alive?: boolean;
}

/**
 * Dynamic API nodes. Every instance heartbeats into Redis (tb:nodes); any instance can start more nodes
 * (independent processes on the next free ports) or stop one (Redis command, the node shuts down cleanly).
 * The load test sends its traffic to every node alive at that moment.
 */
@Injectable()
export class TicketsNodes implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger('TicketsNodes');
  private sub?: Redis;
  private timer?: NodeJS.Timeout;
  private served = 0;
  private readonly startedAt = Date.now();
  readonly port = Number(process.env.PORT ?? 3000);
  readonly instance = `${process.env.INSTANCE_NAME ?? 'app'}:${this.port}`;

  constructor(private readonly redis: RedisService) {}

  async onModuleInit() {
    this.sub = this.redis.client.duplicate();
    this.sub.on('message', (_c, msg) => {
      if (msg === `stop:${this.instance}`) void this.stopSelf();
    });
    await this.sub.subscribe(CMD_CHANNEL);
    await this.beat();
    await this.redis.client.hdel(PENDING_KEY, String(this.port));
    this.timer = setInterval(() => void this.beat(), BEAT_MS);
  }

  async onModuleDestroy() {
    clearInterval(this.timer);
    await this.redis.client.hdel(NODES_KEY, this.instance).catch(() => undefined);
    this.sub?.disconnect();
  }

  /** Counted by the ticket middleware: HTTP requests this node served. */
  count() {
    this.served++;
  }

  private async beat() {
    const info: NodeInfo = {
      instance: this.instance, port: this.port, pid: process.pid, url: `http://localhost:${this.port}`,
      startedAt: this.startedAt, lastSeen: Date.now(), served: this.served,
      rssMb: Math.round(process.memoryUsage().rss / 1048576), startedBy: process.env.STARTED_BY,
    };
    await this.redis.client.hset(NODES_KEY, this.instance, JSON.stringify(info)).catch(() => undefined);
  }

  private async stopSelf() {
    this.logger.warn('Stop command received: shutting down this node');
    clearInterval(this.timer);
    await this.redis.client.hdel(NODES_KEY, this.instance).catch(() => undefined);
    // same path as Ctrl+C: Nest shutdown hooks close the app (hand over leases), then the process exits
    setTimeout(() => process.emit('SIGTERM' as any), 200);
    setTimeout(() => process.exit(0), 15000).unref();
  }

  /** Live nodes (heartbeat seen recently), plus nodes that were started and are still booting. */
  async list() {
    const raw = await this.redis.client.hgetall(NODES_KEY);
    const now = Date.now();
    const nodes: NodeInfo[] = [];
    const dead: string[] = [];
    for (const [k, v] of Object.entries(raw)) {
      const n = JSON.parse(v) as NodeInfo;
      if (now - n.lastSeen > DEAD_MS) dead.push(k);
      else nodes.push({ ...n, alive: true });
    }
    if (dead.length) await this.redis.client.hdel(NODES_KEY, ...dead);
    const pendingRaw = await this.redis.client.hgetall(PENDING_KEY);
    const starting = Object.values(pendingRaw).map((v) => JSON.parse(v) as { port: number; instance: string; at: number })
      .filter((p) => now - p.at < 90000 && !nodes.some((n) => n.port === p.port));
    nodes.sort((a, b) => a.port - b.port);
    return { self: this.instance, nodes, starting };
  }

  async aliveUrls(): Promise<string[]> {
    return (await this.list()).nodes.map((n) => n.url);
  }

  /** Start `count` more nodes on the next free ports (independent processes; they keep running if this one stops). */
  async start(count: number) {
    const { nodes, starting } = await this.list();
    if (nodes.length + starting.length + count > MAX_NODES) throw new BadRequestException(`at most ${MAX_NODES} nodes`);
    const used = new Set([...nodes.map((n) => n.port), ...starting.map((s) => s.port)]);
    const started: { instance: string; port: number; log: string }[] = [];
    let port = FIRST_PORT;
    mkdirSync(join(process.cwd(), 'logs'), { recursive: true });
    while (started.length < count) {
      while (used.has(port) || (await portAnswers(port))) port++;
      used.add(port);
      const name = `node${port - FIRST_PORT + 1}`;
      const log = join('logs', `node-${port}.log`);
      const out = openSync(join(process.cwd(), log), 'a');
      const child = spawn(process.execPath, [join(__dirname, '..', 'main.js')], {
        cwd: process.cwd(), detached: true, windowsHide: true, stdio: ['ignore', out, out],
        env: { ...process.env, PORT: String(port), INSTANCE_NAME: name, STARTED_BY: this.instance },
      });
      child.unref();
      await this.redis.client.hset(PENDING_KEY, String(port), JSON.stringify({ port, instance: `${name}:${port}`, at: Date.now() }));
      started.push({ instance: `${name}:${port}`, port, log });
      this.logger.log(`Started ${name} on :${port} (pid ${child.pid}), log ${log}`);
    }
    return { started };
  }

  async stop(instance: string) {
    if (instance === this.instance) throw new BadRequestException('this page is served by that node; open the UI on another node to stop it');
    const { nodes } = await this.list();
    if (!nodes.some((n) => n.instance === instance)) throw new BadRequestException(`no live node ${instance}`);
    if (nodes.length <= 1) throw new BadRequestException('at least one node must keep running');
    await this.redis.client.publish(CMD_CHANNEL, `stop:${instance}`);
    return { stopping: instance };
  }
}

function portAnswers(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const req = http.get({ host: 'localhost', port, path: '/tickets/zones', timeout: 800 }, (res) => { res.resume(); resolve(true); });
    req.on('timeout', () => { req.destroy(); resolve(false); });
    req.on('error', () => resolve(false));
  });
}
