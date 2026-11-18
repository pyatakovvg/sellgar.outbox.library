import { Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit, Optional } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ClientProxy } from '@nestjs/microservices';
import { firstValueFrom, timeout } from 'rxjs';

import { OutboxEventModel } from './outbox-event.model';
import { OutboxRepository } from './outbox.repository';
import { resolveOutboxStorage } from './outbox-storage';
import { OUTBOX_EVENT_CLIENT, OUTBOX_OPTIONS } from './outbox.tokens';
import { OutboxModuleOptions } from './outbox.types';

@Injectable()
export class OutboxRelayService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OutboxRelayService.name);
  private readonly storage;
  private publishing?: Promise<void>;
  private stopping = false;
  private publishTimer?: ReturnType<typeof setInterval>;
  private metricsTimer?: ReturnType<typeof setInterval>;

  constructor(
    @Inject(OUTBOX_EVENT_CLIENT) private readonly eventClient: ClientProxy,
    @Inject(OUTBOX_OPTIONS) private readonly options: OutboxModuleOptions,
    private readonly repository: OutboxRepository,
    @Optional() private readonly config?: ConfigService,
  ) {
    this.storage = resolveOutboxStorage(options);
  }

  onModuleInit(): void {
    this.publishTimer = setInterval(
      () => this.startPublishing(),
      this.number(this.options.publishIntervalMs, 'OUTBOX_PUBLISH_INTERVAL_MS', 1000),
    );
    this.metricsTimer = setInterval(
      () => {
        this.logMetrics().catch((error) => this.logger.error(error));
      },
      this.number(this.options.metricsIntervalMs, 'OUTBOX_METRICS_INTERVAL_MS', 30000),
    );
    this.startPublishing();
  }

  async onModuleDestroy(): Promise<void> {
    this.stopping = true;
    if (this.publishTimer) clearInterval(this.publishTimer);
    if (this.metricsTimer) clearInterval(this.metricsTimer);
    await this.publishing?.catch(() => undefined);
  }

  private startPublishing(): void {
    this.publishPending().catch((error) => this.logger.error(error));
  }

  private publishPending(): Promise<void> {
    if (this.stopping) return Promise.resolve();
    this.publishing ??= this.publishBatch().finally(() => {
      this.publishing = undefined;
    });
    return this.publishing;
  }

  private async publishBatch(): Promise<void> {
    const maxAttempts = this.number(this.options.maxAttempts, 'OUTBOX_MAX_ATTEMPTS', 10);
    await this.repository.releaseTimedOutProcessingEvents(
      this.number(this.options.processingTimeoutMs, 'OUTBOX_PROCESSING_TIMEOUT_MS', 60000),
      maxAttempts,
    );
    let remaining = this.number(this.options.batchSize, 'OUTBOX_BATCH_SIZE', 50);

    while (remaining > 0 && !this.stopping) {
      const events = await this.repository.claimPublishableEvents({ batchSize: remaining, maxAttempts });
      if (events.length === 0) break;

      for (const event of events) {
        if (this.stopping) break;
        await this.publish(event, maxAttempts);
      }
      remaining -= events.length;
    }
  }

  private async publish(event: OutboxEventModel, maxAttempts: number): Promise<void> {
    const envelope = {
      eventUuid: event.uuid,
      eventType: event.eventType,
      schemaVersion: event.schemaVersion,
      producer: event.producer,
      aggregateType: event.aggregateType,
      [this.storage.aggregateIdField]: event.aggregateId,
      aggregateVersion: event.aggregateVersion,
      occurredAt: event.occurredAt.toISOString(),
      payload: event.payload,
    };
    const timeoutMs = this.number(this.options.publishTimeoutMs, 'OUTBOX_PUBLISH_TIMEOUT_MS', 5000);

    try {
      await firstValueFrom(
        this.eventClient.emit(event.eventType, envelope).pipe(
          timeout({
            first: timeoutMs,
            with: () => {
              throw new Error(`outbox publish timeout after ${timeoutMs}ms`);
            },
          }),
        ),
      );
      await this.repository.markPublished(event);
    } catch (error) {
      const attempts = event.attempts + 1;
      const baseMs = this.number(this.options.retryBaseDelayMs, 'OUTBOX_RETRY_BASE_DELAY_MS', 1000);
      const maxMs = this.number(this.options.retryMaxDelayMs, 'OUTBOX_RETRY_MAX_DELAY_MS', 60000);
      const delayMs = Math.min(baseMs * 2 ** Math.max(attempts - 1, 0), maxMs);
      await this.repository.markFailed(event, new Date(Date.now() + delayMs), error, attempts >= maxAttempts);
    }
  }

  private async logMetrics(): Promise<void> {
    const metrics = await this.repository.getMetrics();
    if (metrics.pendingCount || metrics.failedCount || metrics.processingCount || metrics.deadCount) {
      this.logger.warn(
        `outbox metrics: pending=${metrics.pendingCount}, failed=${metrics.failedCount}, processing=${metrics.processingCount}, dead=${metrics.deadCount}, oldestAgeSeconds=${metrics.oldestUnpublishedAgeSeconds}`,
      );
    }
  }

  private number(option: number | undefined, key: string, fallback: number): number {
    const value = option ?? Number(this.config?.get<string>(key));
    return Number.isFinite(value) && value > 0 ? value : fallback;
  }
}
