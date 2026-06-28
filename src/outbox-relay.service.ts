import { Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit, Optional } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ClientProxy } from '@nestjs/microservices';
import { SchedulerRegistry } from '@nestjs/schedule';
import { firstValueFrom } from 'rxjs';

import { OutboxEventModel } from './outbox-event.model';
import { OutboxRepository } from './outbox.repository';
import { OUTBOX_EVENT_CLIENT, OUTBOX_OPTIONS } from './outbox.tokens';
import { OutboxModuleOptions } from './outbox.types';

@Injectable()
export class OutboxRelayService implements OnModuleInit, OnModuleDestroy {
  private static readonly PUBLISH_INTERVAL_NAME = 'outbox-publisher';
  private static readonly METRICS_INTERVAL_NAME = 'outbox-metrics';

  private readonly logger = new Logger(OutboxRelayService.name);
  private publishing = false;

  constructor(
    @Inject(OUTBOX_EVENT_CLIENT) private readonly eventClient: ClientProxy,
    @Inject(OUTBOX_OPTIONS) private readonly options: OutboxModuleOptions,
    private readonly outboxRepository: OutboxRepository,
    private readonly schedulerRegistry: SchedulerRegistry,
    @Optional() private readonly config?: ConfigService,
  ) {}

  onModuleInit() {
    const publishInterval = setInterval(() => {
      this.publishPending().catch((error) => {
        this.logger.error(error);
      });
    }, this.getConfiguredNumber(this.options.publishIntervalMs, 'OUTBOX_PUBLISH_INTERVAL_MS', 1000));

    const metricsInterval = setInterval(() => {
      this.logMetrics().catch((error) => {
        this.logger.error(error);
      });
    }, this.getConfiguredNumber(this.options.metricsIntervalMs, 'OUTBOX_METRICS_INTERVAL_MS', 30000));

    this.schedulerRegistry.addInterval(OutboxRelayService.PUBLISH_INTERVAL_NAME, publishInterval);
    this.schedulerRegistry.addInterval(OutboxRelayService.METRICS_INTERVAL_NAME, metricsInterval);
  }

  onModuleDestroy() {
    this.deleteInterval(OutboxRelayService.PUBLISH_INTERVAL_NAME);
    this.deleteInterval(OutboxRelayService.METRICS_INTERVAL_NAME);
  }

  private async publishPending() {
    if (this.publishing) {
      return;
    }

    this.publishing = true;

    try {
      await this.outboxRepository.releaseTimedOutProcessingEvents(
        this.getConfiguredNumber(this.options.processingTimeoutMs, 'OUTBOX_PROCESSING_TIMEOUT_MS', 60000),
      );

      const events = await this.outboxRepository.claimPublishableEvents({
        batchSize: this.getConfiguredNumber(this.options.batchSize, 'OUTBOX_BATCH_SIZE', 50),
        maxAttempts: this.getConfiguredNumber(this.options.maxAttempts, 'OUTBOX_MAX_ATTEMPTS', 10),
      });

      for (const event of events) {
        await this.publishEvent(event);
      }
    } finally {
      this.publishing = false;
    }
  }

  private async publishEvent(event: OutboxEventModel) {
    const envelope = {
      eventUuid: event.uuid,
      eventType: event.eventType,
      schemaVersion: event.schemaVersion,
      producer: event.producer,
      aggregateType: event.aggregateType,
      aggregateUuid: event.aggregateUuid,
      aggregateVersion: event.aggregateVersion,
      occurredAt: event.occurredAt.toISOString(),
      payload: event.payload,
    };

    try {
      await firstValueFrom(this.eventClient.emit(event.eventType, envelope));
      await this.outboxRepository.markPublished(event);
    } catch (error) {
      await this.outboxRepository.markFailed(event, this.getNextAttemptAt(event.attempts + 1), error);
    }
  }

  private async logMetrics() {
    const metrics = await this.outboxRepository.getMetrics();

    if (metrics.pendingCount || metrics.failedCount || metrics.processingCount) {
      this.logger.warn(
        `outbox metrics: pending=${metrics.pendingCount}, failed=${metrics.failedCount}, processing=${metrics.processingCount}, oldestAgeSeconds=${metrics.oldestUnpublishedAgeSeconds}`,
      );
    }
  }

  private getNextAttemptAt(attempts: number) {
    const baseMs = this.getConfiguredNumber(this.options.retryBaseDelayMs, 'OUTBOX_RETRY_BASE_DELAY_MS', 1000);
    const maxMs = this.getConfiguredNumber(this.options.retryMaxDelayMs, 'OUTBOX_RETRY_MAX_DELAY_MS', 60000);
    const delayMs = Math.min(baseMs * 2 ** Math.max(attempts - 1, 0), maxMs);

    return new Date(Date.now() + delayMs);
  }

  private deleteInterval(name: string) {
    if (this.schedulerRegistry.doesExist('interval', name)) {
      this.schedulerRegistry.deleteInterval(name);
    }
  }

  private getConfiguredNumber(optionValue: number | undefined, configKey: string, fallback: number) {
    const value = optionValue ?? Number(this.config?.get<string>(configKey));

    if (!Number.isFinite(value) || value <= 0) {
      return fallback;
    }

    return value;
  }
}
