import { Inject, Injectable, Optional } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource } from 'typeorm';

import { OutboxEventModel } from './outbox-event.model';
import { resolveOutboxStorage } from './outbox-storage';
import { OUTBOX_OPTIONS } from './outbox.tokens';
import { OutboxModuleOptions } from './outbox.types';

interface ClaimOptions {
  batchSize: number;
  maxAttempts: number;
}

export interface OutboxMetrics {
  pendingCount: number;
  failedCount: number;
  processingCount: number;
  deadCount: number;
  oldestUnpublishedAgeSeconds: number;
}

type OutboxEventRow = Record<string, any>;

@Injectable()
export class OutboxRepository {
  private readonly storage;

  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    @Optional() @Inject(OUTBOX_OPTIONS) private readonly options?: OutboxModuleOptions,
  ) {
    this.storage = resolveOutboxStorage(options);
  }

  async claimPublishableEvents(options: ClaimOptions): Promise<OutboxEventModel[]> {
    const idColumn = this.storage.aggregateIdColumn;
    const producerMatch = this.storage.hasProducerColumn ? 'AND predecessor.producer = candidate.producer' : '';

    return this.dataSource.transaction(async (manager) => {
      const result = await manager.query(
        `
        WITH claimed AS (
          UPDATE outbox_event AS event
          SET status = 'processing', processing_started_at = NOW(), last_error = NULL
          WHERE event.uuid IN (
            SELECT candidate.uuid FROM outbox_event AS candidate
            WHERE candidate.status IN ('pending', 'failed')
              AND candidate.attempts < $1
              AND COALESCE(candidate.next_attempt_at, candidate.occurred_at) <= NOW()
              AND NOT EXISTS (
                SELECT 1 FROM outbox_event AS predecessor
                WHERE predecessor.aggregate_type = candidate.aggregate_type
                  ${producerMatch}
                  AND predecessor.${idColumn} = candidate.${idColumn}
                  AND predecessor.aggregate_version < candidate.aggregate_version
                  AND predecessor.status <> 'published'
              )
            ORDER BY candidate.occurred_at ASC, candidate.uuid ASC
            LIMIT $2 FOR UPDATE OF candidate SKIP LOCKED
          )
          RETURNING event.*
        )
        SELECT * FROM claimed ORDER BY occurred_at ASC, uuid ASC
      `,
        [options.maxAttempts, options.batchSize],
      );

      return this.rows(result).map((row) =>
        Object.assign(new OutboxEventModel(), {
          uuid: row.uuid,
          producer: this.storage.hasProducerColumn ? row.producer : this.options?.producer,
          aggregateType: row.aggregate_type,
          aggregateId: row[idColumn],
          aggregateVersion: row.aggregate_version,
          eventType: row.event_type,
          schemaVersion: Number(row.schema_version),
          payload: row.payload,
          occurredAt: new Date(row.occurred_at),
          publishedAt: row.published_at ? new Date(row.published_at) : null,
          nextAttemptAt: row.next_attempt_at ? new Date(row.next_attempt_at) : null,
          processingStartedAt: row.processing_started_at ? new Date(row.processing_started_at) : null,
          status: row.status,
          attempts: Number(row.attempts),
          lastError: row.last_error,
        }),
      );
    });
  }

  releaseTimedOutProcessingEvents(timeoutMs: number, maxAttempts = 10) {
    return this.dataSource.manager.query(
      `
      UPDATE outbox_event
      SET status = CASE WHEN attempts >= $2 THEN $3 ELSE 'failed' END,
        next_attempt_at = NOW(), processing_started_at = NULL, last_error = 'processing timeout'
      WHERE status = 'processing'
        AND processing_started_at < NOW() - ($1 * INTERVAL '1 millisecond')
    `,
      [timeoutMs, maxAttempts, this.storage.exhaustedStatus],
    );
  }

  markPublished(event: OutboxEventModel) {
    return this.dataSource.manager.query(
      `
      UPDATE outbox_event SET status = 'published', published_at = NOW(), next_attempt_at = NULL,
        processing_started_at = NULL, attempts = $2, last_error = NULL
      WHERE uuid = $1
    `,
      [event.uuid, event.attempts + 1],
    );
  }

  markFailed(event: OutboxEventModel, nextAttemptAt: Date, error: unknown, exhausted = false) {
    return this.dataSource.manager.query(
      `
      UPDATE outbox_event SET status = $5, next_attempt_at = $3, processing_started_at = NULL,
        attempts = $2, last_error = $4 WHERE uuid = $1
    `,
      [
        event.uuid,
        event.attempts + 1,
        nextAttemptAt,
        error instanceof Error ? error.message : String(error),
        exhausted ? this.storage.exhaustedStatus : 'failed',
      ],
    );
  }

  async getMetrics(): Promise<OutboxMetrics> {
    const [metrics] = await this.dataSource.manager.query(`
      SELECT
        COUNT(*) FILTER (WHERE status = 'pending')::int AS "pendingCount",
        COUNT(*) FILTER (WHERE status = 'failed')::int AS "failedCount",
        COUNT(*) FILTER (WHERE status = 'processing')::int AS "processingCount",
        COUNT(*) FILTER (WHERE status = 'dead')::int AS "deadCount",
        COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(occurred_at) FILTER
          (WHERE status <> 'published')))::int, 0) AS "oldestUnpublishedAgeSeconds"
      FROM outbox_event
    `);
    return metrics;
  }

  private rows(result: unknown): OutboxEventRow[] {
    if (Array.isArray(result) && Array.isArray(result[0])) return result[0];
    if (Array.isArray(result)) return result;
    if (result && typeof result === 'object' && 'raw' in result && Array.isArray(result.raw)) return result.raw;
    throw new Error('Unsupported outbox query result');
  }
}
