import { Injectable } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource, LessThan } from 'typeorm';

import { OutboxEventModel } from './outbox-event.model';

interface ClaimOptions {
  batchSize: number;
  maxAttempts: number;
}

export interface OutboxMetrics {
  pendingCount: number;
  failedCount: number;
  processingCount: number;
  oldestUnpublishedAgeSeconds: number;
}

type OutboxEventRow = Record<string, any>;

@Injectable()
export class OutboxRepository {
  constructor(@InjectDataSource() private readonly dataSource: DataSource) {}

  async claimPublishableEvents(options: ClaimOptions) {
    return this.dataSource.transaction(async (manager) => {
      const queryResult = await manager.query(
        `
          UPDATE outbox_event
          SET
            status = 'processing',
            processing_started_at = NOW(),
            last_error = NULL
          WHERE uuid IN (
            SELECT uuid
            FROM outbox_event
            WHERE status IN ('pending', 'failed')
              AND attempts < $1
              AND COALESCE(next_attempt_at, occurred_at) <= NOW()
            ORDER BY occurred_at ASC
            LIMIT $2
            FOR UPDATE SKIP LOCKED
          )
          RETURNING
            uuid,
            producer,
            aggregate_type,
            aggregate_uuid,
            aggregate_version,
            event_type,
            schema_version,
            payload,
            occurred_at,
            published_at,
            next_attempt_at,
            processing_started_at,
            status,
            attempts,
            last_error
        `,
        [options.maxAttempts, options.batchSize],
      );
      const claimedRows = this.normalizeQueryRows(queryResult);

      return claimedRows.map((row) => this.mapClaimedRow(row));
    });
  }

  releaseTimedOutProcessingEvents(timeoutMs: number) {
    return this.dataSource.manager.update(
      OutboxEventModel,
      {
        status: 'processing',
        processingStartedAt: LessThan(new Date(Date.now() - timeoutMs)),
      },
      {
        status: 'failed',
        nextAttemptAt: new Date(),
        processingStartedAt: null,
        lastError: 'processing timeout',
      },
    );
  }

  markPublished(event: OutboxEventModel) {
    return this.dataSource.manager.update(OutboxEventModel, event.uuid, {
      status: 'published',
      publishedAt: new Date(),
      nextAttemptAt: null,
      processingStartedAt: null,
      attempts: event.attempts + 1,
      lastError: null,
    });
  }

  markFailed(event: OutboxEventModel, nextAttemptAt: Date, error: unknown) {
    return this.dataSource.manager.update(OutboxEventModel, event.uuid, {
      status: 'failed',
      nextAttemptAt,
      processingStartedAt: null,
      attempts: event.attempts + 1,
      lastError: error instanceof Error ? error.message : String(error),
    });
  }

  async getMetrics(): Promise<OutboxMetrics> {
    const [metrics] = await this.dataSource.manager.query(`
      SELECT
        COUNT(*) FILTER (WHERE status = 'pending')::int AS "pendingCount",
        COUNT(*) FILTER (WHERE status = 'failed')::int AS "failedCount",
        COUNT(*) FILTER (WHERE status = 'processing')::int AS "processingCount",
        COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(occurred_at) FILTER (WHERE status IN ('pending', 'failed', 'processing'))))::int, 0) AS "oldestUnpublishedAgeSeconds"
      FROM outbox_event
    `);

    return {
      pendingCount: Number(metrics.pendingCount),
      failedCount: Number(metrics.failedCount),
      processingCount: Number(metrics.processingCount),
      oldestUnpublishedAgeSeconds: Number(metrics.oldestUnpublishedAgeSeconds),
    };
  }

  private mapClaimedRow(row: OutboxEventRow) {
    const event = new OutboxEventModel();

    event.uuid = this.readRowValue(row, 'uuid');
    event.producer = this.readRowValue(row, 'producer');
    event.aggregateType = this.readRowValue(row, 'aggregate_type', 'aggregateType', 'aggregatetype');
    event.aggregateUuid = this.readRowValue(row, 'aggregate_uuid', 'aggregateUuid', 'aggregateuuid');
    event.aggregateVersion = Number(this.readRowValue(row, 'aggregate_version', 'aggregateVersion', 'aggregateversion'));
    event.eventType = this.readRowValue(row, 'event_type', 'eventType', 'eventtype');
    event.schemaVersion = Number(this.readRowValue(row, 'schema_version', 'schemaVersion', 'schemaversion'));
    event.payload = this.readRowValue(row, 'payload');
    event.occurredAt = this.readRowDate(row, 'occurred_at', 'occurredAt', 'occurredat');
    event.publishedAt = this.readOptionalRowDate(row, 'published_at', 'publishedAt', 'publishedat');
    event.nextAttemptAt = this.readOptionalRowDate(row, 'next_attempt_at', 'nextAttemptAt', 'nextattemptat');
    event.processingStartedAt = this.readOptionalRowDate(
      row,
      'processing_started_at',
      'processingStartedAt',
      'processingstartedat',
    );
    event.status = this.readRowValue(row, 'status');
    event.attempts = Number(this.readRowValue(row, 'attempts'));
    event.lastError = this.readRowValue(row, 'last_error', 'lastError', 'lasterror') ?? null;

    return event;
  }

  private normalizeQueryRows(queryResult: unknown): OutboxEventRow[] {
    if (Array.isArray(queryResult) && Array.isArray(queryResult[0])) {
      return queryResult[0] as OutboxEventRow[];
    }

    if (Array.isArray(queryResult)) {
      return queryResult as OutboxEventRow[];
    }

    if (queryResult && typeof queryResult === 'object' && Array.isArray((queryResult as { raw?: unknown }).raw)) {
      return (queryResult as { raw: OutboxEventRow[] }).raw;
    }

    return [];
  }

  private readRowValue(row: OutboxEventRow, ...keys: string[]) {
    for (const key of keys) {
      if (Object.prototype.hasOwnProperty.call(row, key)) {
        return row[key];
      }
    }

    return undefined;
  }

  private readRowDate(row: OutboxEventRow, ...keys: string[]) {
    const value = this.readRowValue(row, ...keys);
    const date = new Date(value);

    if (Number.isNaN(date.getTime())) {
      throw new Error(`invalid outbox date value for keys ${keys.join(', ')}`);
    }

    return date;
  }

  private readOptionalRowDate(row: OutboxEventRow, ...keys: string[]) {
    const value = this.readRowValue(row, ...keys);

    return value ? new Date(value) : null;
  }
}
