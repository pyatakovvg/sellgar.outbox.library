import { Injectable } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource, In, LessThan } from 'typeorm';

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

@Injectable()
export class OutboxRepository {
  constructor(@InjectDataSource() private readonly dataSource: DataSource) {}

  async claimPublishableEvents(options: ClaimOptions) {
    return this.dataSource.transaction(async (manager) => {
      const claimedRows: Array<{ uuid: string }> = await manager.query(
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
          RETURNING uuid
        `,
        [options.maxAttempts, options.batchSize],
      );

      const uuids = claimedRows.map((row) => row.uuid);

      if (uuids.length === 0) {
        return [];
      }

      return manager.find(OutboxEventModel, {
        where: { uuid: In(uuids) },
        order: { occurredAt: 'ASC' },
      });
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
}
