import { Inject, Injectable } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { EntityManager } from 'typeorm';

import { resolveOutboxStorage } from './outbox-storage';
import { OUTBOX_OPTIONS } from './outbox.tokens';
import { OutboxModuleOptions } from './outbox.types';

export interface OutboxEventInput {
  eventType: string;
  aggregateType: string;
  aggregateId: string;
  aggregateVersion: number | string;
  payload: Record<string, unknown>;
  schemaVersion?: number;
  occurredAt?: Date;
}

@Injectable()
export class OutboxWriter {
  private readonly storage;

  constructor(@Inject(OUTBOX_OPTIONS) private readonly options: OutboxModuleOptions) {
    this.storage = resolveOutboxStorage(options);
  }

  add(manager: EntityManager, event: OutboxEventInput): Promise<void> {
    return this.addMany(manager, [event]);
  }

  async addMany(manager: EntityManager, events: OutboxEventInput[]): Promise<void> {
    if (events.length === 0) return;

    const columns = [
      'uuid',
      ...(this.storage.hasProducerColumn ? ['producer'] : []),
      'aggregate_type',
      this.storage.aggregateIdColumn,
      'aggregate_version',
      'event_type',
      'schema_version',
      'payload',
      'occurred_at',
      'status',
      'attempts',
    ];
    const parameters: unknown[] = [];
    const rows = events.map((event) => {
      const values = [
        randomUUID(),
        ...(this.storage.hasProducerColumn ? [this.options.producer] : []),
        event.aggregateType,
        event.aggregateId,
        event.aggregateVersion,
        event.eventType,
        event.schemaVersion ?? 1,
        JSON.stringify(event.payload),
        event.occurredAt ?? new Date(),
        'pending',
        0,
      ];
      return `(${values
        .map((value) => {
          parameters.push(value);
          return `$${parameters.length}`;
        })
        .join(', ')})`;
    });

    await manager.query(`INSERT INTO outbox_event (${columns.join(', ')}) VALUES ${rows.join(', ')}`, parameters);
  }
}
