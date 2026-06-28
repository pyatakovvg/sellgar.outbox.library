import { Inject, Injectable } from '@nestjs/common';
import { randomUUID } from 'crypto';
import { EntityManager } from 'typeorm';

import { OUTBOX_OPTIONS } from './outbox.tokens';
import { OutboxEventModel } from './outbox-event.model';
import { OutboxModuleOptions } from './outbox.types';

export interface OutboxEventInput {
  eventType: string;
  aggregateType: string;
  aggregateUuid: string;
  aggregateVersion: number;
  payload: Record<string, any>;
  schemaVersion?: number;
  occurredAt?: Date;
}

@Injectable()
export class OutboxWriter {
  constructor(@Inject(OUTBOX_OPTIONS) private readonly options: OutboxModuleOptions) {}

  add(manager: EntityManager, event: OutboxEventInput) {
    return this.addMany(manager, [event]);
  }

  addMany(manager: EntityManager, events: OutboxEventInput[]) {
    if (events.length === 0) {
      return Promise.resolve();
    }

    return manager.insert(
      OutboxEventModel,
      events.map((event) => ({
        uuid: randomUUID(),
        producer: this.options.producer,
        aggregateType: event.aggregateType,
        aggregateUuid: event.aggregateUuid,
        aggregateVersion: event.aggregateVersion,
        eventType: event.eventType,
        schemaVersion: event.schemaVersion ?? 1,
        payload: event.payload,
        occurredAt: event.occurredAt ?? new Date(),
        status: 'pending',
        attempts: 0,
      })),
    );
  }
}
