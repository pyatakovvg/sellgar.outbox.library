import { Inject, Injectable } from "@nestjs/common";
import { randomUUID } from "node:crypto";
import { EntityManager } from "typeorm";

import { OUTBOX_OPTIONS } from "./outbox.tokens";
import { OutboxEventModel } from "./outbox-event.model";
import { OutboxModuleOptions } from "./outbox.types";

export interface OutboxEventInput {
  eventType: string;
  aggregateType: string;
  aggregateId: string;
  aggregateVersion: number;
  payload: Record<string, unknown>;
  schemaVersion?: number;
  occurredAt?: Date;
}

@Injectable()
export class OutboxWriter {
  constructor(
    @Inject(OUTBOX_OPTIONS) private readonly options: OutboxModuleOptions,
  ) {}

  add(manager: EntityManager, event: OutboxEventInput): Promise<void> {
    return this.addMany(manager, [event]);
  }

  async addMany(
    manager: EntityManager,
    events: OutboxEventInput[],
  ): Promise<void> {
    if (events.length === 0) {
      return;
    }

    const models = events.map((event) =>
      manager.create(OutboxEventModel, {
        uuid: randomUUID(),
        producer: this.options.producer,
        aggregateType: event.aggregateType,
        aggregateId: event.aggregateId,
        aggregateVersion: event.aggregateVersion,
        eventType: event.eventType,
        schemaVersion: event.schemaVersion ?? 1,
        payload: event.payload,
        occurredAt: event.occurredAt ?? new Date(),
        status: "pending",
        attempts: 0,
      }),
    );
    await manager.getRepository(OutboxEventModel).save(models);
  }
}
