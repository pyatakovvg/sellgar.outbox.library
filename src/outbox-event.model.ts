import { Column, CreateDateColumn, Entity, Index, PrimaryColumn } from 'typeorm';

@Entity('outbox_event')
@Index(['aggregateType', 'aggregateId', 'aggregateVersion'], { unique: true })
@Index(['status', 'nextAttemptAt', 'occurredAt'])
export class OutboxEventModel {
  @PrimaryColumn('uuid', { name: 'uuid' })
  uuid: string;

  @Column({ name: 'producer', type: 'varchar', length: 128 })
  producer: string;

  @Column({ name: 'aggregate_type', type: 'varchar', length: 128 })
  aggregateType: string;

  @Column({ name: 'aggregate_id', type: 'varchar', length: 256 })
  aggregateId: string;

  @Column({ name: 'aggregate_version', type: 'int' })
  aggregateVersion: number | string;

  @Column({ name: 'event_type', type: 'varchar', length: 256 })
  eventType: string;

  @Column({ name: 'schema_version', type: 'int' })
  schemaVersion: number;

  @Column({ name: 'payload', type: 'jsonb' })
  payload: Record<string, unknown>;

  @CreateDateColumn({
    name: 'occurred_at',
    type: 'timestamptz',
    default: () => 'CURRENT_TIMESTAMP',
  })
  occurredAt: Date;

  @Column({ name: 'published_at', type: 'timestamptz', nullable: true })
  publishedAt: Date | null;

  @Column({
    name: 'next_attempt_at',
    type: 'timestamptz',
    nullable: true,
    default: () => 'CURRENT_TIMESTAMP',
  })
  nextAttemptAt: Date | null;

  @Column({
    name: 'processing_started_at',
    type: 'timestamptz',
    nullable: true,
  })
  processingStartedAt: Date | null;

  @Column({ name: 'status', type: 'varchar', length: 64, default: 'pending' })
  status: string;

  @Column({ name: 'attempts', type: 'int', default: 0 })
  attempts: number;

  @Column({ name: 'last_error', type: 'text', nullable: true })
  lastError: string | null;
}
