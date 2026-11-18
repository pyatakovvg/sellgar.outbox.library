import { OutboxModuleOptions } from './outbox.types';

export function resolveOutboxStorage(options?: OutboxModuleOptions) {
  const aggregateIdColumn = options?.storage?.aggregateIdColumn ?? 'aggregate_id';
  const exhaustedStatus = options?.storage?.exhaustedStatus ?? 'failed';
  const aggregateIdField = options?.aggregateIdField ?? 'aggregateId';

  if (aggregateIdColumn !== 'aggregate_id' && aggregateIdColumn !== 'aggregate_uuid') {
    throw new Error('Unsupported outbox aggregate ID column');
  }
  if (exhaustedStatus !== 'failed' && exhaustedStatus !== 'dead') {
    throw new Error('Unsupported outbox exhausted status');
  }
  if (aggregateIdField !== 'aggregateId' && aggregateIdField !== 'aggregateUuid') {
    throw new Error('Unsupported outbox aggregate ID field');
  }

  return {
    aggregateIdColumn,
    hasProducerColumn: options?.storage?.hasProducerColumn ?? true,
    exhaustedStatus,
    aggregateIdField,
  };
}
