export interface OutboxModuleOptions {
  producer: string;
  eventClientToken: string;
  storage?: {
    aggregateIdColumn?: 'aggregate_id' | 'aggregate_uuid';
    hasProducerColumn?: boolean;
    exhaustedStatus?: 'failed' | 'dead';
  };
  aggregateIdField?: 'aggregateId' | 'aggregateUuid';
  publishIntervalMs?: number;
  metricsIntervalMs?: number;
  batchSize?: number;
  maxAttempts?: number;
  retryBaseDelayMs?: number;
  retryMaxDelayMs?: number;
  processingTimeoutMs?: number;
  publishTimeoutMs?: number;
}
