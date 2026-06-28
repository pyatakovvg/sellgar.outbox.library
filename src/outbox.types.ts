export interface OutboxModuleOptions {
  producer: string;
  eventClientToken: string;
  publishIntervalMs?: number;
  metricsIntervalMs?: number;
  batchSize?: number;
  maxAttempts?: number;
  retryBaseDelayMs?: number;
  retryMaxDelayMs?: number;
  processingTimeoutMs?: number;
  publishTimeoutMs?: number;
}
