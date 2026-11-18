require('reflect-metadata');

const assert = require('node:assert/strict');
const { describe, it } = require('node:test');
const { NEVER, Observable, of, throwError } = require('rxjs');

const { OutboxRelayService } = require('../dist/outbox-relay.service');
const { resolveOutboxStorage } = require('../dist/outbox-storage');

function fixture(emit, options = {}, eventOverrides = {}) {
  const event = {
    uuid: 'event-uuid',
    producer: 'test',
    aggregateType: 'product',
    aggregateId: 'product-a',
    aggregateVersion: 4,
    eventType: 'product.updated',
    schemaVersion: 2,
    payload: { name: 'Product' },
    occurredAt: new Date('2026-01-01T00:00:00Z'),
    attempts: 0,
    ...eventOverrides,
  };
  const published = [];
  const failed = [];
  let claimed = false;
  const repository = {
    releaseTimedOutProcessingEvents: async () => undefined,
    claimPublishableEvents: async () => {
      if (claimed) return [];
      claimed = true;
      return [event];
    },
    markPublished: async (value) => published.push(value),
    markFailed: async (...args) => failed.push(args),
  };
  const relay = new OutboxRelayService(
    { emit },
    {
      producer: 'test',
      eventClientToken: 'test',
      publishTimeoutMs: 10,
      ...options,
    },
    repository,
  );
  return { event, relay, published, failed };
}

describe('Shared outbox delivery', () => {
  it('preserves the canonical integration envelope', async () => {
    let envelope;
    const state = fixture((_type, value) => {
      envelope = value;
      return of(null);
    });
    await state.relay.publishPending();
    assert.deepEqual(envelope, {
      eventUuid: state.event.uuid,
      eventType: 'product.updated',
      schemaVersion: 2,
      producer: 'test',
      aggregateType: 'product',
      aggregateId: 'product-a',
      aggregateVersion: 4,
      occurredAt: '2026-01-01T00:00:00.000Z',
      payload: { name: 'Product' },
    });
    assert.equal(state.published.length, 1);
    assert.equal(state.failed.length, 0);
  });

  it('keeps aggregateUuid and bigint wire contracts without exposing storage fields', async () => {
    let envelope;
    const state = fixture(
      (_type, value) => {
        envelope = value;
        return of(null);
      },
      {
        aggregateIdField: 'aggregateUuid',
      },
      { aggregateVersion: '9007199254740993' },
    );
    await state.relay.publishPending();
    assert.equal(envelope.aggregateUuid, 'product-a');
    assert.equal(envelope.aggregateVersion, '9007199254740993');
    assert.equal('aggregateId' in envelope, false);
  });

  it('marks publish failures for retry and marks exhaustion explicitly', async () => {
    const failure = new Error('broker unavailable');
    const state = fixture(() => throwError(() => failure), { maxAttempts: 2 }, { attempts: 1 });
    await state.relay.publishPending();
    assert.equal(state.published.length, 0);
    assert.equal(state.failed[0][0], state.event);
    assert.equal(state.failed[0][2], failure);
    assert.equal(state.failed[0][3], true);
    assert.ok(state.failed[0][1] > new Date());
  });

  it('times out a stalled transport and releases its observable subscription', async () => {
    let unsubscribed = false;
    const state = fixture(
      () =>
        new Observable(() => () => {
          unsubscribed = true;
        }),
    );
    await state.relay.publishPending();
    assert.match(state.failed[0][2].message, /publish timeout/);
    assert.equal(unsubscribed, true);
  });

  it('waits for active delivery on shutdown and does not start another cycle', async () => {
    let started;
    const ready = new Promise((resolve) => {
      started = resolve;
    });
    const state = fixture(() => {
      started();
      return NEVER;
    });
    const delivery = state.relay.publishPending();
    await ready;
    const shutdown = state.relay.onModuleDestroy();
    await Promise.all([delivery, shutdown]);
    await state.relay.publishPending();
    assert.equal(state.failed.length + state.published.length, 1);
  });

  it('rejects unsupported SQL identifiers at configuration time', () => {
    assert.throws(() => resolveOutboxStorage({ storage: { aggregateIdColumn: 'unsafe' } }), /Unsupported/);
  });
});
