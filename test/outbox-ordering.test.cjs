require('reflect-metadata');

const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const { after, before, beforeEach, describe, it } = require('node:test');
const { DataSource } = require('typeorm');
const { of, throwError } = require('rxjs');

const { OutboxEventModel } = require('../dist/outbox-event.model');
const { OutboxRelayService } = require('../dist/outbox-relay.service');
const { OutboxRepository } = require('../dist/outbox.repository');
const { OutboxWriter } = require('../dist/outbox-writer.service');

describe(
  'Outbox aggregate ordering in PostgreSQL',
  {
    skip: process.env.OUTBOX_DATABASE_TESTS !== '1',
  },
  () => {
    const schema = `outbox_test_${randomUUID().replaceAll('-', '')}`;
    let control;
    let source;
    let repository;

    before(async () => {
      const options = {
        type: 'postgres',
        host: process.env.PGHOST,
        port: Number(process.env.PGPORT ?? 5432),
        username: process.env.PGUSER,
        password: process.env.PGPASSWORD,
        database: process.env.PGDATABASE,
      };

      control = await new DataSource(options).initialize();
      await control.query(`CREATE SCHEMA "${schema}"`);
      source = await new DataSource({
        ...options,
        schema,
        entities: [OutboxEventModel],
        extra: { options: `-c search_path=${schema}` },
      }).initialize();
      await source.synchronize();
      repository = new OutboxRepository(source);
    });

    after(async () => {
      if (source?.isInitialized) await source.destroy();
      if (control?.isInitialized) {
        await control.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
        await control.destroy();
      }
    });

    beforeEach(async () => {
      await source.manager.clear(OutboxEventModel);
    });

    const append = (version, overrides = {}) =>
      source.manager.insert(OutboxEventModel, {
        uuid: randomUUID(),
        producer: 'test',
        aggregateType: 'product',
        aggregateId: 'product-a',
        aggregateVersion: version,
        eventType: 'product.updated',
        schemaVersion: 1,
        payload: { version },
        occurredAt: new Date('2026-01-01T00:00:00Z'),
        nextAttemptAt: new Date('2026-01-01T00:00:00Z'),
        ...overrides,
      });
    const claim = () => repository.claimPublishableEvents({ batchSize: 50, maxAttempts: 10 });

    it('writes events in the caller transaction and rolls them back with the domain change', async () => {
      const writer = new OutboxWriter({ producer: 'test', eventClientToken: 'test' });
      const event = {
        aggregateId: 'product-a',
        aggregateType: 'product',
        aggregateVersion: 1,
        eventType: 'product.updated',
        payload: { name: 'Product' },
      };
      await assert.rejects(
        source.transaction(async (manager) => {
          await writer.add(manager, event);
          throw new Error('domain rollback');
        }),
        /domain rollback/,
      );
      assert.equal(await source.manager.count(OutboxEventModel), 0);
      await source.transaction((manager) => writer.addMany(manager, [event, { ...event, aggregateVersion: 2 }]));
      assert.equal(await source.manager.count(OutboxEventModel), 2);
      const [claimed] = await claim();
      assert.equal(claimed.aggregateVersion, 1);
      assert.deepEqual(claimed.payload, event.payload);
    });

    it('reports pending, failed, processing and dead events consistently', async () => {
      await append(1);
      await append(1, { aggregateId: 'product-b', status: 'dead' });
      const metrics = await repository.getMetrics();
      assert.equal(metrics.pendingCount, 1);
      assert.equal(metrics.deadCount, 1);
      assert.equal(metrics.failedCount, 0);
      assert.equal(metrics.processingCount, 0);
      assert.ok(metrics.oldestUnpublishedAgeSeconds >= 0);
    });

    it('publishes by version even when rows and timestamps are out of order', async () => {
      await append(3, { occurredAt: new Date('2025-01-01T00:00:00Z') });
      await append(2);
      await append(1);

      for (const version of [1, 2, 3]) {
        const events = await claim();
        assert.deepEqual(
          events.map((event) => event.aggregateVersion),
          [version],
        );
        await repository.markPublished(events[0]);
      }
      assert.deepEqual(await claim(), []);
    });

    it('does not claim a successor while another publisher holds its predecessor', async () => {
      await append(1);
      await append(2);

      const batches = await Promise.all([claim(), claim()]);
      const claimed = batches.flat();
      assert.deepEqual(
        claimed.map((event) => event.aggregateVersion),
        [1],
      );
      assert.deepEqual(await claim(), []);
    });

    it('does not skip a predecessor locked in another transaction', async () => {
      await append(1);
      await append(2);
      const runner = source.createQueryRunner();
      await runner.startTransaction();
      try {
        await runner.query('SELECT uuid FROM outbox_event WHERE aggregate_version = 1 FOR UPDATE');
        assert.deepEqual(await claim(), []);
      } finally {
        await runner.rollbackTransaction();
        await runner.release();
      }
    });

    it('blocks successors during backoff while allowing unrelated aggregates', async () => {
      await append(1, { status: 'failed', nextAttemptAt: new Date(Date.now() + 60000) });
      await append(2);
      await append(1, { aggregateId: 'product-b' });

      const events = await claim();
      assert.deepEqual(
        events.map((event) => event.aggregateId),
        ['product-b'],
      );
    });

    it('keeps successors blocked after a predecessor exhausts its attempts', async () => {
      await append(1, { status: 'failed', attempts: 10 });
      await append(2);
      assert.deepEqual(await claim(), []);
    });

    it('retries a failed predecessor before publishing its successor', async () => {
      await append(1);
      await append(2);
      const [first] = await claim();
      await repository.markFailed(first, new Date('2026-01-01T00:00:00Z'), new Error('broker unavailable'));
      const [retry] = await claim();
      assert.equal(retry.aggregateVersion, 1);
      assert.equal(retry.attempts, 1);
      await repository.markPublished(retry);
      assert.deepEqual(
        (await claim()).map((event) => event.aggregateVersion),
        [2],
      );
    });

    it('recovers an abandoned claim before allowing its successor', async () => {
      await append(1, { status: 'processing', processingStartedAt: new Date(Date.now() - 120000), attempts: 1 });
      await append(2);
      assert.deepEqual(await claim(), []);
      await repository.releaseTimedOutProcessingEvents(60000);
      const events = await claim();
      assert.deepEqual(
        events.map((event) => event.aggregateVersion),
        [1],
      );
      assert.equal(events[0].attempts, 1);
      await repository.markPublished(events[0]);
      assert.deepEqual(
        (await claim()).map((event) => event.aggregateVersion),
        [2],
      );
    });
    it('drains successive versions in one relay cycle within the batch budget', async () => {
      for (const version of [4, 3, 2, 1]) await append(version);
      const published = [];
      const relay = new OutboxRelayService(
        {
          emit: (_type, envelope) => {
            published.push(envelope.aggregateVersion);
            return of(null);
          },
        },
        { batchSize: 3, publishTimeoutMs: 10 },
        repository,
      );

      await relay.publishPending();
      assert.deepEqual(published, [1, 2, 3]);
      await relay.publishPending();
      assert.deepEqual(published, [1, 2, 3, 4]);
    });

    it('stops the failed aggregate but continues publishing unrelated changes', async () => {
      await append(1);
      await append(2);
      await append(1, { aggregateId: 'product-b' });
      const attempted = [];
      const relay = new OutboxRelayService(
        {
          emit: (_type, envelope) => {
            attempted.push([envelope.aggregateId, envelope.aggregateVersion]);
            return envelope.aggregateId === 'product-a' ? throwError(() => new Error('publish failed')) : of(null);
          },
        },
        { batchSize: 50, publishTimeoutMs: 10 },
        repository,
      );

      await relay.publishPending();
      assert.deepEqual(attempted.sort(), [
        ['product-a', 1],
        ['product-b', 1],
      ]);
      const events = await source.manager.find(OutboxEventModel, {
        order: { aggregateId: 'ASC', aggregateVersion: 'ASC' },
      });
      assert.deepEqual(
        events.map((event) => event.status),
        ['failed', 'pending', 'published'],
      );
    });
  },
);
