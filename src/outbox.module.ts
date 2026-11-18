import { DynamicModule, Module } from '@nestjs/common';
import { ClientProxy } from '@nestjs/microservices';
import { OutboxRelayService } from './outbox-relay.service';
import { OutboxRepository } from './outbox.repository';
import { OUTBOX_EVENT_CLIENT, OUTBOX_OPTIONS } from './outbox.tokens';
import { OutboxModuleOptions } from './outbox.types';
import { OutboxWriter } from './outbox-writer.service';
import { resolveOutboxStorage } from './outbox-storage';

@Module({})
export class OutboxModule {
  static forRoot(options: OutboxModuleOptions): DynamicModule {
    resolveOutboxStorage(options);

    return {
      global: true,
      module: OutboxModule,
      providers: [
        {
          provide: OUTBOX_OPTIONS,
          useValue: options,
        },
        {
          provide: OUTBOX_EVENT_CLIENT,
          useFactory: (eventClient: ClientProxy) => eventClient,
          inject: [options.eventClientToken],
        },
        OutboxRepository,
        OutboxRelayService,
        OutboxWriter,
      ],
      exports: [OutboxWriter, OutboxRepository],
    };
  }
}
