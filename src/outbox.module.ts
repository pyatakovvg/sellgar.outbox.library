import { DynamicModule, Module } from '@nestjs/common';
import { ClientProxy } from '@nestjs/microservices';
import { ScheduleModule } from '@nestjs/schedule';
import { TypeOrmModule } from '@nestjs/typeorm';

import { OutboxEventModel } from './outbox-event.model';
import { OutboxRelayService } from './outbox-relay.service';
import { OutboxRepository } from './outbox.repository';
import { OUTBOX_EVENT_CLIENT, OUTBOX_OPTIONS } from './outbox.tokens';
import { OutboxModuleOptions } from './outbox.types';
import { OutboxWriter } from './outbox-writer.service';

@Module({})
export class OutboxModule {
  static forRoot(options: OutboxModuleOptions): DynamicModule {
    return {
      module: OutboxModule,
      imports: [ScheduleModule.forRoot(), TypeOrmModule.forFeature([OutboxEventModel])],
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
      exports: [OutboxWriter],
    };
  }
}
