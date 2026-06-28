import { DynamicModule, Module } from '@nestjs/common';
import { ModuleRef } from '@nestjs/core';
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
          useFactory: (moduleRef: ModuleRef) => moduleRef.get<ClientProxy>(options.eventClientToken, { strict: false }),
          inject: [ModuleRef],
        },
        OutboxRepository,
        OutboxRelayService,
        OutboxWriter,
      ],
      exports: [OutboxWriter],
    };
  }
}
