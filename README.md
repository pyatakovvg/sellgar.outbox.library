# @sellgar/outbox

Инфраструктурный пакет для transactional outbox в backend-сервисах Sellgar.

Пакет владеет только переиспользуемой механикой outbox: моделью хранения
событий, writer, relay, retry/backoff, claim-логикой и метриками. Доменные
сервисы сами владеют именами событий, payload и правилами версионирования
агрегатов.

`aggregateId` является строковой идентичностью агрегата. Он поддерживает как
бизнес-коды, так и технические UUID и не подменяет идентификатор домена UUID.

Публикация одного события ограничена `publishTimeoutMs` или
`OUTBOX_PUBLISH_TIMEOUT_MS`. Если RabbitMQ/Nest `ClientProxy.emit` зависает,
событие переводится в `failed`, получает `next_attempt_at` и затем повторяется
через обычный retry/backoff.

`OutboxModule` подключается к RabbitMQ через Nest DI token, переданный в
`eventClientToken`. Token должен указывать на уже зарегистрированный
`ClientProxy`; отсутствие provider считается ошибкой конфигурации сервиса.

Порядок событий одного `(producer, aggregateType, aggregateId)` определяется
`aggregateVersion`. В один claim попадает только первая неопубликованная версия
агрегата. Ожидание retry, активная публикация и исчерпание попыток предшественника
блокируют его следующие версии, но не другие агрегаты. Исчерпанные события требуют
вмешательства оператора; автоматического пропуска нет. Время используется только
для выбора между независимыми агрегатами.

Relay выбирает следующую версию после успешной публикации предыдущей без ожидания
следующего интервала; `batchSize` ограничивает суммарное число попыток за цикл.
Доставка остаётся at-least-once: потребителям нужны inbox и защита от старых версий,
а также восстановление проекций при разрывах версий.

Регрессионные тесты используют только отдельную временную схему PostgreSQL.
После `yarn build` из этого пакета, с настроенными `PGHOST`, `PGPORT`,
`PGUSER`, `PGPASSWORD`, `PGDATABASE`:

```bash
OUTBOX_DATABASE_TESTS=1 yarn test
```

Без `OUTBOX_DATABASE_TESTS=1` команда `yarn test` пропускает подключения к БД.

## Подключение

Сервис подключает этот repository как nested submodule в `library/sellgar.outbox.library`
и workspace-зависимость `@sellgar/outbox`. Это один пакет с общей реализацией;
relay/repository не копируются в исходники приложения. Перед сборкой сервиса
собирается workspace библиотеки.

```ts
OutboxModule.forRoot({
  producer: 'service_name',
  eventClientToken: 'EVENT_CLIENT',
});
```

`EVENT_CLIENT` должен быть доступен через глобальный ClientsModule. TypeORM DataSource
принадлежит сервису; модуль не создаёт соединение, таблицу или миграции. Один Nest-контекст
подключает OutboxModule один раз, даже если обслуживает несколько RMQ очередей.

По умолчанию используются колонка `aggregate_id`, колонка `producer`, статус
исчерпанных попыток `failed` и поле события `aggregateId`. Для существующих схем:

```ts
OutboxModule.forRoot({
  producer: 'service_name',
  eventClientToken: 'EVENT_CLIENT',
  storage: {
    aggregateIdColumn: 'aggregate_uuid',
    hasProducerColumn: false,
    exhaustedStatus: 'dead',
  },
  aggregateIdField: 'aggregateUuid',
});
```

При отсутствии колонки producer значение берётся из конфигурации. Версия сохраняет
тип драйвера: PostgreSQL int возвращается числом, bigint — строкой. Порядок сравнивается
в SQL; преобразования bigint в JavaScript number нет. Имена колонок и поля события
ограничены указанными вариантами и проверяются до старта.

Сервис формирует доменное событие и вызывает `OutboxWriter.add(manager, event)`
или `addMany` с текущим EntityManager. Writer использует тот же manager, поэтому
откат доменной транзакции откатывает события. Payload и schemaVersion принадлежат
сервису; библиотека добавляет только инфраструктурный envelope.

Relay запускается при инициализации модуля. При остановке прекращает новые циклы,
ожидает активную публикацию в пределах publish timeout и не закрывает чужой ClientProxy.
Оставшиеся processing записи восстанавливаются по processing timeout.
