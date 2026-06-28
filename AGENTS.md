# @sellgar/outbox

`@sellgar/outbox` - общий инфраструктурный пакет для механики transactional
outbox в backend-сервисах Sellgar.

## Область

- Владеет переиспользуемым хранением outbox, writer, relay, retry/backoff,
  claim locking и метриками.
- Не владеет доменными именами событий, payload-контрактами и правилами
  версионирования агрегатов.
- Код сервиса должен передавать текущий TypeORM `EntityManager` при записи
  события, чтобы строка outbox коммитилась атомарно с доменным изменением.

## Правила

- Сохраняйте владение БД за сервисом. Пакет дает entities и helpers, но каждый
  сервис все равно владеет своей таблицей `outbox_event`.
- Публикация в RabbitMQ должна оставаться за Nest `ClientProxy`; не импортируйте
  сюда доменные модули сервисов.
- Одна зависшая публикация не должна останавливать relay: используйте
  `publishTimeoutMs`/`OUTBOX_PUBLISH_TIMEOUT_MS`, после timeout событие должно
  перейти в `failed` и повториться через backoff.
- Не закрывайте `ClientProxy` после publish timeout внутри библиотеки. Nest
  transport сам управляет подключением; закрытие клиента ломает следующие retry.
- `OutboxModule` должен получать event client через явный Nest DI token из
  `eventClientToken`. Не возвращайте `null` и не маскируйте отсутствие provider.
- Raw SQL `manager.query` в разных версиях TypeORM может вернуть либо rows, либо
  `[rows, affected]`; claim-логика обязана нормализовать результат перед mapper.
- Не добавляйте в пакет знания о product/shop/store-specific payload.
- Предпочитайте явную конфигурацию скрытым env-допущениям.

## Проверка

Запускайте `yarn build` из пакета или через корень monorepo конкретного сервиса.
