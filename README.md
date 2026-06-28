# @sellgar/outbox

Инфраструктурный пакет для transactional outbox в backend-сервисах Sellgar.

Пакет владеет только переиспользуемой механикой outbox: моделью хранения
событий, writer, relay, retry/backoff, claim-логикой и метриками. Доменные
сервисы сами владеют именами событий, payload и правилами версионирования
агрегатов.

Публикация одного события ограничена `publishTimeoutMs` или
`OUTBOX_PUBLISH_TIMEOUT_MS`. Если RabbitMQ/Nest `ClientProxy.emit` зависает,
событие переводится в `failed`, получает `next_attempt_at` и затем повторяется
через обычный retry/backoff.

`OutboxModule` подключается к RabbitMQ через Nest DI token, переданный в
`eventClientToken`. Token должен указывать на уже зарегистрированный
`ClientProxy`; отсутствие provider считается ошибкой конфигурации сервиса.
