# Провайдер наборов

Провайдер - бэкенд, у которого наблюдатель по HTTP запрашивает набор outbound и групп и применяет его к балансировщикам `leastCost`. Код протокола лежит в `provider*.go` этого пакета.

Протокол описан в [`docs/adaptive/provider-protocol.md`](../../../docs/adaptive/provider-protocol.md), настройки провайдера в конфиге - в [`docs/adaptive/config.md`](../../../docs/adaptive/config.md).
