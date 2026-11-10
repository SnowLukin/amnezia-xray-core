# Адаптивное ядро

Клиент VPN держит несколько серверов, но обычный Xray узнаёт о мёртвом сервере поздно: по таймаутам пользователя или по периодическим пингам, которые сами расходуют трафик и батарею и не видят блокировок конкретного протокола. Адаптивное ядро смотрит на настоящие соединения пользователя. Отказ установки, тишина после рукопожатия или замершая загрузка считаются уликой против пути, подозрение подтверждается короткой пробой, и балансировщик переводит новые соединения на следующий по цене путь. Список серверов и их цены задаёт бэкенд, частью прямо в конфиге, частью наборами, которые ядро само запрашивает у бэка во время работы. Приложение сообщает о смене сети и присутствии пользователя и забирает сводку для аналитики.

## Из чего состоит

- Наблюдатель (`healthObservatory`, пакет `app/observatory/health`). Собирает пассивные улики по трафику outbound, запускает пробы, держит состояние путей (`up`, `suspect`, `down`, карантин), пишет события, Status и отчёт.
- Балансировщик `leastCost` (`routing.balancers[]`, стратегия в `app/router`). Отдаёт роутеру самый дешёвый живой путь из участников (`outbounds[].selection`), держит закрепление хоста, возврат на дешёвый путь, переход в `fallbackTag` или `fallbackBalancer` при исчерпании.
- Провайдер (`healthObservatory.providers[]`). HTTP-ручка бэка, у которой ядро по поводам (старт, исчерпание, аренды, смена сети, запрос хоста) берёт набор outbound и групп и применяет его к балансировщикам без перезапуска.
- Мост для приложений. Команды хоста на четырёх слоях: Go API `*health.Observer`, libXray для iOS и Android, gRPC `HealthService` и CLI `xray api health`. Описаны в host-api.md.

## Что кому читать

| Кто | Документы |
|---|---|
| Бэкенд-разработчик: генерирует конфиг и пишет ручку провайдера | [config.md](config.md) целиком, [provider-protocol.md](provider-protocol.md), раздел «Версии» в config.md для выбора шаблона по заголовку `Xray-Adaptive`, «Отчёт» в [telemetry.md](telemetry.md) для аналитики |
| Разработчик приложения | [host-api.md](host-api.md): команды, обязанности хоста, сценарии; [telemetry.md](telemetry.md): Status, события, отчёт, трасса; в [config.md](config.md) блоки `network`, `host`, `resources`, `telemetry` |
| Тот, кто переходит на новую сборку ядра | [changelog.md](changelog.md): что перестаёт загружаться, что появилось и что сделать бэку и приложению, по ревизиям |
| Тот, кто чинит ядро | все документы, код в `app/observatory/health`, разбор конфига в `infra/conf/health.go`, стратегия в `app/router/strategy_leastcost.go`, стенд в `testing/adaptive` |

Справочник описывает текущее поведение и сверяется с кодом тестами: помеченные примеры конфигов и ответов провайдера загружаются настоящим разбором, таблица событий и поля отчёта в telemetry.md сверяются с тем, что ядро пишет.

## Минимальный конфиг

Два сервера с ценами 1 и 2, проба по подозрению и `block`, когда оба мертвы. Что включено в таком конфиге и чего нет, описано в [config.md](config.md), раздел 2; все блоки и пределы там же.

<!-- example: config -->
```json
{
  "outbounds": [
    {
      "tag": "nl-1",
      "protocol": "vless",
      "settings": {"address": "nl-1.example.invalid", "port": 443, "id": "5783a3e7-e373-51cd-8642-c83782b807c5", "encryption": "none"},
      "selection": {"balancer": "auto", "cost": 1, "probe": "basic"}
    },
    {
      "tag": "de-1",
      "protocol": "vless",
      "settings": {"address": "de-1.example.invalid", "port": 443, "id": "5783a3e7-e373-51cd-8642-c83782b807c5", "encryption": "none"},
      "selection": {"balancer": "auto", "cost": 2, "probe": "basic"}
    },
    {"tag": "block", "protocol": "blackhole"}
  ],
  "routing": {
    "rules": [{"network": "tcp,udp", "balancerTag": "auto"}],
    "balancers": [{"tag": "auto", "fallbackTag": "block", "strategy": {"type": "leastCost"}}]
  },
  "healthObservatory": {
    "probes": {
      "profiles": [
        {"name": "basic", "steps": [{"name": "transport", "destinations": ["https://probe.example.invalid/204"]}]}
      ]
    }
  }
}
```

## Тесты и стенды

Юнит-тесты ядра и разбора конфига, включая сверку документов с кодом:

```sh
go test -race ./app/observatory/health/... ./app/router/... ./infra/conf/...
```

Стенд в `testing/adaptive` запускает на одной машине без root серверы Xray на пяти транспортах и AmneziaWG, прокси, который ломает адреса серверов по команде, локальную цель вместо интернета, и клиент из этой ветки. Сценарии проверяют решения по событиям, Status и отчёту.

```sh
go build -o /tmp/xray ./main
python3 testing/adaptive/run.py /tmp/xray                      # все сценарии стенда
python3 testing/adaptive/run.py /tmp/xray failover_blackhole   # выбранные сценарии
python3 testing/adaptive/run.py /tmp/xray --prod               # стенд на конфиге прода
```

`--prod` гоняет сценарии `prod_scenarios.py` на конфиге той формы, которую генерирует бэк: два балансировщика `leastCost`, провайдер с дорогами и `initialAnswer`, проверка связи, отчёт. Бэк на нём изображает строгий провайдер `strict_provider.py`, который сверяет каждый запрос ядра с [provider-protocol.md](provider-protocol.md). Отчёт JSON о прогоне пишется рядом с логами в рабочем каталоге, его путь печатается первой строкой.

Тесты самого стенда, без сборки ядра:

```sh
python3 -B -m unittest discover -s testing/adaptive -p "test_*.py"
```
