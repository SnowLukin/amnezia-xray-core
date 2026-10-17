# Provider protocol 1 — API v2

`network.connectivity.destinations`, `network.labelRequest.destinations` и `providers[].urls` — списки от 1 до 4 адресов. Проверка connectivity запускает адреса параллельно и принимает первый успешный HTTP-ответ в рамках одного probe job. Запрос меток идёт последовательно до первого успешного ответа; каждому адресу даётся настроенный timeout. Зеркала провайдера перебираются последовательно внутри маршрута via; это один refresh с прежними quota/backoff и `run + refreshNumber`. `via` выбирает маршрут, `urls` — зеркала HTTP-ручки. Одиночные `destination` и `url` не принимаются.


Протокол HTTP остаётся `1`; внешняя конфигурация источника и `selection` в ответах следуют API v2. Переименования выполнены на месте: старый `path` и изменение `routing` в ответе отклоняются. Backend не управляет полной конфигурацией VPN через динамический источник.

## Дороги, задержки и версии (14.10.2026)

`apply.balancers` — от 1 до 4 различных непустых тегов `leastCost`, первый основной.
Поводы `exhausted`, `minHealthy`, `whenCostAbove` смотрят только на основной.
Остальные могут служить дорогами к бэку: `via` ссылается на их теги, а
`selection.balancer` в ответе выбирает любой из перечисленных. `maxOutbounds`
и число групп считаются по всему набору; probe-only входит в эти ограничения,
прямой `via` на probe-only по-прежнему запрещён. Блок `probeOnlyRoads: {}`
разрешает проверочные дороги исчерпанного балансировщика (см. ниже). Группа требует включённый
`leastCost.settings.groups` у каждого балансировщика, где есть её члены.
Один tag outbound принадлежит одному балансировщику; новый ответ может перенести его.

В конфиге дороги к бэку оставляют без routing-правил для пользовательского трафика.
Ядро не проверяет отсутствие таких правил: это ответственность автора конфига.
При первом `via` ядро само передаёт кандидатам дороги текущий состав handlers,
поэтому пользовательский запрос для запуска такой дороги не требуется.
`apply.balancer` и `lastFailure` удалены из публичных форм API v2.

Задержка — строка Go duration (`"30s"`) или равномерный включительный промежуток
(`"20s..60s"`, `"500ms..1h30m"`). Обе границы неотрицательные,
левая не больше правой; случайное значение выбирается один раз на случай,
а не на каждом tick. У фиксированного значения дополнительного jitter нет.

| Поле | Умолчание при включённом поводе |
| --- | --- |
| `triggers.exhausted.delay` | `0s..10s` |
| `triggers.minHealthy.delay` | `0s..30s` |
| `triggers.networkChange.settleDelay` | `30s..90s` |
| `triggers.whenCostAbove.after` | `15m..25m` |
| `triggers.periodic.interval` | `1h36m..2h24m` (прежние 2 h ±20%) |

Повод измеряется от его возникновения и перепроверяется перед запросом:
если балансировщик ожил или здоровых путей достаточно, запрос отменяется.
Network change при `unknownNotCheapest` также отменяется, если основной сел на
самый дешёвый путь. Ручной `RequestProviderRefresh` и продление аренды этих
задержек не имеют; у аренды остаётся собственный разброс долей.
Флаги включения и прежние gates/лимиты сохраняются. Поля доли разброса для
этих задержек не принимаются; backoff и прибавка к `Retry-After` не меняются.
Подсказка бэка `nextRefreshIn` сохраняет свой внутренний ±20% разброс.

Каждый POST/GET несёт принадлежащий ядру словарь RFC 8941:

```http
Xray-Adaptive: xray="26.10.10", api=2, provider=1
```

`xray` берётся из `core.Version()`, `api` из `health.APIVersion`, `provider`
из версии протокола. Заголовок запрещён в config/answer/host `headers` и в
`acceptedHeaders` независимо от регистра. `baseVersion` остаётся в теле.
`health.AdaptiveHeader()` отдаёт `(name, value)` без запущенного экземпляра;
libXray `HealthAdaptiveHeader()` отдаёт base64-обёртку общего JSON Result:
`{"success":true,"data":{"name":"Xray-Adaptive","value":"xray=\"26.10.10\", api=2, provider=1"}}`.
Приложение берёт эту пару до connect и ставит её в запрос к бэку.
Версию Go и дополнительные поля в тело не добавляем.

За один refresh получаем не больше двух HTTP-ответов не по протоколу
(включая 502 от nginx и сломанный JSON 200). Второй завершает весь перебор;
сетевые ошибки/таймауты до HTTP-ответа продолжают обход всех дорог.
Авторитетное тело протокола или принятый `Retry-After` останавливает его сразу.
При перегрузе бэк отвечает `503` со случайным целым `Retry-After` на клиента
и телом `{"protocol":1,"code":"unavailable"}`.

### Обычный HTTPS к бэку через IP входа

```json
{
  "tag": "backend-entry",
  "protocol": "freedom",
  "settings": {"redirect": "203.0.113.10:443"},
  "streamSettings": {
    "network": "raw",
    "security": "tls",
    "tlsSettings": {
      "serverName": "api.example",
      "fingerprint": "chrome",
      "alpn": ["http/1.1"]
    }
  }
}
```

У провайдера `urls: ["http://api.example/set"]`, `isPlainHttpAllowed: true`,
`via: ["backend-entry"]`. Схема `http://` относится к HTTP-клиенту ядра:
TLS уже добавляет outbound, на проводе обычный проверяемый HTTPS с SNI бэка,
Chrome ClientHello и ALPN `http/1.1`. `https://` добавил бы второй TLS-слой.
IP входа должен обслуживать или проксировать такой TLS для SNI бэка.
Чужой сертификат отклоняется; `allowInsecure` не требуется.
Такой же outbound можно выдать динамически с `selection.balancer` дороги,
разрешив `freedom` в `allowedProtocols`; в `via` тогда указывают тег дороги.

### Рекомендации бэку

- Один основной и один тёплый резерв на Xray; `minHealthy.outbounds: 2`.
- Общая probe-only учётка на вход для контрольных проб.
- При «всё мертво» выдать новый слот либо `503` со случайным целым `Retry-After`
  и телом протокола; повтор того же набора не лечит отказ и включает backoff.
- Timeout одной попытки 8–10 секунд.
- SNI и `shortId` одного слота сохранять между выдачами.

## Проверочные дороги (14.10.2026)

Необязательный блок провайдера `probeOnlyRoads: {}` включает дороги через
проверочные учётки. Когда очередной `via` — leastCost, который исчерпан или
ещё держит путь, признанный мёртвым, ядро
перед следующим `via` пробует не более двух его живых probe-only участников.
Первым идёт участник с самым поздним успешным candidate-пробованием в текущей
эпохе сети. Проверка должна оставаться свежей по `probes.candidates.recheckInterval`;
retiring, replacing, expired, down и quarantined участники не подходят.
Зеркала перебираются внутри каждой дороги. Общие остановки на протокольном
ответе, `Retry-After`, двух непротокольных HTTP-ответах и низкой памяти сохраняются.
`lastRefresh.attempts` содержит `via` и `outbound` — тег учётки без tagPrefix.
Пользовательский выбор всегда исключает probe-only, даже при exhausted.

Новый слот, выданный в ответ на такой запрос, исчерпанный балансировщик берёт
сразу, не дожидаясь своей пробы: замер стенда 14.10.2026 показал, что отдельная
пометка «слот проверен через эту учётку» времени без связи не сокращает, и её нет.

## Адреса от хоста (15.10.2026)

`SetProviderURLs(provider, urls)` заменяет зеркала провайдера списком хоста: discovery приложения находит домены бэка посреди сессии. В libXray - `HealthSetProviderURLs(provider, urlsJSON)`, JSON Result.

- **Список целиком:** от 1 до 4 адресов без повторов, порядок - порядок попыток.
- **Проверка - как у `urls` конфига:** `http(s)` без логина и `#`; `http` только при `isPlainHttpAllowed`. Ошибки: `invalid_argument`, `unknown_provider`, `not_running`.
- **Когда действует.** Со следующей попытки; идущая доживает со старым адресом.
- **Паузы.** Паузы после неудач (`backoff`) сбрасываются; пауза `Retry-After` и отказ в токене - нет.
- **Обновление не запускается** - его запускает `RequestProviderRefresh`.
- **Хеши `run` и `network.id`** считаются от первого адреса конфига и от замены не меняются.
- **Адреса никуда не уходят:** ни в тело запроса, ни в события, ни в отчёт. Статус показывает `urls_count` и `urls_generation`, событие `provider_urls` - `count` и `generation`.

Решение и отклонённые варианты - `specs/provider-urls-from-discovery.md` хранилища.

## Конфигурация, запрос и ответ

```jsonc
{
  "healthObservatory": {
    "providers": [
      {
        "tag": "fresh",
        "urls": [
          "https://api.example/set"
        ],
        "method": "POST",
        "headers": {
          "Authorization": "Bearer <token>"
        },
        "via": [
          "auto",
          "backup",
          "direct"
        ],
        "timeout": "10s",
        "isPlainHttpAllowed": false,
        "triggers": {
          "exhausted": {},
          "leaseRenewal": {
            "renewAtFraction": 0.5,
            "urgentAtFraction": 0.8
          },
          "backendSchedule": {}
        },
        "limits": {
          "minInterval": "5m",
          "urgentMinInterval": "30s",
          "maxRefreshesPerHour": 6
        },
        "apply": {
          "tagPrefix": "p-",
          "drainTimeout": "5m",
          "maxOutbounds": 64,
          "allowedProtocols": [
            "vless",
            "vmess",
            "trojan",
            "shadowsocks",
            "wireguard",
            "hysteria",
            "socks",
            "http"
          ],
          "acceptedHeaders": [
            "Authorization"
          ],
          "expiredLeaseFallback": {},
          "balancers": [
            "auto"
          ]
        },
        "baseVersion": "cfg-12",
        "initialAnswer": null
      }
    ]
  }
}
```

Без массива providers нет запросов, парсинга initialAnswer и runtime handler замен. Каждый элемент массива — активный источник; выключенный источник удаляется из массива. Provider не требуется ни для одного базового маршрута. Его ошибки не удаляют конфиговые outbound и последний принятый рабочий набор. Каждое новое сообщение проверяется до применения, затем атомарно заменяется только принадлежащий источнику набор; соединения старых обработчиков доживают по drainTimeout.

Умолчания изменяют поводы запросов, но оставляют прежние защитные gates: idle, pin, network down, low memory, Retry-After, backoff и quota. Ручной refresh не обходит их. Отложенный ручной запрос возвращает причину и `retry_in_ms`; его не нужно повторять циклом. Хост может поставить новые headers; значения никогда не попадают в ошибки/сводку. POST остаётся основным; GET остаётся для статического файла.

**Провайдер не изменяет routing.** Источник меняет адреса, группы, аренды и принятые headers. Изменение маршрутизации сигнализируется через `baseVersion` и применяется хостом целым конфигом. Решение согласовано 13.10.2026: `isRoutingAccepted` удалён, поле `routing` в ответе отклоняется; отдельного флага routingUpdates нет.

```jsonc
{
  "protocol": 1,
  "provider": "fresh",
  "run": "provider-scoped-hash",
  "refreshNumber": 7,
  "trigger": "expiry",
  "retryLevel": 1,
  "version": "slots-7",
  "baseVersion": "cfg-12",
  "clientTime": "2026-10-13T13:00:00Z",
  "network": {
    "id": "provider-scoped-network-hash",
    "isKnown": false,
    "isUp": true,
    "labels": {
      "type": "wifi"
    },
    "ipLabels": {
      "asn": "12345"
    }
  },
  "isMemoryLow": false,
  "outbounds": [
    {
      "tag": "x2",
      "isProbeOnly": false,
      "state": "up",
      "activeMs": 840000,
      "probe": {
        "profile": "full",
        "reason": "failover",
        "state": "up",
        "isPassed": true,
        "agoMs": 30000,
        "steps": [
          {
            "name": "transport",
            "isPassed": true,
            "connectMs": 80,
            "ms": 180,
            "bytes": 32768,
            "destination": 0
          }
        ]
      },
      "udp": {
        "state": "ok",
        "rttMs": 41,
        "agoMs": 40000
      },
      "quality": {
        "ttfbCount": 14,
        "ttfbP50Ms": 120,
        "tcpRttMinMs": 38,
        "bulkKbpsP90": 18000
      }
    }
  ],
  "groups": [
    {
      "group": "g",
      "state": "up",
      "members": 6,
      "healthy": 4,
      "quarantined": 1,
      "clientPosition": 2,
      "failed": 0,
      "quarantineMs": 0
    }
  ],
  "witnessQuality": {
    "dialCount": 20,
    "dialP50Ms": 40
  },
  "balancers": [
    {
      "tag": "auto",
      "current": "x2",
      "isOwn": true,
      "cost": 3,
      "isExhausted": false,
      "isPinned": false,
      "heldMs": 840000
    }
  ],
  "lastRefresh": {
    "isOk": false,
    "agoMs": 61000,
    "trigger": "exhausted",
    "attempts": [
      {
        "via": "auto",
        "mirror": 1,
        "isOk": false,
        "status": 503,
        "code": "status",
        "ms": 120
      }
    ]
  }
}
```

```jsonc
{
  "protocol": 1,
  "version": "slots-8",
  "outbounds": [
    {
      "tag": "x2",
      "protocol": "vless",
      "settings": {},
      "selection": {
        "balancer": "auto",
        "cost": 1,
        "probe": "full",
        "isProbeOnly": false
      }
    }
  ],
  "groups": [],
  "expiresAt": {
    "x2": "2026-10-14T13:00:00Z"
  },
  "serverTime": "2026-10-13T13:00:00Z",
  "nextRefreshIn": "2h",
  "headers": {
    "Authorization": "Bearer <new-token>"
  },
  "baseVersion": "cfg-12"
}
```

Все прежние outbound counters, группы и качественные измерения сохраняются в запросе без переименования, если ниже не указано иное; объект опциональной фичи отсутствует, когда нет её данных. Ограничения: tag/group-name не длиннее 128 байт, version/baseVersion — 256 байт; метки по measurement-context (ключ до 24, значение до 32 символов, 6 у пути/IP и 8 у host/сессии). Provider POST имеет hard ceiling 256 KiB: при большом summary сначала опускаются optional quality details с явным isTruncated, essential identity/state/lease не обрезаются; если essential часть не влезла — отказ запроса с кодом request_too_large, без изменения маршрута. Это новая механика ограничения payload и отдельная provider-rich regression после GO. Неиспользуемые группы запрещены, число групп не больше maxOutbounds; это усиленная проверка, текущий 1MiB bound не отменяется. Новый bool не разрешает отправку адресов, SNI, URL ошибок, header values или raw network/run. Размер ответа выводится из maxOutbounds: 4 KiB на участника, максимум 1 MiB. Дополнительный ручной maxBytes убирается, общий hard ceiling остаётся. Ошибки ответа `{protocol:1,code:"rate_limited"}` и HTTP Retry-After сохраняются. Значения trigger (`host`, `expiry`, `min_healthy` и т.д.) сохраняются, хотя ключи включения поводов стали понятнее.

## Request

### Transport

- `POST` of the URL of the provider with the body below and
  `Content-Type: application/json`, the default. A provider that serves a
  static file on a web server is configured with `GET`, which sends no
  body.
- `Accept: application/json`, unless the config sets another.
- The headers of the config, over them those of the initial set, and over
  both the headers an answer or the host set since, see [Token](#token). A
  header the host set before the start lies over the one of the same name
  in the initial set; the other headers of the set apply. Credentials
  travel in headers, never in the URL.
- `https`; plain `http` only where the client allows it explicitly.
- Each attempt has its own timeout, 10 seconds by default. Redirects are
  not followed: a 3xx is a failure of that attempt.
- An answer larger than the limit of the provider is a failure: by default
  4 KiB per outbound the set may hold, 256 KiB for 64 outbounds, at most
  1 MiB.

### Body

```json
{
  "protocol": 1,
  "provider": "fresh",
  "run": "9f2c6a1e5b7d3c40",
  "refreshNumber": 7,
  "trigger": "expiry",
  "retryLevel": 1,
  "version": "slots-7781",
  "baseVersion": "cfg-12",
  "clientTime": "2026-10-08T12:00:03Z",
  "network": {
    "id": "5d0c1f2a9b3e4c71",
    "isKnown": false,
    "isUp": true
  },
  "isMemoryLow": false,
  "outbounds": [
    {
      "tag": "x1",
      "state": "down",
      "reason": "traffic",
      "stateMs": 81000,
      "seenMs": 3600000,
      "quarantineLevel": 2,
      "quarantineMs": 81000,
      "failures": {
        "dial_timeout": 4
      },
      "probe": {
        "profile": "full",
        "state": "down",
        "steps": [
          {
            "name": "transport",
            "isPassed": false,
            "connectMs": 800,
            "ms": 1800,
            "bytes": 0,
            "error": "timeout"
          }
        ],
        "isPassed": false,
        "error": "timeout",
        "agoMs": 30000
      },
      "connsOk": 12,
      "connsFailed": 4,
      "bytesUp": 90112,
      "bytesDown": 1048576,
      "activeMs": 600000,
      "ttfbP50Ms": 300,
      "expiresInMs": 61200000
    },
    {
      "tag": "x2",
      "state": "degraded",
      "probe": {
        "profile": "full",
        "state": "degraded",
        "steps": [
          {
            "name": "transport",
            "isPassed": true,
            "connectMs": 80,
            "ms": 180,
            "bytes": 32768,
            "destination": 0
          },
          {
            "name": "egress",
            "isPassed": false,
            "connectMs": 70,
            "ms": 120,
            "bytes": 0,
            "error": "closed"
          }
        ],
        "isPassed": true,
        "rttMs": 180,
        "agoMs": 840000
      },
      "connsOk": 412,
      "bytesDown": 52428800,
      "activeMs": 840000,
      "expiresInMs": 61200000
    },
    {
      "tag": "x3",
      "state": "unknown",
      "expiresInMs": -5000
    }
  ],
  "balancers": [
    {
      "tag": "auto",
      "current": "x2",
      "isOwn": true,
      "cost": 3,
      "isExhausted": false,
      "isPinned": false,
      "heldMs": 840000
    }
  ],
  "lastRefresh": {
    "isOk": false,
    "agoMs": 61000,
    "trigger": "exhausted",
    "attempts": [
      {
        "via": "auto",
        "mirror": 1,
        "isOk": false,
        "status": 503,
        "code": "status",
        "ms": 120
      }
    ]
  }
}
```

Legacy fields with zero or empty values are left out, except those marked always.
Inside quality, measured zeros remain present; unavailable measurements are omitted.

| Field | Meaning |
| --- | --- |
| `protocol` | Always 1. |
| `provider` | Always. The tag of the provider: one backend may serve several providers of one client, for example one per kind of outbound. |
| `run` | Always. Names the run of the core, hashed for this provider: the first 16 hex digits of SHA-256 of the URL, a zero byte and the run. Stable within a run; two backends cannot match theirs. |
| `refreshNumber` | Always. Grows with every refresh of the run and is the same in every attempt of one refresh: `run` and `refreshNumber` together tell a request sent again through another `via` from a new one. |
| `trigger` | Always. Why the core asks, see [When the core asks](#when-the-core-asks). |
| `retryLevel` | Always. The level of the backoff this refresh waited for: refreshes in a row since the last success that failed, brought an answer the core refused, or, for an exhausted balancer, brought the same set again. A pause the backend asked for with `Retry-After` and refused headers leave it as it is; any other applied answer sets it back to 0. |
| `lastRefresh` | Previous refresh, after success as well as failure: `isOk`, `agoMs`, `trigger`, and up to 16 `attempts` in order. An attempt contains `via` (configured tag), optional `outbound` (credential tag without this provider’s prefix), `mirror` (1-based URL index; 0 for a skipped road), `isOk`, `status` (HTTP or 0), optional `code`, and `ms`. Transport codes: `timeout`, `network`, `status`, `too_large`, `broken_json`, `skipped`. A refused protocol answer/set is `invalid_answer`/`invalid_set`. No URL or error text. Omitted before the first refresh; a refresh that could not construct a POST has an empty attempt list. |
| `version` | The set the core applies now, as the answer named it; none before the first set. |
| `baseVersion` | The base config the core runs with, see [Base config](#base-config); none without one. |
| `clientTime` | Always. The clock of the device in RFC 3339, UTC: the backend sees how far off it is. |
| `network` | Always. `id` names the network the host reported, hashed for this provider like `run` and left out when the host cannot tell networks apart; `isKnown`, always, whether the core remembers an outbound that worked in this network; `isUp`, always, whether the network works. |
| `isMemoryLow` | Always. The host reported that memory is short: the core asks only when it must. |
| `balancers` | Always. Array in `apply.balancers` order. Each balancer: `tag`, always; `current`, the outbound it chose without the prefix of the provider and only when it is one of this provider, which `isOwn`, always, tells; `cost`, the cost of the chosen outbound; `isExhausted` and `isPinned`, always; `heldMs`, how long it has held the chosen outbound. |
| `outbounds` | Always, maybe empty. The set of the provider in the order of the answer, by tag without the prefix; before the first answer the outbounds of the config with the prefix of the provider. Outbounds that left the set are not listed, ones whose lease ended are. |
| `groups` | Present for explicit groups with members owned by this provider. Each summary has `balancer`, `group`, `state` (`up` or `down`), `members`, `healthy`, `quarantined`, `clientPosition`, `failed`, `quarantineMs` and optional `lastFailure`. Counts cover only this provider's members; `healthy` counts proven working members with live leases and is zero while the group is down. `clientPosition` is the zero-based position in the full group. A down group contributes no usable members to `triggers.minHealthy.outbounds`; it introduces no new provider trigger. |

Per outbound:

| Field | Meaning |
| --- | --- |
| `tag` | Always. |
| `isProbeOnly` | Always a boolean. True means probe credentials excluded from user selection. |
| `state` | Always. `up`, `degraded`, `suspect`, `down`, or `unknown` for an outbound the core knows nothing about yet. `degraded` means required probe steps passed but an optional step failed; an `up` candidate is preferred before cost is considered. |
| `reason`, `stateMs` | What set the state (`traffic`, `probe`, `return_probe`, `host_probe`, `network_change`, `network_down`, `network_up`, `replaced`, `rescued` for an outbound whose down a confirmation already running or an assigned audit proved false, or `chosen` for an outbound a balancer chose again after its rest, judged afresh) and how long ago. The `path_state` event distinguishes rescues with `by: confirm` or `by: audit`. |
| `seenMs` | How long the core has kept the counters of the outbound. |
| `quarantineLevel`, `quarantineMs` | While the outbound rests after a failure: the level of its rest, which grows with every failure in a row, and how much longer it lasts. |
| `failures` | Failures of user connections by kind over the life of the outbound: `dial_failed`, `dial_timeout`, `server_unverified` (REALITY did not verify the server), `no_response`, `closed`, `stall`, `partial_stall`. |
| `probe` | The last current probe that finished: an actual snapshot includes `profile` (an explicitly configured name), `reason` (the probe job's reason), `state`, and attempted `steps` with `name`, `isPassed`, `connectMs` (connection establishment), `ms` (whole step), `bytes`, a `destination` index on success (including zero), or an `error` class on failure. If only legacy result fields exist, `profile`, `reason`, `state`, and `steps` are absent. The compatibility fields remain: `isPassed` means state other than `down`, `rttMs` when it passed, `error` as a class when it failed, and `agoMs` since its result. Failure classes include `dial_failed`, `dial_timeout`, `server_unverified`, `closed`, `timeout`, `no_progress`, `status_<N>`, `short_body` and `other`; addresses and error text are never sent. Stale or canceled probes do not replace the snapshot. A network change clears it unless the configured action is IGNORE. |
| `udp` | Optional latest completed UDP probe in the current network: `state` (`ok`, `lossy`, `dead` or `unsupported`), `rttMs` and `agoMs`, all present inside the object, including known zero. The two durations are integer milliseconds. Unknown UDP is omitted. Every network notification clears this observation, including the same or empty ID and IGNORE; stale results and replaced handlers cannot republish it. UDP does not change path health, quarantine, selection or provider triggers. Example: `"udp": {"state":"dead","rttMs":0,"agoMs":40000}`. |
| `connsOk`, `connsFailed`, `bytesUp`, `bytesDown` | User connections and their bytes. |
| `activeMs` | How long a balancer has chosen the outbound, the current stretch included. |
| `ttfbP50Ms` | The median time to the first byte. |
| `expiresInMs` | What is left of the lease, below zero once it ended; none without a lease. |

The body never holds destinations, domains, addresses or server names,
the targets of failures, the text of an error that may name a URL, header
values, or the raw run and network id. A usual set takes a few kilobytes.
The legacy fixture with 128 outbounds and busy counters stays below 64 KiB,
excluding backend-chosen tags and version. The fixture with four raw probe
steps per outbound has a separate ceiling of 150 KiB, including the longest
probe-job reason. Snapshot metadata takes 70 bytes with empty profile and
steps; each step's fixed metadata takes up to 160 bytes. JSON-escaped profile and step-name contents are excluded
as caller-chosen text. Actual probe measurements are retained in full.
These are regression-fixture ceilings, not runtime truncation limits for
arbitrary values.

## Ограничения и применение

В ответе допускаются только protocol/version/baseVersion/serverTime/outbounds/groups/expiresAt/headers/nextRefreshIn/receivedAt. Набор — целиком, а не patch. У каждого outbound уникальный tag, разрешённый protocol и `selection.balancer` из списка `apply.balancers`; membership задаёт cost или объявленную group. Groups требуют включённый `leastCost.settings.groups` у своего балансировщика у своего балансировщика, а probe-only — `probes.candidates`. Profile override должен ссылаться на известный профиль. Объект `routing`, неизвестный ключ, неиспользуемая группа и превышение bounds отклоняют ответ целиком.

Полный tag после `apply.tagPrefix` и group name ограничены 128 байтами; version/baseVersion — 256. Число групп не превышает maxOutbounds. Ответ ограничен 4 KiB × maxOutbounds, максимум 1 MiB. POST запроса — максимум 256 KiB: сначала убираются optional quality details с `isTruncated: true`; если essential identity/state/lease превышает потолок, fetch не начинается (`request_too_large`). При отказе текущий VPN-набор сохраняется.

Неизменённый handler сохраняет соединения. Заменённый или удалённый прекращает принимать новые, draining продолжается до `apply.drainTimeout` (5m default). Renewals с тем же набором не пересоздают handler. Разобранный набор применяется только после validation/build; ошибка не публикует частичный набор.

## Errors and Retry-After

An answer other than 200 may carry an error body of the protocol:

```json
{
  "protocol": 1,
  "code": "rate_limited",
  "message": "..."
}
```

The codes are `unauthorized`, `forbidden`, `rate_limited`, `unavailable`,
`bad_request` and `internal`. The core reads the first 4 KiB of the body,
waiting at most 2 seconds after the status, and keeps only the code;
`message` is kept nowhere, since it may echo the request.

The attempts end when the backend itself answered, readably or against the
schema; an answer that a firewall, a portal or a server on the way may have
sent goes on to the next mirror/`via`, subject to a shared ceiling of two non-protocol HTTP replies per refresh:

| Answer | Next `via` | Then |
| --- | --- | --- |
| 200 with a valid answer | no | Applied; the backoff starts over, unless an exhausted balancer got the same set again. |
| 200 that is no JSON object, is cut short or too large | if fewer than two non-protocol HTTP replies | A failure; the backoff grows. |
| 200 with a JSON object against the protocol, or a set that does not build | no | Refused, see [Answer](#answer); the backoff grows. |
| 401 or 403 with the code `unauthorized` or `forbidden` | no | The headers are refused, see [Token](#token). |
| 429 or 503 with `Retry-After` | no | Every trigger waits as asked; no failure, the backoff does not grow. |
| Any other status with an error body | no | A failure; the backoff grows. |
| Bare status, including 401/403, 429/503, 3xx, 502 from a proxy | if fewer than two non-protocol HTTP replies | Failure; one spare attempt can bypass a portal. |
| Timeout or network failure before an HTTP reply | yes | All configured roads/mirrors remain available for bypassing blocks. |

**Retry-After** counts only with 429 and 503: in whole seconds (no ranges), or as an HTTP
date counted from the `Date` of the same answer, so a wrong clock of the
device neither stretches nor cuts the pause, or from the clock of the
device without `Date`. Zero, a date gone by and anything unreadable count
as none. The core waits at most the internal 30-minute cap,
then spreads the pause by up to the internal 30% jitter, upward only: no
client comes back sooner than asked, and clients told the same pause come
back apart. The pause holds every trigger, the host and an exhausted
balancer too. Under overload the backend should return `503` with `Retry-After` and a protocol error body (`code: "unavailable"`), choosing a fresh random integer delay per client. The core keeps its upward-only addition; it does not extend HTTP syntax.

## Leases

`expiresAt` tells by tag when the backend stops serving an outbound, and
`serverTime` is the clock of the backend when it answered. The clock of the
device may be off by any amount, so the core keeps a lease as what was left
of it when the answer arrived:

- it counts from the later of `serverTime` and the `Date` of the answer
  plus its `Age`, which a cache sets; without either from the clock of the
  device;
- it is at most 30 days;
- it counts down by the longer of the monotonic clock, which stands while
  the device sleeps, and the wall clock: it ends early rather than late.

Every answer is the whole truth about the outbounds in it: a later end
renews a lease, the same end only brings a fresher clock, an earlier end
shortens it, an outbound of the set without an end in the answer has no
lease from then on, and an outbound that leaves the set takes its lease
along. A time the core cannot read refuses the answer.

**Renewal.** The core asks with trigger `expiry` once a lease reached half
of its length, spread by 10% per lease, or ended; from 80% of it on and
once it ended the request is urgent and waits only for the shorter limit.
Every request asks about the leases due by then, whatever its trigger. The
core asks once per value of a lease: a backend that answers with the same
end is not asked again for it, and the lease then ends. A request that
fails is repeated after the backoff, and while the device is idle only
with user traffic. The shares and jitter are internal constants. `triggers.leaseRenewal.isEnabled: false` disables renewal fetches while lease expiration still applies.

**The end.** With присутствующем или отсутствующем `expiredLeaseFallback` без opt-out (default true), an outbound whose lease ended stays a candidate of the
balancer, the last resort: new connections go to it only while no other
usable outbound is left, so when every lease ended traffic still goes
through those outbounds. Old connections are not ended. Such an outbound is
not probed in the background, and while no balancer chooses it, failures of
its old connections are not held against it.

With `expiredLeaseFallback.isEnabled: false`, expired members are excluded from new choices. Existing connections continue; the explicitly configured balancer fallback is used when no usable member remains.


## Token

`headers` of an answer replace headers of the next requests, to rotate the
token of the provider:

- only names the client allows, by default `Authorization`, in any case:
  `authorization` replaces the `Authorization` of the config;
- at most 8, also together with the headers the host and earlier answers
  set, each name at most 256 bytes, each value non-empty, in UTF-8, at
  most 4 KiB, without line breaks or other control characters;
- names of the transport (`Host`, `Content-Length`, `Content-Type`,
  `Transfer-Encoding`, `Connection`, `Keep-Alive`, `TE`, `Trailer`,
  `Upgrade`, `Accept`, `Proxy-*`) never.

A header that breaks these rules refuses the answer. Only an applied
answer changes a header: a refused or lost answer leaves the token in use.
The host may set headers as well; when it changed them while the request
ran, the host wins, and the headers of the answer are not taken. The core
never shows header values anywhere but in the requests of the provider.

The backend refuses the headers with 401 or 403 and an error body with the
code `unauthorized` or `forbidden`. Every trigger, including the host, waits
for the internal one-hour auth retry, with internal jitter, or until the headers change; after the
pause one refresh tries them again, once per pause, and the leases it
asked about are asked about again. The host is told and fetches a new
token when it can. A 401 or 403 without such a body is a failure like any
other, and the next `via` is tried: portals and firewalls on mobile
networks and in hotels answer so. A refusal of headers that the host
replaced while the request ran is no refusal of the new ones.

## Base config

`baseVersion` names the config the core runs with. The backend that
generated the config names it in `baseVersion` of the provider in the
config, or in the answer of the connect that came with the config; a set
the host kept named the config of an earlier run and never becomes the
base. The body of every request tells the base. An answer may name the base
config the backend serves now: when that is another one, the host is told
once per value, fetches the config anew and restarts the core when that is
cheap. An answer that names the base of the core again ends the change. The
set of such an answer applies all the same: the backend answers with
outbounds that work with the base of the request.

## Initial and kept sets

**Initial set.** The host may start the core with an answer of the
backend, the answer of a connect: the core applies it as it starts, with
the same checks and without a request, so it runs on the set from the
first connection. The answer of a connect therefore has to be a valid
answer of protocol 1, with `serverTime` and the leases. Its headers may
carry any name the host may set. The first request of such a core comes
from a trigger, not from the start, except for one urgent request when a
lease had already ended.

**Kept set.** The host keeps the last answer the core applied, with the
headers an answer or the host set in place of its own, `serverTime`
replaced by the time its leases count from and `receivedAt` added, the
clock of the device when it arrived, and starts the next core with it when
there is no fresh answer of a connect. The leases of a kept set count from
its `serverTime` plus the time since `receivedAt` and are renewed when they
would have been without the restart; a lease that ended meanwhile is asked
about once at the start, subject to the same pin/network/memory/activity gates as other refreshes. So after a restart the
backend sees the token it rotated last, the `version` of the kept set and,
after a long pause, an `expiry` request.

## Host

`RequestProviderRefresh` ставит запрос и соблюдает все gates. JSON bridge возвращает `Result.data` с `is_queued`, отложенной `reason` и `retry_in_ms`. Повторять вызов циклом не требуется. `ExportProviderSet(provider, after)` возвращает `{seq,set}`: `set` — JSON object, не JSON-строка. Хост сохраняет set в защищённом хранилище и передаёт как `initialAnswer` при следующем старте; runtime tickets и run не сохраняются между запусками.

## Переход балансировщика и exhausted

Когда балансировщик исчерпан и отправляет новые соединения через `fallbackBalancer`, повод `triggers.exhausted` его провайдера сохраняется. Успех в целевом балансировщике не объявляет рабочими адреса источника; эти попытки считаются спросом на источник и поддерживают обычную перепроверку карантина. Состояние и качество целевого трафика относятся к реально выбранному outbound цели.
