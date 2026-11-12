# Выходные данные адаптивного ядра

Справочник по тому, что ядро отдаёт наружу: `Status`, журнал событий, отчёт за окно, сигналы качества соединений, перепись путей и трасса. Ключи конфига описаны в [config.md](config.md), команды и конверт ответа в [host-api.md](host-api.md), запрос и ответ провайдера в [provider-protocol.md](provider-protocol.md).

## Обзор

| Данные | Как получить | Включается | Назначение |
| --- | --- | --- | --- |
| `Status` | команда `Status`, по запросу | всегда, пока работает `healthObservatory` | локальная диагностика |
| `View` | команда `View`, по запросу | `host.view` | снимок для экрана приложения, поля в [host-api.md](host-api.md) |
| Журнал событий | `DrainEvents` | `telemetry.events` | локальная диагностика |
| Отчёт | `PeekReport`, `TakeReport(isFinal)` | `telemetry.report` | единственное, что предназначено для аналитики |
| Качество | внутри отчёта (`quality`), `Status` и запроса провайдера | `telemetry.quality`, `telemetry.probeMeasurements`, метки сети | см. [Качество](#качество) |
| Перепись путей | внутри отчёта (`census`) и событие `census` | `telemetry.census` (требует `telemetry.report`) | аналитика |
| Трасса | `ExportTrace(after)` | `telemetry.trace` | локальная диагностика и воспроизведение сессии |

`Status` несёт `is_local_only: true`. Это пометка в данных, а не защита: `Status`, журнал и трасса содержат сырой идентификатор сети от хоста, имена серверов и тексты ошибок, поэтому в аналитику их не передают. Туда уходит только отчёт. Что именно в нём есть и чего нет, сказано в разделе [Приватность](#приватность).

Без блока данные не собираются:

- без `telemetry.events` журнала нет, `DrainEvents` возвращает ошибку `feature_disabled`;
- без `telemetry.report` нет отчёта и его счётчиков, `PeekReport` и `TakeReport` возвращают `feature_disabled`; отчёт не зависит от журнала и считает события, даже когда журнал выключен или переполнен;
- сводка качества создаётся, если есть `telemetry.quality`, или `telemetry.probeMeasurements`, или `telemetry.report` вместе с `network.hostLabels` либо `network.labelRequest`. Без `telemetry.quality` в ней нет ничего из TCP-измерений; с одними замерами проб в ней только поля `probe_*`; с одними метками сегмент содержит только `network`. Без `telemetry.report` закрытые сегменты не хранятся, качество видно только в `Status` и запросе провайдера;
- без `telemetry.pathLabels` метки путей не попадают в отчёт, сводку качества и перепись; в `Status` они есть всегда;
- без `telemetry.trace` нет кольца трассы, `ExportTrace` возвращает `feature_disabled`.

`Status` доступен всегда. Поля выключенных механизмов в нём либо отсутствуют, либо равны нулю.

Имена полей во всех выходных данных записаны в snake_case. Исключение одно: качество в запросе провайдера идёт в camelCase (см. [Status и запрос провайдера](#качество-в-status-и-в-запросе-провайдера)). Длительности целые, в миллисекундах, поле оканчивается на `_ms`. Скорость в десятичных килобитах в секунду (1 кбит/с = 1000 бит/с), поле содержит `kbps`. Байты целые. Время в событиях (`ts`) задаётся в миллисекундах Unix. Измерение, которого нет, в JSON отсутствует; измеренный ноль остаётся нулём.

## Status

`Status` строится при каждом вызове из текущего состояния. Пути (`paths`) отсортированы по тегу, балансировщики (`balancers`) тоже.

### Верхний уровень

| Поле | Тип | Что значит |
| --- | --- | --- |
| `census` | объект, необязательный | Только при `telemetry.census`: `is_selected` (сессия выбрана для переписи), `is_running` (перепись идёт), `checked` (сколько путей проверено в текущей или последней переписи). |
| `is_local_only` | bool | Всегда `true`. |
| `features` | объект `строка -> bool` | Действующие флаги, см. [features](#features). |
| `witness_quality` | объект качества, необязательный | Качество соединений через свидетелей наличия сети (`network.witnesses`) в текущей сети, без гистограмм. Есть при `telemetry.quality`. |
| `quality_samples_dropped` | целое, необязательное | Сколько измерений текущей сети отброшено, потому что исчерпан лимит в 64 пути или 16 КиБ имён. |
| `broken_subsystems` | массив строк | Пустой массив или `["app/observatory/health"]`: наблюдатель поймал панику и замер до перезапуска ядра. Паника другой подсистемы завершает процесс, поэтому других значений здесь нет. |
| `run` | строка | Идентификатор запуска ядра, 16 шестнадцатеричных символов. Тот же, что `health_run` в отчёте. |
| `network_id` | строка, необязательная | Сырой идентификатор сети, который передал хост. |
| `network` | объект, необязательный | Метки сети: `host` (от хоста, `network.hostLabels`) и `ip` (по запросу `network.labelRequest`). |
| `is_network_up` | bool | `false`, пока решения стоят на паузе из-за потери сети. |
| `connectivity` | строка, необязательная | Состояние проверки связи (`network.connectivity`): `ok`, `paused`, `doubt`, `ignored`. Без проверки поля нет. `ok` значит, что проверка включена и не видела отказа, а не что она прошла: сразу после старта, до первой проверки, тоже `ok`. |
| `generation` | целое | Номер поколения сети: растёт при смене сети, на которую реагирует хотя бы один балансировщик. |
| `is_idle` | bool | Пользовательского трафика нет дольше порога простоя. Выбор на старте по `startup.probe` считается спросом, как соединение пользователя. |
| `is_memory_low` | bool | Хост сообщил о нехватке памяти. |
| `probes_queued`, `probes_running` | целое | Пробы в очереди и выполняющиеся. |
| `groups` | массив, необязательный | Явные группы балансировщиков, см. [groups](#groups). |
| `paths` | массив | Пути (outbound), которые наблюдатель знает, см. [paths](#paths). |
| `balancers` | массив | Балансировщики `leastCost`, см. [balancers](#balancers). |
| `providers` | массив, необязательный | Включённые провайдеры, см. [providers](#providers). |
| `probe_requests` | массив, необязательный | Запросы проб хоста, от старых к новым, см. [probe_requests](#probe_requests). |

### paths

| Поле | Тип | Что значит |
| --- | --- | --- |
| `labels` | объект, необязательный | Метки пути, если они заданы, см. [Метки пути](#метки-пути). |
| `can_sleep`, `can_close_unchosen_idle` | bool, необязательные | Поддерживает ли outbound сон туннеля и закрытие тихих соединений. Поля появляются, когда это выяснено. |
| `quality` | объект качества, необязательный | Качество пути в текущей сети в snake_case, без гистограмм (`*_hist` есть только в отчёте), см. [Качество](#качество). |
| `outbound` | строка | Тег. |
| `is_probe_only` | bool | Проверочная учётка: только проверяется, пользовательский трафик не получает. |
| `state` | строка | `up`, `degraded`, `suspect`, `down` или `unknown`. |
| `reason` | строка, необязательная | Причина последней смены состояния, значения как у `path_state.reason`, плюс `chosen` (путь снова выбран после отдыха, без события). |
| `for_ms` | целое | Сколько путь в текущем состоянии. |
| `failures` | целое | Накопленные неудачные соединения, ещё не превращённые в решение. |
| `quarantine_ms`, `quarantine_level`, `is_provisional` | необязательные | Только у `down` в карантине: сколько осталось, уровень карантина, признак временного карантина (путь признан отказавшим, пока проверка связи не проходила). У проверочной учётки в `down` этих полей нет: она не в карантине, а ждёт перепроверки, см. `path_state` в разделе [Выбор и состояние путей](#выбор-и-состояние-путей). |
| `rtt_ms` | целое, необязательное | Время до ответа в последней успешной пробе, включая новое соединение через путь. Не скорость канала, см. [provider-protocol.md](provider-protocol.md), раздел 4. |
| `last_error` | строка, необязательная | Сырой текст ошибки последней неудачной пробы. Может содержать адрес. |
| `server_name` | строка, необязательная | Имя сервера TLS или REALITY, без него адрес сервера. |
| `conns_ok`, `conns_failed` | целое | Пользовательские соединения за жизнь пути. |
| `bytes_up`, `bytes_down` | целое | Байты пользовательских соединений за жизнь пути. |
| `failures_by_kind` | объект, необязательный | Число неудачных соединений по виду отказа (виды в [Общие значения](#общие-значения)). |
| `ttfb_p50_ms`, `ttfb_p90_ms` | целое | Время до первого байта за жизнь пути по грубой сетке корзин (50, 100, 200, 300, 500, 750, 1000, 1500, 2000, 3000, 5000, 8000 мс). Считается только при `telemetry.report` или `telemetry.metrics`, без замеров равно 0. |
| `probes`, `probes_failed`, `probe_bytes` | целое | Пробы пути за жизнь и их трафик в обе стороны. |
| `probe` | объект, необязательный | Последняя завершённая проба: `profile`, `reason`, `state`, `ago_ms`, `steps[]` с полями `name`, `is_passed`, `connect_ms`, `ms`, `bytes` и `destination` (индекс адреса при успехе) или `error` (класс при отказе). |
| `udp` | объект | Последняя UDP-проба: `state` (`unknown` без пробы, иначе `ok`, `lossy`, `dead`, `unsupported`), `answered`, `sent`, `rtt_ms`, `jitter_ms`, `age_ms`, `reason`. |
| `active_ms` | целое | Сколько времени путь был выбран, за жизнь. |
| `expires_in_ms`, `is_expired` | необязательные | Только у outbound с арендой провайдера: остаток аренды (ниже нуля после окончания) и признак, что аренда кончилась. |
| `is_asleep` | bool, необязательное | Outbound отпустил туннель; следующее соединение создаст его заново. |

### balancers

| Поле | Тип | Что значит |
| --- | --- | --- |
| `fallback_balancer` | строка, необязательная | Тег балансировщика из `leastCost.settings.fallbackBalancer`. |
| `is_falling_back` | bool | Трафик сейчас уходит в `fallback_balancer`. |
| `balancer` | строка | Тег балансировщика. |
| `active` | строка | Текущий выбор. Пустая строка при исчерпании. |
| `pinned` | строка, необязательная | Путь, закреплённый хостом или на старте (`startup.pin`). |
| `is_exhausted` | bool | Пригодных кандидатов нет. |
| `outbound` | строка | Текущий outbound: тот, в который роутер отправляет новые соединения балансировщика сейчас, см. [Через какой outbound идёт трафик](#через-какой-outbound-идёт-трафик). До первого выбора равен `fallbackTag`. |
| `outbound_labels` | объект, необязательный | Метки текущего outbound. |
| `active_for_ms` | целое, необязательное | Сколько держится текущий выбор. |
| `return_in_ms`, `return_target` | необязательные | Когда начнётся возврат на более дешёвый путь и на какой (`returnToCheaper`). |
| `candidates` | массив строк | Кандидаты балансировщика. |

### groups

`balancer`, `group`, `state` (`up` или `down`), `members` (число участников), `healthy` (участники в `up`, без истёкшей аренды, в группе не `down`), `quarantined`, `client_position` (позиция кольца, с которой берётся следующий участник), `failed` (отказы подряд), `quarantine_ms` (остаток карантина группы, 0 у `up`), `last_failure` (класс ошибки, вид отказа или причина состояния последнего отказавшего участника, необязательное).

### providers

| Поле | Что значит |
| --- | --- |
| `provider`, `method`, `version` | Тег, HTTP-метод, версия последнего применённого набора. |
| `members`, `probe_only_verified`, `draining` | Outbound в наборе, прошедшие проверку проверочные учётки среди них, старые обработчики, которые ещё закрываются. |
| `is_fetching`, `refreshes`, `failures` | Запрос идёт сейчас; число успешных обновлений; неудачи подряд. |
| `last_error` | Последняя ошибка: класс неудачного запроса (`timeout`, `tls`, `network`, `status`), `answer larger than <N> bytes` у слишком большого ответа, `provider request exceeds 256 KiB` у слишком большого запроса, причина пропуска дороги или причина, по которой ответ или набор отклонён. |
| `set_age_ms` | Возраст набора. |
| `next_trigger`, `next_in_ms` | Ближайший повод обновления и когда он наступит (может быть отрицательным, если ждёт). |
| `backoff_ms`, `retry_after_ms`, `attempts_last_hour` | Остаток паузы после неудач, остаток паузы `Retry-After`, попытки за последний час. |
| `renew_in_ms`, `expired`, `skew_ms`, `clock` | Ближайшее продление аренды, число истёкших аренд, расхождение часов, по которым считаются аренды, и источник этих часов (`server`, `date`, `saved`, `device`). |
| `headers`, `is_auth_refused`, `auth_retry_in_ms` | Имена заголовков, заданных ответом или хостом (значения не выводятся); бэкенд отверг заголовки; когда будет следующая попытка. |
| `answer_seq` | Номер `seq`, который `ExportProviderSet` возвращает вместе с набором. |
| `base_version`, `answer_base_version`, `is_base_changed` | Базовый конфиг при старте, базовый конфиг из последнего ответа, хосту сообщено о другом базовом конфиге. |
| `urls_count`, `urls_generation` | Число зеркал и сколько раз хост их заменил. Адреса не выводятся. |
| `request` | Последний запрос хоста (`RequestProviderRefresh`) с ревизии 7: `id`, `state` (`pending`, `applied`, `failed`), [`code`], `ago_ms`, [`wait_ms`]. То же, что `providers[].request` в `View`, см. [host-api.md](host-api.md). Нет до первого запроса. |

Протокол и смысл этих величин описаны в [provider-protocol.md](provider-protocol.md).

### probe_requests

Каждый элемент: `run`, `is_udp`, `id`, `balancer`, `age_ms`, `is_done`, `end` (необязательное, значения как у события `probe_request`) и `outbounds[]` с полями `outbound`, `result`, `source`, `probe_reason`, `rtt_ms`, `error`, `age_ms`, `quarantine_ms` и `udp` (вид как `paths[].udp`, только у UDP-запроса). Хранятся открытые запросы и 4 последних закрытых (1 при нехватке памяти). Подробно формат разобран в [host-api.md](host-api.md). `error` у неудачного элемента - класс ошибки, как в `probe_result.error`, у TCP и у UDP; сырой текст ошибки пробы остаётся только в `paths[].last_error`.

### features

Плоская карта флагов, которые действуют в этом запуске. Глобальные ключи: `census`, `trace`, `dial_failures`, `dial_timeout`, `server_unverified`, `first_reply`, `early_close`, `stalled_requests`, `partial_stalls`, `udp_replies`, `lone_failure_probe`, `step_profiles`, `candidates`, `probe_requests`, `udp_probe`, `connectivity`, `witnesses`, `network_memory`, `host_labels`, `label_request`, `pinning`, `view`, `user_present`, `memory_pressure`, `tunnel_sleep`, `close_unchosen_idle`, `events`, `report`, `quality`, `path_labels`, `probe_measurements`, `metrics`, `session_labels`.

Для каждого балансировщика добавляются ключи `balancers.<тег>.fallback_balancer`, `.probe_before_switch`, `.close_pending`, `.close_silent`, `.down_audit`, `.groups`, `.return_to_cheaper`, `.return_on_network_change`, `.network_changes`, `.startup_pin`, `.startup_probe`. Для каждого провайдера `providers.<тег>.is_enabled`, `.periodic`, `.min_healthy`, `.cost_above`, `.max_age`, `.network_change`, `.exhausted`, `.lease_renewal`, `.backend_schedule`, `.expired_lease_fallback`.

### Пример

```json
{
  "census": {"is_selected": false, "is_running": false, "checked": 0},
  "is_local_only": true,
  "features": {
    "census": true, "trace": false, "dial_failures": true, "dial_timeout": true, "server_unverified": true,
    "first_reply": true, "early_close": true, "stalled_requests": true, "partial_stalls": false,
    "udp_replies": false, "lone_failure_probe": false, "step_profiles": false, "candidates": false,
    "probe_requests": false, "udp_probe": false, "connectivity": true, "witnesses": false,
    "network_memory": false, "host_labels": true, "label_request": false, "pinning": false,
    "view": true, "user_present": false, "memory_pressure": true, "tunnel_sleep": false, "close_unchosen_idle": false,
    "events": true, "report": true, "quality": true, "path_labels": true, "probe_measurements": false,
    "metrics": false, "session_labels": true,
    "balancers.auto.fallback_balancer": false, "balancers.auto.probe_before_switch": true,
    "balancers.auto.close_pending": true, "balancers.auto.close_silent": true,
    "balancers.auto.down_audit": false, "balancers.auto.groups": false,
    "balancers.auto.return_to_cheaper": false, "balancers.auto.return_on_network_change": false,
    "balancers.auto.network_changes": false, "balancers.auto.startup_pin": false,
    "balancers.auto.startup_probe": false
  },
  "broken_subsystems": [],
  "run": "232c4c611bfbaee9",
  "network_id": "wifi:3f2a91c0",
  "network": {"host": {"type": "wifi"}},
  "is_network_up": true,
  "connectivity": "ok",
  "generation": 0,
  "is_idle": false,
  "is_memory_low": false,
  "probes_queued": 0,
  "probes_running": 0,
  "paths": [
    {
      "outbound": "de-1",
      "is_probe_only": false,
      "state": "down",
      "reason": "probe",
      "for_ms": 41250,
      "failures": 0,
      "quarantine_ms": 78750,
      "quarantine_level": 1,
      "last_error": "dial tcp 203.0.113.20:443: i/o timeout",
      "server_name": "de.example.invalid",
      "conns_ok": 0,
      "conns_failed": 3,
      "bytes_up": 0,
      "bytes_down": 0,
      "failures_by_kind": {"dial_timeout": 3},
      "ttfb_p50_ms": 0,
      "ttfb_p90_ms": 0,
      "probes": 1,
      "probes_failed": 1,
      "probe_bytes": 0,
      "probe": {"profile": "default", "reason": "failover", "state": "down", "ago_ms": 41250,
        "steps": [{"name": "default", "is_passed": false, "connect_ms": 0, "ms": 6000, "bytes": 0, "error": "dial_timeout"}]},
      "udp": {"state": "unknown", "answered": 0, "sent": 0, "rtt_ms": 0, "jitter_ms": 0, "age_ms": 0},
      "active_ms": 0
    },
    {
      "labels": {"country": "nl", "node": "nl-ams-12"},
      "quality": {
        "bytes_down": 4237,
        "ttfb_count": 1, "ttfb_p50_ms": 60, "ttfb_p90_ms": 60,
        "dial_count": 1, "dial_p50_ms": 40, "dial_p90_ms": 40,
        "tcp_count": 1, "tcp_rtt_p50_ms": 30, "tcp_rtt_p90_ms": 30, "tcp_rtt_min_ms": 22
      },
      "outbound": "nl-1",
      "is_probe_only": false,
      "state": "up",
      "reason": "probe",
      "for_ms": 40180,
      "failures": 0,
      "rtt_ms": 188,
      "server_name": "nl.example.invalid",
      "conns_ok": 1,
      "conns_failed": 0,
      "bytes_up": 130,
      "bytes_down": 4237,
      "ttfb_p50_ms": 100,
      "ttfb_p90_ms": 100,
      "probes": 1,
      "probes_failed": 0,
      "probe_bytes": 33590,
      "probe": {"profile": "default", "reason": "failover", "state": "up", "ago_ms": 40180,
        "steps": [{"name": "default", "is_passed": true, "connect_ms": 104, "ms": 188, "bytes": 33590, "destination": 0}]},
      "udp": {"state": "unknown", "answered": 0, "sent": 0, "rtt_ms": 0, "jitter_ms": 0, "age_ms": 0},
      "active_ms": 40180
    }
  ],
  "balancers": [
    {"is_falling_back": false, "balancer": "auto", "active": "nl-1", "is_exhausted": false,
      "outbound": "nl-1", "outbound_labels": {"country": "nl", "node": "nl-ams-12"},
      "active_for_ms": 40180, "candidates": ["de-1", "nl-1"]}
  ]
}
```

### Через какой outbound идёт трафик

Текущий outbound балансировщика - это outbound, в который сейчас уходят его новые соединения. Его тег стоит в `balancers[].outbound`, его метки - в `outbound_labels` (`selection.labels` пути или набора провайдера). Уже открытые соединения остаются на том пути, где открылись. `outbound` выводится по тем же правилам, что роутер:

- выбор балансировщика (`active`, в том числе закреплённый `pinned`), если он может принимать соединения. Путь с истёкшей арендой при выключенном `apply.expiredLeaseFallback` и проверочная учётка не могут: тогда `active` не пуст, а `outbound` равен `fallbackTag`;
- при исчерпании (`is_exhausted: true`, `active` пуст) путь, который балансировщик ещё держит (удержание, см. `fallbackBalancer` в [config.md](config.md)): ядро ещё не знает, пропала сеть или пути;
- после удержания с заданным `fallback_balancer` текущий outbound того балансировщика по тем же правилам (цепочка не длиннее 4 балансировщиков, считая исходный). `is_falling_back` становится `true` на первом соединении, которое ушло туда, а `outbound` показывает целевой путь сразу;
- иначе `fallbackTag` балансировщика из `routing.balancers`. У него обычно нет меток, `outbound_labels` тогда отсутствует.

До первого соединения через балансировщик он ещё не выбирал: `active` пуст, `is_exhausted: false`, а `outbound` по последнему правилу равен `fallbackTag`, хотя первое же соединение выберет путь. Поэтому `outbound`, равный `fallbackTag`, означает заглушку только при `is_exhausted: true` или непустом `active`. Команда `View` называет этот случай `state: connecting` с `phase: waiting`.

`outbound` пуст только у балансировщика, чей `fallback_balancer` не `leastCost`: выбор такого балансировщика ядро не знает. Переназначение роутера через API (`xray api bo`) `outbound` не учитывает.

`active` показывает другое: выбор самого балансировщика. Он бывает пуст, пока трафик ещё идёт через удерживаемый путь или резервный балансировщик. Пока в `active` пусто, путь, с которого балансировщик ушёл, виден только в событии `exhausted.from` и в `switch_history` отчёта.

### Метки пути

`paths[].labels` и `balancers[].outbound_labels` выводятся всегда, когда у пути есть метки. Метки берутся из `selection.labels` пути, объединённых с `labels` его группы (не больше 6 после объединения), или из набора провайдера. Новый набор провайдера с тем же outbound и другими метками меняет метки на месте: обработчик и соединения остаются, `Status` показывает новые метки со следующего вызова. `telemetry.pathLabels` решает только, попадают ли метки путей в отчёт (`path_labels`), в сводку качества и в перепись.

## Журнал событий

### Формат

`DrainEvents` возвращает накопленные события и забывает их. Ответ: `{"events": [...], "dropped": n}`. Каждая запись:

```json
{"seq": 27, "ts": 1791896412345, "type": "path_state", "fields": {"outbound": "nl-1", "state": "down", "reason": "probe", "quarantine_ms": 120000, "quarantine_level": 1}}
```

`seq` начинается с 1 в каждом запуске ядра и растёт на единицу на каждое событие. Пропуск в `seq` означает потерянные события. `ts` задаётся в миллисекундах Unix по часам устройства. `fields` отсутствует, если у события нет полей.

Журнал хранит не больше `telemetry.events.maxEvents` записей (по умолчанию 1000, потолок 4096) и не больше `telemetry.events.maxBytes` байт сериализованного JSON (по умолчанию 65536, потолок 256 КиБ). При переполнении выбрасываются самые старые записи; событие больше `maxBytes` целиком выбрасывается само и прежних записей не трогает. `dropped` в ответе считает выброшенные с прошлого `DrainEvents`. Отчёт считает события сам и от переполнения журнала не зависит, а потерю показывает в `events_dropped`.

### Общие значения

Состояния пути: `up`, `degraded`, `suspect`, `down`, `unknown`.

Виды отказа пользовательского соединения (`kinds`, `failures_by_kind`, `evidence`): `dial_failed`, `dial_timeout`, `server_unverified`, `no_response`, `closed`, `stall`, `partial_stall`.

Причины проб (`probe_start.reason`, `probe_result.reason`): `confirm`, `failover`, `wake`, `network_change`, `remembered`, `recheck`, `lone`, `candidate`, `audit`, `group_recheck`, `return`, `host`, `census`, `startup`, `standby`. Причина `startup` - проверка выбора балансировщика с блоком `startup.probe`, пока в этом запуске у него не было подтверждённого пути; её результат обрабатывается как результат `network_change`. Причина `standby` (ревизия 6) - проверка пути с `probePolicy: standby`, пока балансировщик его не выбрал ([config.md](config.md), раздел 3.1); результат меняет состояние пути, как у любой пробы, а отказ не уводит трафик: путь и так не выбран. У UDP-проб свои причины: `udp_switch`, `udp_suspect`, `host`.

Классы ошибки пробы (`probe_result.error`, `steps[].error`, `connectivity.error`): `dial_failed`, `dial_timeout`, `server_unverified`, `no_progress`, `timeout`, `closed`, `status_<код HTTP>`, `short_body`, `other`. Текст ошибки в эти поля не попадает.

Необязательные поля в таблицах ниже взяты в квадратные скобки. Поля `*_ms` и счётчики целые, `is_*` логические, `cost` и `multiplier` дробные.

### Выбор и состояние путей

| Событие | Поля | Когда |
| --- | --- | --- |
| `switch` | `balancer`, `from`, `to`, `reason`, `held_ms`, `cost` | Новые соединения пошли через другой путь. `held_ms` - сколько держался прежний, `cost` - цена нового. Значения `reason` ниже. |
| `path_state` | `outbound`, `state`, `reason`, [`by`], [`evidence_ms`], [`quarantine_ms`, `quarantine_level`], [`is_provisional`], [`failures`, `targets`, `kinds`, `is_strong`] | Путь сменил состояние. Подробно ниже. |
| `first_success` | `balancer`, `outbound`, `after` | Первое удачное пользовательское соединение через последний выбор балансировщика. `after` - `reason` того `switch`. Завершает старт ядра или сбой (`start_ms` и `incidents` отчёта). |
| `exhausted` | `balancer`, `from`, `candidates`, [`fallback_balancer`], [`held`] | Пригодных кандидатов не осталось. `from` - прежний выбор, `candidates` - число кандидатов. `held` (равен `from`, если тот не пуст) - путь, который балансировщик пока держит для новых соединений; когда удержание кончается, отдельного события нет, видно по `Status.balancers[].outbound`. |
| `fallback_balancer` | `balancer`, `to`, `is_active` | `is_active: true` - первое соединение ушло в балансировщик `to`; `false` - переход закончился, балансировщик снова выбрал свой путь. |
| `pinned_down` | `balancer`, `outbound`, [`is_start_pinned`] | Закреплённый путь отказал, балансировщик остаётся на нём. `is_start_pinned` - закрепление из `leastCost.settings.startup.pin`, а не от хоста. |
| `pin` | `balancer`, `outbound` | Хост закрепил путь. |
| `release` | `balancer`, `outbound`, `by` | Закрепление снято: `by` = `host` (хост) или `timer` (истёк `startup.pin.timeout`). |
| `stale_request` | `request`, `balancer`, `run` | Закрепление или его снятие с чужим `run` проигнорированы. `request` = `pin` или `release`, `run` - значение от хоста. |
| `fresh_path_down` | `outbound` | Новый или заменённый путь провайдера впервые признан `down` в первые пять минут после включения. |
| `group_state` | `balancer`, `group`, `state`, `members`, `failed`, `quarantine_ms` | Явная группа стала `down` после отказов подряд или `up` после успешной проверки. |
| `pending_closed` | `outbound`, `connections`, `reason`, [`is_silent`] | Закрыты соединения отказавшего пути: в момент решения и при уходе с пути те, что ничего не получили; с `is_silent: true` - те, что замолчали после решения (через `leastCost.settings.failover.closeSilent.after`, по умолчанию 8 с). `reason` - причина `down`. |
| `return_start` | `balancer`, `from`, `to` | Начата проба более дешёвого пути для возврата. |
| `return_failed` | `balancer`, `outbound`, `successes` | Возврат отменён после неудачной пробы. `successes` - сколько проб подряд перед этим прошло. |
| `return_delayed` | `balancer`, `outbound`, `delay_ms` | Возврат отложен из-за штрафа за прошлую неудачу. |
| `return_penalty` | `balancer`, `outbound`, `multiplier` | Путь, на который только что вернулись, отказал в течение 5 минут после возврата. `multiplier` - новый множитель штрафа, на который удлиняется ожидание следующего возврата на этот путь. Окно 5 минут не настраивается. |

Значения `switch.reason`:

- `start`, `start_pinned`, `start_remembered`, `start_with` - выбор при старте: по цене, по стартовому закреплению, по памяти сетей, по `startup.outbound`. Решением не считаются;
- `failover` - уход с отказавшего пути;
- `recovered` - возврат после того, как проба доказала, что путь снова работает;
- `remembered` - выбор по памяти сетей после смены сети;
- `return` - возврат на более дешёвый путь (`returnToCheaper`);
- `network_change` - перевыбор после смены сети;
- `network_up` - проверка связи снова прошла и вернула пути, отброшенные, пока она не проходила, балансировщику, у которого не было пригодных;
- `candidates_changed` - изменился список кандидатов;
- `promoted` - проверочная учётка, прошедшая проверку, стала обычным путём и заменила мёртвый или исчерпанный выбор;
- `pin` - закрепление хостом;
- `retired` - прежний путь ушёл из набора провайдера;
- `expired` - кончилась аренда прежнего пути, а сам он работал. Если он ещё и отказал, причина `failover`.

`path_state.reason` по состояниям:

| `state` | `reason` |
| --- | --- |
| `up` | `traffic`, `probe`, `host_probe`, `candidate`, `rescued` |
| `degraded` | имя первого шага профиля, который не прошёл (если имени нет, `probe`) |
| `down` | `traffic`, `probe`, `return_probe`, `host_probe`, `candidate` |
| `unknown` | `network_up`, `replaced` (провайдер изменил настройки пути), `probe_changed` (провайдер изменил профиль пробы неизменённого пути) |

`quarantine_ms`, `quarantine_level` и `is_provisional` есть только у `down` с карантином. Проверочная учётка, не прошедшая проверку (`state: down`, `reason: candidate`), в карантин не попадает, и этих полей у неё нет. Её перепроверяют после паузы, которая растёт как карантин: 2, 4, 8, 16, 30 минут с разбросом 30%. Счёт неудач подряд обнуляют успешная проверка и смена сети. Перепроверка идёт, пока пользователь активен, сеть на месте, памяти хватает и балансировщик не закреплён ([config.md](config.md), раздел 3.5, `candidates`).

Подозрение (`state: suspect`, `reason: traffic`) пишется по пассивным уликам против выбранного пути и несёт `failures` (неудачные соединения), `targets` (разных адресатов), `kinds` (объект `вид отказа -> число`) и `is_strong` (улика сильная: отказ установки соединения или проверки сервера). `by` есть только при `reason: rescued`: `confirm` (подтверждающая проба, уже бежавшая к моменту решения), `audit` (проверка решения `down`) или `traffic` (сработало соединение пользователя, начатое после решения `down`). `evidence_ms` есть у `suspect` и `down`: сколько прошло с момента, когда пользователь начал ждать самое раннее неудачное соединение с последнего успеха пути. Для соединения без ответа это его начало, для замершего - последняя запись или чтение. `quarantine_ms` и `quarantine_level` есть у каждого `down`, `is_provisional: true` - у временного карантина. Смены состояния при потере сети (подозрение снимается в `unknown`) и при смене сети событий не пишут.

### Пробы

| Событие | Поля | Когда |
| --- | --- | --- |
| `probe_start` | `outbound`, `reason`, `balancer`, `running`, [`request`] | Проба началась. `running` - число выполняющихся проб путей вместе с этой. `request` - номер запроса хоста у причины `host`. |
| `probe_result` | `outbound`, `reason`, `balancer`, `is_ok`, `rtt_ms`, `bytes`, `error`, `is_stale`, `profile`, `state`, `steps`, [`request`] | Проба завершилась. `error` - пустая строка при успехе, иначе класс. `bytes` - трафик в обе стороны поверх TLS. `state` - вывод из результата: `up`, `degraded` или `down`. `is_stale: true` - за время пробы сменились сеть, обработчик или профиль, и результат путь не изменил (шаги всё равно учтены в отчёте). `steps` - только попытанные шаги, поля как у `Status.paths[].probe.steps`. |
| `probe_request` | `is_udp`, `request`, `balancer`, `outbounds`, `end`, `ms`, `probed`, `reused`, `ok`, `failed`, `down`, `skipped`, `expired`, `gone` | Закончился запрос хоста, который ждал пробы. Запрос, на который ответ был готов сразу, события не пишет. `outbounds` - число путей. `end`: `done`, `timeout`, `network_change`, `gone` (балансировщик исчез), `replaced` (UDP-путь заменён). `probed` и `reused` считают ответы свежей пробой и недавним успехом, остальные - элементы по результату. |
| `probe_only_promoted` | `provider`, `outbound` | Провайдер сделал проверочную учётку, прошедшую проверку, обычным путём. |

Проба переписи пишет `probe_result` и тогда, когда её отменили (вытеснила другая проба, сменилась сеть): `error` у такой пробы пустой. Остальные отменённые пробы `probe_result` не пишут.

### UDP

| Событие | Поля | Когда |
| --- | --- | --- |
| `udp_probe` | `outbound`, `reason`, `state`, `answered`, `sent`, `rtt_ms`, `jitter_ms`, `destination_index`, `class` | Текущая UDP-проба завершилась. `reason`: `udp_switch`, `udp_suspect`, `host`. `state`: `ok`, `lossy`, `dead`, `unsupported`. `destination_index` - индекс адреса с нуля. `class` - причина немедленного отказа у `unsupported`: `disabled`, `closed`, `dial_failed`, `dial_timeout`, `server_unverified`, `send_timeout`, `reset`, `refused`, `eof`, `tls`, `timeout`, `other`; иначе пустая строка. Устаревшие и отменённые пробы события не пишут. |
| `udp_suspect` | `outbound`, `flows`, `targets`, `window_ms` | Пассивные UDP-улики против выбранного пути. TCP-состояние пути не меняется. |
| `udp_skip` | `outbound`, `reason` | UDP-пробу не удалось начать. `reason`: `per_hour`, `asleep`, `idle`, `memory`, `recent`. Каждая причина пишется один раз до следующей пробы или сброса. |

### Сеть и устройство

| Событие | Поля | Когда |
| --- | --- | --- |
| `connectivity` | `is_ok`, `rtt_ms`, `error` | Результат проверки связи. `error` - класс, при успехе пустая строка. |
| `network_down` | `error` | Решения поставлены на паузу. `error` - класс неудачной проверки или `host`, если о потере сообщил хост. |
| `network_up` | `by` | Решения возобновлены: `connectivity` (проверка прошла), `traffic` или `probe` (путь сработал), `witness` (сработало соединение через свидетеля). |
| `connectivity_doubt` | `failed_checks`, `paused_ms` | Проверка не проходила дольше 1 минуты (константа); решения возобновлены временно. |
| `connectivity_ignored` | `failed_checks` | Проверка не проходит, хотя путь работает: три неудачные проверки подряд, каждая после успеха. До следующей смены сети она не используется. |
| `network_change` | `network`, `is_known`, `generation`, [`is_ignored`], [`labels`] | Хост сообщил о смене сети. `network` - хеш идентификатора от хоста (`h:` и 12 hex-цифр), одинаковый в пределах запуска; сам идентификатор есть только в Status. `is_known` - сеть есть в памяти сетей (`network.memory`). `is_ignored: true` - ни один балансировщик не реагирует на смену сети (`networkChanges` нигде нет), выученное сохраняется, `generation` не растёт. `labels` - метки хоста новой сети. |
| `network_labels` | `is_ok`, `error`, `labels` | Завершился запрос меток сети (`network.labelRequest`). `error` при успехе пустой, иначе `request`, `timeout`, `network`, `status_<код>`, `no_labels`, `canceled`. `labels` - метки текущей сети (объект или `null`). |
| `memory_low` | `is_low` | Хост сообщил о нехватке памяти (`true`) или об её окончании. |
| `outbound_sleep` | `outbound`, `idle_ms`, `reason` | Outbound отпустил туннель. `idle_ms` - время с последнего использования. `reason`: `idle` (после `resources.tunnelSleep.afterLastUse`), `down` (раньше, пока путь в карантине), `memory` (раньше, при нехватке памяти). |
| `idle_closed` | `outbound`, `connections` | Закрыты тихие соединения пути, который никто не выбрал (`resources.closeUnchosenIdle`). Отказа за этим нет. |

### Провайдер

| Событие | Поля | Когда |
| --- | --- | --- |
| `provider_fetch` | `provider`, `trigger`, `retry`, `attempt`, `mirror`, `via`, `outbound`, `method`, `refresh`, `is_ok`, `status`, `ms`, `bytes`, [`code`], [`retry_after_ms`], [`error`] | Один запрос обновления через одну запись `via` к одному зеркалу. Варианты и поля ниже. |
| `provider_apply` | `provider`, `trigger`, `is_ok`, [`version`], [`is_changed`], [`added`, `replaced`, `removed`, `was_exhausted`], [`unchanged`], [`error`], [`ignored_keys`], [`leases`, `renewed`, `expired`, `skew_ms`, `clock`], [`headers`, `is_headers_stale`], [`base_version`] | Ответ применён, совпал с действующим набором или отвергнут. Варианты ниже. |
| `provider_skip` | `provider`, `trigger`, `reason`, [`wait_ms`] | Повод требует обновления, но начать его нельзя. `reason`: `min_interval`, `backoff`, `retry_after`, `max_per_hour`, `idle`, `network_down`, `pinned`, `memory`, `auth`. Пишется один раз на пару повод и причина до начала обновления. |
| `provider_expired` | `provider`, `outbound`, `was_active`, `renewal` | Аренда пути кончилась. `was_active` - путь был выбран. `renewal` - почему не было продления: `refused` (бэкенд ответил без него), `failed` (последнее обновление с запросом не удалось), `off` (продление выключено: `providers[].triggers.leaseRenewal` задан как `{"isEnabled": false}`), `saved` (кончилась в сохранённом хостом наборе до старта), `pending` (обновление с запросом ещё не завершилось). |
| `provider_auth_failed` | `provider`, `trigger`, `status`, `via`, `code` | Бэкенд отверг заголовки (`status` 401 или 403, `code` `unauthorized` или `forbidden`). Все поводы, включая хост, ждут новых заголовков или конца паузы. |
| `provider_request` | `provider`, `id`, `state`, [`code`] | Закрыт запрос хоста `id` (ревизия 7): `state` `applied` или `failed`, у `failed` есть `code` - код тела ошибки протокола, `rejected` или `unreachable`. Запрос закрывает только обновление, начатое после него. |
| `provider_headers` | `provider`, `by`, `names` | Изменились заголовки запросов: `by` = `answer`, `initial` или `host`. `names` - имена изменённых заголовков, значения не выводятся. |
| `provider_base_changed` | `provider`, `from`, `to` | Применённый ответ назвал другой базовый конфиг `to`, ядро работает с `from`. Пишется один раз на значение. |
| `provider_urls` | `provider`, `count`, `generation` | Хост заменил зеркала (`SetProviderURLs`). Адреса не выводятся. |
| `provider_drained` | `provider`, `outbound`, `is_replaced` | Закрыт outbound, ушедший из набора, или старый обработчик заменённого: по `providers[].apply.drainTimeout`, при снятии закрепления, сразу без соединений или раньше срока, чтобы закрывающихся было не больше `providers[].apply.maxOutbounds`. |

Значения `trigger`: `host`, `exhausted`, `expiry`, `min_healthy`, `cost_above`, `network_change`, `next_refresh`, `max_age`, `interval`, а у `provider_apply` ещё `initial` (стартовый набор).

`provider_fetch` бывает трёх видов:

- обычный запрос: все поля из таблицы. `mirror` - номер зеркала с 1, `outbound` - путь, через который пошёл запрос, `refresh` - номер обновления, `retry` - уровень паузы после неудач, `bytes` - размер ответа. `code` - код ошибки из тела ответа по протоколу, `retry_after_ms` - пауза `Retry-After` после ограничения сверху. `is_ok: true` означает, что бэкенд ответил, в том числе ответом с нарушением протокола; отказ такого ответа пишет `provider_apply`. `error` при неудаче - класс `timeout`, `tls`, `network` или `status` (код в `status`); у ответа сверх предела размера текст `answer larger than <N> bytes`, у ответа, который не разобран как JSON, причина вида `invalid answer: ...` без адресов;
- пропущенная запись `via`: `provider`, `trigger`, `retry`, `attempt`, `via`, `outbound`, `method`, `refresh`, `is_ok: false` и `error` со значением `balancer_exhausted`, `current_outbound_down`, `probe_only_outbound`, `outbound_down` или `no_such_outbound`;
- запрос больше 256 КиБ: `provider`, `trigger`, `is_ok: false`, `code: "request_too_large"`.

`provider_apply`:

- применён и изменил набор: `provider`, `trigger`, `version`, `is_ok: true`, `is_changed: true`, `added`, `replaced`, `removed`, `unchanged`, `was_exhausted` (у балансировщика в этот момент не было пригодных);
- совпал с действующим: `provider`, `trigger`, `version`, `is_ok: true`, `is_changed: false`, `unchanged`;
- к обоим вариантам добавляются: [`ignored_keys`] (до 16 ключей ответа, которых ядро не знает и пропустило, с путём: `ttl`, `selection.weight`, `groups[].region`), [`leases`, `renewed`, `expired`, `skew_ms`, `clock`] (если ответ называет аренды), [`headers`, `is_headers_stale`] (если ответ задал заголовки; `is_headers_stale` - хост сменил заголовки, пока шёл запрос), [`base_version`] (базовый конфиг из ответа);
- отвергнут при сборке: `provider`, `trigger`, `version`, `is_ok: false`, `error` (текст до 200 символов), [`ignored_keys`]. Стартовый набор, на котором сборка или запуск outbound упали с паникой, отвергается так же с `error: "panic"`, и ядро стартует без него; наблюдатель при этом не замораживается. Если паника случилась уже после регистрации outbound набора, в коде решений, событие то же, но наблюдатель замораживается (`broken_subsystems`);
- отвергнут при разборе ответа или стартового набора: `provider`, `trigger`, `is_ok: false`, `error`.

`error` у `provider_apply` - текст причины, а не класс. Подробно о полях аренды, часах и заголовках см. [provider-protocol.md](provider-protocol.md).

### Перепись и сбои

| Событие | Поля | Когда |
| --- | --- | --- |
| `census` | `network`, `started_ms_ago`, `duration_ms`, `is_complete`, `paths`, [`path_labels`] | Перепись завершилась или прервалась. Поля как у `census` в отчёте, без `additional`, см. [Перепись путей](#перепись-путей). |
| `panic_recovered` | `scope`, `where` | Перехвачена паника в рабочей горутине. `scope`: `connection` или `subsystem` (только у наблюдателя). `where` - статическое имя функции в исходниках. Значение паники и аргументы не пишутся. Та же строка уходит в лог ошибок Xray. |

Паника подсистемы вне наблюдателя завершает процесс, как и 20 паник соединений с одного места за минуту; места наблюдателя в этот счёт не входят. Перед таким завершением строка `panic_recovered` дополнительно пишется в stderr: лог Xray пишется асинхронно и может не успеть. Отказ самого наблюдателя (паника его цикла, его кода на пути соединения или применения стартового набора после регистрации outbound) отменяет его пробы и работу провайдера, сохраняет последний применённый набор и замораживает текущий выбор путей. Новые и уже открытые соединения после этого не меняют состояние путей и не попадают в счётчики отчёта.

## Отчёт

### Окно

Отчёт описывает окно: отрезок от предыдущего взятого отчёта (или старта ядра) до текущего момента. Своего таймера у ядра нет, хост сам вызывает команды.

- `TakeReport(isFinal: false)` возвращает отчёт с `report_reason: "interval"`, если в окне было хотя бы одно решение и окно длится не меньше `telemetry.report.interval` (по умолчанию 24 ч). Иначе данных нет. После выдачи начинается новое окно.
- `TakeReport(isFinal: true)` закрывает запуск. Отчёт с `report_reason: "stop"` выдаётся, если за весь запуск было хоть одно решение или сессия попала в выборку тихих сессий. Перед этим закрываются незавершённые сбои (`outcome: "open"`), UDP-потоки и прерывается перепись (`reason: close`). После этого вызова ни `TakeReport`, ни `PeekReport` ничего не возвращают.
- `PeekReport` возвращает то, что сейчас вернул бы `TakeReport(isFinal: true)`, с `report_reason: "snapshot"`, и ничего не закрывает. Хост сохраняет снимок, чтобы отправить окно, если процесс убьют до остановки.

Решения, которые делают окно достойным отчёта: `switch` с причиной не из стартовых, `exhausted`, `fallback_balancer`, `return_failed`, `pinned_down` не от стартового закрепления, `provider_apply` отвергнутый или изменивший набор (кроме `initial`), `provider_expired` с `renewal` не `saved`, `provider_auth_failed`, `provider_base_changed`, `census` (перепись завершилась или прервалась) и перехваченная паника.

`report_index` - номер отчёта в запуске с 1. Снимок несёт тот же номер, что получит следующий взятый отчёт. Получатель хранит одну запись на пару `health_run` + `report_index`: если пришли и снимок, и взятый отчёт, остаётся взятый. Ядро ставит только `stop`, `interval` и `snapshot`; другие значения `report_reason` может подставить хост.

`telemetry.report.quietSampleFraction` (от 0 до 1, по умолчанию 0) - доля запусков, которые дают финальный отчёт и снимок даже без решений. Выбор делается один раз при старте и виден в `is_quiet_sample`. Отчётов `interval` без решений выборка не добавляет.

### Верхний уровень

| Поле | Тип | Что значит |
| --- | --- | --- |
| `census` | объект, необязательный | Снимок переписи, завершённой в окне, см. [Перепись путей](#перепись-путей). |
| `labels` | объект, необязательный | Метки сессии из `telemetry.report.sessionLabels.labels`. |
| `path_labels` | объект `тег -> метки`, необязательный | Метки путей из `paths` и из сегментов качества. Только при `telemetry.pathLabels`. |
| `health_run` | строка | Идентификатор запуска, как `Status.run`. |
| `report_reason` | строка | `stop`, `interval`, `snapshot`. |
| `report_index` | целое | Номер отчёта в запуске. |
| `window_ms`, `session_ms` | целое | Длина окна и запуска по настенным часам (время сна устройства входит). |
| `recent` | объект, необязательный | Исход пользовательских соединений перед отчётом, см. ниже. |
| `census_probes`, `census_probe_bytes` | целое | Пробы переписи и их трафик. В `probes` и `probe_bytes` не входят. |
| `fallback_balancer_count` | целое | Сколько раз начинался переход в другой балансировщик. |
| `fallback_balancer_ms` | объект `тег -> мс`, необязательный | Время в переходе по исходным балансировщикам в этом окне, включая незавершённый переход. |
| `panics_recovered` | целое | Перехваченные паники в окне, в том числе не попавшие в журнал. |
| `udp_probe_count`, `udp_probe_ok_count`, `udp_probe_lossy_count`, `udp_probe_dead_count` | целое | Текущие UDP-пробы и их результаты. `unsupported` входит только в общее число. |
| `udp_probe_second_destination_count` | целое | UDP-пробы, дошедшие до второго адреса. |
| `udp_suspect_count`, `udp_suspect_confirmed_count` | целое | События `udp_suspect` и пробы `dead` с причиной `udp_suspect`. |
| `events` | целое | События, записанные в окне (считаются и при выключенном журнале). |
| `events_dropped` | целое | События, которые журнал выбросил в окне. |
| `switches`, `switch_reasons`, `switch_history` | целое, объект, массив | Число `switch`, их число по `reason` и 10 последних в виде `{from, to, reason}`. |
| `exhausted_count`, `pinned_down_count` | целое | События `exhausted` и `pinned_down`. |
| `group_down_count` | целое | `group_state` с `down`. |
| `path_down_count`, `path_suspect_count`, `path_degraded_count` | целое | `path_state` с этими состояниями. |
| `path_rescued_count` | целое | `path_state` с `reason: rescued`. Само по себе не решение; балансировщик, вернувшийся на путь, пишет `switch` с `recovered`. |
| `audit_count`, `audit_passed_count`, `audit_skipped_count` | целое | Начатые проверки решений `down`, проверки, доказавшие, что путь жив, и решения, проверку которых не удалось начать. |
| `wake_probe_count`, `wake_probe_failed_count`, `wake_skipped_count` | целое | Начатые пробы по сигналу присутствия пользователя, неудачные из них (в том числе устаревшие) и пропущенные (свежее доказательство, интервал или лимит). |
| `lone_probe_count`, `lone_probe_failed_count` | целое | Начатые и неудачные пробы одиночного отказа. |
| `candidate_probes`, `candidate_probes_failed` | целое | Начатые и неудачные пробы проверочных учёток. |
| `probe_only_promoted_count` | целое | Проверочные учётки, ставшие обычными путями. |
| `fresh_path_down_count` | целое | События `fresh_path_down`. |
| `pending_closed_count`, `idle_closed_count` | целое | Сумма `connections` событий `pending_closed` и `idle_closed`. |
| `probes`, `probes_failed`, `probe_bytes` | целое | События `probe_result` (без переписи), неудачные и сумма их `bytes`. |
| `host_probe_requests`, `host_probes` | целое | События `probe_request` и пробы с причиной `host`. |
| `return_attempts`, `return_failures` | целое | `return_start` и `return_failed`. |
| `network_changes`, `network_known`, `network_down_count` | целое | `network_change`, из них с `is_known`, и `network_down`. |
| `provider_fetches`, `provider_fetch_failures`, `provider_triggers` | целое, целое, объект | `provider_fetch`, из них без `is_ok`, и их число по `trigger`. |
| `provider_retry_after` | целое | `provider_fetch` с `retry_after_ms`. |
| `provider_applied`, `provider_rejected` | целое | `provider_apply`, изменившие набор (кроме `initial`), и отвергнутые. |
| `provider_expired` | целое | `provider_expired`. |
| `provider_renewals` | целое | `provider_apply` с `is_ok` и `renewed > 0`, кроме `initial`. |
| `provider_auth_failed`, `provider_base_changed`, `provider_urls_set` | целое | События `provider_auth_failed`, `provider_base_changed`, `provider_urls`. |
| `provider_refreshes`, `provider_refreshes_dropped` | массив, целое | Первые 10 обновлений провайдера, закончившихся в окне, и сколько ещё не поместилось. |
| `incidents`, `incidents_dropped` | массив, целое | Первые 10 сбоев, закончившихся в окне, и сколько ещё не поместилось. |
| `start_ms` | целое, необязательное | От старта ядра до первого удачного пользовательского соединения. Есть только в окне, где оно произошло. |
| `evidence` | объект, необязательный | Точность пассивных улик по видам, см. ниже. |
| `pins`, `releases` | целое | События `pin` и `release`. |
| `memory_low_count` | целое | `memory_low` с `is_low: true`. |
| `outbound_sleeps` | целое | События `outbound_sleep`. |
| `active` | объект `балансировщик -> тег` | Выбор на момент отчёта, пустая строка при исчерпании. |
| `is_balancer_exhausted` | bool | Хотя бы один балансировщик исчерпан. |
| `is_network_up`, `connectivity` | bool, строка | Как в `Status`. |
| `is_quiet_sample` | bool | Запуск попал в выборку тихих сессий. |
| `groups` | массив, необязательный | Явные группы, как `Status.groups`. |
| `paths` | объект `тег -> путь` | Первые 64 пути по тегу, см. ниже. |
| `quality` | массив, необязательный | Сегменты качества окна, см. [Качество](#качество). |
| `quality_segments_dropped` | целое, необязательное | Непустые сегменты, выброшенные лимитом в 4 сегмента. |

Счётчики выключенных механизмов присутствуют и равны нулю. `switch_reasons`, `switch_history`, `provider_triggers`, `provider_refreshes` и `incidents` при отсутствии данных - пустые объект или массив.

### paths.<tag>

Путь, который наблюдатель забыл и увидел снова, считается с нуля.

| Поле | Что значит |
| --- | --- |
| `state` | Состояние на момент отчёта. |
| `ttfb_life_p50_ms` | Медиана времени до первого байта за всю жизнь пути, по грубой сетке корзин от 50 мс (как `Status.paths[].ttfb_p50_ms`), 0 без замеров. Не путать с `quality[].paths.<tag>.ttfb_p50_ms`, которое считается за сегмент по сетке от 10 мс. |
| `active_ms`, `degraded_ms` | Время, когда путь был выбран и когда был в `degraded`, в окне. |
| `bytes_up`, `bytes_down`, `conns_ok`, `conns_failed` | Пользовательский трафик и соединения в окне. TCP и UDP вместе, на уровне транспорта. |
| `probes`, `probe_bytes` | Пробы пути и их трафик в окне. |
| `probe_steps` | Массив `{name, passed, failed}` по шагам, которые реально выполнялись в окне, включая устаревшие пробы. Порядок по текущему профилю, затем имена из прежнего профиля в порядке появления. |
| `downs_strong`, `downs_weak` | Новые решения `down` по сильным и слабым пассивным уликам. Включают неудачные пробы одиночного отказа вместе с отказом соединения и не включают пробы по пробуждению и смене сети. |
| `audits`, `audits_passed` | Проверки решений `down` этого пути и успешные из них. |
| `lone_probes`, `lone_probes_failed` | Пробы одиночного отказа этого пути и неудачные из них. |
| `udp_probes`, `udp_probes_dead` | UDP-пробы пути и мёртвые из них. |
| `udp_rtt_p50_ms`, `udp_jitter_p50_ms` | Медианы RTT и джиттера ответов UDP-проб в окне по той же грубой сетке от 50 мс, 0 без замеров. |
| `udp_state` | Последний результат UDP-пробы или `unknown`. |
| `udp_flows`, `udp_flows_unanswered` | UDP-потоки, закончившиеся в окне; без ответа - хотя бы один отправленный буфер и ни одного полученного байта. Открытый поток относится к окну, в котором закончится; финальный отчёт закрывает все. |
| `udp_bytes_up`, `udp_bytes_down`, `udp_writes`, `udp_reads` | Полезная нагрузка UDP и непустые буферы (примерно пакеты) на уровне outbound, без обрамления прокси. |
| `udp_first_reply_p50_ms` | Медиана времени от первого отправленного до первого полученного буфера, 0 без замеров. |
| `udp_ports` | Необязательный объект по корзинам первого порта назначения: `dns` (53), `quic` (443), `stun` (3478, 3479, 19302), `rtc` (50000 и выше), `other`. В каждой `flows` и `unanswered`. Пустые корзины не выводятся. |

UDP-потоки считаются при `telemetry.report` или `evidence.udpReplies`. Гистограммы UDP обнуляются, когда отчёт взят.

### incidents

Сбой - это отказ выбранного пути балансировщика, который длится, пока пользовательское соединение не пройдёт через другой путь. Подозрение, которое трафик или проба опровергли до решения, сбоем не считается.

| Поле | Что значит |
| --- | --- |
| `balancer` | Балансировщик. |
| `kind` | `failover` - было куда уйти; `exhausted` - пригодных не было. |
| `from`, `to` | Отказавший путь и путь, через который пошло первое удачное соединение (`to` отсутствует у незавершённого сбоя без перехода). |
| `detect_ms` | От начала сбоя до решения `down` (0, если решения не было). |
| `switch_ms` | От решения `down` (или начала сбоя) до выбора другого пути. Необязательное. |
| `recover_ms` | От выбора другого пути до первого удачного соединения через него (`first_success`). Только у `recovered`. |
| `total_ms` | От начала сбоя до этого соединения или до остановки. |
| `backend_ms` | У сбоя `exhausted`, если путь принесло обновление провайдера: от первой попытки обновления до применения набора. Необязательное. |
| `outcome` | `recovered` или `open` (запуск остановлен до восстановления; только в финальном отчёте). |
| `network` | Плоский объект меток хоста (`network.hostLabels`) на момент начала сбоя. Необязательное. |

Начало сбоя - момент, когда пользователь начал ждать самое раннее неудачное соединение (`evidence_ms` у `path_state`), а без него первое подозрение. Когда путь на самом деле сломался, ядро не видит. Перцентили считают по этому массиву; отдельного события на каждый сбой нет.

### evidence и recent

`evidence` - объект по видам отказа. Для каждого вида: `failures` (неудачные соединения этого вида), `suspected` (подозрения выбранного пути, в которых участвовал вид), `confirmed` (из них стали `down`), `refuted` (опровергнуты трафиком или пробой до решения), `rescued` (решение `down` позже опровергнуто подтверждающей пробой, проверкой или новым соединением, которое сработало). Точность вида равна (`confirmed` - `rescued`) / `suspected`. Виды без счётчиков не выводятся.

`recent` смотрит на 64 последних исхода пользовательских соединений: `ok` и `failed` - сколько удачных и неудачных было за последнюю минуту перед отчётом, `last_success_ago_ms` и `last_failure_ago_ms` - как давно был последний успех и последний отказ среди этих 64 (поля отсутствуют, если таких исходов нет). Объекта нет, пока не было ни одного исхода. В финальном отчёте `recent` показывает, отказывали ли соединения перед остановкой; остановка пользователем при отказах без записи в `incidents` - повод присмотреться, хотя одно переподключение ничего не доказывает.

### provider_refreshes

Каждый элемент: `provider`, `trigger`, `is_ok`, `ms` (сумма попыток) и `attempts[]` с полями `via`, `outbound` (путь без префикса провайдера, необязательное), `mirror` (с 1; 0 у пропущенной записи), `is_ok`, `status`, `code` (необязательное), `ms`. Это тот же состав, что `lastRefresh` в запросе провайдера. Адресов нет.

### Пример

```json
{
  "labels": {"cohort": "adaptive-a"},
  "path_labels": {"nl-1": {"country": "nl", "node": "nl-ams-12"}},
  "health_run": "232c4c611bfbaee9",
  "report_reason": "interval",
  "report_index": 2,
  "window_ms": 86400512,
  "session_ms": 172801024,
  "recent": {"ok": 14, "failed": 0, "last_success_ago_ms": 2310, "last_failure_ago_ms": 3605112},
  "census_probes": 0,
  "census_probe_bytes": 0,
  "fallback_balancer_count": 0,
  "panics_recovered": 0,
  "udp_probe_count": 0,
  "udp_probe_ok_count": 0,
  "udp_probe_lossy_count": 0,
  "udp_probe_dead_count": 0,
  "udp_probe_second_destination_count": 0,
  "udp_suspect_count": 0,
  "udp_suspect_confirmed_count": 0,
  "events": 41,
  "events_dropped": 0,
  "switches": 2,
  "switch_reasons": {"failover": 1, "recovered": 1},
  "switch_history": [
    {"from": "nl-1", "to": "de-1", "reason": "failover"},
    {"from": "de-1", "to": "nl-1", "reason": "recovered"}
  ],
  "exhausted_count": 0,
  "group_down_count": 0,
  "path_down_count": 1,
  "path_suspect_count": 1,
  "path_degraded_count": 0,
  "path_rescued_count": 0,
  "audit_count": 0,
  "audit_passed_count": 0,
  "audit_skipped_count": 0,
  "wake_probe_count": 3,
  "wake_probe_failed_count": 0,
  "wake_skipped_count": 5,
  "lone_probe_count": 0,
  "lone_probe_failed_count": 0,
  "candidate_probes": 0,
  "candidate_probes_failed": 0,
  "probe_only_promoted_count": 0,
  "fresh_path_down_count": 0,
  "pending_closed_count": 2,
  "idle_closed_count": 0,
  "probes": 6,
  "probes_failed": 1,
  "probe_bytes": 114496,
  "host_probe_requests": 0,
  "host_probes": 0,
  "return_attempts": 0,
  "return_failures": 0,
  "pinned_down_count": 0,
  "network_changes": 0,
  "network_known": 0,
  "network_down_count": 0,
  "provider_fetches": 1,
  "provider_fetch_failures": 0,
  "provider_triggers": {"interval": 1},
  "provider_retry_after": 0,
  "provider_applied": 0,
  "provider_rejected": 0,
  "provider_expired": 0,
  "provider_renewals": 1,
  "provider_auth_failed": 0,
  "provider_base_changed": 0,
  "provider_urls_set": 0,
  "provider_refreshes": [
    {"provider": "main", "trigger": "interval", "is_ok": true, "ms": 412,
      "attempts": [{"via": "auto", "outbound": "nl-1", "mirror": 1, "is_ok": true, "status": 200, "ms": 412}]}
  ],
  "provider_refreshes_dropped": 0,
  "incidents": [
    {"balancer": "auto", "kind": "failover", "from": "nl-1", "to": "de-1", "detect_ms": 5210,
      "switch_ms": 1830, "recover_ms": 640, "total_ms": 7680, "outcome": "recovered", "network": {"type": "wifi"}}
  ],
  "incidents_dropped": 0,
  "evidence": {"no_response": {"failures": 3, "suspected": 1, "confirmed": 1, "refuted": 0, "rescued": 0}},
  "pins": 0,
  "releases": 0,
  "memory_low_count": 0,
  "outbound_sleeps": 0,
  "active": {"auto": "nl-1"},
  "is_balancer_exhausted": false,
  "is_network_up": true,
  "is_quiet_sample": false,
  "connectivity": "ok",
  "paths": {
    "nl-1": {
      "udp_probes": 0, "udp_probes_dead": 0, "udp_rtt_p50_ms": 0, "udp_jitter_p50_ms": 0, "udp_state": "unknown",
      "state": "up", "ttfb_life_p50_ms": 100, "active_ms": 86340120, "bytes_up": 2811904, "bytes_down": 96468992,
      "conns_ok": 412, "conns_failed": 3, "probes": 5, "probe_bytes": 98112,
      "probe_steps": [{"name": "default", "passed": 4, "failed": 1}],
      "degraded_ms": 0, "downs_strong": 0, "downs_weak": 1, "audits": 0, "audits_passed": 0,
      "lone_probes": 0, "lone_probes_failed": 0,
      "udp_flows": 2, "udp_flows_unanswered": 0, "udp_bytes_up": 1240, "udp_bytes_down": 3810,
      "udp_writes": 6, "udp_reads": 6, "udp_first_reply_p50_ms": 50,
      "udp_ports": {"dns": {"flows": 2, "unanswered": 0}}
    },
    "de-1": {
      "udp_probes": 0, "udp_probes_dead": 0, "udp_rtt_p50_ms": 0, "udp_jitter_p50_ms": 0, "udp_state": "unknown",
      "state": "up", "ttfb_life_p50_ms": 200, "active_ms": 60392, "bytes_up": 18044, "bytes_down": 210533,
      "conns_ok": 9, "conns_failed": 0, "probes": 1, "probe_bytes": 16384,
      "probe_steps": [{"name": "default", "passed": 1, "failed": 0}],
      "degraded_ms": 0, "downs_strong": 0, "downs_weak": 0, "audits": 0, "audits_passed": 0,
      "lone_probes": 0, "lone_probes_failed": 0,
      "udp_flows": 0, "udp_flows_unanswered": 0, "udp_bytes_up": 0, "udp_bytes_down": 0,
      "udp_writes": 0, "udp_reads": 0, "udp_first_reply_p50_ms": 0
    }
  },
  "quality": [
    {
      "network": {"host": {"type": "wifi"}},
      "is_network_known": false,
      "duration_ms": 86398220,
      "paths": {
        "nl-1": {
          "bytes_down": 96468992,
          "ttfb_count": 12, "ttfb_p50_ms": 80, "ttfb_p90_ms": 150,
          "ttfb_ms_hist": [0, 0, 0, 0, 1, 2, 4, 2, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
          "dial_count": 12, "dial_p50_ms": 60, "dial_p90_ms": 100,
          "dial_ms_hist": [0, 0, 0, 1, 2, 4, 3, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
          "tcp_count": 12, "tcp_rtt_p50_ms": 50, "tcp_rtt_p90_ms": 80, "tcp_rtt_min_ms": 41,
          "bulk_count": 2, "bulk_kbps_p50": 20000, "bulk_kbps_p90": 30000, "bulk_kbps_max": 24310,
          "bulk_kbps_hist": [0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0],
          "bulk_peak_count": 2, "bulk_peak_kbps_p50": 30000, "bulk_peak_kbps_p90": 50000,
          "bulk_peak_kbps_hist": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0],
          "bulk_saturated_count": 1, "bulk_unsaturated_count": 1,
          "bulk_saturated_kbps_p50": 50000, "bulk_saturated_kbps_p90": 50000,
          "bulk_saturated_kbps_hist": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0],
          "bulk_policed_count": 0,
          "bulk_rtt_p50_ms": 125, "bulk_spliced_count": 0,
          "rx_out_of_order_fraction": 0.0021, "tx_retransmit_fraction": 0.0004
        },
        "de-1": {
          "bytes_down": 210533,
          "ttfb_count": 9, "ttfb_p50_ms": 200, "ttfb_p90_ms": 250,
          "ttfb_ms_hist": [0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 4, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
          "dial_count": 9, "dial_p50_ms": 150, "dial_p90_ms": 200,
          "dial_ms_hist": [0, 0, 0, 0, 0, 0, 0, 1, 3, 3, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
          "tcp_count": 9, "tcp_rtt_p50_ms": 80, "tcp_rtt_p90_ms": 100, "tcp_rtt_min_ms": 71,
          "tx_retransmit_fraction": 0.0011
        }
      }
    }
  ]
}
```

## Качество

Сигналы качества измеряют пользовательские TCP-соединения и собственные пробы. На выбор пути они не влияют.

### Сегменты

Сегмент - непрерывный отрезок одной сети внутри окна отчёта. Смена сети закрывает текущий сегмент, если он не пуст, и открывает новый. Взятый отчёт отдаёт все сегменты окна и открывает новый в той же сети. Хранится не больше 4 непустых сегментов (закрытые плюс открытый), более старые выбрасываются и считаются в `quality_segments_dropped`. Сегмент непуст, если в нём есть пути, свидетель, отброшенные замеры или метки сети.

| Поле сегмента | Что значит |
| --- | --- |
| `network` | Метки сети `host` и `ip` на момент отчёта или закрытия сегмента. Необязательное. |
| `is_network_known` | Сеть была в памяти сетей на момент смены. |
| `duration_ms` | Длина сегмента. |
| `paths` | До 4 путей с наибольшим `bytes_down` (при равенстве по тегу). Необязательное. |
| `witness` | Сводный замер соединений через свидетелей (`network.witnesses`), то есть прямого трафика мимо туннеля. Необязательное. |
| `paths_dropped` | Сколько измеренных путей сегмента не попали в 4 лучших. Необязательное. |
| `samples_dropped` | Замеры, отброшенные, когда в сегменте уже было 64 пути или 16 КиБ их тегов. При ненулевом значении 4 лучших пути точны только среди принятых. Необязательное. |

Замер учитывается только в той сети, в которой соединение началось. Запрос меток (`network.labelRequest`) пользовательским трафиком не считается и в качество не входит.

### Поля пути

Счётчик `*_count` присутствует, если больше нуля, перцентили и гистограмма - только вместе со своим счётчиком. Перцентиль равен верхней границе корзины, см. [Гистограммы](#гистограммы).

| Поле | Что значит |
| --- | --- |
| `probe_count`, `probe_failed_count`, `probe_reasons` | Пробы пути в сегменте, неудачные из них и их число по причине. Только при `telemetry.probeMeasurements`. Пробы переписи, UDP-пробы, устаревшие и отменённые не входят. |
| `probe_connect_p50_ms` | Медиана фазы соединения первого шага пробы. |
| `bytes_down` | Байты, прочитанные завершёнными соединениями пути в сегменте. По нему выбираются 4 лучших пути. |
| `bytes_down_overflow` | Сумма переполнила 64-битное число и зафиксирована на максимуме. |
| `ttfb_count`, `ttfb_p50_ms`, `ttfb_p90_ms`, `ttfb_ms_hist` | Время до первого байта: от первой записи приложения (без неё от установки соединения) до первого прочитанного байта. |
| `dial_count`, `dial_p50_ms`, `dial_p90_ms`, `dial_ms_hist` | Время установки соединения outbound. |
| `tcp_count`, `tcp_rtt_p50_ms`, `tcp_rtt_p90_ms` | Сглаженный RTT ядра ОС в момент первого ответа. |
| `tcp_rtt_min_ms` | Наименьший минимальный RTT среди соединений сегмента. |
| `bulk_*`, `upload_*` | Скорость, см. [Скорость](#скорость). |
| `bulk_rtt_p50_ms` | Медиана RTT под нагрузкой на последней пройденной отметке скачивания. |
| `bulk_spliced_count` | Соединения, прошедшие через splice (XTLS Vision). Их скорость не измеряется. Поле есть, если хоть одно соединение прочитало 1 МиБ. |
| `rx_out_of_order_fraction` | Доля принятого вне очереди между первым ответом и последней отметкой: на Linux по пакетам, на Darwin по байтам. |
| `tx_retransmit_fraction` | Доля повторно отправленных байт от всех отправленных за жизнь соединения. |
| `speed_limit_down_kbps`, `speed_limit_up_kbps` | `speedLimit` outbound у последнего завершённого соединения пути в сегменте. Есть только у ограниченных направлений. |

### Скорость

Скорость считается только у TCP-соединений, в десятичных кбит/с. Отметки по прочитанным байтам для скачивания и по записанным для отдачи: 1, 4, 16 и 64 МиБ.

| Поле | Что значит |
| --- | --- |
| `bulk_count`, `bulk_kbps_p50`, `bulk_kbps_p90`, `bulk_kbps_max`, `bulk_kbps_hist` | Средняя скорость скачивания: байты, прочитанные приложением от первого ответа до последней пройденной отметки, делённые на это время. Нужна хотя бы отметка 1 МиБ. Паузы приложения (плеер набрал буфер) её занижают. Соединения через splice не входят. |
| `bulk_peak_count`, `bulk_peak_kbps_p50`, `bulk_peak_kbps_p90`, `bulk_peak_kbps_hist` | Пиковая скорость скачивания: самый быстрый отрезок между соседними отметками по счётчику байт, принятых ядром ОС. Первый отрезок начинается с первого ответа. Отрезки короче 20 мс не считаются. Счётчик ядра не зависит от того, как быстро приложение дочитывает буфер сокета. |
| `bulk_saturated_count`, `bulk_unsaturated_count`, `bulk_policed_count` | Из соединений с пиком: упёрлись в путь, не упёрлись, упёрлись в шейпер (входят и в упёршиеся). Остаток (`bulk_peak_count` минус упёршиеся и неупёршиеся) классифицировать не по чему. Поля есть при `bulk_peak_count > 0`, нули выводятся. |
| `bulk_saturated_kbps_p50`, `bulk_saturated_kbps_p90`, `bulk_saturated_kbps_hist` | Пиковая скорость упёршихся соединений, оценка потолка пути. Только при `bulk_saturated_count > 0`. |
| `bulk_policed_kbps_p50`, `bulk_policed_kbps_p90`, `bulk_policed_kbps_hist` | Пиковая скорость упёршихся в шейпер, примерно его порог. Только при `bulk_policed_count > 0`. |
| `upload_peak_*`, `upload_saturated_*`, `upload_unsaturated_count`, `upload_policed_*` | То же для отдачи. Запись приложения только наполняет буфер сокета, поэтому скорость берётся по байтам, подтверждённым получателем. Отрезки только между отметками отдачи, первый от 1 до 4 МиБ, то есть пик есть у соединений, записавших хотя бы 4 МиБ. Средней скорости отдачи нет. |

Что задавало темп направления, решается по двум сигналам: насколько RTT под нагрузкой вырос над минимальным и сколько потерь было между отметками.

- Упёрлось в очередь: RTT под нагрузкой больше минимального на `max(30 мс, половина минимального)`. Для скачивания сравнивается RTT приёма на последней отметке с минимальным RTT на первом ответе. Для отдачи сглаженный RTT на последней отметке отдачи с наименьшим известным до неё.
- Шейпер: очередь не выросла, RTT известен, потерь не меньше 3%. Шейпер отбрасывает всё сверх порога вместо того, чтобы копить очередь. Потери скачивания - принятое вне очереди, отдачи - повторно отправленные байты.
- Упёрлось, причина неясна: потерь от 1%, но для шейпера мало или RTT неизвестен. Управляемая очередь (fq_codel) тоже держит себя короткой и отбрасывает, но редко, поэтому порог шейпера выше.
- Не упёрлось: сигналы есть, но ни очереди, ни потерь. Темп задавал сайт или приложение.

Эти признаки эвристические и ничего не доказывают про отдельное соединение. Сервер, который ждёт свой источник, раздувает RTT приёма. Пороги 30 мс, 1% и 3% подобраны без проверки на данных. Сравнивать пути по потолку лучше по `bulk_saturated_kbps_*`: у неупёршихся скорость задавал не путь. Доля `bulk_policed_count` в разрезе сети и протокола показывает, где режут, `bulk_policed_kbps_p50` - до какой скорости.

### Платформы

Источник счётчиков зависит от ОС:

| Величина | Linux, Android | Darwin (iOS, macOS) | Остальные (Windows и др.) |
| --- | --- | --- | --- |
| `tcp_rtt_*` | `tcpi_rtt` | `tcpi_srtt`, в миллисекундах | нет |
| `tcp_rtt_min_ms` | `tcpi_min_rtt` | `tcpi_srtt` на первом ответе | нет |
| RTT под нагрузкой скачивания (`bulk_rtt_p50_ms`, очередь скачивания) | `tcpi_rcv_rtt`, RTT приёма | нет | нет |
| Пик скачивания | `tcpi_bytes_received` (ядро 4.1+) | `tcpi_rxbytes` | нет |
| Пик отдачи | `tcpi_bytes_acked` (4.1+) | отправленные минус повторные (подтверждённых нет) | нет |
| Потери скачивания | `tcpi_rcv_ooopack` / `tcpi_segs_in`, пакеты (5.4+) | байты вне очереди / принятые байты | нет |
| Потери отдачи, `tx_retransmit_fraction` | `tcpi_bytes_retrans` / `tcpi_bytes_sent` (4.19+) | повторные / отправленные байты | нет |

На старом ядре Linux отсутствующие счётчики не подменяются нулём, соответствующих полей просто нет. На платформах без TCP_INFO остаются только `bytes_down`, `ttfb_*`, `dial_*`, `bulk_kbps_*` и `bulk_spliced_count`.

Скачивание классифицируется только по RTT приёма. На Darwin его нет: RTT отправителя обновляется по подтверждению данных, которые отправляет само устройство, а при чистом скачивании оно шлёт почти одни подтверждения. Поэтому с iOS и macOS `bulk_saturated_count`, `bulk_unsaturated_count` и `bulk_policed_count` равны нулю (все пики попадают в неклассифицированный остаток), `bulk_saturated_kbps_*`, `bulk_policed_kbps_*` и `bulk_rtt_p50_ms` отсутствуют. Пики, средняя скорость и гистограммы от этого не зависят. Классификация отдачи на Darwin опирается на srtt, который при отдаче обновляется; он целый в миллисекундах, и порог 30 мс на коротких RTT грубый.

### Гистограммы

Время (`ttfb_ms_hist`, `dial_ms_hist`): 23 числа, корзина `i` - до границы `10, 20, 30, 40, 50, 60, 80, 100, 125, 150, 200, 250, 300, 400, 500, 750, 1000, 1500, 2000, 3000, 5000, 8000` мс включительно, последняя - выше 8000.

Скорость (`bulk_kbps_hist`, `bulk_peak_kbps_hist`, `bulk_saturated_kbps_hist`, `bulk_policed_kbps_hist`, `upload_peak_kbps_hist`, `upload_saturated_kbps_hist`, `upload_policed_kbps_hist`): 18 чисел, корзина `i` - до `128, 256, 512, 1000, 2000, 3000, 5000, 8000, 12000, 20000, 30000, 50000, 80000, 120000, 200000, 300000, 500000` кбит/с включительно, последняя - выше 500000.

Перцентиль `q` равен верхней границе корзины, в которой набирается ранг `round(q * N)` (не меньше 1). Для последней корзины выдаётся последняя конечная граница (8000 мс или 500000 кбит/с). У `bulk_kbps_max` настоящее значение. Перцентили разных отчётов не усредняют: для любого среза (провайдер, сервер, сеть, неделя) корзины складывают и считают перцентиль по сумме.

### Качество в Status и в запросе провайдера

`Status.paths[].quality` и `Status.witness_quality` показывают текущую сеть с момента её смены, не сбрасываются взятием отчёта и охватывают все принятые пути (до 64). Формат тот же, что у пути в сегменте отчёта (snake_case, с полями `probe_*`), но без гистограмм `*_hist`. На 64 путях это десятки килобайт.

В запрос провайдера качество каждого outbound идёт в camelCase без гистограмм и без полей `probe_*`: `bytesDown`, `bytesDownOverflow`, `ttfbCount`, `ttfbP50Ms`, `ttfbP90Ms`, `dialCount`, `dialP50Ms`, `dialP90Ms`, `tcpCount`, `tcpRttP50Ms`, `tcpRttP90Ms`, `tcpRttMinMs`, `bulkCount`, `bulkKbpsP50`, `bulkKbpsP90`, `bulkKbpsMax`, `bulkPeakCount`, `bulkPeakKbpsP50`, `bulkPeakKbpsP90`, `bulkSaturatedCount`, `bulkUnsaturatedCount`, `bulkSaturatedKbpsP50`, `bulkSaturatedKbpsP90`, `bulkPolicedCount`, `bulkPolicedKbpsP50`, `bulkPolicedKbpsP90`, те же `upload*`, `bulkRttP50Ms`, `bulkSplicedCount`, `rxOutOfOrderFraction`, `txRetransmitFraction`, `speedLimitDownKbps`, `speedLimitUpKbps`. Значения и правила присутствия совпадают с полями snake_case. Где именно они лежат в запросе, см. [provider-protocol.md](provider-protocol.md).

## Перепись путей

Перепись - редкая выборочная проверка всех путей сессии одной пробой, чтобы аналитика видела состояние резервов, которыми пользователь не пользуется. Включается блоком `telemetry.census` (требует `telemetry.report`), настройки `sampleFraction`, `startAfter`, `onDemand` описаны в [config.md](config.md). Результаты не меняют ни состояние путей, ни выбор, ни качество.

Сессия выбирается при старте с вероятностью `sampleFraction`. Выбранная сессия запускает не больше одной переписи на сеть за запуск, после `startAfter` непрерывной сети и при пользовательском трафике в этой сети. В перепись входят все обычные пути всех `leastCost`; от группы - один следующий по кольцу участник; пути с `probePolicy: onDemand` - только перечисленные в `onDemand`. Выбранный путь с успехом свежее `startAfter` засчитывается без пробы. Проб не больше 16 на перепись. Проба переписи имеет самый низкий приоритет и уступает любой другой.

Снимок попадает в `census` отчёта того окна, в котором перепись закончилась или прервалась, и в событие `census`. Если в окне закончилось несколько переписей, первая лежит в `census`, остальные в `census.additional` (тот же формат, без вложенного `additional`).

| Поле | Что значит |
| --- | --- |
| `network` | `is_known`, а также `host` и `ip` - метки сети на начало переписи. |
| `started_ms_ago` | Сколько прошло от начала переписи до отчёта или события. |
| `duration_ms` | Длительность переписи. |
| `is_complete` | Все пути обработаны без прерывания. |
| `paths` | Объект `ключ -> строка`. Ключ - тег пути или имя группы. |
| `path_labels` | Метки путей на начало переписи, по тем же ключам. Только при `telemetry.pathLabels`. Необязательное. |
| `additional` | Остальные снимки окна. Только в отчёте. |

Строка пути:

| Поле | Что значит |
| --- | --- |
| `result` | `ok`, `failed`, `skipped`. `ok` ставится и при `degraded`. |
| `source` | `probe` (проверено пробой) или `recent` (засчитан свежий успех выбранного пути). Необязательное. |
| `reason` | Почему `skipped`: `on_demand` (путь по запросу не перечислен в `onDemand`), `limit` (исчерпан лимит 16 проб), `priority` (проба вытеснена более важной), `interrupted` (проба отменена), `network_change`, `network_lost`, `idle`, `memory`, `close` (остановка ядра), `path_changed` (путь изменился). |
| `error` | Класс ошибки пробы у `failed`, как в `probe_result.error` (`other`, если класса нет). |
| `member` | Проверенный участник группы. |
| `observed_state` | Состояние пути по мнению наблюдателя в момент проверки. |
| `is_selected` | Путь был выбран балансировщиком. Выводится только `true`. |
| `connect_ms`, `ms` | Сумма фаз соединения и сумма длительностей шагов пробы; у `recent` в `ms` последний RTT пробы. |

`Status.census` показывает `is_selected`, `is_running` и `checked`. В отчёте рядом с `census` лежат `census_probes` и `census_probe_bytes`.

```json
{
  "census": {
    "network": {"is_known": false, "host": {"type": "cellular"}},
    "started_ms_ago": 412000,
    "duration_ms": 61000,
    "is_complete": true,
    "paths": {
      "nl-1": {"result": "ok", "source": "recent", "observed_state": "up", "is_selected": true, "ms": 188},
      "de-1": {"result": "failed", "source": "probe", "error": "dial_timeout", "observed_state": "unknown", "ms": 6000},
      "group-1": {"result": "ok", "source": "probe", "member": "group-1-b1", "observed_state": "up", "connect_ms": 61, "ms": 190},
      "standby-1": {"result": "skipped", "reason": "on_demand", "observed_state": "unknown"}
    }
  },
  "census_probes": 2,
  "census_probe_bytes": 32768
}
```

Нулевые `connect_ms` и `ms` в JSON не выводятся.

## Трасса

Трасса хранит сырые данные сессии для разбора и для воспроизведения сессии с другими настройками. Включается блоком `telemetry.trace`.

Что хранит:

- каждое событие ядра с теми же полями, что в журнале, с двумя заменами: идентификатор сети в `network` хешируется, а `error`, не похожее на класс (длиннее 32 символов или с символами кроме `a-z`, `_` и пробела), заменяется на `other`;
- запись на каждое пользовательское соединение через наблюдаемый outbound, если не задано `telemetry.trace.connections: {"isEnabled": false}`.

Чего не хранит: адресов, доменов, SNI, URL, содержимого трафика и DNS. Адресат соединения записан как хеш с солью, которая создаётся при старте трассы и нигде не сохраняется. Внутри одной трассы по нему видно, что два соединения шли к одному адресату, но не к какому; две трассы между собой не сопоставить. Метки сети от хоста (в `network_change.labels`) и метки по IP (в `network_labels.labels`) хранятся как есть, включая оператора и ASN, если хост их передаёт. Остальные строковые поля событий, кроме `error`, не фильтруются (например, `stale_request.run`).

Записи соединений пишутся только для соединений, которые наблюдатель отслеживает. Если выключены все пассивные улики, `telemetry.quality`, `telemetry.report`, `telemetry.metrics` и прочие потребители трекера, соединения проходят без трекера, и в трассе остаются только события.

Формат записи, общие поля `seq` (номер с 1) и `t` (мс от начала трассы):

```json
{"type": "event", "event": "switch", "fields": {"balancer": "auto", "from": "nl-1", "to": "de-1", "reason": "failover", "held_ms": 86340120, "cost": 2}, "seq": 118, "t": 86402311}
```

```json
{"type": "conn", "path": "nl-1", "net": "tcp", "outcome": "ok", "up": "1-64k", "down": ">1m", "port": "443", "ms": 5120, "target": "h:3fa91c02b7d4", "dial_ms": 61, "first_byte_ms": 140, "seq": 119, "t": 86402350}
```

Поля записи соединения: `path` (тег), `net` (`tcp` или `udp`), `outcome` (`ok` - что-то прочитано; вид отказа; `closed` - закрыто с ошибкой без данных; `user_closed` - отменено приложением; `empty` - закрыто без ошибки и без данных), `up` и `down` (корзины `0`, `<1k`, `1-64k`, `64k-1m`, `>1m`), `port` (`443`, `80`, `other`), `ms` (длительность), `target` (хеш адресата, необязательное), `dial_ms` и `first_byte_ms` (от начала соединения, необязательные).

Размер кольца задаёт `telemetry.trace.maxBytes`: от 65536 до 2097152 байт, по умолчанию 262144 (256 КиБ). При переполнении выбрасываются самые старые строки.

`ExportTrace(after)` возвращает `{"seq": n, "is_more": false, "dropped": n, "started": "...", "records": [...]}`: записи с номером больше `after` в виде JSON-массива, номер последней отданной записи, остались ли записи сверх предела порции, число выброшенных с начала трассы строк и время начала трассы по настенным часам (RFC 3339). Ядро кольцо не очищает; хост дописывает полученное в свой файл и в следующий раз передаёт полученный `seq`. Пропуск номеров означает потерю. `after` больше номера последней записи (номер из прошлого запуска ядра) считается нулём. Предел порции и слои команды описаны в [host-api.md](host-api.md).

Выгрузка собирает порцию в один буфер под замком трассы, и мост сериализует её ещё раз; запись соединений в это время ждёт замок. Без предела порции выгрузка всего кольца (`after` = 0 или после перезапуска ядра) при кольце 2 МиБ стоит нескольких мегабайт сверх самого кольца, что в iOS Network Extension с лимитом памяти около 50 МБ может привести к завершению расширения системой. На iOS держите `maxBytes` по умолчанию и выгружайте порциями с пределом.

## Приватность

В отчёт (`TakeReport`, `PeekReport`) ядро гарантированно не кладёт:

- сырой идентификатор сети, адреса и имена серверов (`server_name`), URL проб, зеркал и бэкенда;
- тексты ошибок: в отчёте только классы и коды (`evidence`, `groups[].last_failure`, `census.paths[].error`, `provider_refreshes[].attempts[].code`);
- значения заголовков провайдера и учётные данные;
- адресатов пользовательских соединений, домены, SNI, содержимое трафика.

В трассу не попадают адресаты (только хеш с солью), сырой идентификатор сети (хеш), текст в поле `error` событий. `Status` и журнал этих гарантий не дают: в них сырой `network_id`, `server_name`, `last_error` пути, текст `provider_apply.error` (он может цитировать значения ответа бэка, например адрес сервера outbound), а в `paths[].last_error` лежит сырой текст ошибки пробы вместе с её URL. Поэтому `Status`, события и результаты команд хоста не отправляют в аналитику и сторонние сервисы сбора ошибок без собственной фильтрации.

Чего код не проверяет и что обязаны соблюдать автор конфига, бэкенд провайдера и хост:

- Значения меток. Формат значения - 1-32 символа из `A-Z`, `a-z`, `0-9`, `.`, `_`, `:`, `-`, и значение не IPv4- или IPv6-адрес (в том числе с портом). Адрес в метке конфига или ответа провайдера отклоняет их, в метках хоста и `labelRequest` такая метка отбрасывается. Доменные имена под правило подходят, ядро их не отвергает. Метки попадают в отчёт: метки сессии в `labels`, метки путей в `path_labels` и `census.path_labels`, метки хоста в `quality[].network.host`, `incidents[].network`, `census.network.host`, метки по IP в `quality[].network.ip` и `census.network.ip`. Поэтому `network.labelRequest.labels` не должен указывать на заголовок с адресом клиента (например, `X-Forwarded-For`), хост не должен передавать в `NetworkChangedWithLabels` адреса, SSID или имена сетей, а `selection.labels`, `groups[].labels`, метки путей провайдера и `sessionLabels` не должны содержать адресов и доменов.
- Теги путей, имена групп и балансировщиков. Они идут в отчёт как есть (ключи `paths`, `active`, `switch_history`, `incidents`, `census.paths`). Тег, содержащий адрес сервера, раскроет его в аналитике.
- Ответ `ProbeResult` и `Status`. Хост не пересылает их в аналитику целиком; `paths[].last_error` в Status содержит URL пробы. Поле `error` элемента запроса пробы - класс.

## Пределы

| Буфер | Предел |
| --- | --- |
| Журнал событий | `maxEvents` по умолчанию 1000, потолок 4096; `maxBytes` по умолчанию 65536, потолок 262144 байт |
| Трасса | от 65536 до 2097152 байт, по умолчанию 262144 |
| Пути в отчёте (`paths`) и учёт шагов проб | 64 пути, первые по тегу |
| `switch_history` | 10 последних |
| `incidents` | первые 10 за окно, остальное в `incidents_dropped` |
| `provider_refreshes` | первые 10 за окно, до 16 попыток в каждом, остальное в `provider_refreshes_dropped` |
| `recent` | 64 последних исхода, окно 1 минута |
| Сегменты качества | 4 непустых на отчёт, по 4 пути в каждом |
| Принятые пути качества | 64 тега и 16 КиБ их имён на сеть (отдельно для `Status` и для окна отчёта) |
| Перепись | 16 проб на перепись, 64 неотданных снимка, 4096 переписанных сетей за запуск (дальше новые сети не выбираются) |
| Закрытые запросы проб хоста в `Status` | 4, при нехватке памяти 1 |
| Метки хоста | 8 (лишние отбрасываются по порядку имени), JSON не больше 8192 байт |
| Метки по IP (`labelRequest`) | 6; кеш по сетям размером `network.memory.maxNetworks` (по умолчанию 16, потолок 64) |
| Метки пути | 6 после объединения с метками группы |
| Метки сессии | 8 |
| Имя и значение метки | имя 1-24 символа `[a-z0-9_]`, значение 1-32 символа `[A-Za-z0-9._:-]` |
| Текст `provider_apply.error` при сборке набора | 200 символов |
