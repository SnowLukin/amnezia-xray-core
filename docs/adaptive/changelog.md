# Изменения адаптивного ядра по ревизиям

Список того, что меняется для бэка и приложения с новой сборкой ядра. На каждую новую ревизию (`rev` в заголовке `Xray-Adaptive`) сюда добавляется раздел: что перестаёт загружаться, что появилось, что сделать бэку и приложению. Текущее поведение целиком описано в справочнике рядом: [config.md](config.md), [provider-protocol.md](provider-protocol.md), [host-api.md](host-api.md), [telemetry.md](telemetry.md). Правило версий - в config.md, раздел 7.

## Ревизия 5

| | Ревизия 4 | Ревизия 5 |
|---|---|---|
| Заголовок | `xray="26.10.31", api=2, rev=4, provider=1` | `xray="26.10.31", api=2, rev=5, provider=1` |

Добавлена одна команда хоста, `View`, и блок конфига `healthObservatory.host.view`, который её включает. `View` отдаёт компактный снимок для экрана приложения: готовое состояние трафика пользователя (`ok`, `searching`, `blocked`, `no_network`), текущий выход с протоколом и метками, что ядро делает сейчас, какие пути проверяются в эту секунду и что должен сделать хост (`needs`). До этого каждое приложение собирало то же самое из Status по-своему. Поля, правило состояния и сценарий «Экран приложения» - в [host-api.md](host-api.md), блок конфига - в [config.md](config.md), раздел 3.7.

Больше ничего не изменилось. Всё, что загружалось на ревизии 4, загружается; ответ провайдера, запрос ядра, события, отчёт и трасса прежние. В Status добавился один флаг `features.view`.

Бэк:

1. Добавить в шаблон для клиентов с `rev` от 5 блок `"host": {"view": {"balancer": "<тег>"}}` внутри `healthObservatory`, где тег - балансировщик `leastCost`, через который идёт трафик пользователя. Если такой балансировщик в шаблоне один, достаточно `"view": {}`.
2. Клиентам с `rev` меньше 5 блок не слать: их ядро отклонит весь конфиг с текстом `healthObservatory.host.view: unknown key`.
3. Конфиг с блоком отклоняется, если `balancer` не называет балансировщик `leastCost` или не задан, а таких балансировщиков не один. Тексты ошибок в config.md, раздел 4.

Приложение:

1. Экран подключения строить по `View`, а не по Status: опрос раз в секунду, пока экран виден, реже или никак в фоне. Перерисовывать и пересылать снимок, только когда изменились `seq` или `run`.
2. На `state` показывать готовое состояние, на `needs` действовать: `restart` - перезапуск ядра, `session` - новый токен, `config` - новый конфиг. Свою логику вывода состояния из `Status.balancers[]` и `broken_subsystems` можно убрать.
3. Без блока в конфиге `View` отвечает `feature_disabled`: так будет у клиентов, которым бэк ещё отдаёт шаблон ревизии 4.
4. Status остаётся диагностикой и нужен по-прежнему для фонового тика и отчёта об ошибке.

Чего в ревизии 5 нет: `View` есть в Go API ядра, gRPC и CLI (`xray api health view`); функций моста libXray, C-экспорта и Swift для него пока нет.

## Ревизия 4

| | Ревизия 3 | Ревизия 4 |
|---|---|---|
| Заголовок | `xray="26.10.10", api=2, rev=3, provider=1` | `xray="26.10.31", api=2, rev=4, provider=1` |
| Ядро `xray-core-adaptive` | `e8d57cb`, ветка `feat/adaptive-routing` | ветка `finalize/adaptive`. Мост закреплён на `7424a0e0`; более поздние коммиты ветки меняют только стенд и документы |
| Мост `libxray-adaptive` | `1afb9b2`, ветка `feat/health-host` | `152c2ee`, ветка `finalize/adaptive` |

Между ревизиями 155 коммитов ядра в трёх слоях: слияние исходного Xray 26.10.10 -> 26.10.31, ограничение скорости outbound с новыми полями качества и правки по аудиту. Ключи JSON внутри `healthObservatory`, `outbounds[].selection` и `strategy.settings` балансировщика `leastCost` не добавлялись, не удалялись и не переименовывались. Изменились проверки при загрузке, одно умолчание, разбор ответа провайдера и поведение при исчерпании, появился ключ `outbounds[].speedLimit`.

Чем ревизия 4 проверена: тесты ядра с детектором гонок, стенд `testing/adaptive` (119 сценариев из 120, упавший нестабилен и на ревизии 3), стенд на конфиге формы бэка со строгим имитатором ручки (`run.py --prod`, 129 проверок из 130), Android на эмуляторе против локального стенда. Чем не проверена: настоящие телефоны и мобильные сети, туннель на iOS, живой бэк.

### Как перейти с ревизии 3

Бэк:

1. Собрать CLI из новой ветки и прогнать каждый шаблон и пример ответа: `go build -o xray ./main`, затем `xray run -test -c <конфиг>`. Причины, по которым загружавшийся конфиг теперь отклоняется, собраны в 0.1.
2. Убрать тег `<c>` из шаблонов AmneziaWG `i1`-`i5` в конфиге и в ответах ручки: ядро отклоняет такой outbound при загрузке, а в ответе ручки он отклоняет весь набор.
3. Проверить `apply.tagPrefix` провайдера: ни один outbound базового конфига вне балансировщиков провайдера не должен начинаться с этого префикса (`tagPrefix: "b"` и outbound `block` конфиг отклонят).
4. В разборе отчёта переименовать `paths.<tag>.ttfb_p50_ms` в `ttfb_life_p50_ms`.
5. Шаблон выбирать по правилу «наибольшая ревизия, не больше `rev` клиента». Точное сравнение `rev == 3` клиента с ревизией 4 не найдёт.
6. Новые ключи ответа и `null` слать только клиентам с `rev` от 4: ядро ревизии 3 по-прежнему отклоняет ответ целиком.
7. Под нагрузкой отвечать 503 с телом протокола `{"protocol":1,"code":"unavailable"}` и `Retry-After`. Ответ 429 или 503 без тела протокола перебор дорог больше не останавливает.

Приложение (по хосту ревизии 3 на Android):

1. Пересобрать AAR прежним рецептом из ядра и моста ревизии 4. Go 1.27.3, `x/mobile` теперь `v0.0.0-20261011034528-197770444867` (версию брать из `go.mod` моста). Наша сборка без `-s -w` весит 102 МБ, со `-s -w` мы её не собирали. Список хостов в `.so` проверить заново: исходный Xray обновился.
2. Функции моста, которые зовёт хост, остались с прежними сигнатурами: `runXrayFromJSON`, `stopXray`, `getXrayState`, `registerDialerController`, `registerListenerController` и все `health*`. Добавлена `healthExportTraceUpTo`.
3. Код отказа провайдера больше нельзя достать из `Status.providers[].last_error`: там теперь класс `status` без числа. Число есть только в событии `provider_auth_failed` (поле `status`), для него в конфиге нужен блок `telemetry.events`. Признак `is_auth_refused` в Status остался.
4. Страну, город и протокол выхода читать из `Status.balancers[].exit` и `exit_labels`. При исчерпании `active` пуст, а трафик ещё до минуты идёт через удерживаемый путь: состояние «трафик в заглушке» определять по тому, что `exit` равен `fallbackTag`.
5. Паника подсистемы ядра вне наблюдателя теперь завершает процесс. Сервис туннеля должен переживать падение процесса ядра и подниматься заново. Непустой `Status.broken_subsystems` означает замороженного наблюдателя: ядро перезапустить в ближайшую паузу.
6. `healthNetworkChanged` с тем же `id` без `healthNetworkLost` перед ним только меняет метки, пути не перепроверяются. Возврат в ту же сеть после потери работает как раньше.
7. Сохранённый набор, записанный ядром ревизии 3, при старте проверяется так же, как ответ ручки. Если в нём шаблон AWG с `<c>`, ядро один раз стартует без сохранённого набора и возьмёт новый у бэка.

### По итогам живой проверки ревизии 3 на Android

Связку ядро ревизии 3, приложение и бэк проверили на настоящем телефоне. Ниже - что из найденного закрыто ревизией 4, а что решается конфигом или приложением.

| Наблюдение | Что в ревизии 4 | Что сделать |
|---|---|---|
| Приложению нужны метки выбранного выхода в Status всегда | `Status.balancers[].exit` - тег outbound, в который сейчас идут новые соединения балансировщика; `exit_labels` - его метки. `paths[].labels` тоже выводятся всегда. От `telemetry.pathLabels` это больше не зависит | приложению: сравнивать пару (`exit`, `exit_labels`) между опросами Status |
| Незнакомый ключ в наборе отклонял весь ответ | Незнакомые ключи верхнего уровня, `selection` и `groups[]` игнорируются, до 16 имён попадают в `ignored_keys` события `provider_apply`. `null` равен отсутствию ключа | новые необязательные ключи можно слать клиентам с `rev` от 4 |
| Паспорт скорости пути для строки выхода в приложении | Ключ вне протокола ядро пропустит, но приложению не передаст | если значение должно дойти до приложения через ядро, класть его меткой, например `"labels": {"mbps": "250"}`: оно появится в `exit_labels`. До 6 меток на путь, значение до 32 символов `[A-Za-z0-9._:-]`. Если ядро должно само держать потолок скорости, для этого есть `outbounds[].speedLimit` |
| Нужен список изменений ключей к каждой сборке ядра | этот файл | - |
| Переход на резерв AWG того же входа занимает 20-35 с | Логика вердикта не менялась. Сильная улика - 3 отказа установки соединения за 60 с, каждый не раньше `evidence.dialTimeout.timeout` (6 с). Слабая (тишина, обрыв) требует 3 отказа к 2 разным адресатам и подтверждающую пробу до `steps[].timeout` (8 с). Перед переключением ядро пробует кандидата, спящему туннелю AWG нужно рукопожатие. На эмуляторе при частых соединениях от отказа входа до восстановления трафика выходило 9 с, при редких отказ обнаруживался за 12-18 с | ускоряется конфигом: `evidence.dialTimeout.timeout` 3-4 с, `evidence.firstReply.timeout` 3 с, `steps[].timeout` 4-5 с, один адрес на шаг пробы (адреса шага теперь опрашиваются по очереди, каждый со своим таймаутом), `failover.closeSilent.after` короче 8 с. Чтобы понять, где уходит время в медленном случае, нужны события одного такого прогона: `path_state` с `evidence_ms`, `probe_result`, `switch` |
| Вход отказал целиком: запрос набора через 19 с, связь не позже чем через 58 с | Исчерпанный балансировщик до минуты держит последний путь и перебирает все пути раундами через 5, 5, 10, 20, 40 с, не дожидаясь карантинов. Ответ без тела протокола и таймаут больше не заканчивают всё обновление: у каждой дороги `via` свой потолок попыток | `providers[].triggers.exhausted.delay` (умолчание `0s..10s`) и `providers[].timeout` (10 с на попытку) уменьшить, например до `0s..2s` и 5 с. Цена обхода всех дорог: когда не отвечает ни одна, обновление длится дольше прежнего |
| Возврат с AWG на Xray после восстановления не проверен | Логика не менялась. Нужен блок `returnToCheaper` в `strategy.settings` балансировщика; у слота Xray `cost` меньше, чем у двойника AWG. Период берётся у текущего, дорогого пути: умолчание 30 минут, первая попытка в случайной точке периода и не раньше `minHoldTime` (2 минуты). Возврат после 3 успешных проб подряд с паузой от 20 с. Без пользовательского трафика возврата нет | для проверки задать двойнику AWG `"returnToCheaper": {"interval": "2m"}` и держать трафик; в Status видно `return_in_ms` и `return_target`, при возврате приходит `switch` с `reason: return` |
| Приложение выключает VPN по провалу своей проверки `/health`, пока ядро ищет путь | Это код приложения, ядро туннель не выключает | при конфиге с провайдером состояние брать из Status ядра: `is_exhausted`, `exit`, `providers[].is_auth_refused`. Свою проверку оставить сигналом для интерфейса, а не поводом остановить туннель |
| Смена Wi-Fi и мобильной сети на ходу не проверена | Хост сообщает новую сеть через `healthNetworkChanged(id)`, потерю через `healthNetworkLost()`. После потери ядро само проверяет связь (первая проверка примерно через 2 с), если в конфиге есть `network.connectivity` | включить `network.connectivity` в шаблоне, сценарии в host-api.md: «Смена сети Wi-Fi - LTE», «Пропажа и возврат сети» |
| Холодный старт, когда домен API заблокирован | Ядро закрывает два уровня из трёх. Работающий туннель: провайдер ходит к бэку по дорогам `via`, в том числе через проверочные учётки. Старт без ответа бэка: сохранённый набор из `healthExportProviderSet` в `initialAnswer`, пока аренды живы. Запасные адреса ручки хост задаёт через `healthSetProviderURLs` | первый запрос конфига по запасному адресу или IP, проверка подписи списка адресов и показ пользователю остаются на приложении |
| Отчёты ядра на бэке | В отчёте имена путей идут с `tagPrefix` провайдера, в теле запроса провайдера - без него. Поле `ttfb_p50_ms` переименовано | при сопоставлении отчёта с запросами учитывать префикс; переименовать поле в разборе |

### Рекомендации к шаблону

- Проверка связи: `healthObservatory.network.connectivity` через `direct` на два адреса, свой и внешний. Без неё ядро не отличает пропажу сети от отказа всех входов, а исчерпанный балансировщик держит последний путь всю минуту, даже когда сети нет.
- `telemetry.events` и `telemetry.report`: без событий приложение не узнаёт код отказа провайдера.
- Проверочные учётки: `probes.candidates.keepVerified: 1` и `recheckInterval` 2-3 часа вместо умолчания 15 минут.
- Необязательный второй шаг пробы до внешнего адреса (`steps[].isOptional`), чтобы ядро отличало «вход мёртв» от «вход жив, выход не работает». Во втором случае путь становится `degraded`, а не исключается.
- `returnToCheaper` в основном балансировщике, иначе после ухода на двойника AWG ядро само на слот не вернётся.
- Страну пользователь меняет переподключением: приложение передаёт её в запросе конфига, отдельного заголовка провайдера для этого нет.

### 0. Что перестанет работать при простой замене ядра

Только то, из-за чего старый конфиг, старый ответ провайдера или старый код приложения перестанут работать или начнут работать иначе.

#### 0.1 Конфиг, который загружался на ревизии 3 и теперь отклоняется

| Что | Ревизия 3 | Ревизия 4 | Что сделать |
|---|---|---|---|
| `providers[].triggers.networkChange` без памяти сетей | загружался | Если блок `networkChange` есть, `mode` не равен `any` (в том числе не задан, умолчание `unknownNotCheapest`) и нет блока `healthObservatory.network.memory`, конфиг отклоняется: `healthObservatory.providers[<tag>].triggers.networkChange.mode:unknownNotCheapest requires healthObservatory.network.memory` | добавить `"network": {"memory": {}}` или поставить `mode: "any"` |
| `returnToCheaper.onNetworkChange` без памяти сетей | загружался | Блок `onNetworkChange` со `scope` не `all` (в том числе без `scope`, умолчание `unknown`) без `network.memory`: `balancer <tag>.returnToCheaper.onNetworkChange.scope:unknown requires healthObservatory.network.memory` | добавить `network.memory` или `scope: "all"` |
| `returnToCheaper.onNetworkChange` вместе с `networkChanges.action: "forget"` | загружался | `balancer <tag>: leastCost.settings.returnToCheaper.onNetworkChange requires networkChanges.action:recheck` | убрать `onNetworkChange` или поставить `action: "recheck"` |
| outbound конфига под `apply.tagPrefix` провайдера | загружался; после первого ответа такой outbound молча удалялся провайдером | Любой outbound конфига, чей тег начинается с `tagPrefix`, должен иметь `selection.balancer` из `apply.balancers` этого провайдера. Иначе: `healthObservatory.providers[<tag>]: outbound <o> is under apply.tagPrefix <p> and the provider would remove it`. Типичный случай: `tagPrefix: "b"` и outbound `block`, `tagPrefix: "d"` и `direct` | выбрать префикс с разделителем, которого нет в тегах базового конфига (`"p-"`, `"fresh:"`) |
| ссылка на несуществующий тег | загружался | `balancer <tag>.fallbackTag: unknown outbound <x>`; `healthObservatory.network.connectivity.outbound: unknown outbound <x>`; `healthObservatory.network.labelRequest.outbound: unknown outbound <x>` (обе только при включённом блоке); `healthObservatory.providers[<tag>].via: unknown outbound or balancer <x>`; `healthObservatory.network.witnesses.outbounds: <w> matches no outbound` (префикс должен совпасть с началом тега outbound конфига или пересечься с `tagPrefix` провайдера) | проверить, что все теги из этих ключей есть в `outbounds` (для `via` - ещё в `routing.balancers`) |
| значение выше потолка, которое раньше молча обрезалось | загружался, значение обрезалось до потолка | Отказ с текстом `<путь> must be from A to B`: `probes.limits.maxConcurrent` 1..8, `probes.requests.maxOutbounds` 1..16, `probes.requests.timeout` до `10m0s`, `steps[].timeout` и `progressTimeout` до `30s`, `network.connectivity.timeout` до `30s`, `telemetry.events.maxEvents` 1..4096, `telemetry.events.maxBytes` 1..262144, `network.memory.maxNetworks` 1..64, `providers[].apply.drainTimeout` до `30m0s` | привести значения в диапазон |
| значение выше нового потолка | загружался без ограничения сверху | Отказ с тем же текстом: `probes.limits.perMinute` 1..60, `probes.limits.perServerNamePerMinute` 1..10, `probes.udp.requests` 1..32, `probes.udp.interval` до `2s`, `probes.udp.timeout` до `10s`, `probes.udp.perHour` 1..60, `steps[].minBytes` 0..1048576, `network.labelRequest.requestsPerHour` 1..60, `network.labelRequest.timeout` до `30s`, `providers[].limits.maxRefreshesPerHour` 1..60 | привести значения в диапазон |
| `providers[].triggers.periodic.interval` с нулевой левой границей (`"0s"`, `"0s..1h"`) | загружался | `healthObservatory.providers[<tag>].triggers.periodic.interval must be greater than 0s` | левая граница больше нуля |
| `providers[].apply.allowedProtocols` | `[]` принимался как умолчание, неизвестное имя принималось | `[]`: `...apply.allowedProtocols must not be empty; omit it for the default`. Имя, которого нет среди протоколов outbound: `...apply.allowedProtocols: unknown protocol <name>`. Регистр приводится к нижнему | убрать пустой массив, проверить имена |
| метка со значением-адресом | `"192.168.1.10"`, `"fe80::1"`, `"1.2.3.4:443"` проходили (алфавит разрешает цифры, точки и двоеточия) | Значение, которое разбирается как IPv4, IPv6 или адрес с портом, отклоняет конфиг: `<путь>.labels.<имя>: invalid label value`. Действует для `selection.labels`, `groups[].labels`, `telemetry.report.sessionLabels.labels` | не класть адреса в метки |
| шаблон AmneziaWG `i1`-`i5` с тегом вне списка | загружался; отказ проявлялся при первом соединении через outbound | Допустимые теги: `b`, `t`, `r`, `rc`, `rd`, `d`, `ds`, `dz`. Любой другой, в том числе `<c>`, отклоняет конфиг: `invalid AmneziaWG i1 > unknown tag <c>`. `b` требует непустой hex чётной длины, `r`, `rc`, `rd`, `dz` - целое число | убрать `<c>` и другие неизвестные теги из `i1`-`i5` |
| ключ outbound, похожий на `selection` или `speedLimit` | молча игнорировался (кроме написания в другом регистре `Selection`, оно отклонялось и раньше) | Ключ, который не равен `selection` или `speedLimit` точно, но отличается от них не более чем на 2 правки без учёта регистра (`selections`, `seletion`, `section`, `speedlimit`, `SpeedLimit`, `speedLimits`): `outbounds[].<ключ>: unknown key; use selection` (или `use speedLimit`) | проверить имена ключей outbound в генераторе |
| `outbounds[].speedLimit` в произвольной форме | ключа не существовало, любой `speedLimit` молча игнорировался | Ключ читается строго. Неизвестный ключ внутри: `outbounds[].speedLimit.<ключ>: unknown key`. Значение не вида число + `kbps`, `mbps`, `gbps`: `speedLimit.uplink must be a number with kbps, mbps or gbps, such as 20mbps`. Ненулевое значение ниже 64 kbps: `speedLimit must be at least 64kbps per direction` | если бэк уже клал `speedLimit` в конфиг в другой форме (числа, `"20mb"`, `{"down": ...}`), привести к `{"uplink": "5mbps", "downlink": "20mbps"}` |

#### 0.2 Бэк: ответ провайдера, запрос ядра, отчёт

| Что | Ревизия 3 | Ревизия 4 | Что сделать |
|---|---|---|---|
| outbound WireGuard с `<c>` в `i1`-`i5` в ответе | набор применялся, outbound отказывал на первом соединении | весь ответ отклоняется, набор остаётся прежним; в `provider_apply.error` текст с `invalid AmneziaWG i1 > unknown tag <c>` | убрать `<c>` из шаблонов, которые отдаёт ручка |
| метка-адрес в `selection.labels` или `groups[].labels` ответа | принималась | весь ответ отклоняется: `outbound <tag> selection.labels.<имя>: invalid label value` | не класть адреса в метки |
| `speedLimit` у outbound ответа | молча игнорировался | строгий разбор, как в конфиге; ошибка формы отклоняет весь ответ | отдавать `speedLimit` только в форме rev 4 и только клиентам с `rev` от 4 |
| прежний `version` при новом содержимом | ответ считался тем же набором, ядро оставалось на старом наборе | набор применяется, если изменились `outbounds` или `groups`, каким бы ни был `version` | если бэк менял набор, не меняя `version`, и рассчитывал, что клиент его не применит - это больше не так |
| `groups[].group` в теле запроса | имя группы с `tagPrefix` провайдера | имя без префикса, как его назвал бэк | если ручка срезала префикс или сопоставляла по имени с префиксом - убрать |
| заголовок `Xray-Adaptive` | `xray="26.10.10", api=2, rev=3, provider=1` | `xray="26.10.31", api=2, rev=4, provider=1` | выбор шаблона по правилу «наибольшая ревизия не больше `rev` клиента»; точное сравнение `rev == 3` перестанет находить шаблон |
| `paths.<tag>.ttfb_p50_ms` в отчёте | поле с этим именем | переименовано в `ttfb_life_p50_ms`, смысл тот же | поправить разбор отчёта на бэке |
| 429 или 503 с `Retry-After` без тела протокола | останавливал перебор дорог и зеркал | перебор продолжается (так может ответить прокси перед зеркалом); пауза действует, только если перебор на таком ответе и кончился | под нагрузкой отвечать 503 с телом `{"protocol":1,"code":"unavailable"}` и `Retry-After`: только такой ответ останавливает перебор сразу |
| неизвестный ключ и `null` в ответе | отклоняли весь ответ | игнорируются (см. раздел 2) | клиентам с `rev=3` новые ключи и `null` по-прежнему не отдавать: старое ядро отклонит ответ |

#### 0.3 Приложение

| Что | Ревизия 3 | Ревизия 4 | Что сделать |
|---|---|---|---|
| `NetworkChanged` и `NetworkChangedWithLabels` с тем же непустым `id` | полный сброс сети: карантины, пробы, рост `generation`, событие `network_change` | если решения не на паузе (не было `NetworkLost`, проверка связи не поставила паузу), вызов только заменяет метки хоста; сброса и события нет | если приложение повторяло тот же `id`, чтобы ядро перепроверило пути, перед повтором вызвать `NetworkLost()` |
| `NetworkChangedWithLabels` с битыми метками (не JSON-объект строк, длиннее 8192 байт) | вызов отклонён, смена сети не применена, `false` | смена сети применена без меток, вызов всё равно возвращает ошибку (`false` в libXray, `invalid_argument` в gRPC) | не повторять вызов по `false` |
| исчерпание балансировщика | `active` пуст, новые соединения сразу идут в `fallbackBalancer` или `fallbackTag` | балансировщик до минуты (или до прошедшей проверки связи) отдаёт последний путь; `active` пуст, `is_exhausted: true`, реальный выход виден в новом поле `exit` | логику «`active` пуст, значит трафик в заглушке» заменить чтением `balancers[].exit` |
| паника в подсистеме вне наблюдателя | подсистема выключалась, процесс жил, тег подсистемы появлялся в `broken_subsystems` | процесс завершается как при необработанной панике (код 2, трасса в stderr). То же при 20 паниках соединений с одного места за минуту | хост должен перезапускать туннель после падения процесса ядра |
| событие `probe_only_promoted` | поле `tag` | поле `outbound` | поправить разбор события |
| `provider_fetch.error` и `Status.providers[].last_error` | сырой текст: `status 503`, текст сетевой ошибки, `balancer exhausted`, `current outbound down`, `probe-only outbound`, `outbound down`, `no such outbound` | класс: `status` (код в поле `status`), `timeout`, `tls`, `network`; причины пропуска дороги `balancer_exhausted`, `current_outbound_down`, `probe_only_outbound`, `outbound_down`, `no_such_outbound` | поправить сравнения строк в приложении и на стенде |
| `network_change.network` в событиях и трассе | сырой идентификатор сети от хоста | хеш `h:` и 12 hex-цифр, одинаковый в пределах запуска; сырой `id` остался только в `Status.network_id` | не сопоставлять значение события со своим `id` |
| `ProbeResult`: `outbounds[].error` | сырой текст ошибки пробы | класс ошибки (`dial_timeout`, `status_<код>`, `other` и т. д.) | поправить разбор, если приложение показывало или сравнивало текст |
| событие `provider_apply` | поля `rules`, `routing_error`, `is_routing_ignored` | этих полей нет; добавлено `ignored_keys` | убрать чтение старых полей |
| gRPC `Pin` и `Release` с `run` другого запуска | код `invalid_argument` | код `stale_run` | поправить обработку кода, если приложение ходит через gRPC |
| умолчание `telemetry.trace.maxBytes` | 2097152 (2 МиБ) | 262144 (256 КиБ) | если нужен прежний объём трассы, задать `maxBytes` явно; выгружать порциями |
| libXray вне `Health*` | в корневом пакете были `SetTunFd`, `RunXray`, `TestXray`, `Ping`, `XrayVersion`, `CountGeoData`, `ReadGeoFiles`, `GetFreePorts`, `ConvertShareLinksToXrayJson`, `ConvertXrayJsonToShareLinks`, `NewXrayRunRequest`, `NewXrayRunFromJSONRequest`; `RegisterProcessFinder(finder)` | этих функций в корневом пакете нет; остались `RunXrayFromJSON`, `StopXray`, `GetXrayState`, добавлены `Invoke`, `SetDNS`, `ResetDNS`; `RegisterProcessFinder(finder, sdkVersion)` | проверить, какие из убранных функций вызывает приложение. |

### 1. Конфиг

Правило строгого разбора не менялось: неизвестный ключ, `null` и неверный тип отклоняются внутри `healthObservatory`, `outbounds[].selection`, `strategy.settings` балансировщика `leastCost`, а теперь ещё и внутри `outbounds[].speedLimit`. Сам объект outbound и всё остальное по-прежнему читает нестрогий декодер Xray .

| Что | Ревизия 3 | Ревизия 4 | Что сделать |
|---|---|---|---|
| новый ключ `outbounds[].speedLimit` | не существовал, молча игнорировался | объект с ключами `uplink` и `downlink` (строки). Скорость в битах в секунду: неотрицательное число и единица `kbps`, `mbps` или `gbps` (регистр единицы не важен), множители десятичные: `"20mbps"` = 2 500 000 байт/с. Пустая строка и ноль с единицей - без лимита. Минимум 64 kbps на направление. Голое число, `bps`, `"20mb"`, `"1e30gbps"` - ошибка. Бюджет общий для всех соединений outbound, пробы ядра тратят его же | отдавать только клиентам с `rev` от 4; ядро rev 3 ключ молча пропустит, лимита не будет |
| ключи outbound, похожие на `selection` и `speedLimit` | игнорировались | отклоняются, см. 0.1 | - |
| `settings.domainStrategy` у outbound `wireguard` | ключ читался: `ForceIP` (умолчание), `ForceIPv4`, `ForceIPv6`, `ForceIPv4v6`, `ForceIPv6v4`; неизвестное значение - ошибка `unsupported domain strategy` | ключа в структуре нет, нестрогий декодер его молча пропускает. Адрес эндпоинта-домена выбирается по `streamSettings.sockopt.domainStrategy` outbound; без `sockopt` - случайный из всех A и AAAA | если бэк задавал `domainStrategy` в настройках WireGuard, перенести в `streamSettings.sockopt.domainStrategy`. Пришло из слияния исходного Xray |
| шаблоны AmneziaWG `i1`-`i5` | не проверялись при загрузке | проверяются при загрузке конфига и ответа провайдера, для outbound и inbound; список тегов и текст ошибки в 0.1. Вторая проверка стоит при создании outbound: `invalid i5: unknown tag <c>` | - |
| остальные ключи `awg` | `jc`, `jmin`, `jmax`, `s1`-`s4`, `h1`-`h4`, `i1`-`i5`, `randomTrailers`, `disableCookies`, `headerProtectionKey`, `contentPaddingAddition`, `rekeyAfterTime`, `rekeyTimeout`, `rejectAfterTime`, `keepaliveTimeout`, `maxHandshakeAttempts` | без изменений | - |
| умолчание `telemetry.trace.maxBytes` | 2097152 | 262144; диапазон прежний, 65536..2097152 | см. 0.3 |
| потолки и диапазоны | часть значений обрезалась молча, у части не было потолка | вне диапазона - отказ; полный список чисел в 0.1. Без изменения смысла остались: `candidates.keepVerified` 1..4, `apply.maxOutbounds` 1..128, `tunnelSleep.afterLastUse` от `10s`, `closeUnchosenIdle.after` `10s`..`10m0s`, `trace.maxBytes` 65536..2097152, `groups[].failedInRowToLeave` 1..16 | - |
| тексты прежних ошибок диапазона | `must be greater than 0s`; `must be positive (maximum N)`; `closeUnchosenIdle.after must be in [10s, 10m]`; `minBytes must not be negative`; `events.maxBytes must be positive` | `must be at least 1ns`; `must be from 1 to N`; `must be from 10s to 10m0s`; `must be from 0 to 1048576`; `must be from 1 to 262144`. Длительности печатаются как в Go (`10m0s`) | поправить сравнения текстов ошибок, если они есть в тестах бэка |
| ссылки на теги и `tagPrefix` | не проверялись | проверяются, см. 0.1 | - |
| зависимости от `network.memory`, `action: forget` | не проверялись | проверяются, см. 0.1 | - |
| `apply.allowedProtocols` | не проверялся | проверяется, см. 0.1. Умолчание прежнее: `vless`, `vmess`, `trojan`, `shadowsocks`, `wireguard`, `hysteria`, `socks`, `http` | - |
| метки: алфавит, длина, число | имя 1..24 `[a-z0-9_]`, значение 1..32 `[A-Za-z0-9._:-]`; до 6 у пути и группы (после слияния), до 8 у сессии. Тексты: `...: invalid label length or name`, `...: invalid label value` | то же, плюс значение не IP-адрес и не адрес с портом. Тексты: `<путь>.labels.<имя>: invalid label name`, `<путь>.labels.<имя>: invalid label value`, `at most N labels are allowed` | см. 0.1 |
| правила зависимостей уровня `healthObservatory` | часть проверялась при загрузке JSON, часть при старте ядра | все проверяются при загрузке (`Build` зовёт `ValidateFeatures`). Изменились тексты: `healthObservatory.evidence.loneFailureProbe requires evidence.dialFailures, evidence.dialTimeout or evidence.serverUnverified`; `healthObservatory.probes.profiles[i].steps requires probes.stepProfiles` (и для `isOptional`); `healthObservatory.network.connectivity requires .outbound and .destinations`; `healthObservatory.network.witnesses.outbounds must be nonempty when witnesses`; `healthObservatory.telemetry.report.sessionLabels requires telemetry.report`; `healthObservatory.telemetry.report.quietSampleFraction requires telemetry.report`; `healthObservatory.telemetry.census requires telemetry.report` | поправить сравнения текстов, если есть |
| `connectivity` или `labelRequest` без `outbound` | свой текст про обязательный `outbound` | раньше срабатывает проверка ссылок: `healthObservatory.network.connectivity.outbound: unknown outbound ` (с пустым тегом) | - |
| `leastCost` без `fallbackTag` в конфиге protobuf | проверялось только загрузчиком JSON | проверяется и при сборке балансировщика: `balancer <tag>: leastCost requires explicit fallbackTag`; ядро не стартует, если селектор `leastCost` не создался (`balancer <tag>: failed to create leastCost selector`) | для JSON-конфигов ничего: загрузчик JSON требовал `fallbackTag` и раньше |
| `vless` без TLS и без `encryption`, `trojan` без TLS | запрещены к публичному адресу; «частный» адрес определялся по встроенному списку диапазонов; отсутствующий адрес считался частным | частным считается адрес, для которого Go отвечает `IsPrivate`, `IsLoopback` или `IsLinkLocalUnicast`; отсутствующий адрес требует защиты. Тексты: `vless without TLS or other encryption is prohibited unless the server address is a private IP or domain`, `trojan without TLS is prohibited unless the server address is a private IP or domain` | касается только outbound без TLS. Пришло из слияния исходного Xray |
| схема protobuf (для тех, кто собирает конфиг не из JSON) | - | health: удалены `ProviderConfig.url`, `is_enabled`, `max_bytes`, `ProviderApply.is_routing_accepted`, старая форма `ProbeProfile` без `steps`, одиночные `destination`; задержки провайдера стали `DurationRange` (`interval = 14`, `network_settle = 15`, `CostTrigger.after = 4`); добавлен `SelectorConfig.fallback_tag = 16`. wireguard: `DeviceConfig.domain_strategy` удалён, `DNS` 11 -> 10, `parameters` 10 -> 1000, `PeerConfig.protocol_version` 6 -> 1000. proxyman: `SenderConfig.speed_limit = 7` | сериализованный protobuf-конфиг старого ядра новому не подходит; для JSON неважно |

### 2. Протокол провайдера

Версия протокола прежняя: `provider=1`. Состав тела запроса не менялся, кроме двух строк ниже .

| Что | Ревизия 3 | Ревизия 4 | Что сделать |
|---|---|---|---|
| заголовки запроса | `Xray-Adaptive`, `Accept`, `Content-Type` (только POST), плюс заголовки конфига, ответа и хоста | набор заголовков тот же; изменилось только значение `Xray-Adaptive` (раздел 5) | - |
| тело: `groups[].group` | с `tagPrefix` | без префикса, как теги outbound | см. 0.2 |
| тело: `outbounds[].quality` и `witnessQuality` | `bytesDown`, `ttfb*`, `dial*`, `tcp*`, `bulkCount`, `bulkKbpsP50/P90/Max`, `bulkRttP50Ms`, `bulkSplicedCount`, `rxOutOfOrderFraction`, `txRetransmitFraction` | добавлены `bulkPeakCount`, `bulkPeakKbpsP50/P90`, `bulkSaturatedCount`, `bulkUnsaturatedCount`, `bulkSaturatedKbpsP50/P90`, `bulkPolicedCount`, `bulkPolicedKbpsP50/P90`, те же с префиксом `upload`, `speedLimitDownKbps`, `speedLimitUpKbps`. Гистограмм в запросе нет | новые поля можно игнорировать |
| как применяется набор | новизну решал `version`: тот же `version` - тот же набор, даже при другом содержимом | новизну решает хеш `outbounds` и `groups`. Содержимое то же: ничего не пересобирается, берутся аренды, заголовки, `baseVersion`, `nextRefreshIn` и новый `version` (`is_changed: false`). Содержимое другое: набор применяется при любом `version`. Хеш чувствителен к порядку ключей и записи чисел внутри outbound; `"groups": null` на него не влияет | менять `version` вместе с набором или не слать его вовсе; лишняя смена хеша безвредна, неизменённые outbound не пересоздаются |
| неизвестный ключ ответа | на верхнем уровне, в `selection`, в `groups[]` отклонял весь ответ (`unknown key "<k>"`) | на верхнем уровне, в `selection`, `selection.returnToCheaper`, `groups[]`, `groups[].returnToCheaper` игнорируется. До 16 имён с путём (`ttl`, `selection.weight`, `groups[].region`) попадают в поле `ignored_keys` события `provider_apply` (и в его копию в трассе), и при успехе, и при отказе на сборке. В `Status` и в отчёте пропущенных ключей нет | новые необязательные ключи можно слать клиентам с `rev` от 4; что клиент не понял, видно по `ignored_keys` |
| `routing` и `healthObservatory` в ответе | отклоняли ответ: `unknown key "routing"`, `healthObservatory is not accepted in an answer` | отклоняют ответ, если значение не `null`: `routing is not accepted in an answer`, `healthObservatory is not accepted in an answer` | не слать; поправить сравнение текста |
| `receivedAt` в ответе по сети | неизвестный ключ, отклонял ответ | игнорируется (читается только в начальном наборе) | - |
| `null` | `null` у `groups`, `nextRefreshIn`, у ключей `selection` и групп отклонял ответ; `null` у тега в `expiresAt` и у имени в `headers` давал ошибку | `null` у любого ключа равен его отсутствию: на верхнем уровне, в `selection`, группах, `returnToCheaper`; у тега в `expiresAt` - аренды нет; у имени в `headers` - заголовка нет. Обязательный ключ со значением `null` считается отсутствующим (`"cost": null` -> `needs cost above zero`) | клиентам с `rev=3` `null` не слать |
| `"headers": {}` | при `acceptedHeaders: []` отклонял ответ | пустой объект и объект из одних `null` заголовков не ставят и ответ не отклоняют | - |
| метки в ответе (`selection.labels`, `groups[].labels`) | до 6, имя 1..24 `[a-z0-9_]`, значение 1..32 `[A-Za-z0-9._:-]`; тексты `...labels has invalid key`, `has invalid value`, `has invalid bounded label` | то же, плюс значение не IP-адрес. Тексты: `outbound <tag> selection.labels.<имя>: invalid label name` или `invalid label value` | см. 0.2 |
| дороги `via`: пропуск | причины пропуска текстом с пробелами | дорога пропускается (`code: skipped` в `lastRefresh.attempts`), если outbound нет, он проверочный, в карантине, либо балансировщик исчерпан или держит мёртвый путь. Причины в `provider_fetch.error` теперь с подчёркиваниями (см. 0.3) | - |
| дороги `via`: число попыток | два ответа без тела протокола заканчивали всё обновление; таймауты не ограничивались | потолки действуют на одну дорогу: после 2 ответов без тела протокола (сюда же таймаут после того, как запрос ушёл целиком) или 4 таймаутов любого вида ядро переходит к следующей `via`, и каждая следующая дорога получает ещё попытку. Ответ с телом протокола по-прежнему заканчивает перебор. При нехватке памяти перебор кончается после первой неудачи | отвечать быстро; повисший запрос теперь обходит все дороги |
| таймаут попытки | `providers[].timeout`, умолчание 10 с | без изменений; добавлено различение «запрос ушёл целиком» для потолка выше | - |
| `Retry-After` без тела протокола | останавливал перебор | не останавливает, см. 0.2 | - |
| `probeOnlyRoads` | при исчерпанном балансировщике или мёртвом текущем пути вместо дороги пробуются проверочные учётки | логика та же. Удерживаемый при исчерпании путь дорогой к бэку не служит: дорога берётся по текущему выбору, а он пуст | - |
| аренды | остаток от `serverTime` или `Date`; начальный набор с `receivedAt` в будущем получал нулевой возраст | остальное без изменений. `receivedAt` позже часов устройства: возрастом набора считается модуль разницы, аренды кончаются раньше. `null` у тега в `expiresAt` - аренды нет | - |
| `baseVersion` | до 256 байт, имя базового конфига | без изменений; `null` равен отсутствию | - |
| повод `exhausted`: задержка | случайная из `triggers.exhausted.delay`, умолчание `0s..10s` | верхняя граница удваивается за каждое неудачное обновление подряд (не больше 8 удвоений), до 1 мин; более широкий `delay` конфига не сужается | при падении популярного входа клиенты после первой неудачи приходят шире |
| отказ в заголовках (401 или 403 с `code` `unauthorized` или `forbidden`) | все поводы ждут 1 ч или новых заголовков | поведение то же: ждёт и повод `host`, паузу снимает `SetProviderHeaders`. Поля `Status.providers[].is_auth_refused`, `auth_retry_in_ms` и событие `provider_auth_failed` (`provider`, `trigger`, `status`, `via`, `code`) не менялись | - |
| начальный набор, на котором outbound падает с паникой | паника обрывала старт ядра | набор отклоняется, событие `provider_apply` с `trigger: initial`, `is_ok: false`, `error: "panic"`, ядро стартует без набора. Паника после регистрации outbound замораживает наблюдателя | хосту: если ядро не взяло сохранённый набор, не передавать его снова |
| сохранённый набор старого ядра как `initialAnswer` нового | - | проходит те же проверки, что ответ: набор с `<c>` в шаблонах AWG, меткой-адресом или `speedLimit` в старой форме отклоняется целиком, ядро стартует без набора до первого обновления | после обновления приложения не удивляться одному старту без сохранённого набора |
| outbound, убранный через gRPC `HandlerService` во время доживания | не закрывался наблюдателем повторно | наблюдатель закрывает его сам в конце доживания и при остановке | - |

### 3. Status, события, отчёт, трасса

| Что | Ревизия 3 | Ревизия 4 | Что сделать |
|---|---|---|---|
| `Status.balancers[].exit` | поля не было | строка, выводится всегда. Outbound, в который роутер сейчас отправляет новые соединения балансировщика: (1) текущий выбор, если он может принимать соединения; (2) при исчерпании - удерживаемый путь; (3) после удержания - выход `fallbackBalancer` по тем же правилам (не больше 3 переходов); (4) иначе `fallbackTag`. Пусто, только если `fallbackBalancer` не `leastCost`, или у балансировщика, добавленного мимо JSON без `fallback_tag`. Путь с истёкшей арендой при выключенном `expiredLeaseFallback` и проверочный путь выходом не бывают: тогда `active` не пуст, а `exit` равен `fallbackTag`. Переназначение роутера через API не учитывается | страну и сервер выхода читать из `exit` и `exit_labels`, а не из `active` |
| `Status.balancers[].exit_labels` | поля не было | объект, необязательный: метки outbound из `exit` (`selection.labels` конфига, слитые с метками группы, или метки из набора провайдера). Отсутствует, если у outbound нет меток: так у `fallbackTag`, значит трафик не идёт через путь с метками. От `telemetry.pathLabels` не зависит | сравнивать пару (`exit`, `exit_labels.country`) с прошлым опросом |
| `Status.balancers[].held` | не было | отдельного поля нет. Удержание видно так: `is_exhausted: true`, `active` пуст, `exit` не пуст и не равен `fallbackTag` | - |
| `Status.paths[].labels` | выводились только при `telemetry.pathLabels` | выводятся всегда, когда у пути есть метки. `telemetry.pathLabels` решает только попадание в отчёт (`path_labels`), сводку качества и перепись | метки можно читать без `pathLabels` |
| признаки исчерпания | `is_exhausted`, пустой `active`, событие `exhausted` (`balancer`, `from`, `candidates`, `fallback_balancer`) | те же, плюс `exit` в Status и необязательное поле `held` в событии `exhausted` (равно `from`, если тот не пуст). Конец удержания отдельного события не имеет | - |
| `Status.broken_subsystems` | массив тегов подсистем, поймавших панику | поле то же; значение теперь либо пустой массив, либо `["app/observatory/health"]`: паника другой подсистемы завершает процесс | при непустом массиве перезапустить ядро |
| `Status.paths[].quality`, `Status.witness_quality` | поля качества без скорости пиков | добавлены поля из строки про качество ниже, но без гистограмм `*_hist` (они только в отчёте) | - |
| `Status.providers[].last_error` | сырой текст | класс или короткий текст без адресов: `timeout`, `tls`, `network`, `status`, `answer larger than <N> bytes`, причина пропуска дороги, причина отказа набора | см. 0.3 |
| `Status.features` | у выключенного провайдера из protobuf был ключ `providers.<tag>.is_enabled: false` | ключа с `false` не бывает; у действующих провайдеров `is_enabled: true`, как раньше | - |
| событие `path_state` | `evidence_ms` только у `down`; `by` у `rescued`: `confirm`, `audit` | `evidence_ms` есть и у `suspect`; новое значение `by: "traffic"` (сработало соединение, начатое после решения `down`) | - |
| событие `provider_apply` | `rules`, `routing_error`, `is_routing_ignored` | удалены; добавлено `ignored_keys`; новое значение `error: "panic"` у начального набора; `is_changed: true` теперь и при прежнем `version` с новым содержимым | см. 0.3 |
| событие `provider_fetch` | `error` - сырой текст | `error` - класс, см. 0.3. У ответа, не разобранного как JSON, остался текст `invalid answer: ...` | - |
| событие `network_change` | `network` - сырой `id` | `network` - хеш, см. 0.3 | - |
| событие `probe_only_promoted` | `provider`, `tag` | `provider`, `outbound` | см. 0.3 |
| событие `probe_result` у пробы переписи | `error` мог быть пустым у неудачной пробы | у неудачной неотменённой пробы всегда класс (`other`, если своего нет) | - |
| событие `connectivity_ignored` | после первого успеха пути во время серии неудачных проверок | после 3 неудачных проверок подряд, каждая из которых пришла после успеха | - |
| событие `network_up` | после `NetworkLost` приходило по `NetworkChanged`, трафику или свидетелю | может прийти с `by: "connectivity"` без вызова хоста: ядро само проверяет связь после `NetworkLost` | - |
| событие `panic_recovered` | `scope: subsystem` у любой подсистемы | `scope: subsystem` только у наблюдателя; строка уходит в лог ошибок Xray, перед завершением процесса ещё и в stderr | - |
| журнал событий | событие больше `events.maxBytes` вытесняло весь журнал | такое событие выбрасывается одно (`dropped` растёт на 1), журнал не трогается | - |
| отчёт: `paths.<tag>.ttfb_p50_ms` | имя `ttfb_p50_ms` | имя `ttfb_life_p50_ms` | см. 0.2 |
| отчёт: имена путей | ключи `paths`, `active`, `switch_history`, `incidents` - теги ядра, у outbound провайдера с `tagPrefix` | без изменений. В запросе провайдера теги и теперь группы идут без префикса, в отчёте - с префиксом | при сопоставлении отчёта с запросами учитывать префикс |
| отчёт: качество `quality[].paths.<tag>` | `probe_*`, `bytes_down`, `ttfb_count/p50/p90`, `dial_*`, `tcp_*`, `bulk_count`, `bulk_kbps_p50/p90/max`, `bulk_rtt_p50_ms`, `bulk_spliced_count`, `rx_out_of_order_fraction`, `tx_retransmit_fraction` | добавлены гистограммы `ttfb_ms_hist`, `dial_ms_hist`, `bulk_kbps_hist`; пик скачивания `bulk_peak_count`, `bulk_peak_kbps_p50/p90/hist`; `bulk_saturated_count`, `bulk_unsaturated_count`, `bulk_saturated_kbps_p50/p90/hist`; `bulk_policed_count`, `bulk_policed_kbps_p50/p90/hist`; те же для отдачи с префиксом `upload_` (средней скорости отдачи нет); `speed_limit_down_kbps`, `speed_limit_up_kbps` | новые поля можно игнорировать; перцентили разных отчётов не усреднять, складывать корзины |
| отчёт: `path_labels` | только при `telemetry.pathLabels` | без изменений | - |
| отчёт: улики забытого пути | состояние улик забытого пути оставалось в счётчиках | при забывании пути его записи из учёта улик убираются | - |
| трасса: размер по умолчанию | 2 МиБ | 256 КиБ | см. 0.3 |
| трасса: ответ `ExportTrace` | `{seq, dropped, started, records}`; всегда всё кольцо после `after` | добавлено `is_more`; с пределом порции `seq` - номер последней отданной записи, при `is_more: true` сразу спросить снова с этим `seq`. Строка больше предела уходит одна. `after` больше последнего номера кольца (номер из прошлого запуска) считается нулём | на iOS выгружать порциями через `ExportTraceUpTo` |
| трасса: поля событий | на `other` заменялся только текст в поле `error`; `network` перехешировался солью трассы | на `other` заменяется текст в любом строковом поле с именем на `error`; `network_change.network` в трассе тот же хеш, что в событиях | - |

### 4. Поведение, заметное снаружи

| Что | Ревизия 3 | Ревизия 4 | Что сделать |
|---|---|---|---|
| исчерпание: удержание последнего пути | новые соединения сразу шли в `fallbackBalancer` или `fallbackTag` | балансировщик отдаёт последний выбранный путь 1 минуту (`exhaustedHold`). Пока решения стоят на паузе из-за сети (`NetworkLost` или непрошедшая проверка связи), удержание длится и после минуты. Прошедшая проверка связи снимает удержание у всех балансировщиков сразу. Под пином удержания нет | с `network.connectivity` переход в заглушку занимает одну проверку связи; без неё - минуту, даже если сети нет |
| исчерпание: проверка связи | не запускалась | при включённой `network.connectivity` (и если она не `ignored`) исчерпание сразу ставит проверку связи | включить `network.connectivity` в рабочем конфиге |
| исчерпание: раунды перепроверки | путь перепроверялся только по концу своего карантина (2 мин, удвоение до 30 мин) | пока пользователь пытается соединиться (нет простоя), нет паузы сети и нет `doubt`, ядро пробует все пути балансировщика, не дожидаясь карантинов: первый раунд через 5 с после исчерпания, следующие через 5, 10, 20, 40 с, затем каждые 60 с. Путь с кончившимся карантином пробуется сразу, как раньше. Первый прошедший пробу путь становится выбранным (`switch` с `recovered`). Неудачная проба пути в карантине карантин не удлиняет. Пробы идут в пределах `probes.limits` | после возврата сети связь восстанавливается в пределах минуты без действий хоста |
| спасение пути трафиком | успех соединения через путь в карантине отбрасывался | соединение, начатое после решения `down` и сработавшее, снимает карантин (`path_state` с `reason: rescued`, `by: traffic`, уровень карантина на один ниже) и исчерпание | - |
| неудачная проба после успеха | могла признать путь мёртвым | проба, начатая до того, как через путь что-то успешно прошло, путь мёртвым не признаёт (для причин, кроме трафика) | - |
| политика паник | паника в подсистеме выключала её до перезапуска процесса; паника в коде наблюдателя на пути соединения уходила в соединение | (1) подсистема вне наблюдателя: процесс завершается; (2) одно место соединения, 20 паник за 60 с: процесс завершается; места наблюдателя в счёт не идут; (3) наблюдатель (его цикл, его код на пути соединения, применение начального набора после регистрации outbound): замораживается - выбор застывает, пробы и провайдер остановлены, команды отвечают успехом, но ничего не меняют, признак - `broken_subsystems` | см. 0.3; при заморозке перезапустить ядро |
| скорость обнаружения отказа | - | Логика вердикта не менялась. Сильная улика: 3 отказа уровня пути (`dial_failed`, `dial_timeout`, `server_unverified`) в окне 60 с - при `failover.confirmWithProbe: auto` путь сразу `down`, без пробы. Слабая: 3 отказа (`no_response`, `closed`, `stall`, `partial_stall`) к 2 разным адресатам в окне 60 с - сначала подтверждающая проба. Числа 3, 2 и 60 с из JSON не задаются. Повторы одного соединения считаются один раз | Ключи, от которых зависит время: `evidence.dialTimeout.timeout` (6 с: одна улика на соединение не раньше этого срока), `evidence.firstReply.timeout` (5 с), `evidence.stalledRequests.timeout` (8 с), `failover.confirmWithProbe`, `failover.shouldProbeBeforeSwitch` (проба кандидата перед переключением), `steps[].timeout` (8 с) и число адресов шага, `probes.limits`, `network.connectivity` (подозрение ждёт проверку связи, если последняя удачная старше 4 её таймаутов). Если приложение обрывает попытку раньше `dialTimeout`, ядро получает не больше одной улики на попытку |
| адреса шага пробы `steps[].destinations` | все адреса шага спрашивались одновременно, шаг проходил по первому успеху | адреса спрашиваются по очереди: следующий только после неудачи предыдущего, у каждого свой `timeout`. Проверка связи (`connectivity.destinations`) по-прежнему спрашивает все адреса сразу | шаг с несколькими адресами признаёт путь мёртвым позже: до `timeout` на каждый адрес |
| AWG: срок ожидания ответа туннеля | фиксированные 11 с | `2 * rekeyTimeout + 1 с`, где `rekeyTimeout` - верхняя граница заданного в `awg.rekeyTimeout`, без него 5 с (те же 11 с) | при большом `rekeyTimeout` мёртвый туннель обнаруживается позже |
| AWG: неудачное пробуждение туннеля | каждое соединение, получившее закешированную ошибку пробуждения, давало наблюдателю отказ пути | отказ сообщает только соединение, которое само запускало пробуждение | - |
| AWG: `sendThrough` | на ленивом пути сокет туннеля открывался без адреса `sendThrough` | адрес выставляется до пробуждения | - |
| AWG: домен назначения внутри туннеля | один случайный адрес по `domainStrategy` | перебор всех адресов домена для TCP | - |
| возврат на более дешёвый путь | - | Логика не менялась, добавлены две проверки конфига (0.1). Чтобы балансировщик вернулся с дорогого пути (например, с AWG-двойника) на дешёвый (слот Xray), нужно: (1) блок `returnToCheaper` в `strategy.settings` балансировщика, `{}` достаточно; (2) у дешёвого пути `cost` меньше; (3) дешёвый путь не `probePolicy: onDemand` и не в карантине; (4) возврат не выключен у текущего, дорогого пути через `selection.returnToCheaper: {"isEnabled": false}` (у члена группы - у группы). Период берётся у текущего пути: его `selection.returnToCheaper.interval`, иначе `interval` балансировщика (умолчание 30 мин, первая попытка в случайной точке периода), но не раньше `minHoldTime` (2 мин) на текущем пути. Возврат после 3 успешных проб подряд с паузой не меньше 20 с (числа не настраиваются), `switch` с `reason: return`. Отказ пути в первые 5 мин после возврата удваивает ожидание следующей попытки, до 2 ч. В простое (нет пользовательского трафика), под пином, при исчерпании и при нехватке памяти возврата нет. `returnToCheaper` у outbound из ответа провайдера требует блока у балансировщика в конфиге | чтобы возврат был быстрее, задать `interval` у дорогого пути или у его группы, а не у дешёвого |
| смена сети с тем же `id` | полный сброс | только метки, см. 0.3. Во время паузы сети тот же `id` проходит полный сброс, как раньше | - |
| потеря сети (`NetworkLost`) | проверка связи после сигнала не планировалась; паузу снимали `NetworkChanged`, успех трафика или свидетель | с `network.connectivity` ядро само проверяет связь: первая проверка примерно через 2 с, затем реже, до 30 с; прошла - пауза снята (`network_up`, `by: connectivity`). Исчерпанный балансировщик на время паузы отдаёт последний путь, поэтому первое удачное соединение тоже снимает паузу | после возврата сети всё равно слать `NetworkChanged` |
| проверка связи `network.connectivity` | один успех пути при неудачных проверках выключал проверку до смены сети (`connectivity_ignored`) | выключается после 3 неудачных проверок подряд, каждая после успеха трафика, пробы или свидетеля; мигающая сеть проверку не выключает. Запускается ещё и при исчерпании и после `NetworkLost`. `outbound` должен существовать в конфиге, `timeout` до 30 с | - |
| `resources.closeUnchosenIdle` | тишиной считалось отсутствие чтения | тишина считается и по записи: соединение, которое только отправляет, тихим не считается | - |
| устаревшие пробы | проба, чей результат уже не нужен, доживала до своего таймаута и занимала место в `maxConcurrent` | отменяется сразу | - |
| `speedLimit` | не существовал | после простоя лимит пропускает разом четверть секунды трафика, не меньше 64 КиБ; ограниченное соединение не использует splice. Изменение лимита в ответе провайдера пересоздаёт outbound | - |

### 5. Заголовок `Xray-Adaptive`

| Что | Ревизия 3 | Ревизия 4 | Что сделать |
|---|---|---|---|
| точная строка | `Xray-Adaptive: xray="26.10.10", api=2, rev=3, provider=1` | `Xray-Adaptive: xray="26.10.31", api=2, rev=4, provider=1` | шаблон выбирать по `api`, внутри - наибольшей ревизии не больше `rev` клиента; `xray` сборки адаптивного ядра не различает |
| где задаётся ревизия | `ConfigRevision = 3` | `ConfigRevision = 4` в `adaptive_header.go:17`; `APIVersion = 2` там же, строка 11; `providerProtocol = 1` в `settings.go:92`; версия Xray из `core/core.go:21-23, 62-63` | - |
| что означает rev 4 | - | одна ревизия покрывает и конфиг, и ответ провайдера: строгий `speedLimit`, новые отказы при загрузке, терпимый разбор ответа (`ignored_keys`, `null`), применение набора по содержимому, потолки попыток на дорогу, удержание пути при исчерпании | клиентам с `rev=3` не отдавать `speedLimit` (лимита не будет), неизвестные ключи ответа и `null` (ответ отклонится) |
| где хост берёт заголовок | Go `health.AdaptiveHeader()`, libXray `HealthAdaptiveHeader()` | то же, плюс gRPC `AdaptiveHeader` и CLI `xray api health header` | - |

### 6. Команды хоста

#### 6.1 Go API, gRPC, CLI

| Что | Ревизия 3 | Ревизия 4 | Что сделать |
|---|---|---|---|
| Go: выгрузка трассы порциями | только `ExportTrace(after uint64) (TraceData, error)` | добавлено `ExportTraceUpTo(after uint64, maxBytes int) (TraceData, error)`; `ExportTrace` зовёт его с 0. В `TraceData` новое поле `IsMore` (`is_more`) | - |
| Go: `NetworkChangedWithLabels(id, labels string) error` | при ошибке разбора меток смена не применялась | смена применяется без меток, ошибка возвращается | см. 0.3 |
| Go: `health.ValidateLabels` | не отклонял адреса | отклоняет значение-адрес; тексты `<имя>: invalid label name`, `<имя>: invalid label value` | - |
| Go: новая `wireguard.ValidateAmneziaTemplate(spec string) error` | не было | проверка одной строки шаблона `i1`-`i5` | - |
| gRPC `HealthService`: новые методы | не было | `SetProviderURLs(SetProviderURLsRequest{provider, urls})`, `ExportTrace(ExportTraceRequest{after, max_bytes})`, `AdaptiveHeader(AdaptiveHeaderRequest{})`; у последнего `data` = `{"name", "value"}`, работает без наблюдателя | - |
| gRPC `Pin`, `Release` | отказ всегда `invalid_argument` | при непустом `run` другого запуска `stale_run`, иначе `invalid_argument` | см. 0.3 |
| код ошибки `rate_limited` у команд | был в перечне кодов | убран (ядро его нигде не возвращало); в теле ошибки протокола провайдера код `rate_limited` остаётся допустимым | - |
| CLI `xray api health` | `status`, `events`, `network`, `lost`, `present`, `pin`, `release`, `refresh`, `report`, `probe`, `headers`, `set`, `probe-result`, `memory` | добавлены `urls <provider>` (JSON-массив из stdin), `trace [after] [--max-bytes=N]`, `header`. Второе позиционное число у `trace` - ошибка `health trace: expected [after] [--max-bytes=N]` | - |
| gRPC `HandlerService.RemoveOutbound` | удалял outbound и сам закрывал его | только удаляет из менеджера (как в исходном Xray); доживающий outbound провайдера закрывает наблюдатель | - |
| CLI `xray api bi` | один тег | несколько тегов подряд (из исходного Xray). Опрос балансировщика через `GetBalancerInfo` больше не считается спросом пользователя и ядро не будит | - |

#### 6.2 libXray, функции `Health*`

Сравнение моста `1afb9b2` -> `152c2ee`, файл `xray_health.go`.

| Что | Ревизия 3 | Ревизия 4 | Что сделать |
|---|---|---|---|
| добавлено | - | `HealthExportTraceUpTo(after, maxBytes int64) string`: как `HealthExportTrace`, но записей не больше `maxBytes` байт (0 - без предела); `maxBytes` меньше 0 или больше `MaxInt32` - `invalid_argument` с текстом `trace portion is out of range`; пока `data.is_more` равно `true`, спросить снова с `data.seq` | использовать на iOS вместо `HealthExportTrace` |
| убрано | - | ничего | - |
| сигнатура изменилась | - | ни у одной функции `Health*` | - |
| `HealthRelease(balancer, run) bool` | `o != nil && o.Release(...)` | дополнительно требует блока `host.pinning`: без него `false` без обращения к ядру | - |
| `HealthNetworkChangedWithLabels(id, labels) bool` | `false` значил, что смена не применена | `false` при битых метках значит: смена применена без меток | см. 0.3 |
| `HealthNetworkChanged(id) bool` | - | сигнатура та же; тот же `id` без `NetworkLost` только меняет метки | см. 0.3 |
| `HealthStatus() string` | - | формат контейнера тот же; в `data` новые `balancers[].exit`, `exit_labels`, метки путей без `pathLabels`, новые поля качества | - |
| `HealthExportTrace(after int64) string` | `data` = `{seq, dropped, started, records}` | в `data` добавлено `is_more` (без предела порции всегда `false`) | - |
| `HealthProbeResult(run string, id int64) string` | `outbounds[].error` - текст | `outbounds[].error` - класс | см. 0.3 |
| `HealthDrainEvents() string` | - | формат тот же; изменились поля отдельных событий (раздел 3) | - |
| `HealthAdaptiveHeader() string` | `value` с `rev=3` | `value` = `xray="26.10.31", api=2, rev=4, provider=1`; формат `{success, data: {name, value}}` в base64 тот же | - |
| без изменений | - | `HealthNetworkLost`, `HealthUserPresent`, `HealthPin`, `HealthRequestProviderRefresh`, `HealthSetMemoryLow`, `HealthPeekReport`, `HealthTakeReport`, `HealthRequestProbe`, `HealthSetProviderHeaders`, `HealthSetProviderURLs`, `HealthExportProviderSet` | - |
| закреплённое ядро | `e44d1ec3` | `7424a0e0` (рабочий код равен `3e4ace6e`) | - |

Вне `Health*` корневой пакет моста изменился сильно после слияния с исходным libXray, см. последнюю строку 0.3.

### Что не проверено

- Поведение ревизии 3 в таблицах взято из сравнения исходников, сама она для сверки не собиралась.
- Слияние исходного Xray 26.10.10 -> 26.10.31 целиком не разбиралось (транспорты, TUN, geodata). В таблицы попали только `domainStrategy` у WireGuard и проверка TLS у `vless` и `trojan`.
- Мост вне `Health*`: перечислены только экспортируемые функции корневого пакета. Форматы `Invoke` и передача дескриптора TUN не сверялись.
- Загрузкой `xray run -test` проверены отказы из 0.1 для `<c>` в шаблоне AWG, `tagPrefix`, `via`, `fallbackTag`, потолка `maxRefreshesPerHour`, `networkChange` без `network.memory`, метки-адреса и `speedLimit`. Тексты остальных ошибок приведены по коду. Полный текст идёт с обёрткой, например `failed to build outbound config with tag <tag> > ... > invalid AmneziaWG i1 > unknown tag <c>`.
- Для конфигов protobuf пределы из 0.1 не действуют, кроме пустого `tagPrefix`.
- Скорость обнаружения отказа на настоящих устройствах не измерялась.
