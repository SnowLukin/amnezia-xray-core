# Health observatory: API v2

Публичные поверхности adaptive API согласованы 13 октября 2026 года. Апстримные ключи Xray и Amnezia сохраняются. Старые adaptive-имена не являются алиасами: конфиг с ними отклоняется. Внутренние protobuf DTO служат сборке ядра; клиентский JSON задаётся схемой ниже.

Ядро выбирает доступный outbound по стоимости, проверяет пассивные улики пробами и принимает ограниченные наборы от провайдера. Backend задаёт политику и кандидатов; хост передаёт сигналы устройства и забирает агрегированный отчёт.

## Каркас

```jsonc
{
  "outbounds": [
    {
      "tag": "nl-1",
      "protocol": "vless",
      "settings": {},
      "selection": {
        "balancer": "auto",
        "cost": 1,
        "probe": "full"
      }
    }
  ],
  "routing": {
    "balancers": [
      {
        "tag": "auto",
        "fallbackTag": "block",
        "strategy": {
          "type": "leastCost",
          "settings": {
            "failover": {},
            "groups": [],
            "returnToCheaper": {},
            "startup": {},
            "networkChanges": {}
          }
        }
      }
    ]
  },
  "healthObservatory": {
    "evidence": {},
    "probes": {},
    "network": {},
    "host": {},
    "resources": {},
    "telemetry": {},
    "providers": []
  }
}
```

Это схема расположения блоков, а не готовый конфиг VLESS. В полном конфиге остаются обычные `inbounds`, `dns`, `routing.rules`, `settings` и `streamSettings` Xray.

## Правила конфигурации

1. Конфиг, команды хоста и протокол провайдера — camelCase; события, Status и отчёт — snake_case. Измеренные длительности — целые `…Ms` / `…_ms`.
2. Дополнительная фича включается наличием блока. Нет блока — фича выключена; `{}` включает прежние умолчания. Это относится к конфигу ядра и ответу провайдера.
3. `isEnabled` допускается только как `false`, чтобы выключить механизм с умолчанием true: базовые TCP-улики, `closePending`, `closeSilent`, `triggers.exhausted`, `leaseRenewal`, `backendSchedule`, `expiredLeaseFallback`. Override `returnToCheaper` пути/группы может выключить унаследованную политику. `isEnabled:true` и флаги у дополнительных блоков отвергаются.
4. Фичи с обязательными значениями задаются прямо: `groups: []`, `providers: []`, `startup.pin: {outbound,…}`, `probes.udp: {destinations,…}`, `network.connectivity: {destinations,outbound,…}`. `probePolicy`, `confirmWithProbe`, `networkChanges.action` остаются режимами.
5. Периоды и таймауты положительные. `0s` допускается только для немедленных задержек и бессрочного пина; отсутствующий `startup.pin` не закрепляет путь.
6. Неизвестные ключи, старые внешние имена и нарушенные зависимости отвергаются с полным путём. JSON и прямой protobuf-конфиг имеют одинаковые умолчания.
7. Отсутствующая дополнительная фича не создаёт свою фоновую работу, трекеры, очереди или запросы. Общие структуры принадлежат включённому потребителю.
8. Каждый путь имеет явный профиль `selection.probe` либо наследует `groups[].probe`. Адреса пробы задаёт backend, молчаливого встроенного Google URL нет.
9. Защитные потолки, quota/backoff, Retry-After и stale-result проверки обязательны. Присутствие блока не отменяет эти ограничения.

## Outbound и leastCost

```jsonc
{
  "outbounds": [
    {
      "tag": "nl-1",
      "protocol": "vless",
      "settings": {},
      "selection": {
        "balancer": "auto",
        "cost": 1,
        "probe": "full",
        "probePolicy": "regular",
        "returnToCheaper": {
          "interval": "10m"
        },
        "labels": {
          "node": "nl-ams-12",
          "location": "NL"
        }
      }
    },
    {
      "tag": "pool-a",
      "protocol": "vless",
      "settings": {},
      "selection": {
        "balancer": "auto",
        "group": "pool"
      }
    },
    {
      "tag": "pool-probe",
      "protocol": "vless",
      "settings": {},
      "selection": {
        "balancer": "auto",
        "group": "pool",
        "isProbeOnly": true
      }
    },
    {
      "tag": "direct",
      "protocol": "freedom"
    },
    {
      "tag": "block",
      "protocol": "blackhole"
    }
  ],
  "routing": {
    "balancers": [
      {
        "tag": "auto",
        "fallbackTag": "block",
        "strategy": {
          "type": "leastCost",
          "settings": {
            "failover": {
              "shouldProbeBeforeSwitch": true,
              "confirmWithProbe": "auto",
              "closePending": {},
              "closeSilent": {
                "after": "8s"
              }
            },
            "groups": [
              {
                "name": "pool",
                "cost": 1,
                "probe": "full",
                "failedInRowToLeave": 3
              }
            ],
            "returnToCheaper": {
              "interval": "30m",
              "minHoldTime": "2m"
            },
            "startup": {
              "outbound": "nl-1"
            }
          }
        }
      }
    ]
  }
}
```

`selection` описывает участие в балансировщике; `path` звучит как сетевой маршрут и не объясняет, почему без него адрес не наблюдается. Концепция «путь» остаётся в Status, событиях и документации: это выбранный туннель. Отдельный объект на outbound сохраняю: исключаем списки regex и дублирование свойств адреса в наблюдателе.

Цена, проба, резерв и интервал возврата остаются у одиночного участника или группы; член группы их не переопределяет. `isProbeOnly` и метки — свойства самого адреса. Ссылка selection.group требует объявленного элемента groups[]. Выключенные подготовленные группы не хранятся в конфиге. Одна группа и один balancer на участника; принадлежность к нескольким балансировщикам пока не нужна и умножила бы состояния и неоднозначные пины.

Порядок при равной цене, случайная стартовая позиция группы, её карантин и `failedInRowToLeave` сохраняются. `up` остаётся предпочтительнее `degraded` до сравнения цены. Пин не считает `down` рабочим: возвращает сигнал хосту по действующему контракту. `fallbackTag` остаётся upstream-ключом; для leastCost обязателен явный fallback, чтобы исчерпание не отправило трафик в первый outbound конфигурации.

`probePolicy` выбирает политику проб: `regular` (умолчание) или `onDemand` (нынешний isLazy). Это режим планирования, а не отрицательный bool. On-demand не означает probe-only и не исключает самый дешёвый неизвестный путь из начального выбора. Он сохраняет нынешние пропуски периодического возврата/автоматического плана host probes; explicit host request, initial choice и failover сохраняются. Включение нового режима не меняет цену и право передавать user traffic. Возвратный override у адреса/группы задаёт период именно для периодических попыток, сохраняя ручную пробу и failover. При отсутствии override наследуется balancer; `isEnabled:false` выключает унаследованный возврат. Для включённого override требуется блок `returnToCheaper` балансировщика.

## Наблюдение, сеть, ресурсы и телеметрия

```jsonc
{
  "healthObservatory": {
    "evidence": {
      "dialFailures": {},
      "dialTimeout": {
        "timeout": "6s"
      },
      "serverUnverified": {},
      "firstReply": {
        "timeout": "5s"
      },
      "earlyClose": {},
      "stalledRequests": {
        "timeout": "8s"
      },
      "partialStalls": {
        "maxAge": "30s"
      },
      "udpReplies": {
        "timeout": "5s"
      },
      "loneFailureProbe": {
        "after": "2m"
      }
    },
    "probes": {
      "profiles": [],
      "stepProfiles": {},
      "limits": {
        "maxConcurrent": 1,
        "perMinute": 12,
        "perServerNamePerMinute": 3,
        "hostPerMinute": 4
      },
      "candidates": {
        "keepVerified": 1,
        "recheckInterval": "15m"
      },
      "requests": {
        "maxOutbounds": 8,
        "timeout": "2m"
      },
      "udp": {
        "destinations": [
          "stun.example.test:3478"
        ],
        "requests": 5,
        "interval": "200ms",
        "timeout": "2s",
        "perHour": 6
      }
    },
    "network": {
      "connectivity": {
        "outbound": "direct",
        "destinations": [
          "https://edge.example/204",
          "https://backup.example/204"
        ],
        "timeout": "4s"
      },
      "witnesses": {
        "outbounds": [
          "direct"
        ]
      },
      "memory": {
        "maxNetworks": 16
      },
      "hostLabels": {},
      "labelRequest": {
        "outbound": "direct",
        "destinations": [
          "https://edge.example/whoami",
          "https://backup.example/whoami"
        ],
        "labels": {
          "asn": "X-Network-Asn",
          "country": "X-Network-Country"
        },
        "timeout": "4s",
        "requestsPerHour": 6
      }
    },
    "host": {
      "pinning": {},
      "userPresent": {
        "freshness": "60s",
        "minInterval": "3h"
      },
      "memoryPressure": {}
    },
    "resources": {
      "tunnelSleep": {
        "afterLastUse": "5m"
      },
      "closeUnchosenIdle": {
        "after": "15s"
      }
    },
    "telemetry": {
      "events": {
        "maxEvents": 1000,
        "maxBytes": 65536
      },
      "report": {
        "interval": "24h",
        "quietSampleFraction": 0,
        "sessionLabels": {
          "labels": {
            "cohort": "adaptive-a"
          }
        }
      },
      "quality": {},
      "pathLabels": {},
      "probeMeasurements": {},
      "metrics": {}
    },
    "providers": []
  }
}
```

Все показанные bool, кроме примеров в разделе 4, — предлагаемые умолчания. Адреса у выключенных сетевых блоков — примеры; их нет в умолчаниях. У включённой UDP-пробы и проверки сети адреса обязательны.

Witness tracking не требует active connectivity HTTP check: после NetworkLost успешный witness снимает паузу; отказ witness сам по себе не доказывает отсутствие интернета.

Status доступен по запросу без отдельного collector: он представляет уже необходимое состояние. Журнал, отчёт и качество независимы. Счётчик отчёта обновляется непосредственно переходом состояния, поэтому включённый отчёт не требует включённого журнала. Качество можно читать через Status/провайдер без отчёта: в этом случае создаётся только current scope, без report-open histogram/closed segments. ClosePending/CloseSilent получают минимальный registry abort/progress без включения выключенных detector/TTFB/TCP_INFO. UDP-улики сохраняют требование recent TCP success: минимальный счётчик этого успеха относится к включённому UDP detector, даже когда TCP detector и quality выключены. Метки сети и замеры проб не должны требовать качества: сегмент наблюдений способен содержать только метки или только замеры проб.

Память меток по IP получает собственный bounded LRU на `network.memory.maxNetworks` с умолчанием 16, даже если память решений выключена; лучше отдельный внутренний storage с тем же ceiling, чем скрытая зависимость двух фич. Сами значения меток opaque: лимиты и формат прежней спеки, без IP, доменов и SSID. Labels никогда не влияют на выбор.

Сон туннеля и завершение невыбранных тихих соединений разделяются: второе работает только на outbound, поддерживающем соответствующий ресурсный контроль, и полезно даже при выключенном сне. Unsupported тип не создаёт tracker; его capabilities видны в Status. Это изменение механики нужно доказать отдельно, раздел 10.

Минимальный наблюдатель не запускает дополнительных периодических проб, quality sampling, UDP sweep и экспорт. При полном отключении пассивных улик он сохраняет нынешний старт на самом дешёвом пути без стартовой пробы; перед последующим переключением проверяет кандидата, если shouldProbeBeforeSwitch включён. Он использует известные отказы собственных проб и явные запросы. Обнаружение нового отказа работающего пути в таком намеренно урезанном профиле требует включённого источника улик или явной пробы; сам факт пользовательского dial_error остаётся ошибкой соединения, но не отключённой пассивной уликой. Пользовательский трафик всё ещё является сигналом «есть работа», а не требующей флага аналитикой.

## Профили проб

```jsonc
{
  "healthObservatory": {
    "probes": {
      "stepProfiles": {},
      "profiles": [
        {
          "name": "large",
          "steps": [
            {
              "name": "transport",
              "destinations": [
                "https://edge.example/bytes/32768"
              ],
              "method": "GET",
              "expectedStatuses": [
                200
              ],
              "minBytes": 32768,
              "timeout": "8s",
              "progressTimeout": "4s",
              "isOptional": false
            }
          ]
        },
        {
          "name": "full",
          "steps": [
            {
              "name": "transport",
              "destinations": [
                "https://a.example/bytes/32768",
                "https://b.example/bytes/32768"
              ],
              "minBytes": 32768,
              "timeout": "8s",
              "progressTimeout": "4s",
              "isOptional": false
            },
            {
              "name": "egress",
              "destinations": [
                "https://edge.example/204"
              ],
              "timeout": "5s",
              "isOptional": true
            }
          ]
        }
      ]
    }
  }
}
```

Одношаговый профиль не требует `stepProfiles`. Два и более шага либо optional-шаг требуют его; 1–4 шага и 1–4 адреса на шаг, обязательные сначала. Пропадает двусмысленность двух форм `destination` и `steps` в одном профиле. Это преобразование формы, не сетевого поведения: один шаг остаётся одной пробой, required/optional, fallback-адреса и отдельный mux-worker сохраняются. У каждого пути обязателен явный профиль, собственный или у группы. Встроенного адреса проверки нет.

`probeReason`, имя профиля, исходы шагов и UDP-оценка продолжают описывать фактическую работу. Совмещать TCP, HTTP и UDP в один «общий timeout» не используем: они измеряют разные вещи.

## Host API

Не используем один универсальный `Execute` и строковые имена команд: типизированные функции проще для хостов и аудита. На всех слоях одно имя операции; транспортные префиксы `Health` / `LibXrayHealth` и Swift lowerCamelCase остаются.

| Смысл | Go / gRPC | CLI после `xray api health` | libXray / C | Swift |
| --- | --- | --- | --- | --- |
| Смена сети без меток | `NetworkChanged(id)` | `network <id>` | `HealthNetworkChanged` / `LibXrayHealthNetworkChanged` | `networkChanged` |
| Смена сети и метки атомарно | `NetworkChangedWithLabels(id, labels)` | `network <id> --label k=v` | `HealthNetworkChangedWithLabels` / `LibXrayHealthNetworkChangedWithLabels` | `networkChangedWithLabels` |
| Сеть потеряна | `NetworkLost()` | `lost` | `HealthNetworkLost` / `LibXrayHealthNetworkLost` | `networkLost` |
| Пользователь вернулся | `UserPresent()` | `present` | `HealthUserPresent` / `LibXrayHealthUserPresent` | `userPresent` |
| Давление памяти | `SetMemoryLow(isLow)` | `memory --low=true` | `HealthSetMemoryLow` / `LibXrayHealthSetMemoryLow` | `setMemoryLow` |
| Закрепить / отпустить | `Pin(b,o,run)` / `Release(b,run)` | `pin` / `release` | `HealthPin`, `HealthRelease` / C с тем же suffix | `pin`, `release` |
| Попросить набор | `RequestProviderRefresh(provider)` | `refresh [provider]` | `HealthRequestProviderRefresh` / C с тем же suffix | `requestProviderRefresh` |
| Заголовки источника | `SetProviderHeaders(provider,headers)` | `headers <provider>` из stdin | `HealthSetProviderHeaders` / C с тем же suffix | `setProviderHeaders` |
| Сохранить последний набор | `ExportProviderSet(provider,after)` | `set <provider> [after]` | `HealthExportProviderSet` / C с тем же suffix | `exportProviderSet` |
| Заказать пробу | `RequestProbe(request)` | `probe <balancer> [outbounds] --udp` | `HealthRequestProbe` / `LibXrayHealthRequestProbe` | `requestProbe` |
| Прочитать результат | `ProbeResult(run,id)` | `probe-result <run> <id>` | `HealthProbeResult` / `LibXrayHealthProbeResult` | `probeResult` |
| Локальное состояние | `Status()` | `status` | `HealthStatus` / `LibXrayHealthStatus` | `status` |
| Забрать журнал | `DrainEvents()` | `events` | `HealthDrainEvents` / `LibXrayHealthDrainEvents` | `drainEvents` |
| Снимок сводки / закрыть окно | `PeekReport()` / `TakeReport(isFinal)` | `report` / `report take|final` | `HealthPeekReport`, `HealthTakeReport` / C с тем же suffix | `peekReport`, `takeReport` |

В gRPC отдельные `NetworkLost` и `Release`, а не перегрузка `NetworkChanged.lost` и `Pin.outbound=""`. Отдельные PeekReport и TakeReport вместо комбинаций двух bool `take/final`. `NetworkChangedWithLabels` остаётся отдельной операцией ради существующего вызова без меток; это принятая совместимость входа из measurement-context, не алиас запрещённого config-имени.

`NetworkChangedWithLabels` всегда передаёт базовый сигнал смены сети. При
`network.hostLabels: false` аргумент меток игнорируется целиком; при
включённом флаге ошибочный JSON возвращает ошибку без смены сети. Валидный объект
содержит строковые значения: не более восьми допустимых меток, имя 1–24 символа
`a-z0-9_`, значение 1–32 символа `A-Za-z0-9._:-`; неверные записи отбрасываются.

Запрос `network.labelRequest` запускается асинхронно после успеха пользовательского
трафика или witness **текущей** сети. Probe и connectivity check не дают такого
разрешения. Он не создаёт controller evidence и не попадает в пользовательскую
телеметрию. Кеш по ID ограничен `network.memory.maxNetworks` даже при выключенной
памяти решений; пустой ID получает новую запись при каждой смене. После отказа
допускается один повтор через 10 минут, с прежними ограничениями idle/lost/memory
и requestsPerHour. Пробы резервов собираются при `telemetry.probeMeasurements`
независимо от `telemetry.quality`; берётся первая фаза соединения логической пробы,
UDP-пробы и отменённые результаты исключаются.


API использует единый Result для операций, уже отдающих JSON: Go возвращает типизированные data/error, gRPC передаёт ту же DTO и транспортный error отдельно, libXray сохраняет внешний base64 контейнер, C — JSON-строку, Swift декодирует DTO. CLI показывает decoded JSON. У существующих signal-функций сохраняется ABI bool/void (в частности NetworkChanged без меток); false означает, что сигнал не принят. Подробное effective состояние читается через Status. Это осознанное исключение к унификации ради старого хоста, а не два алиаса операции. Функции запуска/остановки libXray и кита не меняются.

```jsonc
{
  "success": true,
  "run": "232c4c611bfbaee9",
  "data": {
    "is_queued": true,
    "retry_in_ms": 0
  }
}
```

```jsonc
{
  "success": false,
  "error": {
    "code": "feature_disabled",
    "message": "requires healthObservatory.probes.requests"
  }
}
```

Коды: `not_running`, `feature_disabled`, `invalid_argument`, `unknown_balancer`, `unknown_outbound`, `unknown_provider`, `stale_run`, `not_found`, `rate_limited`, `busy`. JSON не содержит bool с другим типом в другой функции. JSON-команды выключенной фиче возвращают `feature_disabled`; legacy bool-сигналы возвращают false. Хост может посылать их без предварительного Status.

```jsonc
{
  "balancer": "auto",
  "outbounds": [
    "a1",
    "b1"
  ],
  "isUdp": true
}
```

```jsonc
{
  "id": 3,
  "run": "232c4c611bfbaee9",
  "outbounds": [
    "a1",
    "b1"
  ]
}
```

```jsonc
{
  "id": 3,
  "run": "232c4c611bfbaee9",
  "balancer": "auto",
  "is_udp": true,
  "age_ms": 2100,
  "is_done": true,
  "end": "done",
  "outbounds": [
    {
      "outbound": "a1",
      "result": "ok",
      "source": "probe",
      "rtt_ms": 113
    }
  ]
}
```

`ExportProviderSet` отдаёт прежний `{seq,set}`, включая credentials: только защищённое локальное хранилище. Смена названия не делает набор аналитикой. Между запусками хост сохраняет сам set, не runtime ticket/run. Пины и их release сохраняют существующую защиту run. Расширение run на ProbeResult закрывает совпадение числового id после перезапуска; это новая проверка команды, для неё нужен собственный тест.

## Выходные DTO

Status — **только локальная диагностика**, не разрешённая аналитическая DTO. API использует явный `is_local_only: true`; это предупреждение в данных, а защита — в коде хоста, принимающем только тип Summary. `server_name`, raw `network_id`, `last_error`, состав providers/probe_requests/groups сохраняются локально. Журнал также локальный. Credentials есть только в ExportProviderSet, не в Status.

```jsonc
{
  "is_local_only": true,
  "run": "232c4c611bfbaee9",
  "network_id": "opaque-local-network",
  "network": {
    "host": {
      "type": "wifi"
    },
    "ip": {
      "asn": "12345"
    }
  },
  "is_network_up": true,
  "connectivity": "ok",
  "generation": 1,
  "is_idle": false,
  "is_memory_low": false,
  "features": {
    "quality": false,
    "events": false,
    "report": true,
    "udp_probe": false
  },
  "probes_queued": 0,
  "probes_running": 0,
  "paths": [
    {
      "outbound": "b1",
      "state": "up",
      "reason": "probe",
      "for_ms": 4072,
      "is_probe_only": false,
      "server_name": "node.example",
      "labels": {
        "node": "nl-ams-12"
      }
    }
  ],
  "balancers": [
    {
      "balancer": "auto",
      "active": "b1",
      "is_exhausted": false,
      "active_for_ms": 4072,
      "candidates": [
        "a1",
        "b1"
      ]
    }
  ]
}
```

`features` в полном Status — карта всех effective флагов, не только четырёх в примере; её сборка только при вызове Status. Поля ресурсов/health/probes/group/provider сохраняют нынешние имена, кроме названий, явно перечисленных в миграции. Недоступное измерение отсутствует, измеренный ноль остаётся.

```jsonc
{
  "events": [
    {
      "seq": 27,
      "ts": 1791896412345,
      "type": "path_state",
      "fields": {
        "outbound": "b1",
        "state": "suspect",
        "reason": "traffic",
        "is_strong": false
      }
    }
  ],
  "dropped": 0
}
```

```jsonc
{
  "health_run": "232c4c611bfbaee9",
  "report_reason": "snapshot",
  "report_index": 1,
  "is_quiet_sample": false,
  "window_ms": 88523,
  "session_ms": 88523,
  "switches": 2,
  "switch_reasons": {
    "start": 1,
    "failover": 1
  },
  "switch_history": [
    {
      "from": "a1",
      "to": "b1",
      "reason": "failover"
    }
  ],
  "path_down_count": 1,
  "probes": 3,
  "probes_failed": 1,
  "probe_bytes": 68082,
  "active": {
    "auto": "b1"
  },
  "is_balancer_exhausted": false,
  "is_network_up": true,
  "labels": {
    "cohort": "adaptive-a"
  },
  "path_labels": {
    "b1": {
      "node": "nl-ams-12"
    }
  },
  "paths": {
    "b1": {
      "state": "up",
      "active_ms": 4134,
      "bytes_up": 130,
      "bytes_down": 4237,
      "conns_ok": 1,
      "conns_failed": 0,
      "probes": 1,
      "probe_bytes": 33590
    }
  },
  "quality": [
    {
      "is_network_known": false,
      "duration_ms": 87407,
      "network": {
        "host": {
          "type": "wifi"
        },
        "ip": {
          "asn": "12345"
        }
      },
      "paths": {
        "b1": {
          "ttfb_count": 1,
          "ttfb_p50_ms": 60,
          "tcp_count": 1,
          "tcp_rtt_p50_ms": 20,
          "probe_count": 1,
          "probe_failed_count": 0,
          "probe_connect_p50_ms": 104,
          "probe_reasons": {
            "failover": 1
          }
        }
      }
    }
  ]
}
```

Счётчики текущего отчёта сохраняются все: audit/wake/lone, candidate/promoted/fresh_path, pending/idle close, UDP, providers/expiry/auth/base_version/Retry-After, pin/release/network/memory/sleep, байты и probe steps. Нули выключенных счётчиков остаются понятными; опциональные объекты измерений не создаются. `switch_path` строками с разделителями заменяется `switch_history` из объектов (последние 10). Так тег с `>` или `:` не ломает разбор хостом.

Поля качества (`ttfb_*`, `dial_*`, `tcp_rtt_*`, `bulk_kbps_*`, `bulk_rtt_p50_ms`, `bulk_spliced_count`, `rx_out_of_order_fraction`, `tx_retransmit_fraction`, witness) не переименовываю: они называют измерения и имеют нужные единицы. Максимумы: 4 последние непустые сегмента, top-4 path на сегмент; drop counters не скрываются. Метки не содержат raw сетевого отпечатка. Новый запрос whoami не является пользовательским трафиком и не попадает в quality.

Ядро не знает PostHog, connection_id и событие продукта. Обёртка хоста превращает Summary в событие `app/xray_health_summary`; транспорт получает готовый payload через один `HealthSummarySink.send(summary)` (название интерфейса условное, реализация у хостов). Fan-out в два sender не требует изменений ядра. Хостовые `report_reason=stop/recovered`, `connection_id`, `user_present_count` остаются. Снимок для recovery защищён как текущий локальный файл; повторная отправка дедуплицируется по `health_run+report_index` на хосте.

## Фичи и умолчания

Все строки ниже предполагают наличие наблюдателя и leastCost, если не сказано иное. `true` — функция нужна основному отказоустойчивому выбору; `false` — дополнительная работа/поведение. В примере продуктового профиля их можно включить явно независимо от дефолта библиотеки.

| Фича | Включение v2 | Default | Почему / зависимость |
| --- | --- | --- | --- |
| Наблюдатель, leastCost, quarantine, failover | `healthObservatory` + `leastCost` | По наличию | Сам продукт; нет — stock Xray |
| Ошибка dial | `evidence.dialFailures.isEnabled` | true | Обнаружить отказ транспорта |
| Timeout dial | `evidence.dialTimeout.isEnabled` | true | Найти зависший transport; свой timeout |
| Server unverified | `evidence.serverUnverified.isEnabled` | true | Невалидное REALITY verification доказывает отказ пути |
| Нет первого ответа | `evidence.firstReply.isEnabled` | true | Blackhole после успешного dial |
| Закрылось до ответа | `evidence.earlyClose.isEnabled` | true | Lazy transport показывает отказ при закрытии |
| Запрос без прогресса | `evidence.stalledRequests.isEnabled` | true | Зависший путь с существующими соединениями |
| Частичный stall | `evidence.partialStalls` | false | Дополнительная эвристика; включается самостоятельно |
| UDP-улики | `evidence.udpReplies` | false | Отдельный тип traffic tracking |
| Lone-проба | `evidence.loneFailureProbe` | false | Дополнительный запрос; требует хотя бы одного path-level источника: dialFailures, dialTimeout или serverUnverified |
| Проба перед switch | `failover.shouldProbeBeforeSwitch` | true | Не переводить на непроверенный туннель |
| Подтверждение suspect | `failover.confirmWithProbe` | auto | Это политика основного failover, не выключающий флаг |
| Down audit | `failover.downAudit` | false | Дополнительная проверка после strong passive down; требует confirmWithProbe:auto и хотя бы одного path-level detector |
| Закрыть ждущие соединения down | `failover.closePending.isEnabled` | true | Даёт приложению возможность повторить на новом пути |
| Закрыть замолчавшие соединения down | `failover.closeSilent.isEnabled` | true | Не держать реально зависший запрос; active не закрывается |
| Возврат дешевле по таймеру | `returnToCheaper` | false | Экономия поверх основного failover |
| Возврат на смене сети | `returnToCheaper.onNetworkChange` | false | Требует returnToCheaper и networkChanges этого balancer |
| Группы | `groups[]` | false | Массив объявленных групп; ссылки group должны разрешаться |
| Политика проб по потребности | `selection.probePolicy` / группа | regular | onDemand выбирает режим бывшего isLazy; initial/failover/explicit host probe не блокирует |
| Стартовать на знакомом пути | `startup.outbound` | Пусто | Явный тег включает предпочтение, не новый timer |
| Пины хоста | `host.pinning` | false | Ручная политика продукта |
| Стартовый pin | `startup.pin` | false | Требует host.pinning, чтобы хост мог Release |
| Реакция выбора на смену сети | `networkChanges` | false | Дополнительный reset/recheck; measurement epochs по NetworkChanged не зависят от него |
| Память сетей | `network.memory` | false | Дополнительный bounded LRU решений |
| Проверка интернета | `network.connectivity` | false | Отдельные прямые запросы; destinations/outbound обязательны |
| Свидетели наличия сети | `network.witnesses` | false | Непустой список; работают без active connectivity check, в том числе после Host NetworkLost; failure witness не роняет сеть |
| UserPresent | `host.userPresent` | false | Дополнительные пробы по сигналу ОС; 3h — только интервал включённого механизма |
| Сигнал памяти | `host.memoryPressure` | false | Дополнительные ограничители/освобождение; поддержка сигнала без sleep/quality допустима |
| Сон AWG | `resources.tunnelSleep` | false | Меняет lifecycle туннеля |
| Тихие невыбранные соединения | `resources.closeUnchosenIdle` | false | Независимо от tunnelSleep; только способные outbound |
| Многошаговые пробы | `probes.stepProfiles` | false | Single mandatory step допустим без фичи |
| Probe-only кандидаты | `probes.candidates` + `selection.isProbeOnly` | false | Источник не обязателен; кандидат не получает user traffic |
| UDP-проба | `probes.udp` | false | Адреса обязательны; измеряет, не меняет TCP health/выбор |
| Пробы хоста | `probes.requests` | false | UDP request дополнительно требует probes.udp |
| TCP quality | `telemetry.quality` | false | Измеряет, не выбирает |
| Журнал событий | `telemetry.events` | false | Локальная диагностика; report не зависит |
| Отчёт окна | `telemetry.report` | false | Аналитика, хост сам забирает без core report timer |
| Перепись путей | `telemetry.census` | Нет блока | Требует report и sampleFraction; независимая выборка, не меняет выбор |
| Выборка тихих сессий | `telemetry.report.quietSampleFraction` > 0 | 0 | Добавляет долю, не выключающий флаг; требуется report |
| Метрики | `telemetry.metrics` | false | Экспорт; требует штатный metrics exporter |
| Status | Вызов `Status()` | Доступен | Read-on-demand, дополнительной фоновой фичи нет |
| Метки сети хоста | `network.hostLabels` | false | Независимы от качества и сети решений |
| Метки сети по IP | `network.labelRequest` | false | Собственный ограниченный кеш; IP не хранится; hostLabels-off host не мешает |
| Метки путей | `telemetry.pathLabels` | false | selection/group.labels валидируются, когда заданы; не меняют выбор |
| Метки сессии | `telemetry.report.sessionLabels` | false | Требует report |
| Замеры проб резервов | `telemetry.probeMeasurements` | false | Использует выполненные пробы, не добавляет сетевых запросов |
| Источник свежих адресов | `providers[i]` | false | Базовый набор работает независимо |
| Refresh по exhaustion | `triggers.exhausted.isEnabled` | true у включённого provider | Запрашивает ресурс, который закончился |
| Periodic/minHealthy/cost/maxAge/network refresh | Наличие каждого блока | false | Дополнительные поводы; minHealthy считает real usable |
| Продление аренды | `triggers.leaseRenewal.isEnabled` | true у provider | Сохранить выданный слот |
| Запрос по nextRefreshIn | `triggers.backendSchedule.isEnabled` | true у provider | Повод задаёт бэк, все лимиты сохраняются |
| Истёкшая аренда как последний резерв | `apply.expiredLeaseFallback.isEnabled` | true у provider | Текущее сохранение последнего шанса; не гарантирует серверный слот |

Исключения к «любой комбинации»: только явные связи из таблицы. Ошибки, например: `auto.returnToCheaper.onNetworkChange requires auto.returnToCheaper and auto.networkChanges`; `selection pool-probe: isProbeOnly requires healthObservatory.probes.candidates or healthObservatory.telemetry.census`; `RequestProbe isUdp requires probes.udp`; `evidence.loneFailureProbe requires an enabled path-level detector`; `auto.failover.downAudit requires confirmWithProbe:auto and an enabled path-level detector`; `telemetry.report.sessionLabels requires telemetry.report`. Метрики без exporter — ошибка конфига, а runtime JSON-команда без активной фичи — `feature_disabled` (legacy signal ABI возвращает false).

## Независимость и стоимость выключения

`healthObservatory` без optional-фич включает только шесть базовых детекторов; чтобы выключить всю обработку пользовательских соединений, задайте всем шести `evidence.*.isEnabled: false`. Выключенные events/report/quality не создают своих буферов. Соединение проходит без observer context/dialer/writer wrappers, когда включённым механизмам не нужны улики, счётчики, activity или success proof. Работа собственных проб при этом сохраняется. Return/provider/network/resource-механики могут требовать лёгкий activity tracker; он не включает детекторы, quality или sweeper выключенной фичи.

Events и report независимы: report считает события даже при выключенном журнале; выключенный report не держит закрытые quality-сегменты. `PeekReport` читает текущий снимок; `TakeReport(isFinal)` закрывает только готовое окно. У ядра нет отдельного таймера отправки отчёта, хост вызывает команду. Выборка тихих сессий по умолчанию 0; выбранная сессия может дать final/snapshot без решения, но не периодические quiet отчёты.

`Status` и `DrainEvents` — локальная диагностика. `ExportProviderSet` содержит credentials и предназначен только для защищённого локального сохранения. В аналитику передаётся только типизированная сводка; адреса пользователя, raw network id и значения headers туда не входят.

## Events

Every event is `{"seq": n, "ts": unix_ms, "type": "...", "fields": {...}}`.
`seq` grows by one per event, so gaps show dropped events.

| Type | Fields | When |
| --- | --- | --- |
| `switch` | `balancer`, `from`, `to`, `reason`, `held_ms`, `cost` | New connections go to another outbound. `reason`: `start`, `start_pinned`, `start_remembered`, `start_with`, `failover`, `return`, `recovered`, `remembered`, `network_change`, `network_up` (the network check passed again and brought back outbounds ruled out while it failed, to a balancer that had nothing usable), `candidates_changed`, `promoted` (a fresh passed candidate became real and recovered a dead or exhausted choice), `pin`, `retired` (the previous one left its provider set), `expired` (the lease of the previous one ended; one that failed as well is left with `failover`). `held_ms` is the time on the previous one. |
| `path_state` | `outbound`, `state`, `reason`; for `suspect` also `failures`, `targets`, `kinds`, `is_strong`; for `down` also `quarantine_ms`, `quarantine_level` and `is_provisional` when it was ruled out while the network check was failing; for `rescued` also `by`: `confirm` or `audit` | An outbound became `up`, `degraded`, `suspect`, `down` or `unknown`. For `degraded`, `reason` is the first failed optional step. Other reasons: `traffic`, `probe`, `return_probe`, `host_probe`, `network_change`, `network_down`, `network_up`, `replaced` (a provider
changed its settings), `probe_changed` (a provider changed the profile of an unchanged outbound), `rescued` (a confirmation already running or an audit assigned to the down proved it false, see the evidence and failover settings above). `kinds` counts failures by kind: `dial_failed`, `dial_timeout`, `server_unverified`, `no_response`, `closed`, `stall`, `partial_stall`. For an outbound with a tunnel, `dial_failed` and `dial_timeout` tell that the tunnel could not be set up or that its server did not answer a connection, see the evidence and failover settings above. |
| `probe_start` | `outbound`, `reason`, `balancer`, `running`, `request` | A probe started. `reason`: `confirm`, `failover`, `network_change`, `wake`, `remembered`, `recheck`, `lone`, `audit`, `return`, `group_recheck`, `candidate`, `host`. `running` counts the outbound probes running now, this one included. `request` is the id of the request a `host` probe is for. |
| `udp_probe` | `outbound`, `reason`, `state`, `answered`, `sent`, `rtt_ms`, `jitter_ms`, `destination_index`, `class` | A current UDP probe finished. Destination index is zero-based; `class` describes an immediate refusal for `unsupported`. Stale and canceled probes publish no observation. |
| `udp_suspect` | `outbound`, `flows`, `targets`, `window_ms` | Passive UDP evidence met the selected-path rule. |
| `udp_skip` | `outbound`, `reason` | A UDP probe could not start; each reason is written once until the next probe or reset. Event reasons are `per_hour`, `asleep`, `idle`, `memory` and `recent`; other blocked conditions are returned to the host or remain silent. |
| `probe_result` | `outbound`, `reason`, `balancer`, `is_ok`, `rtt_ms`, `bytes`, `error`, `is_stale`, `request`, `profile`, `state`, `steps` | A probe finished. `steps` lists only attempted steps with `name`, `is_passed`, `connect_ms`, `ms`, `bytes`, a `destination` index on success or an `error` class on failure. `bytes` counts both directions above TLS. `is_stale` means the network, handler or probe profile changed while it ran, so its result did not change the path. Its attempted steps still count in the report. |
| `probe_only_promoted` | `outbound`, `provider` | A provider made a fresh verified candidate real. |
| `fresh_path_down` | `outbound` | A new real provider handler was first judged down within five minutes of activation. |
| `group_state` | `balancer`, `group`, `state`, `members`, `failed`, `quarantine_ms` | An explicit group became `down` after consecutive failures or `up` after a successful recovery check. Unvisited members of a down group are skipped. |
| `probe_request` | `request`, `balancer`, `outbounds`, `end`, `ms`, `probed`, `reused`, `ok`, `failed`, `down`, `skipped`, `expired`, `gone` | A request of the host that waited for a probe ended; one answered at once writes none. `end`: `done`, `timeout`, `network_change`, `gone`. The counts are by result; `probed` and `reused` count the answers of a probe and of a recent proof. |
| `exhausted` | `balancer`, `from`, `candidates` | No usable candidate is left; traffic goes to `fallbackTag`. |
| `pending_closed` | `outbound`, `connections`, `reason`, `is_silent` | Connections of an outbound found down were ended: at the decision and at the switch away from it those that had received nothing, or, with `is_silent` true, `stallTimeout` after the decision those that received nothing since it, see the evidence and failover settings above. `reason` is the one the outbound went down for. |
| `return_start` | `balancer`, `from`, `to` | Probing a cheaper outbound to return to it. |
| `return_failed` | `balancer`, `outbound`, `successes` | The return was abandoned after a failed probe. |
| `return_delayed` | `balancer`, `outbound`, `delay_ms` | The return waits longer because the previous one failed. |
| `return_penalty` | `balancer`, `outbound`, `multiplier` | A return failed within `penaltyWindow`. |
| `pin`, `release` | `balancer`, `outbound`; for `release` also `by`: `host` or `timer` | The host pinned or released a balancer, or the start pin ran out. |
| `stale_request` | `request`, `balancer`, `run` | A pin or release meant for another run of the core was ignored. |
| `pinned_down` | `balancer`, `outbound`, `is_start_pinned` | The pinned outbound failed; the balancer stays on it. `is_start_pinned` is `true` when the pin is `startPinnedOutbound` rather than one of the host. |
| `connectivity` | `is_ok`, `rtt_ms`, `error` | Result of a network check; `error` is a failure class, never the error text. |
| `network_down` | `error` | The network check failed; decisions paused. `error` is a failure class, or `host` when the host reported the loss. |
| `network_up` | `by` | Decisions resumed: `connectivity` (a check passed), `traffic` or `probe` (an outbound worked). |
| `connectivity_doubt` | `failed_checks`, `paused_ms` | The check kept failing for `maxPause`; decisions resumed provisionally. |
| `connectivity_ignored` | `failed_checks` | The check failed while an outbound worked; it is not used until the next network change. |
| `network_change` | `network`, `is_known`, `generation`, `is_ignored` | The host reported a network change. `is_ignored` when no balancer forgets on it, as each has `networkChangeAction` `ignore`: what was learned stays and `generation` does not grow. |
| `memory_low` | `is_low` | The host reported that memory is low (`is_low` true) or no longer. |
| `provider_fetch` | `provider`, `trigger`, `retry`, `attempt`, `via`, `outbound`, `method`, `refresh`, `is_ok`, `status`, `ms`, `bytes`, `code`, `retry_after_ms`, `error` | One request of a refresh through one `via` entry; `outbound` is what the entry resolved to, `refresh` the number of the refresh as the body tells it, `bytes` the size of the answer. `code` is the code of an error body of the protocol, `retry_after_ms` the pause `Retry-After` asked for, after the ceiling and before the spread. A skipped entry has `is_ok` false and only `error`: `balancer exhausted`, `outbound down` or `no such outbound`. `error` never holds the URL. `trigger`: `host`, `exhausted`, `expiry`, `min_healthy`, `cost_above`, `network_change`, `next_refresh`, `max_age`, `interval`; `retry` is the level of the backoff, as the body of the request tells it: refreshes in a row since the last success that failed, brought an answer the core refused, or brought an exhausted balancer the same set; a `Retry-After` pause and refused headers leave it as it is. An answer that breaks the protocol has `is_ok` true: the backend answered, and the refusal is the `provider_apply`'s. |
| `provider_apply` | `provider`, `trigger`, `is_ok`, `is_changed`, `added`, `replaced`, `removed`, `unchanged`, `version`, `was_exhausted`, `rules`, `routing_error`, `is_routing_ignored`, `leases`, `renewed`, `expired`, `skew_ms`, `clock`, `headers`, `is_headers_stale`, `base_version`, `error` | The answer was applied (`is_changed`), was the applied set already, or was rejected (`is_ok` false with `error`); `trigger` `initial` is the initial set at the start. `was_exhausted` tells whether the balancer had nothing usable at that moment. `rules` counts the routing rules applied with the set; `is_routing_ignored` tells that the answer had routing while `apply.isRoutingAccepted` is off. When the answer names leases: `leases` counts them, `renewed` the ones it gave a later end that has not passed, ended ones too, `expired` the leases that ended after it, where one that had ended and got a later end that passed as well gives no second `provider_expired`; `skew_ms` is how far the clock they count from is ahead of the device, and `clock` which one that is: `server`, `date` (the `Date` of the answer plus `Age`), `saved` (the `serverTime` of a kept initial set plus its age) or `device`. `headers` names the headers of the answer; `is_headers_stale` tells that the host changed the headers while the request ran, and the answer did not set them, or that the host had set some names of the initial set before the start, which stay its own. `base_version` is the base config the answer named. |
| `provider_expired` | `provider`, `outbound`, `was_active`, `renewal` | The lease of an outbound ended, or an answer brought it ended. `was_active` tells whether a balancer chose the outbound. `renewal` tells why no later end came: `refused` (the backend answered without one), `failed` (the last refresh that asked failed), `off` (`renewAtFraction` is 0), `saved` (it ended in a set the host kept, before the start) or `pending` (no refresh that asked about it has ended: no network, the limits, or the one that asks is still under way). |
| `provider_auth_failed` | `provider`, `trigger`, `status`, `via`, `code` | The backend refused the headers of a request with `status` 401 or 403 and the error `code` `unauthorized` or `forbidden`; every trigger but the host waits, see Token. |
| `provider_headers` | `provider`, `by`, `names` | Headers of the requests changed: `by` an `answer`, the `initial` set or the `host`. `names` are the headers whose values changed; the values are never told. |
| `provider_base_changed` | `provider`, `from`, `to` | An applied answer named the base config `to` while the core runs with `from`: the host is to fetch the config anew, see Base config. Once per value, until an answer names `from` again. |
| `provider_skip` | `provider`, `trigger`, `reason`, `wait_ms` | A trigger wants a refresh that cannot start yet. `reason`: `min_interval`, `backoff`, `retry_after`, `max_per_hour`, `idle`, `network_down`, `pinned` (the balancer of the provider is pinned), `memory`, `auth` (the backend refused the headers). Reported once per trigger and reason until a refresh starts. |
| `provider_drained` | `provider`, `outbound`, `is_replaced` | An outbound that left the set, or the old handler of a replaced one, was closed: after `drainTimeout`, or at the release of a pin that held it past that, at once without connections, or early to keep at most `maxOutbounds` draining. |
| `outbound_sleep` | `outbound`, `idle_ms`, `reason` | An outbound released what it held, see resource controls above. `idle_ms` is the time since its last use; `reason` is `idle` (after `resources.tunnelSleep.afterLastUse`, or 10 s after the last connection ended once `idle_closed` ended its idle ones), `down` (sooner, while it rests in quarantine) or `memory` (sooner, while memory is low). |
| `idle_closed` | `outbound`, `connections` | Connections of an outbound that can sleep, which no balancer has chosen for `resources.tunnelSleep.afterLastUse`, were ended after they received nothing for `resources.closeUnchosenIdle.after`, see resource controls above. The path works: unlike `pending_closed`, no failure is behind it. |

## Report: словарь полей

| Field | Meaning |
| --- | --- |
| `health_run` | The run of the core, as `run` in Status. |
| `report_reason` | `stop`, `interval` or `snapshot`. |
| `report_index` | Number of the report in the run, from 1. |
| `is_quiet_sample` | This run was selected by `telemetry.report.quietSampleFraction`; selection does not change between report windows. |
| `window_ms`, `session_ms` | Length of the window and of the run in milliseconds of the wall clock. |
| `events` | Events written in the window. |
| `events_dropped` | Events the log dropped in the window because it was full: they never reached `DrainEvents`. |
| `switches`, `switch_reasons`, `switch_history` | `switch` events, their count by reason, and the latest 10 as typed `{from,to,reason}` objects. |
| `exhausted_count`, `pinned_down_count` | Events of those types. |
| `path_down_count`, `path_suspect_count`, `path_degraded_count` | `path_state` events with those states. |
| `group_down_count` | `group_state` events with state `down`. |
| `path_rescued_count` | `path_state` events with reason `rescued`: downs a confirming probe or audit proved false. A rescue changes no choice by itself and is no decision; a balancer it brings back switches with `recovered`. |
| `pending_closed_count` | Connections of outbounds found down ended by `pending_closed` events: those that had received nothing and, with `is_silent`, those that went silent after the decision. |
| `idle_closed_count` | Connections of outbounds no balancer chooses ended by `idle_closed` events, so that the outbounds can sleep. Ending them changes no choice and is no decision. |
| `udp_probe_count`, `udp_probe_ok_count`, `udp_probe_lossy_count`, `udp_probe_dead_count` | Current completed UDP probes and their result counts. `unsupported` contributes only to the total. UDP observations are not decisions and do not count as TCP probes. |
| `udp_probe_second_destination_count` | UDP probes that tried the second destination. |
| `udp_suspect_count`, `udp_suspect_confirmed_count` | Passive UDP suspicions, and `dead` probes whose reason was `udp_suspect`. |
| `probes`, `probes_failed`, `probe_bytes` | `probe_result` events, the failed ones, and their `bytes`. |
| `host_probe_requests`, `host_probes` | `probe_request` events, and `probe_result` events with reason `host`. |
| `return_attempts`, `return_failures` | `return_start` and `return_failed` events. |
| `network_changes`, `network_known`, `network_down_count` | `network_change` events, the ones with `is_known`, and `network_down` events. |
| `provider_fetches`, `provider_fetch_failures`, `provider_triggers` | `provider_fetch` events, the ones without `is_ok`, and their count by `trigger`. |
| `provider_retry_after` | `provider_fetch` events with `retry_after_ms`: a busy backend asked to wait. |
| `provider_applied`, `provider_rejected` | `provider_apply` events that changed the set, but for the initial set, the rejected ones. |
| `provider_expired` | `provider_expired` events: leases that ended. |
| `provider_renewals` | `provider_apply` events with `is_ok` and `renewed` above zero, but for the initial set: answers that renewed leases. A renewal keeps what the balancer has and is no decision. |
| `provider_auth_failed` | `provider_auth_failed` events: the backend refused the headers. |
| `provider_base_changed` | `provider_base_changed` events: the backend serves another base config. |
| `audit_count`, `audit_passed_count`, `audit_skipped_count` | Started down-audit probes, completed successful audits (including stale completions), and eligible downs skipped because the assigned audit did not start. A rescued path may have been proved by confirm or audit; these counters count only audit work. |
| `pins`, `releases` | `pin` and `release` events. |
| `memory_low_count` | `memory_low` events with `is_low`. |
| `outbound_sleeps` | `outbound_sleep` events: outbounds that released what they held. A sleep changes no choice and is no decision. |
| `active`, `is_balancer_exhausted`, `is_network_up`, `connectivity` | At the time of the report: the chosen outbound by balancer, whether a balancer is exhausted, and the network as in Status. |
| `paths` | The first 64 outbounds by tag. `state` and `ttfb_p50_ms` (over the whole life of the outbound, 0 without samples) as they are now; `active_ms`, `degraded_ms`, `bytes_up`, `bytes_down`, `conns_ok`, `conns_failed`, `probes` and `probe_bytes` cover this report window. `probe_steps` gives `name`, `passed` and `failed` for steps actually attempted in the window, including stale probes. Steps follow the current profile order; names from an earlier profile in the same window follow in their first observed order. An outbound forgotten and seen again counts from zero. |
| `wake_probe_count`, `wake_probe_failed_count`, `wake_skipped_count` | Started wake probes, completed failed wake probes (including stale results), and eligible unique selected paths skipped per host call because proof is fresh or the wake interval/job limit applies. A queued cancellation has no start count; a canceled probe has no result count. Other probe work, pins, exhausted balancers, or unavailable networks add no skip count. |
| `candidate_probes`, `candidate_probes_failed` | Started candidate probes and completed failed candidate probes, including stale results. Canceled probes have no completion count. |
| `probe_only_promoted_count` | Passed candidates the provider made real while their verification remained within the reuse window. |
| `fresh_path_down_count` | New or replaced real provider outbounds first judged down within five minutes of activation, once per incarnation. |
| `lone_probe_count`, `lone_probe_failed_count` | Started lone probes and completed failed lone probes, including stale results; canceled probes have no result count. |
| `lone_probes`, `lone_probes_failed` | The same lone counters for this outbound in the report window. A failed current lone probe contributes one `downs_strong`; a stale completion contributes no down. |
| `downs_strong`, `downs_weak` | New downs backed by strong or weak passive evidence for that outbound in this window. They include failed current lone probes paired with a dial failure, but exclude pure wake and network-change probe failures. Use `wake_probe_count` and `wake_probe_failed_count` for the wake failure rate; passive down counters are not its denominator. |
| `udp_probes`, `udp_probes_dead`, `udp_rtt_p50_ms`, `udp_jitter_p50_ms`, `udp_state` | Per-path completed UDP probes, dead ones and reply RTT/jitter medians in this report window, at histogram bucket upper bounds (0 without samples). `udp_state` is the latest current result, or `unknown`. Taking a report clears histogram samples, but keeps the snapshot. |
| `udp_flows`, `udp_flows_unanswered` | Routed UDP link trackers that ended in this window; unanswered means at least one uplink buffer and no downlink bytes. A SOCKS UDP association can create more than one routed link after mux reconnect. An open tracker belongs to the window where it ends; final stop ends all open trackers. |
| `udp_bytes_up`, `udp_bytes_down`, `udp_writes`, `udp_reads` | Payload bytes and nonempty buffers at the logical outbound link. Each buffer is approximately one UDP packet. Downlink is counted before the underlying writer, so bytes received by the core do not imply that the application read them. These fields do not include proxy framing or AWG handshakes; the existing mixed `bytes_up/down` and `conns_ok/failed` keep their transport semantics. |
| `udp_first_reply_p50_ms` | Median time from the first uplink buffer to the first downlink buffer for routed links ending in this window, at the histogram bucket upper bound; 0 without samples. |
| `udp_ports` | Nonzero `dns` (53), `quic` (443), `stun` (3478-3479 and 19302), `rtc` (50000-65535), and `other` buckets. Each contains `flows` and `unanswered`. The first routed destination port fixes the bucket for the whole routed link, even if later packets name other destinations. |
| `groups` | Current explicit-group summaries as described in Outbound groups. |
| `quality` | Measured network segments of this report window, each with top-four admitted paths and optional witness; see Quality measurements. |
| `quality_segments_dropped` | Nonzero count of older nonempty quality segments discarded by the four-segment limit, reset when a report is taken. |

Per-outbound fields `audits`, `audits_passed`, `lone_probes`, `lone_probes_failed`,
`downs_strong`, `downs_weak` and the per-path UDP fields belong to `paths.<tag>`.
The UDP fields ending in `_count` are global report counters.

## Provider protocol

См. [PROVIDER.md](PROVIDER.md). Протокол остаётся 1; ответ меняет только ограниченный набор outbounds/groups, а новую полную конфигурацию хост получает по `baseVersion`.

## Переход в другой балансировщик

`leastCost.settings.fallbackBalancer` — тег другого элемента `routing.balancers`; отсутствие настройки оставляет прежний `fallbackTag`. Переход происходит только после исчерпания всех пригодных кандидатов, включая `probePolicy:onDemand`. Новое соединение получает текущий выбор цели, затем её следующий переход или конечный `fallbackTag`. `fallbackTag` обязателен у каждого leastCost. Цепочка содержит не более четырёх балансировщиков; неизвестный тег, переход на себя, цикл с перечислением цепочки и превышение длины отклоняются при загрузке.

```jsonc
{
  "routing": {
    "rules": [
      { "network": "udp", "balancerTag": "udp" },
      { "domain": ["domain:claude.ai", "domain:anthropic.com", "domain:openai.com", "domain:gemini.google.com"], "balancerTag": "ai" },
      { "network": "tcp,udp", "balancerTag": "main" }
    ],
    "balancers": [
      { "tag": "udp", "fallbackTag": "block", "strategy": { "type": "leastCost", "settings": { "fallbackBalancer": "main" } } },
      { "tag": "ai", "fallbackTag": "block", "strategy": { "type": "leastCost", "settings": { "fallbackBalancer": "main" } } },
      { "tag": "main", "fallbackTag": "block", "strategy": { "type": "leastCost", "settings": {} } }
    ]
  }
}
```

Участники задают `selection.balancer:"udp"` для AWG, `"ai"` для AI-пути и `"main"` для основного набора; каждый имеет явный профиль собственной проверки. Доменные правила требуют sniffing на inbound (либо домен, уже полученный маршрутизатором); обычное соединение по IP не несёт имя приложения. Проба AI-пути обращается к явно заданному AI-эндпоинту.

Пин сохраняет выбранный адрес и `pinned_down`, переход не начинается. Открытые соединения не мигрируют. Каждая попытка пользователя через исчерпанный источник остаётся спросом на него, поэтому обычная перепроверка карантина продолжается; после восстановления пути новые соединения автоматически возвращаются к источнику. Повод провайдера `exhausted` сохраняется. Улики и качество трафика в цели принадлежат выбранному outbound цели.

Целью может быть stock-балансировщик; это плохая практика, поскольку в нём нет учёта здоровья. Stock-стратегии и конфиги без `fallbackBalancer` сохраняют прежнее поведение. UDP-улики по-прежнему не меняют TCP health; AWG проверяется по TCP через тот же UDP-туннель и по его транспортным отказам.

Начальный bootstrap сохраняется: ядро не добавляет скрытую cold-start пробу. Если неизвестный путь недоступен с самого старта, переход начинается после обычного обнаружения отказа; успех самого первого соединения не гарантируется.

| Выход | Новые данные |
| --- | --- |
| `exhausted.fields` | `fallback_balancer`: тег настроенной цели |
| `fallback_balancer.fields` | `balancer`, `to`, `is_active`: true при первом реальном переходе, false при его окончании |
| `Status.balancers[]` | `fallback_balancer`, `is_falling_back` |
| Отчёт | `fallback_balancer_count`: число начал; `fallback_balancer_ms`: время по тегам источников |

```jsonc
{ "seq": 8, "ts": 1791896400000, "type": "fallback_balancer", "fields": { "balancer": "ai", "to": "main", "is_active": true } }
```

```jsonc
{ "fallback_balancer_count": 1, "fallback_balancer_ms": { "ai": 125000 } }
```

## Перепись путей (`telemetry.census`)

```json
{
  "healthObservatory": {
    "telemetry": {
      "report": {},
      "census": {
        "sampleFraction": 0.03,
        "startAfter": "3m",
        "onDemand": ["de-fallback", "cdn"]
      }
    }
  }
}
```

Блок есть — перепись включена; нет блока — нет выборки, очереди или запросов переписи. `sampleFraction` обязателен, конечное число `0 < x <= 1`. Монетка бросается один раз при старте ядра; `startAfter` по умолчанию `3m` и должен быть положительным. Требуется `telemetry.report`. `onDemand` принимает теги путей и имена групп только с `probePolicy:onDemand`; неизвестное имя и обычный путь — ошибка загрузки.

Выбранная сессия запускает не больше одной переписи на каждую сеть в этом запуске, после непрерывной сети в течение `startAfter` и пользовательского трафика. Новая сеть может получить свою перепись после той же задержки. Ядро ничего не хранит между запусками.

Все обычные пути всех `leastCost` входят автоматически. От группы берётся один следующий по кольцу участник вне карантина; если таких нет — следующий по кольцу. Пути «по запросу» проверяются только из `onDemand`, остальные получают `result:skipped, reason:on_demand`. `isProbeOnly` включаются с обязательными шагами их полного профиля. Для них достаточно `probes.candidates` **или** `telemetry.census`; перепись не включает автоматическое обслуживание кандидатов. У выбранного пути успех свежее `startAfter` используется без запроса с `source:recent`. Не больше 16 сетевых проб; последующие пути по цене и порядку получают `reason:limit`.

Причина пробы — `census`, ниже всех остальных, включая UDP. Общие интервалы, параллельность, минутные квоты и не больше одной переписи на server name в минуту сохраняются. Любая более приоритетная проба вытесняет перепись. Смена сети, простой, нехватка памяти или остановка ядра выпускают неполный снимок.

Результаты не меняют состояние, карантин, пассивные улики, выбор балансировщика, поводы провайдера или качество пользователя. `census_probes` и `census_probe_bytes` считаются отдельно от обычных проб. `observed_state` и `is_selected` фиксируют мнение наблюдателя в момент проверки; `member` именует фактически проверенного участника группы.

Снимок лежит в `report.census` того окна, в котором перепись завершилась либо прервалась; событие `census` содержит тот же объект. Внешние поля отчёта `health_run` и `report_index` связывают снимок со статистикой сессии. Метки сети, путей и сессии лежат рядом по обычному контракту. Содержимое не включает адресов, SNI и сырых текстов ошибок.

```json
{
  "census": {
    "network": {"is_known": false},
    "started_ms_ago": 412000,
    "duration_ms": 61000,
    "is_complete": true,
    "paths": {
      "reality-nl": {"result": "ok", "source": "recent", "observed_state": "up", "is_selected": true},
      "awg-nl": {"result": "failed", "source": "probe", "error": "dial_timeout", "observed_state": "unknown"},
      "relay": {"result": "ok", "source": "probe", "member": "relay-b1", "connect_ms": 61, "ms": 190, "observed_state": "up"},
      "standby-1": {"result": "skipped", "reason": "on_demand"}
    }
  },
  "census_probes": 2,
  "census_probe_bytes": 32768
}
```

Это иллюстрация формы; реальные значения зависят от профиля и сети. `Status.census` сообщает `is_selected`, `is_running` и `checked`; без блока `census` дополнительной фоновой функции нет. Хост передаёт `census`, `census_probes` и `census_probe_bytes` в `app/xray_health_summary` как есть вместе с метками и общими идентификаторами.

Если за окно закончилось несколько переписей, первый снимок лежит непосредственно в `census`, остальные — в `census.additional` (массив того же снимка без рекурсивного additional). `census.network.host` и `census.network.ip` содержат включённые метки сети именно этой переписи; `census.path_labels` — снимок включённых меток путей. Эти метки сохраняются на начало переписи и не подменяются поздней сменой сети или провайдером. Общие метки отчёта описывают окно по обычному контракту. Хост переносит новые вложенные поля без преобразования. Очередь завершённых снимков ограничена 64, множество отпечатков уже переписанных сетей — 4096; после достижения границы новые сети не выбираются повторно вместо забытых старых.
