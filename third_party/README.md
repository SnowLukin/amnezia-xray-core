# Локальная зависимость AmneziaWG

`amneziawg-go/` содержит полный снимок модуля
`github.com/amnezia-vpn/amneziawg-go/v3` версии `v3.1.20260828`.
Исходная MIT-лицензия и уведомления об авторских правах сохранены.
Контрольная сумма исходного модуля из `go.sum`:

```text
h1:D8d8gGvwXcTxUIsE4z6F6vjy4/VZddu95vMNtOygh1c=
```

В `device/timers.go` сброс `duration` при срабатывании таймера перенесён
под существующий `modifyingLock`. Раньше эта запись могла одновременно
выполняться с `Mod`, `Del` и `IsPending`, а также стирать новый срок,
заданный через `Mod`. Остальной исходный код сохранён без изменений.
Добавлен `device/timers_race_test.go`, который проверяет одновременное
чтение состояния и срабатывание таймера, а также повторный запуск во
время выполнения callback.

Корневой `go.mod` подключает снимок относительным `replace`.
Проверка из корня этого репозитория:

```sh
GOWORK=off go test -race github.com/amnezia-vpn/amneziawg-go/v3/device -run '^TestTimer' -count=1
GOWORK=off go test -race ./proxy/wireguard/...
GOWORK=off go test -race ./app/observatory/health -run '^TestWireGuard' -count=1
```

`replace` не наследуется модулями, которые используют xray-core как
зависимость. Встраивающий модуль, например `kit/libXray`, должен добавить
собственный `replace github.com/amnezia-vpn/amneziawg-go/v3` с относительным
путём к `third_party/amneziawg-go` внутри используемой копии xray-core.
При сборке через Go workspace то же правило задаётся в `go.work`.
