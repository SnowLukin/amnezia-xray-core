# Проверка скорости quality

Runner и wrapper находятся в `testing/adaptive/quality/speed/`.
Команды выполняются из корня репозитория с уже собранным кандидатом после
ребейза на завершённый этап 1. Запуск согласуется в общей очереди стенда.
Наличие исходников не подтверждает результат: фактические замеры, входы,
хеши и исходы сохраняются в отдельном отчёте каждого прогона.

`quality_rate_proxy.py` загружает определения существующего
`testing/adaptive/chaos.py`. У оригинала безусловный `asyncio.run(main())` в
конце файла: wrapper проверяет точную форму этого вызова и снимает только его,
затем запускает тот же original `main()`. Изменённый entry point или pump API
вызывают явную ошибку вместо молчаливой несовместимости.

Начальный mode фиксируется при приёме соединения и передаётся обеим pump через
`ContextVar`. Соединение, начатое с mode, отличным от `rate`, использует
оригинальный pump. Для нового TCP в `rate:<positive bits/sec>` используется
read размером 4096 байт и отдельный monotonic deadline каждого направления:

```python
deadline = max(deadline, loop.time()) + len(data) * 8 / rate
await asyncio.sleep(max(0.0, deadline - loop.time()))
```

Ожидание происходит перед forwarding. Простой, блокировка drain и задержка
планировщика не дают кредита на последующий burst. Смена rate/pass применяется
к следующему read; уже запланированный chunk завершается в выбранном для него
mode. Legacy pause/reset/stall/reset_after/frozen и byte accounting сохранены.
UDP использует original whitelist и отвергает `rate`. TCP control, как и
оригинал, принимает произвольный mode; поэтому fixture задаёт только
положительный integer rate. `rate:0`/отрицательный rate отклоняется pump при
первом payload.

Wrapper имеет прежний CLI:

```text
quality_rate_proxy.py <control_port> <listen:upstream>... [udp:<listen:upstream>]...
```

Существующий optional override в
`testing/adaptive/bench.py`:

```python
CHAOS_SCRIPT = os.environ.get("BENCH_CHAOS_SCRIPT", os.path.join(HERE, "chaos.py"))
```

`Stand` передаёт `CHAOS_SCRIPT` вместо hardcoded пути в существующий chaos
`Popen`. Без environment override default остаётся original `chaos.py`.
`speed_runner.py` требует публичный `bench.CHAOS_SCRIPT` и проверяет, что он
указывает на соседний wrapper, до старта `Stand`.

Runner рассчитан на final candidate с интегрированными stage-1
`report.quietShare` и quality exports. Он использует существующие local
binary/certificates, не собирает бинарник и не генерирует сертификаты.
Единственная path — `a1` TCP+TLS, upstream через proxy `rate:2000000`, 8 MiB
plain HTTP. Все остальные chaos ports — `pass`. Modes установлены и сохранены
до `Client.__enter__`, поэтому TLS/data не получают первоначального большого
burst. Затем 16 MiB идут через `direct`: `connectivity.witnesses = ["direct"]`,
правило exact `witness.test` перед balancer rule и local DNS address
`127.0.0.1`. Inner TLS выключен явно. NetworkChanged не вызывается, чтобы не
добавлять recheck probes. Вторая case повторяет ту же fixture с
`passive.shouldSkipQuality = true`.

Допуски установлены в исходнике до первого прогона:

- Path `bulk_kbps_max`: 1800–2200 Kbit/s для ровно 2 000 000 bit/s proxy rate.
- Direct witness: не менее 20 000 Kbit/s и не менее десятикратного path max.
- Скорость всего HTTP body с TLS dial: 1600–2200 Kbit/s.
- Finished evidence: bounded polling до 8 секунд после закрытия sockets.

Path p90 проверяется как bucket единственного raw speed sample. Значение
2000 Kbit/s попадает в bucket 2000; raw чуть выше границы попадает в 3000.
Rate и tolerance после результата не подгоняются. 8 MiB занимают примерно
34 секунды; quality использует последнюю пройденную отметку 4 MiB, а не все
байты после неё.

На macOS counted witness speed обязателен. На Linux direct может splice:
`bulk_spliced_count > 0` при отсутствии witness speed даёт явный результат
`not_comparable`, а не speed-comparison pass. Skip case обязана не отдавать
quality measurements и сохранять выбор пути. Обе cases проверяют отсутствие
decision/down/suspect/probe событий и probe target hits; начальный start switch
не считается решением. Это отдельная fixture, не полный failover/invariance
стенд.

После Finished сохраняются Status, snapshot, due-only take result, Status после
take и final report. При `report.interval = "0s"` take может быть `null`: это
не закрывает окно. `quietShare = 1` требуется для snapshot/final тихого запуска.
Фактический stage-1 контракт требует проверки после интеграции.

Пример прогона из корня worktree после получения слота:

```sh
python3 testing/adaptive/quality/speed/speed_runner.py \
  --xray /absolute/path/to/already-built-candidate \
  --server-xray /absolute/path/to/existing-server-binary \
  --source-commit final-candidate-commit \
  --port-offset 24000 \
  --output /absolute/path/to/evidence-parent
```

`--server-xray` необязателен и по умолчанию равен `--xray`; остальные параметры
обязательны. Runner сам задаёт local HOST/LISTEN, plain HTTP и wrapper override.
Он проверяет integrated source fields/API, существующие certificates и
executables до запуска server/client. Это source preflight, не доказательство
соответствия binary исходникам; metadata хранит source identity label и SHA-256
binary/bench/wrapper/runner/original chaos/target.

Каждый запуск создаёт новую папку `quality-speed-<UTC>-<uuid>` с полными
server/client configs, logs, events, reports, modes/counters, response headers,
ожидаемыми/полученными bytes и wall/monotonic timestamps. `enabled` и
`skip-control` имеют отдельные evidence files. Exit code: 0 — passed, 1 —
failed, 2 — not_comparable. Null/incomplete evidence сохраняется и не выдаётся
за успешную проверку.

CPU samples содержат raw `ps` output и ярлык coarse/diagnostic-only. Они не
подменяют штатный throughput/CPU benchmark и не проверяют отдельный порог 2%.
Static review wrapper и взаимное чтение runner выполнены; actual syntax/import,
legacy mode equivalence, rate changes и fixture acceptance ещё не запускались.

`speed_runner.py` выводит корень репозитория через `parents[4]` от своего
финального пути и импортирует именно его `testing/adaptive/bench.py`.
`quality_rate_proxy.py` через тот же `parents[4]` находит original
`testing/adaptive/chaos.py`; wrapper остаётся соседним файлом runner.
Не запускайте эти файлы из scratch-пакета: там нет полного integrated source
tree, bench или certificates. Existing path/API/environment/preflight guards
сохраняются и должны отклонять неполную интеграцию до запуска стенда.
