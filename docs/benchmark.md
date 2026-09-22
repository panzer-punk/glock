# План бенчмарков Glock

[English](benchmark.en.md)

Документ фиксирует план сравнительных тестов для POC. Цель — ответить на два разных вопроса, а не просто найти «самый быстрый» backend.

## Вопросы, на которые отвечаем

| Ось сравнения | Вопрос |
|---|---|
| **Локальный mutex** (одна машина) | Нужен ли отдельный daemon, или хватит `flock`? |
| **Сетевой mutex** | Имеет с sense dedicated lock-сервис, если уже есть Redis? |

Эти оси **не смешивать** в одном отчёте.

## Этапы

### Этап 1 — локальный mutex (приоритет)

Сравнить на одной машине, в условиях близких к PHP-FPM:

- **Glock** (`UnixSocketBackend`)
- **`flock`** — file lock между worker'ами

### Этап 2 — сетевой mutex

После появления `TCPBackend`:

- **Glock** (`TCPBackend`, localhost)
- **Redis** — `SET key token NX PX ttl` + безопасный unlock через Lua (compare-and-del)

### Этап 3 — изоляция overhead (Go)

Отдельные microbench'и сервера без PHP, чтобы понять, сколько latency добавляют PHP-клиент и протокол, а сколько — сам backend.

## Где работает mutex

| Backend | Область действия |
|---|---|
| `flock` | процессы на одной машине |
| Glock (unix / tcp) | процессы / worker'ы, при tcp — и между хостами |
| Redis | между машинами |

## Сценарии (обязательные)

Каждый сценарий прогонять для всех backend'ов этапа.

### S1 — Baseline без contention

- 1 worker, много разных ключей
- Показывает потолок throughput и чистый overhead протокола

### S2 — Hot lock (главный сценарий)

- N параллельных PHP-процессов (8–32), **один ключ**
- Именно здесь mutex реально болит; без этого сценария бенчмарк мало что скажет

### S3 — Many keys под нагрузкой

- N worker'ов, у каждого свой ключ
- Throughput при низкой конкуренции

### S4 — Disconnect cleanup

- Worker держит lock, соединение обрывается (kill / close socket)
- Измерить: через сколько ms lock освободился, не осталось ли «залипших» lock'ов
- Killer feature Glock; для Redis/flock сравнение особенно показательно

### S5 — Persistent vs per-request connection

- Одно соединение на worker vs connect на каждую операцию
- Для Glock и Redis — отдельно; для FPM обычно важен persistent connection

## Метрики

### Latency (главное)

Для каждого backend и сценария:

- **tryLock + unlock** — p50, p95, p99
- **lock + unlock** (блокирующий), когда lock уже свободен
- **lock под contention** — время ожидания в S2

Среднее менее важно, чем хвосты (p95/p99).

### Throughput

- ops/sec при фиксированном **hold time**
- Hold time задавать явно, иначе цифры несопоставимы:
  - **0 ms** — чистый overhead
  - **1–10 ms** — короткая критическая секция
  - **50–200 ms** — реалистичная бизнес-логика

### Поведение при contention

- число retry/spin в tryLock-сценарии
- задержка «пробуждения» после unlock в blocking lock
- **CPU usage** при ожидании (poll vs block vs Redis blocking primitives)

### Корректность

- нет залипших lock'ов после disconnect
- unlock только владельцем (secret / token)

## Правила честного сравнения

1. **Одна машина**, одна версия PHP, одинаковое число worker'ов.
2. **Warmup** перед замерами.
3. **Redis** — только безопасный паттерн (SET NX + Lua unlock), не «голый SET».
4. **Glock TCP vs Redis** — оба на localhost, persistent connection, одинаковый TTL.
5. Hold time и число worker'ов — в отчёте явно.
6. Несколько прогонов, медиана / перцентили, не один run.

## Что не гонять на POC-этапе

- microbench «lock/unlock в одном процессе без contention» как единственный тест
- cross-datacenter latency
- Redis Cluster vs single Glock (разные классы систем)
- ops/sec без указания hold time

## Матрица прогонов

```text
Этап 1 (локально):
  Backends: Glock UnixSocket | flock
  Scenarios: S1, S2, S3, S4, S5

Этап 2 (сеть, localhost):
  Backends: Glock TCP | Redis
  Scenarios: S1, S2, S3, S4, S5

Этап 3:
  Go microbench сервера (без PHP)
```

## Инструменты (текущие и планируемые)

Сейчас в репозитории:

- `php/load_test.php` — blocking lock, один процесс
- `php/load_test_try_lock.php` — tryLock с retry

Планируется:

- параллельный запуск N worker'ов (shell / make target)
- адаптеры для `flock`, Redis с тем же сценарным API
- `TCPBackend` в Go
- скрипт агрегации результатов (p50/p95/p99, ops/sec)

## Формат отчёта

Для каждого прогона фиксировать:

```text
date, git commit
backend, scenario (S1–S5)
workers, key strategy (hot / many)
hold_time, connection mode (persistent / per-request)
p50 / p95 / p99 latency (ms)
ops/sec
errors, stuck locks after disconnect
notes (PHP version, OS, Redis version, etc.)
```

## Порядок работ

1. Довести S2 + S4 для **Glock UnixSocket vs flock** — главный локальный вопрос.
2. Добавить **TCPBackend**, повторить матрицу против **Redis**.
3. Go microbench — когда нужно отделить server overhead от PHP client.

## Связанные файлы

- [README](../README.md) — описание POC
- `php/load_test.php`, `php/load_test_try_lock.php` — текущие нагрузочные скрипты
