# Glock benchmark plan

[Russian](benchmark.md)

This document captures the comparative testing plan for the POC. The goal is to answer two distinct questions—not to find the “fastest backend” in isolation.

## Questions we are trying to answer

| Axis | Question |
|---|---|
| **Local mutex** (single host) | Do we need a dedicated daemon, or are `flock` / in-process locks enough? |
| **Network mutex** | Does a dedicated lock service make sense when Redis is already available? |

Do **not** mix these axes in a single report.

## Phases

### Phase 1 — local mutex (priority)

Compare on one machine, under conditions close to PHP-FPM:

- **Glock** (`UnixSocketBackend`)
- **`flock`** — file lock across workers
- **`APCu`** *(optional)* — single-process PHP only (CLI daemon, long-running worker). **Not a mutex across FPM workers.**

### Phase 2 — network mutex

After `TCPBackend` lands:

- **Glock** (`TCPBackend`, localhost)
- **Redis** — `SET key token NX PX ttl` + safe unlock via Lua (compare-and-del)

### Phase 3 — overhead isolation (Go)

Separate server microbenchmarks without PHP, to see how much latency comes from the PHP client/protocol vs the backend itself.

## Mutex scope by backend

| Backend | Scope |
|---|---|
| `flock` | processes on one machine |
| Glock (unix / tcp) | workers / processes; with tcp, across hosts |
| Redis | across machines |
| APCu | inside a single PHP process only |

## Scenarios (required)

Run every scenario for all backends in the phase.

### S1 — Baseline, no contention

- 1 worker, many distinct keys
- Shows throughput ceiling and pure protocol overhead

### S2 — Hot lock (primary scenario)

- N parallel PHP processes (8–32), **one key**
- This is where mutex pain shows up; benchmarks without it are mostly noise

### S3 — Many keys under load

- N workers, each with its own key
- Throughput at low contention

### S4 — Disconnect cleanup

- Worker holds a lock, connection drops (kill / close socket)
- Measure: time until lock is released, any stuck locks
- Glock’s killer feature; especially telling vs Redis/flock

### S5 — Persistent vs per-request connection

- One connection per worker vs connect per operation
- For Glock and Redis separately; persistent connections matter for FPM

## Metrics

### Latency (most important)

Per backend and scenario:

- **tryLock + unlock** — p50, p95, p99
- **lock + unlock** (blocking) when the lock is already free
- **lock under contention** — wait time in S2

Tail latency (p95/p99) matters more than the mean.

### Throughput

- ops/sec at a fixed **hold time**
- Hold time must be explicit or numbers are incomparable:
  - **0 ms** — pure overhead
  - **1–10 ms** — short critical section
  - **50–200 ms** — realistic business logic

### Contention behavior

- retry/spin count in tryLock scenarios
- wake latency after unlock in blocking lock
- **CPU usage** while waiting (poll vs block vs Redis blocking primitives)

### Correctness

- no stuck locks after disconnect
- unlock only by owner (secret / token)

## Fair comparison rules

1. **Same machine**, same PHP version, same worker count.
2. **Warmup** before measurements.
3. **Redis** — safe pattern only (SET NX + Lua unlock), not a bare SET.
4. **Glock TCP vs Redis** — both on localhost, persistent connection, same TTL.
5. Hold time and worker count documented in the report.
6. Multiple runs; report median / percentiles, not a single run.

## Out of scope for the POC stage

- microbench “lock/unlock in one process, no contention” as the only test
- cross-datacenter latency
- Redis Cluster vs single Glock (different system classes)
- ops/sec without stating hold time

## Run matrix

```text
Phase 1 (local):
  Backends: Glock UnixSocket | flock | APCu*
  Scenarios: S1, S2, S3, S4, S5

Phase 2 (network, localhost):
  Backends: Glock TCP | Redis
  Scenarios: S1, S2, S3, S4, S5

Phase 3:
  Go server microbenchmarks (no PHP)

* APCu — only if there is an explicit single-process PHP use case
```

## Tooling (current and planned)

In the repo today:

- `php/load_test.php` — blocking lock, single process
- `php/load_test_try_lock.php` — tryLock with retry

Planned:

- parallel launch of N workers (shell / make target)
- adapters for `flock`, Redis, APCu with the same scenario API
- `TCPBackend` in Go
- result aggregation script (p50/p95/p99, ops/sec)

## Report format

For each run, record:

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

## Work order

1. Finish S2 + S4 for **Glock UnixSocket vs flock** — the main local question.
2. Add **TCPBackend**, rerun the matrix against **Redis**.
3. **APCu** — only for an explicit single-process use case.
4. Go microbench — when server overhead needs to be separated from the PHP client.

## Related files

- [README](../README.en.md) — POC overview
- `php/load_test.php`, `php/load_test_try_lock.php` — current load-test scripts
