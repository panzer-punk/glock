# Glock

[Russian](README.md)

Proof-of-concept lock service aimed primarily at PHP in the classic **php-fpm** and **php-cli** setup: a mutex between processes over a Unix socket on one host.

## Features

- **Sessions** — each connection to the server is a separate session; non-TTL locks (`lock()`) are tied to it and released automatically when the connection closes.
- **Blocking and non-blocking locks** — `lock()` waits until the lock is available; `tryLock()` returns immediately.
- **TTL** — locks with TTL (`tryLock()`) are not bound to the session: they are not released on disconnect and live until the TTL expires or an explicit `unlock()`.
- **Namespace isolation** — locks are scoped to a namespace; the same key in different namespaces does not conflict.
- **Secret** — a successful lock returns a `secret`: pass it to `unlock()`, and you can also use it in the application as a fencing token (for example, send it to storage with a write so stale operations from a previous holder are rejected). It is currently a UUID; a monotonic fencing token is still in the TODO.

## POC limitations

This is **not the final version** of the POC. The following is intentionally out of scope or still in progress:

- **Distribution** — the service runs as a single process; coordination across multiple instances is not implemented.
- **Fault tolerance** — server failure behavior and lock recovery are still under development.
- **PHP client** — `GlockClient.php` is a work in progress and not the final version.
- **Project layout** — package and file organization may change.
- **Connection multiplexing** — a connection handles one in-flight request at a time (`write`, then `read`). A second frame while the first is still in `Handle` drops the connection. Multiplexing with a stream/request id (HTTP/2-style) is out of POC scope.

## TODO

- [ ] **Namespace API** — create namespaces on demand or via a dedicated API; currently only `default` exists at runtime
- [x] **Go tests**
- [ ] **Basic fault tolerance** — server failure behavior and lock recovery
- [ ] **Persistence** — persist namespaces and TTL locks so they survive a server restart
- [ ] **Monotonic fencing token** — add a `SecretFactory` that issues a monotonically increasing fencing token instead of a UUID
- [ ] **Architecture and project structure refactoring**
- [ ] **Logging**
- [ ] **Observability** — metrics
- [ ] **Benchmarks**
- [ ] **Optimization**
- [ ] **TCPBackend**
- [ ] **PHP client as a Composer package**
- [ ] **Distribution** — coordination across multiple instances (approach not decided yet)

## Layout

```
cmd/glock/          — server (Go)
internal/           — locking logic, namespaces, protocol
php/GlockClient.php — PHP client
php/load_test*.php  — load-test scripts
docs/benchmark.md   — comparative benchmark plan
```

## Benchmarks

Plan for comparing Glock with `flock` and Redis — [docs/benchmark.en.md](docs/benchmark.en.md) ([Russian](docs/benchmark.md)).

## Quick start

Start the server:

```bash
go run ./cmd/glock
```

PHP usage example:

```php
require 'php/GlockClient.php';

$client = new GlockClient('/tmp/glock_test.sock');
$client->connect();

$secret = $client->lock('default', 'my-resource');
try {
    // critical section; $secret can be used as a fencing token
} finally {
    $client->unlock('default', 'my-resource', $secret);
    $client->close();
}
```

Non-blocking attempt:

```php
if ($client->tryLock('default', 'my-resource')) {
    // lock acquired
}
```

## License

[MIT](LICENSE)
