# Glock

[Russian](README.md)

Proof-of-concept for a distributed locking service aimed primarily at PHP. It provides a mutex mechanism for PHP applications over a Unix socket.

## Features

- **Automatic lock release** — when a client disconnects, all locks held by that connection are released.
- **Blocking and non-blocking locks** — `lock()` waits until the lock is available; `tryLock()` returns immediately.
- **Namespace isolation** — locks are scoped to a namespace; the same key in different namespaces does not conflict.

## POC limitations

This is **not the final version** of the POC. The following is intentionally out of scope or still in progress:

- **Distribution** — the service runs as a single process; coordination across multiple instances is not implemented.
- **Sessions and fault tolerance** — session handling and failure behavior are still under development.
- **PHP client** — `GlockClient.php` is a work in progress and not the final version.
- **Project layout** — package and file organization may change.

## Layout

```
cmd/glock/          — server (Go)
internal/           — locking logic, namespaces, protocol
php/GlockClient.php — PHP client
php/load_test*.php  — load-test scripts
```

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
    // critical section
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
