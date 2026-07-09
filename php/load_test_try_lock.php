#!/usr/bin/env php
<?php

declare(strict_types=1);

require __DIR__ . '/GlockClient.php';

$socketPath = '/tmp/glock_test.sock';
$namespace = 'default';
$key = 'load-test';
$iterations = 3;
$holdMinDelayUs = 1000;
$holdMaxDelayUs = 10000;
$idleMinDelayUs = 0;
$idleMaxDelayUs = 0;
$retryMinDelayUs = 1000;
$retryMaxDelayUs = 10000;

foreach (array_slice($argv, 1) as $arg) {
    if (str_starts_with($arg, '--socket=')) {
        $socketPath = substr($arg, 9);
        continue;
    }

    if (str_starts_with($arg, '--namespace=')) {
        $namespace = substr($arg, 12);
        continue;
    }

    if (str_starts_with($arg, '--key=')) {
        $key = substr($arg, 6);
        continue;
    }

    if (str_starts_with($arg, '--iterations=')) {
        $iterations = (int) substr($arg, 13);
        continue;
    }

    if (str_starts_with($arg, '--hold-min-us=')) {
        $holdMinDelayUs = (int) substr($arg, 14);
        continue;
    }

    if (str_starts_with($arg, '--hold-max-us=')) {
        $holdMaxDelayUs = (int) substr($arg, 14);
        continue;
    }

    if (str_starts_with($arg, '--idle-min-us=')) {
        $idleMinDelayUs = (int) substr($arg, 14);
        continue;
    }

    if (str_starts_with($arg, '--idle-max-us=')) {
        $idleMaxDelayUs = (int) substr($arg, 14);
        continue;
    }

    if (str_starts_with($arg, '--retry-min-us=')) {
        $retryMinDelayUs = (int) substr($arg, 15);
        continue;
    }

    if (str_starts_with($arg, '--retry-max-us=')) {
        $retryMaxDelayUs = (int) substr($arg, 15);
        continue;
    }
}

$client = new GlockClient($socketPath);
$client->connect();

$stats = [
    'ok' => 0,
    'fail' => 0,
    'retries' => 0,
];

$startedAt = hrtime(true);

for ($i = 0; $i < $iterations; $i++) {
    try {
        $secret = acquireTryLock($client, $namespace, $key, $retryMinDelayUs, $retryMaxDelayUs, $stats);

        if ($holdMaxDelayUs > 0) {
            usleep(random_int($holdMinDelayUs, $holdMaxDelayUs));
        }

        $client->unlock($namespace, $key, $secret);
        $stats['ok']++;
    } catch (Throwable $e) {
        $stats['fail']++;
        if ($stats['fail'] <= 5) {
            fwrite(STDERR, sprintf("iteration %d failed: %s\n", $i + 1, $e->getMessage()));
        }
    }

    if ($idleMaxDelayUs > 0) {
        usleep(random_int($idleMinDelayUs, $idleMaxDelayUs));
    }
}

$client->close();

$elapsedSec = (hrtime(true) - $startedAt) / 1_000_000_000;

printf(
    "done: ok=%d fail=%d retries=%d elapsed=%.3fs avg=%.3fms/op\n",
    $stats['ok'],
    $stats['fail'],
    $stats['retries'],
    $elapsedSec,
    ($elapsedSec * 1000) / max($iterations, 1)
);

exit($stats['fail'] > 0 ? 1 : 0);

/**
 * @param array{ok: int, fail: int, retries: int} $stats
 */
function acquireTryLock(
    GlockClient $client,
    string $namespace,
    string $key,
    int $retryMinDelayUs,
    int $retryMaxDelayUs,
    array &$stats,
): string {
    while (true) {
        try {
            [$secret, $acquired] = $client->tryLockWithSecret($namespace, $key);
            if ($acquired) {
                return $secret;
            }
        } catch (RuntimeException $e) {
            if ($e->getMessage() !== 'Failed to lock') {
                throw $e;
            }
        }

        $stats['retries']++;
        usleep(random_int($retryMinDelayUs, $retryMaxDelayUs));
    }
}
