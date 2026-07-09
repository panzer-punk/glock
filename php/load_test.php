#!/usr/bin/env php
<?php

declare(strict_types=1);

require __DIR__ . '/GlockClient.php';

$socketPath = '/tmp/glock_test.sock';
$namespace = 'default';
$key = 'load-test';
$iterations = 1000;
$holdMinDelayUs = 1000;
$holdMaxDelayUs = 10000;
$idleMinDelayUs = 0;
$idleMaxDelayUs = 0;

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
}

$client = new GlockClient($socketPath);
$client->connect();

$stats = [
    'ok' => 0,
    'fail' => 0,
];

$startedAt = hrtime(true);

for ($i = 0; $i < $iterations; $i++) {
    try {
        $secret = $client->lock($namespace, $key);

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
    "done: ok=%d fail=%d elapsed=%.3fs avg=%.3fms/op\n",
    $stats['ok'],
    $stats['fail'],
    $elapsedSec,
    ($elapsedSec * 1000) / max($iterations, 1)
);

exit($stats['fail'] > 0 ? 1 : 0);
