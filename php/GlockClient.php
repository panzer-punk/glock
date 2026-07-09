<?php

declare(strict_types=1);

final class GlockClient
{
    public const PROTO_VERSION = 1;
    public const HEADER_SIZE = 6;

    public const PACKET_LOCK = 0;
    public const PACKET_TRY_LOCK = 1;
    public const PACKET_UNLOCK = 2;
    public const PACKET_ERROR = 3;
    public const PACKET_SUCCESS = 4;

    public const BLOCK_NAMESPACE = 0;
    public const BLOCK_KEY = 1;
    public const BLOCK_TTL = 3;
    public const BLOCK_SECRET = 4;
    public const BLOCK_SUCCESS = 5;
    public const BLOCK_ERROR = 6;

    private $socket = null;

    public function __construct(private readonly string $socketPath)
    {
    }

    public function connect(): void
    {
        if (is_resource($this->socket)) {
            return;
        }

        $uri = 'unix://' . $this->socketPath;
        $socket = @stream_socket_client($uri, $errno, $errstr);

        if ($socket === false) {
            throw new RuntimeException("connect failed: {$errstr} ({$errno})");
        }

        stream_set_timeout($socket, 5);
        $this->socket = $socket;
    }

    public function close(): void
    {
        if (is_resource($this->socket)) {
            fclose($this->socket);
        }

        $this->socket = null;
    }

    public function lock(string $namespace, string $key): string
    {
        $response = $this->request(self::PACKET_LOCK, [
            [self::BLOCK_NAMESPACE, $namespace],
            [self::BLOCK_KEY, $key],
        ]);

        $this->expectSuccess($response);

        return $response['blocks'][self::BLOCK_SECRET] ?? '';
    }

    public function lockUntilSuccess(string $namespace, string $key, int $retryDelayUs = 1000): string
    {
        while (true) {
            [$secret, $acquired] = $this->tryLockWithSecret($namespace, $key);
            if ($acquired) {
                return $secret;
            }

            usleep($retryDelayUs);
        }
    }

    public function tryLock(string $namespace, string $key, ?int $ttlNs = null): bool
    {
        [, $acquired] = $this->tryLockWithSecret($namespace, $key, $ttlNs);

        return $acquired;
    }

    /**
     * @return array{0: string, 1: bool} [secret, acquired]
     */
    public function tryLockWithSecret(string $namespace, string $key, ?int $ttlNs = null): array
    {
        $blocks = [
            [self::BLOCK_NAMESPACE, $namespace],
            [self::BLOCK_KEY, $key],
        ];

        if ($ttlNs !== null) {
            $blocks[] = [self::BLOCK_TTL, pack('J', $ttlNs)];
        }

        $response = $this->request(self::PACKET_TRY_LOCK, $blocks);

        if ($response['type'] === self::PACKET_ERROR) {
            throw new RuntimeException($this->errorMessage($response));
        }

        if ($response['type'] !== self::PACKET_SUCCESS) {
            throw new RuntimeException('unexpected packet type: ' . $response['type']);
        }

        $acquired = !array_key_exists(self::BLOCK_SUCCESS, $response['blocks'])
            || ord($response['blocks'][self::BLOCK_SUCCESS]) === 1;

        return [
            $response['blocks'][self::BLOCK_SECRET] ?? '',
            $acquired,
        ];
    }

    public function unlock(string $namespace, string $key, string $secret): void
    {
        $response = $this->request(self::PACKET_UNLOCK, [
            [self::BLOCK_NAMESPACE, $namespace],
            [self::BLOCK_KEY, $key],
            [self::BLOCK_SECRET, $secret],
        ]);

        $this->expectSuccess($response);
    }

    private function request(int $type, array $blocks): array
    {
        if (!is_resource($this->socket)) {
            throw new RuntimeException('not connected');
        }

        $packet = $this->buildPacket($type, $blocks);
        $written = fwrite($this->socket, $packet);
        if ($written === false || $written !== strlen($packet)) {
            throw new RuntimeException('failed to send packet');
        }

        return $this->readPacket();
    }

    private function readPacket(): array
    {
        $header = $this->readExact(self::HEADER_SIZE);
        $payloadLength = unpack('N', substr($header, 2, 4))[1];
        $payload = $payloadLength > 0 ? $this->readExact($payloadLength) : '';

        return $this->parsePacket($header . $payload);
    }

    private function readExact(int $length): string
    {
        if (!is_resource($this->socket)) {
            throw new RuntimeException('not connected');
        }

        $data = '';

        while (strlen($data) < $length) {
            $chunk = fread($this->socket, $length - strlen($data));
            if ($chunk === false || $chunk === '') {
                throw new RuntimeException('connection closed while reading');
            }

            $data .= $chunk;
        }

        return $data;
    }

    private function expectSuccess(array $response): void
    {
        if ($response['type'] === self::PACKET_ERROR) {
            throw new RuntimeException($this->errorMessage($response));
        }

        if ($response['type'] !== self::PACKET_SUCCESS) {
            throw new RuntimeException('unexpected packet type: ' . $response['type']);
        }
    }

    private function errorMessage(array $response): string
    {
        if (array_key_exists(self::BLOCK_ERROR, $response['blocks'])) {
            return $response['blocks'][self::BLOCK_ERROR];
        }

        return 'unknown error';
    }

    private function buildPacket(int $type, array $blocks): string
    {
        $payload = '';

        foreach ($blocks as [$blockType, $value]) {
            $payload .= pack('Cn', $blockType, strlen($value)) . $value;
        }

        return pack('CCN', self::PROTO_VERSION, $type, strlen($payload)) . $payload;
    }

    private function parsePacket(string $data): array
    {
        if (strlen($data) < self::HEADER_SIZE) {
            throw new RuntimeException('invalid packet');
        }

        $version = ord($data[0]);
        $type = ord($data[1]);
        $payloadLength = unpack('N', substr($data, 2, 4))[1];
        $blocks = [];
        $offset = self::HEADER_SIZE;
        $end = self::HEADER_SIZE + $payloadLength;

        while ($offset < $end) {
            if ($offset + 3 > $end) {
                throw new RuntimeException('invalid packet block header');
            }

            $blockType = ord($data[$offset]);
            $blockLength = unpack('n', substr($data, $offset + 1, 2))[1];
            $offset += 3;

            if ($offset + $blockLength > $end) {
                throw new RuntimeException('invalid packet block payload');
            }

            $blocks[$blockType] = substr($data, $offset, $blockLength);
            $offset += $blockLength;
        }

        return [
            'version' => $version,
            'type' => $type,
            'blocks' => $blocks,
        ];
    }
}
