<?php

declare(strict_types=1);

namespace BabelQueue\Transport\Internal;

use BabelQueue\Exceptions\BabelQueueException;

/**
 * A minimal RFC 6455 client connection (handshake, masked text frames, ping/pong, close) over
 * `stream_socket_client`, just enough for the Pulsar WebSocket producer and consumer endpoints. Pure userland, no
 * extension (GR-7). Not part of the public API.
 *
 * @internal
 */
final class WebSocketConnection
{
    private const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

    /** @param resource $stream */
    private function __construct(private $stream)
    {
    }

    /**
     * @return array{secure: bool, host: string, port: int, path: string}|null null when it is not a ws / wss / http / https URL
     */
    public static function parseService(string $url): ?array
    {
        $parts = parse_url($url);
        if ($parts === false || ! isset($parts['scheme'], $parts['host'])) {
            return null;
        }

        $scheme = strtolower($parts['scheme']);
        if (! in_array($scheme, ['ws', 'wss', 'http', 'https'], true)) {
            return null;
        }

        $secure = $scheme === 'wss' || $scheme === 'https';

        return [
            'secure' => $secure,
            'host' => $parts['host'],
            'port' => $parts['port'] ?? ($secure ? 443 : 80),
            'path' => rtrim($parts['path'] ?? '', '/'),
        ];
    }

    /**
     * Connect and upgrade. `$path` is the request target, query string included.
     *
     * @throws BabelQueueException when the connection or the upgrade fails
     */
    public static function open(string $serviceUrl, string $path, ?string $authToken, float $timeoutSeconds): self
    {
        $service = self::parseService($serviceUrl);
        if ($service === null) {
            throw new \InvalidArgumentException(sprintf('Not a Pulsar WebSocket service URL: [%s].', $serviceUrl));
        }

        $address = sprintf('%s://%s:%d', $service['secure'] ? 'ssl' : 'tcp', $service['host'], $service['port']);
        $stream = @stream_socket_client($address, $errno, $errstr, $timeoutSeconds, STREAM_CLIENT_CONNECT);
        if ($stream === false) {
            throw new BabelQueueException(sprintf('Cannot connect to the Pulsar WebSocket service %s: %s (%d)', $address, $errstr, $errno));
        }

        $seconds = (int) floor($timeoutSeconds);
        stream_set_timeout($stream, $seconds, (int) (($timeoutSeconds - $seconds) * 1_000_000));
        $connection = new self($stream);

        try {
            $key = base64_encode(random_bytes(16));
            $request = sprintf("GET %s HTTP/1.1\r\nHost: %s:%d\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n", $path, $service['host'], $service['port'], $key);
            if ($authToken !== null && $authToken !== '') {
                $request .= 'Authorization: Bearer ' . $authToken . "\r\n";
            }
            $connection->write($request . "\r\n");

            $head = '';
            while (! str_contains($head, "\r\n\r\n")) {
                $byte = fread($stream, 1); // byte-wise: nothing past the header block is consumed
                if ($byte === false || $byte === '') {
                    throw new BabelQueueException('The Pulsar WebSocket handshake was cut off: ' . trim($head));
                }
                $head .= $byte;
                if (strlen($head) > 16384) {
                    throw new BabelQueueException('The Pulsar WebSocket handshake response is too large.');
                }
            }

            $status = strstr($head, "\r\n", true);
            if ($status === false || ! str_contains($status, ' 101')) {
                throw new BabelQueueException('The Pulsar WebSocket upgrade was refused: ' . ($status === false ? 'no status line' : $status));
            }

            $expected = base64_encode(sha1($key . self::GUID, true));
            if (preg_match('/^Sec-WebSocket-Accept:\s*(\S+)\s*$/im', $head, $m) !== 1 || ! hash_equals($expected, $m[1])) {
                throw new BabelQueueException('The Pulsar WebSocket handshake returned a wrong Sec-WebSocket-Accept.');
            }
        } catch (\Throwable $e) {
            $connection->close();

            throw $e;
        }

        return $connection;
    }

    public function sendText(string $data): void
    {
        $this->send(0x1, $data);
    }

    /** Best effort: a close that fails changes nothing. */
    public function sendClose(): void
    {
        try {
            $this->send(0x8, pack('n', 1000));
        } catch (BabelQueueException) {
        }
    }

    public function close(): void
    {
        if (is_resource($this->stream)) {
            fclose($this->stream);
        }
    }

    /**
     * Read one complete (possibly fragmented) data message; answers pings, fails on a close frame.
     *
     * @param  float|null  $waitSeconds  null = wait for the connection's own timeout (an error); a number = give up
     *         quietly after that long when NOTHING has arrived and return null (a frame that has started is read whole)
     *
     * @throws BabelQueueException on a close, a timeout, or a broken connection
     */
    public function readMessage(?float $waitSeconds = null): ?string
    {
        if ($waitSeconds !== null && ! $this->hasData($waitSeconds)) {
            return null;
        }

        $message = '';
        while (true) {
            $head = $this->readExact(2);
            $fin = (ord($head[0]) & 0x80) !== 0;
            $opcode = ord($head[0]) & 0x0F;
            $length = ord($head[1]) & 0x7F;
            if ($length === 126) {
                $length = self::unpackInt('n', $this->readExact(2));
            } elseif ($length === 127) {
                $length = self::unpackInt('J', $this->readExact(8));
            }
            if ($length > 16_777_216) {
                throw new BabelQueueException('A Pulsar WebSocket message is unreasonably large.');
            }
            $mask = (ord($head[1]) & 0x80) !== 0 ? $this->readExact(4) : null;
            $data = $length > 0 ? $this->readExact($length) : '';
            if ($mask !== null && $length > 0) {
                $data ^= str_pad($mask, $length, $mask);
            }

            if ($opcode === 0x8) {
                throw new BabelQueueException('The Pulsar WebSocket service closed the connection.');
            }
            if ($opcode === 0x9) {
                $this->send(0xA, $data);

                continue;
            }
            if ($opcode === 0xA) {
                continue;
            }

            $message .= $data;
            if ($fin) {
                return $message;
            }
        }
    }

    /** True when a byte can be read now or within `$seconds`; bytes already buffered by PHP count. */
    private function hasData(float $seconds): bool
    {
        $meta = stream_get_meta_data($this->stream);
        if ($meta['unread_bytes'] > 0) {
            return true;
        }

        $read = [$this->stream];
        $write = $except = null;
        $whole = (int) floor($seconds);
        $ready = @stream_select($read, $write, $except, $whole, (int) (($seconds - $whole) * 1_000_000));
        if ($ready === false) {
            throw new BabelQueueException('Waiting on the Pulsar WebSocket connection failed.');
        }

        return $ready > 0;
    }

    private function send(int $opcode, string $data): void
    {
        $length = strlen($data);
        $header = chr(0x80 | ($opcode & 0x0F));
        if ($length < 126) {
            $header .= chr(0x80 | $length);
        } elseif ($length < 65536) {
            $header .= chr(0x80 | 126) . pack('n', $length);
        } else {
            $header .= chr(0x80 | 127) . pack('J', $length);
        }

        $mask = random_bytes(4);
        $this->write($header . $mask . ($data ^ str_pad($mask, $length, $mask)));
    }

    private function write(string $bytes): void
    {
        $total = strlen($bytes);
        for ($written = 0; $written < $total;) {
            $n = @fwrite($this->stream, substr($bytes, $written));
            if ($n === false || $n === 0) {
                throw new BabelQueueException('Writing to the Pulsar WebSocket service failed.');
            }
            $written += $n;
        }
    }

    private function readExact(int $length): string
    {
        $buffer = '';
        while (strlen($buffer) < $length) {
            $chunk = fread($this->stream, max(1, $length - strlen($buffer)));
            if ($chunk === false || $chunk === '') {
                $meta = stream_get_meta_data($this->stream);
                throw new BabelQueueException($meta['timed_out'] ? 'Timed out waiting on the Pulsar WebSocket service.' : 'The Pulsar WebSocket connection closed unexpectedly.');
            }
            $buffer .= $chunk;
        }

        return $buffer;
    }

    private static function unpackInt(string $format, string $bytes): int
    {
        $unpacked = unpack($format, $bytes);

        return is_array($unpacked) ? (int) $unpacked[1] : 0;
    }
}
