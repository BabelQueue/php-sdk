<?php

declare(strict_types=1);

/**
 * A throwaway loopback stand-in for the Pulsar WebSocket service, used by the PulsarWebSocketProducer /
 * PulsarWebSocketSubscription tests (our own test code; it only ever listens on 127.0.0.1).
 *
 * usage: php pulsar_ws_server.php <recordFile> <mode> [<subscriptionType> [<seedFile>]]
 *   mode (producer connections): ok | reject | ping_fragment | refuse | bad_accept | close
 *   subscriptionType: how the mini broker redelivers a nack: Exclusive / Failover keep redeliveryCount, Shared / Key_Shared add 1
 *   seedFile: JSON lines {"payload": "...", "properties": {...}} delivered to the first consumer connection
 *
 * It prints the port on its first stdout line and serves any number of connections at once until killed.
 *  - producer connection (/ws/v2/producer/...): each frame is appended to <recordFile> as {path, authorization, frame},
 *    answered per mode, and (mode ok) delivered to the consumer connection like a real topic would.
 *  - consumer connection (/ws/v2/consumer/...): its request line and every ack / nack frame are appended to
 *    <recordFile>.consumer as {event: open|ack|nack, path|messageId, authorization}.
 */

[$script, $recordFile, $mode, $subscriptionType, $seedFile] = $argv + [null, null, 'ok', 'Exclusive', null];
$consumerFile = $recordFile . '.consumer';
$server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
if ($server === false) {
    fwrite(STDERR, "listen failed: $errstr\n");
    exit(1);
}
fwrite(STDOUT, explode(':', stream_socket_get_name($server, false))[1] . "\n");
fflush(STDOUT);

/** @var array<int, array{sock: resource, buf: string, open: bool, kind: string, auth: ?string, path: string}> $conns */
$conns = [];
/** @var list<array{messageId: string, payload: string, properties: array, redeliveryCount: int}> $pending */
$pending = [];
/** @var array<string, array> $inflight */
$inflight = [];
$consumerId = null;
$counter = 1;

function frameBytes(int $opcode, string $data, bool $fin = true): string
{
    $len = strlen($data);

    return chr(($fin ? 0x80 : 0) | $opcode) . ($len < 126 ? chr($len) : chr(126) . pack('n', $len)) . $data;
}

/** @return array{0: int, 1: string, 2: int}|null opcode, payload, consumed bytes */
function parseFrame(string $buf): ?array
{
    if (strlen($buf) < 2) {
        return null;
    }
    $len = ord($buf[1]) & 0x7F;
    $offset = 2;
    if ($len === 126) {
        if (strlen($buf) < 4) {
            return null;
        }
        $len = unpack('n', substr($buf, 2, 2))[1];
        $offset = 4;
    } elseif ($len === 127) {
        if (strlen($buf) < 10) {
            return null;
        }
        $len = unpack('J', substr($buf, 2, 8))[1];
        $offset = 10;
    }
    $masked = (ord($buf[1]) & 0x80) !== 0;
    $mask = '';
    if ($masked) {
        if (strlen($buf) < $offset + 4) {
            return null;
        }
        $mask = substr($buf, $offset, 4);
        $offset += 4;
    }
    if (strlen($buf) < $offset + $len) {
        return null;
    }
    $data = substr($buf, $offset, $len);
    if ($masked && $len > 0) {
        $data ^= str_pad($mask, $len, $mask);
    }

    return [ord($buf[0]) & 0x0F, $data, $offset + $len];
}

function deliver(array $msg, array &$conns, ?int $consumerId, array &$inflight): bool
{
    if ($consumerId === null || ! isset($conns[$consumerId])) {
        return false;
    }
    $inflight[$msg['messageId']] = $msg;
    fwrite($conns[$consumerId]['sock'], frameBytes(0x1, json_encode([
        'messageId' => $msg['messageId'],
        'payload' => base64_encode($msg['payload']),
        'properties' => (object) $msg['properties'],
        'publishTime' => '2026-10-25T00:00:00Z',
        'redeliveryCount' => $msg['redeliveryCount'],
    ])));

    return true;
}

while (true) {
    $read = [$server];
    foreach ($conns as $c) {
        $read[] = $c['sock'];
    }
    $w = $e = null;
    if (@stream_select($read, $w, $e, 3600) === false) {
        break;
    }

    foreach ($read as $sock) {
        if ($sock === $server) {
            $new = @stream_socket_accept($server, 0);
            if ($new !== false) {
                $conns[(int) $new] = ['sock' => $new, 'buf' => '', 'open' => false, 'kind' => '', 'auth' => null, 'path' => ''];
            }
            continue;
        }

        $id = (int) $sock;
        $chunk = fread($sock, 65536);
        if ($chunk === false || $chunk === '') {
            fclose($sock);
            unset($conns[$id]);
            if ($consumerId === $id) {
                $consumerId = null;
                foreach ($inflight as $m) { // unacknowledged messages go back to the topic
                    array_unshift($pending, $m);
                }
                $inflight = [];
            }
            continue;
        }
        $conns[$id]['buf'] .= $chunk;

        if (! $conns[$id]['open']) {
            $buf = $conns[$id]['buf'];
            if (! str_contains($buf, "\r\n\r\n")) {
                continue;
            }
            [$head, $rest] = explode("\r\n\r\n", $buf, 2);
            preg_match('#^GET (\S+) HTTP/1.1#', $head, $p);
            preg_match('/^Sec-WebSocket-Key:\s*(\S+)/mi', $head, $k);
            $auth = preg_match('/^Authorization:\s*(.+?)\s*$/mi', $head, $a) ? $a[1] : null;
            $conns[$id]['buf'] = $rest;
            $conns[$id]['auth'] = $auth;
            $conns[$id]['path'] = $p[1];
            $isConsumer = str_starts_with($p[1], '/ws/v2/consumer/') || str_contains($p[1], '/ws/v2/consumer/');

            if (! $isConsumer && $mode === 'refuse') {
                fwrite($sock, "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n");
                fclose($sock);
                unset($conns[$id]);
                continue;
            }
            $accept = (! $isConsumer && $mode === 'bad_accept') ? 'AAAA' : base64_encode(sha1($k[1] . '258EAFA5-E914-47DA-95CA-C5AB0DC85B11', true));
            fwrite($sock, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: $accept\r\n\r\n");
            $conns[$id]['open'] = true;
            $conns[$id]['kind'] = $isConsumer ? 'consumer' : 'producer';

            if ($isConsumer) {
                file_put_contents($consumerFile, json_encode(['event' => 'open', 'path' => $p[1], 'authorization' => $auth]) . "\n", FILE_APPEND);
                $consumerId = $id;
                if ($seedFile !== null && is_file($seedFile)) {
                    foreach (array_filter(explode("\n", (string) file_get_contents($seedFile))) as $line) {
                        $seed = json_decode($line, true);
                        $pending[] = ['messageId' => base64_encode('id-' . $counter++), 'payload' => $seed['payload'], 'properties' => $seed['properties'] ?? [], 'redeliveryCount' => 0];
                    }
                    $seedFile = null;
                }
                while ($pending !== []) {
                    deliver(array_shift($pending), $conns, $consumerId, $inflight);
                }
            }
        }

        while (($frame = parseFrame($conns[$id]['buf'])) !== null) {
            [$opcode, $data, $used] = $frame;
            $conns[$id]['buf'] = substr($conns[$id]['buf'], $used);

            if ($opcode === 0x8) {
                fclose($sock);
                unset($conns[$id]);
                if ($consumerId === $id) {
                    $consumerId = null;
                }
                break;
            }
            if ($opcode !== 0x1) {
                continue;
            }

            if ($conns[$id]['kind'] === 'consumer') {
                $msg = json_decode($data, true);
                $isNack = ($msg['type'] ?? '') === 'negativeAcknowledge';
                file_put_contents($consumerFile, json_encode(['event' => $isNack ? 'nack' : 'ack', 'messageId' => $msg['messageId'] ?? null]) . "\n", FILE_APPEND);
                $held = $inflight[$msg['messageId']] ?? null;
                unset($inflight[$msg['messageId'] ?? '']);
                if ($isNack && $held !== null) {
                    if (in_array($subscriptionType, ['Shared', 'Key_Shared'], true)) {
                        $held['redeliveryCount']++;
                    }
                    deliver($held, $conns, $consumerId, $inflight) || $pending[] = $held;
                }
                continue;
            }

            // producer connection
            $decoded = json_decode($data, true);
            file_put_contents($recordFile, json_encode(['path' => $conns[$id]['path'], 'authorization' => $conns[$id]['auth'], 'frame' => $decoded]) . "\n", FILE_APPEND);
            $context = $decoded['context'] ?? '';

            if ($mode === 'close') {
                fwrite($sock, frameBytes(0x8, pack('n', 1011)));
                fclose($sock);
                unset($conns[$id]);
                break;
            }
            $reply = $mode === 'reject'
                ? json_encode(['result' => 'send-error:3', 'errorMsg' => 'Topic not found', 'context' => $context])
                : json_encode(['result' => 'ok', 'messageId' => 'CAAQAA==', 'context' => $context]);
            if ($mode === 'ping_fragment') {
                fwrite($sock, frameBytes(0x9, 'hb'));
                $half = intdiv(strlen($reply), 2);
                fwrite($sock, frameBytes(0x1, substr($reply, 0, $half), false) . frameBytes(0x0, substr($reply, $half)));
            } else {
                fwrite($sock, frameBytes(0x1, $reply));
            }

            if ($mode !== 'reject') {
                $new = [
                    'messageId' => base64_encode('id-' . $counter++),
                    'payload' => (string) base64_decode($decoded['payload'], true),
                    'properties' => $decoded['properties'] ?? [],
                    'redeliveryCount' => 0,
                ];
                deliver($new, $conns, $consumerId, $inflight) || $pending[] = $new;
            }
        }
    }
}
