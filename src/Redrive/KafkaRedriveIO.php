<?php

declare(strict_types=1);

namespace BabelQueue\Redrive;

use BabelQueue\Transport\KafkaProduce;
use BabelQueue\Transport\KeyedKafkaProducer;
use BabelQueue\Transport\KafkaTransport;
use Closure;
use InvalidArgumentException;
use Throwable;

/**
 * The Kafka {@see RedriveIO} for {@see Redrive::run()} — drains a `<queue>.dlq` topic through a
 * {@see KafkaRedriveClient} and writes through the §6 producer seam. The PHP mirror of the Go
 * `kafka.Transport` redrive path (and the Java / .NET equivalents).
 *
 * - **Key preserved** ({@see KeyedRedriveIO}): a redriven record carries the DLQ record's key
 *   unchanged (null stays null, empty stays empty); the §6.5 `bq-` header projection and the
 *   envelope's `created_at` timestamp are built by {@see KafkaTransport}.
 * - **Verbatim restore**: when the redrive writes back the very record it popped (same topic,
 *   payload and key — Redrive's restore of a skipped / dry-run / undecodable / failed message) it
 *   writes that record's own value, key and headers untouched with a fresh timestamp, not a
 *   re-projected copy.
 * - **Rewind** ({@see RewindingRedriveIO}): an offset commit is cumulative, so a popped-but-unprocessed
 *   record would be skipped by a later commit. {@see self::rewind()} seeks every partition that still
 *   has an unacknowledged record back to the lowest such offset. {@see self::ack()} commits only the
 *   contiguous acknowledged prefix of a partition, never past an unacknowledged record.
 *
 * `$handle` is the opaque `array{topic, partition, offset}` returned by {@see self::pop()}. Topics are
 * `$topicPrefix . $queue` (the §6.1 mapping, like {@see KafkaTransport}).
 */
final class KafkaRedriveIO implements KeyedRedriveIO, RewindingRedriveIO
{
    /**
     * Popped records not yet fully settled, by topic then partition then offset.
     *
     * @var array<string, array<int, array<int, array{payload: string, headers: array<string, string>, key: string|null, acked: bool, seq: int}>>>
     */
    private array $inFlight = [];

    /** Pop order, to match a write-back to the earliest popped identical record (see findUnacked). */
    private int $popSequence = 0;

    private readonly KafkaTransport $transport;

    /** @var Closure(): int */
    private readonly Closure $nowMs;

    /**
     * @param  Closure(): int|null  $nowMs  clock in epoch ms for the fresh restore timestamp (default: system time)
     */
    public function __construct(
        private readonly KafkaRedriveClient $client,
        private readonly KeyedKafkaProducer $producer,
        private readonly string $topicPrefix = '',
        ?Closure $nowMs = null,
    ) {
        $this->transport = new KafkaTransport($producer, 'default', $topicPrefix);
        $this->nowMs = $nowMs ?? static fn (): int => (int) round(microtime(true) * 1000);
    }

    public function pop(string $queue): ?array
    {
        $topic = $this->topicPrefix . $queue;
        $record = $this->client->poll($topic);
        if ($record === null) {
            return null;
        }

        $key = $record['key'] ?? null;
        $this->inFlight[$record['topic']][$record['partition']][$record['offset']] = [
            'payload' => $record['payload'],
            'headers' => $record['headers'],
            'key' => $key,
            'acked' => false,
            'seq' => $this->popSequence++,
        ];

        return [
            'body' => $record['payload'],
            'handle' => ['topic' => $record['topic'], 'partition' => $record['partition'], 'offset' => $record['offset']],
            'key' => $key,
        ];
    }

    public function ack(mixed $handle): void
    {
        [$topic, $partition, $offset] = self::unpack($handle);

        $entry = $this->inFlight[$topic][$partition][$offset] ?? null;
        if ($entry === null || $entry['acked']) {
            return; // already settled
        }

        $this->inFlight[$topic][$partition][$offset]['acked'] = true;

        // Commit the contiguous acknowledged prefix of the partition only (a commit is cumulative).
        $settled = $this->ackedPrefix($topic, $partition);
        if ($settled === []) {
            return;
        }
        $commit = $settled[count($settled) - 1];

        try {
            $this->client->commitOffset($topic, $partition, $commit);
        } catch (Throwable $e) {
            $this->inFlight[$topic][$partition][$offset]['acked'] = false; // not committed: still owed a redelivery

            throw $e;
        }

        foreach ($settled as $o) {
            unset($this->inFlight[$topic][$partition][$o]);
        }
        if ($this->inFlight[$topic][$partition] === []) {
            unset($this->inFlight[$topic][$partition]);
        }
        if (($this->inFlight[$topic] ?? []) === []) {
            unset($this->inFlight[$topic]);
        }
    }

    public function publish(string $queue, string $body): void
    {
        $this->publishKeyed($queue, $body, null);
    }

    public function publishKeyed(string $queue, string $body, ?string $key): void
    {
        $topic = $this->topicPrefix . $queue;

        $original = $this->findUnacked($topic, $body, $key);
        if ($original !== null) {
            // Restore: the popped record exactly as it was — value, key, headers — with a fresh timestamp.
            KafkaProduce::send($this->producer, $topic, $original['payload'], $original['headers'], ($this->nowMs)(), $original['key']);

            return;
        }

        $this->transport->publishKeyed($body, $queue, $key);
    }

    public function rewind(mixed $handle): void
    {
        $errors = [];
        foreach ($this->inFlight as $topic => $partitions) {
            foreach ($partitions as $partition => $records) {
                $unacked = [];
                foreach ($records as $offset => $record) {
                    if (! $record['acked']) {
                        $unacked[] = $offset;
                    }
                }
                if ($unacked === []) {
                    continue;
                }

                try {
                    $this->client->seek((string) $topic, (int) $partition, min($unacked));
                } catch (Throwable $e) {
                    $errors[] = $e;

                    continue; // keep it tracked; the other partitions are still handed back
                }

                // The records of this partition are re-delivered by the seek; forget what was popped.
                unset($this->inFlight[$topic][$partition]);
            }
            if (($this->inFlight[$topic] ?? []) === []) {
                unset($this->inFlight[$topic]);
            }
        }

        if (count($errors) === 1) {
            throw $errors[0];
        }
        if ($errors !== []) {
            throw new RedriveRecoveryFailed(
                'redrive: seeking back ' . count($errors) . ' partitions failed: ' . $errors[0]->getMessage(),
                $errors,
            );
        }
    }

    /**
     * The offsets of the leading run of acknowledged records of a partition, ascending.
     *
     * @return list<int>
     */
    private function ackedPrefix(string $topic, int $partition): array
    {
        $records = $this->inFlight[$topic][$partition] ?? [];
        ksort($records);
        $prefix = [];
        foreach ($records as $offset => $record) {
            if (! $record['acked']) {
                break;
            }
            $prefix[] = $offset;
        }

        return $prefix;
    }

    /**
     * The popped, still unacknowledged record that $topic / $body / $key is a write-back of, or null.
     *
     * Several popped records can share payload and key but differ in headers; Redrive settles the batch in
     * pop order, so the earliest popped unacknowledged match is the one being written back.
     *
     * @return array{payload: string, headers: array<string, string>, key: string|null, acked: bool, seq: int}|null
     */
    private function findUnacked(string $topic, string $body, ?string $key): ?array
    {
        $match = null;
        foreach ($this->inFlight[$topic] ?? [] as $records) {
            foreach ($records as $record) {
                if (
                    ! $record['acked'] && $record['payload'] === $body && $record['key'] === $key
                    && ($match === null || $record['seq'] < $match['seq'])
                ) {
                    $match = $record;
                }
            }
        }

        return $match;
    }

    /**
     * @return array{0: string, 1: int, 2: int}
     */
    private static function unpack(mixed $handle): array
    {
        if (
            ! is_array($handle)
            || ! is_string($handle['topic'] ?? null)
            || ! is_int($handle['partition'] ?? null)
            || ! is_int($handle['offset'] ?? null)
        ) {
            throw new InvalidArgumentException('KafkaRedriveIO: the handle is not one returned by pop()');
        }

        return [$handle['topic'], $handle['partition'], $handle['offset']];
    }
}
