<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Redrive;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Redrive\KafkaRedriveClient;
use BabelQueue\Redrive\KafkaRedriveIO;
use BabelQueue\Redrive\Redrive;
use BabelQueue\Redrive\RedriveOptions;
use BabelQueue\Redrive\RedriveRecoveryFailed;
use BabelQueue\Transport\KeyedKafkaProducer;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * A scripted consumer: a log per topic/partition, a fetch position per partition, and a record of every
 * commit and seek. `seek()` really moves the position back, so a rewound record is delivered again.
 */
final class FakeRedriveClient implements KafkaRedriveClient
{
    /** @var array<string, array<int, list<array{payload: string, headers: array<string, string>, key: string|null}>>> */
    private array $log = [];

    /** @var array<string, array<int, int>> */
    private array $position = [];

    /** @var list<array{string, int, int}> */
    public array $commits = [];

    /** @var list<array{string, int, int}> */
    public array $seeks = [];

    public ?RuntimeException $commitFailure = null;

    /** @var list<int> partitions whose seek throws */
    public array $failSeekOn = [];

    /** @param  array<string, string>  $headers */
    public function append(string $topic, int $partition, string $payload, ?string $key, array $headers = []): void
    {
        $this->log[$topic][$partition][] = ['payload' => $payload, 'headers' => $headers, 'key' => $key];
        $this->position[$topic][$partition] ??= 0;
    }

    public function poll(string $topic): ?array
    {
        foreach ($this->log[$topic] ?? [] as $partition => $records) {
            $at = $this->position[$topic][$partition];
            if ($at < count($records)) {
                $this->position[$topic][$partition] = $at + 1;
                $r = $records[$at];

                return [
                    'topic' => $topic,
                    'partition' => $partition,
                    'offset' => $at,
                    'payload' => $r['payload'],
                    'headers' => $r['headers'],
                    'key' => $r['key'],
                ];
            }
        }

        return null;
    }

    public function commitOffset(string $topic, int $partition, int $offset): void
    {
        if ($this->commitFailure !== null) {
            throw $this->commitFailure;
        }
        $this->commits[] = [$topic, $partition, $offset];
    }

    public function seek(string $topic, int $partition, int $offset): void
    {
        if (in_array($partition, $this->failSeekOn, true)) {
            throw new RuntimeException("seek of partition {$partition} refused");
        }
        $this->seeks[] = [$topic, $partition, $offset];
        $this->position[$topic][$partition] = $offset;
    }
}

final class RecordingRedriveProducer implements KeyedKafkaProducer
{
    /** @var list<array{topic: string, payload: string, headers: array<string, string>, ts: int|null, key: string|null, keyed: bool}> */
    public array $records = [];

    /** @var list<string> topics whose writes throw */
    public array $failTopics = [];

    public function produce(string $topic, string $payload, array $headers, ?int $timestampMs = null): void
    {
        $this->add($topic, $payload, $headers, $timestampMs, null, false);
    }

    public function produceKeyed(string $topic, string $payload, array $headers, ?int $timestampMs, ?string $key): void
    {
        $this->add($topic, $payload, $headers, $timestampMs, $key, true);
    }

    /** @param  array<string, string>  $headers */
    private function add(string $topic, string $payload, array $headers, ?int $ts, ?string $key, bool $keyed): void
    {
        if (in_array($topic, $this->failTopics, true)) {
            throw new RuntimeException("write to {$topic} refused");
        }
        $this->records[] = ['topic' => $topic, 'payload' => $payload, 'headers' => $headers, 'ts' => $ts, 'key' => $key, 'keyed' => $keyed];
    }
}

final class KafkaRedriveIOTest extends TestCase
{
    private const NOW = 1900000000000;

    private FakeRedriveClient $client;

    private RecordingRedriveProducer $producer;

    private KafkaRedriveIO $io;

    protected function setUp(): void
    {
        $this->client = new FakeRedriveClient();
        $this->producer = new RecordingRedriveProducer();
        $this->io = new KafkaRedriveIO($this->client, $this->producer, '', static fn (): int => self::NOW);
    }

    private static function dead(string $id, string $queue = 'orders', string $reason = 'failed'): string
    {
        return '{"job":"urn:babel:orders:created","trace_id":"trace-' . $id . '","data":{"order_id":1042},'
            . '"meta":{"id":"' . $id . '","queue":"' . $queue . '","lang":"php","schema_version":1,"created_at":1749132727000},'
            . '"attempts":3,"dead_letter":{"reason":"' . $reason . '","original_queue":"' . $queue . '","attempts":3}}';
    }

    /** @return array<string, array{0: string|null}> */
    public static function keys(): array
    {
        return ['keyed' => ['order-1042'], 'empty key' => [''], 'null key' => [null]];
    }

    #[DataProvider('keys')]
    public function testPopSurfacesTheRecordKeyUntouched(?string $key): void
    {
        $this->client->append('orders.dlq', 0, self::dead('m1'), $key);

        $popped = $this->io->pop('orders.dlq');

        self::assertNotNull($popped);
        self::assertSame(self::dead('m1'), $popped['body']);
        self::assertSame($key, $popped['key']);
        self::assertNull($this->io->pop('orders.dlq'));
    }

    #[DataProvider('keys')]
    public function testRedriveCarriesTheDlqRecordKeyToTheSourceTopic(?string $key): void
    {
        $this->client->append('orders.dlq', 0, self::dead('m1'), $key);

        $result = Redrive::run($this->io, 'orders.dlq');

        self::assertSame(1, $result->redriven);
        self::assertCount(1, $this->producer->records);
        $r = $this->producer->records[0];
        self::assertSame('orders', $r['topic']);
        self::assertSame($key, $r['key']);
        self::assertSame($key !== null, $r['keyed'], 'a null key is a plain keyless produce, never a synthesised key');
        self::assertSame(1749132727000, $r['ts'], 'the republish keeps the envelope created_at as the record timestamp');
        self::assertSame('urn:babel:orders:created', $r['headers']['bq-job']);
        self::assertSame('0', $r['headers']['bq-attempts'], 'a redriven message restarts at attempts 0');
        self::assertArrayNotHasKey('dead_letter', EnvelopeCodec::decode($r['payload']));
        self::assertSame([['orders.dlq', 0, 0]], $this->client->commits);
    }

    #[DataProvider('keys')]
    public function testRestoreWritesTheOriginalRecordVerbatimWithAFreshTimestamp(?string $key): void
    {
        $headers = ['bq-job' => 'urn:babel:orders:created', 'bq-attempts' => '3', 'bq-original-topic' => 'orders'];
        $this->client->append('orders.dlq', 0, self::dead('m1'), $key, $headers);

        $result = Redrive::run($this->io, 'orders.dlq', new RedriveOptions(dryRun: true));

        self::assertSame(0, $result->redriven);
        self::assertSame(1, $result->skipped);
        self::assertCount(1, $this->producer->records);
        $r = $this->producer->records[0];
        self::assertSame('orders.dlq', $r['topic']);
        self::assertSame(self::dead('m1'), $r['payload']);
        self::assertSame($headers, $r['headers'], 'the original headers are written back, not re-projected');
        self::assertSame($key, $r['key']);
        self::assertSame(self::NOW, $r['ts'], 'a fresh timestamp, not the envelope created_at');
        self::assertSame([['orders.dlq', 0, 0]], $this->client->commits);
    }

    public function testAWriteWithADifferentKeyIsNotARestore(): void
    {
        $this->client->append('orders.dlq', 0, self::dead('m1'), 'k1', ['x-custom' => 'v']);
        $popped = $this->io->pop('orders.dlq');
        self::assertNotNull($popped);

        $this->io->publishKeyed('orders.dlq', $popped['body'], 'other');

        self::assertCount(1, $this->producer->records);
        self::assertArrayNotHasKey('x-custom', $this->producer->records[0]['headers']);
        self::assertSame('other', $this->producer->records[0]['key']);
    }

    public function testASettledRecordIsNoLongerARestoreCandidate(): void
    {
        $this->client->append('orders.dlq', 0, self::dead('m1'), 'k1', ['x-custom' => 'v']);
        $popped = $this->io->pop('orders.dlq');
        self::assertNotNull($popped);
        $this->io->ack($popped['handle']);

        $this->io->publishKeyed('orders.dlq', $popped['body'], 'k1');

        self::assertArrayNotHasKey('x-custom', $this->producer->records[0]['headers']);
    }

    public function testARestoreMatchesTheEarliestPoppedIdenticalRecord(): void
    {
        // Two records share payload and key but differ in out-of-band headers; the one popped first
        // (partition 1) is restored first even though partition 0 is iterated first.
        $this->client->append('orders.dlq', 0, self::dead('x'), 'kx');
        $this->client->append('orders.dlq', 1, self::dead('a'), 'k1', ['traceparent' => 'first']);
        self::assertNotNull($this->io->pop('orders.dlq'));
        $first = $this->io->pop('orders.dlq');
        self::assertNotNull($first);
        $this->client->append('orders.dlq', 0, self::dead('a'), 'k1', ['traceparent' => 'second']);
        $second = $this->io->pop('orders.dlq');
        self::assertNotNull($second);

        $this->io->publishKeyed('orders.dlq', $first['body'], 'k1');
        self::assertSame('first', $this->producer->records[0]['headers']['traceparent']);

        $this->io->ack($first['handle']);
        $this->io->publishKeyed('orders.dlq', $second['body'], 'k1');
        self::assertSame('second', $this->producer->records[1]['headers']['traceparent']);
    }

    public function testPlainPublishIsAKeylessProjectedWrite(): void
    {
        $this->io->publish('orders', self::dead('m1'));

        self::assertCount(1, $this->producer->records);
        self::assertNull($this->producer->records[0]['key']);
        self::assertFalse($this->producer->records[0]['keyed']);
        self::assertSame('orders', $this->producer->records[0]['topic']);
    }

    public function testTopicPrefixMapsQueuesToTopics(): void
    {
        $io = new KafkaRedriveIO($this->client, $this->producer, 'app.', static fn (): int => self::NOW);
        $this->client->append('app.orders.dlq', 0, self::dead('m1'), 'k');

        Redrive::run($io, 'orders.dlq');

        self::assertSame('app.orders', $this->producer->records[0]['topic']);
        self::assertSame([['app.orders.dlq', 0, 0]], $this->client->commits);
    }

    public function testAckCommitsOnlyTheContiguousAcknowledgedPrefix(): void
    {
        foreach (['a', 'b', 'c'] as $id) {
            $this->client->append('orders.dlq', 0, self::dead($id), null);
        }
        $first = $this->io->pop('orders.dlq');
        $second = $this->io->pop('orders.dlq');
        $third = $this->io->pop('orders.dlq');
        self::assertNotNull($first);
        self::assertNotNull($second);
        self::assertNotNull($third);

        $this->io->ack($second['handle']);
        $this->io->ack($third['handle']);
        self::assertSame([], $this->client->commits, 'a commit past an unacknowledged record would skip it');

        $this->io->ack($first['handle']);
        self::assertSame([['orders.dlq', 0, 2]], $this->client->commits);
    }

    public function testAckWithAForeignHandleIsRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->io->ack('not-a-handle');
    }

    public function testFailedCommitLeavesTheRecordOwedARedelivery(): void
    {
        $this->client->append('orders.dlq', 0, self::dead('a'), null);
        $popped = $this->io->pop('orders.dlq');
        self::assertNotNull($popped);
        $this->client->commitFailure = new RuntimeException('commit refused');

        try {
            $this->io->ack($popped['handle']);
            self::fail('the failed commit must surface');
        } catch (RuntimeException $e) {
            self::assertSame('commit refused', $e->getMessage());
        }

        $this->io->rewind($popped['handle']);
        self::assertSame([['orders.dlq', 0, 0]], $this->client->seeks);
    }

    public function testRewindSeeksEveryPartitionWithAnUnackedRecordBackToItsLowestOffset(): void
    {
        $this->client->append('orders.dlq', 0, self::dead('p0a'), null);
        $this->client->append('orders.dlq', 0, self::dead('p0b'), null);
        $this->client->append('orders.dlq', 1, self::dead('p1a'), null);
        $this->client->append('orders.dlq', 1, self::dead('p1b'), null);
        $this->client->append('orders.dlq', 2, self::dead('p2a'), null);
        $handles = [];
        while (($p = $this->io->pop('orders.dlq')) !== null) {
            $handles[$p['body']] = $p['handle'];
        }
        self::assertCount(5, $handles);
        $this->io->ack($handles[self::dead('p0a')]);   // partition 0: offset 0 done, 1 unacked
        $this->io->ack($handles[self::dead('p2a')]);   // partition 2: everything done

        $this->io->rewind($handles[self::dead('p0b')]);

        self::assertEqualsCanonicalizing(
            [['orders.dlq', 0, 1], ['orders.dlq', 1, 0]],
            $this->client->seeks,
        );
        $again = [];
        while (($p = $this->io->pop('orders.dlq')) !== null) {
            $again[] = $p['body'];
        }
        self::assertEqualsCanonicalizing([self::dead('p0b'), self::dead('p1a'), self::dead('p1b')], $again);
    }

    public function testRewindDoesNotRedeliverARestoredAcknowledgedHandle(): void
    {
        $this->client->append('orders.dlq', 0, self::dead('a'), null);
        $this->client->append('orders.dlq', 0, self::dead('b'), null);
        $a = $this->io->pop('orders.dlq');
        $b = $this->io->pop('orders.dlq');
        self::assertNotNull($a);
        self::assertNotNull($b);
        $this->io->ack($a['handle']);

        $this->io->rewind($a['handle']);

        self::assertSame([['orders.dlq', 0, 1]], $this->client->seeks);
        $next = $this->io->pop('orders.dlq');
        self::assertNotNull($next);
        self::assertSame(self::dead('b'), $next['body']);
        self::assertNull($this->io->pop('orders.dlq'));
    }

    public function testRewindWithNothingInFlightIsANoOp(): void
    {
        $this->io->rewind(['topic' => 'orders.dlq', 'partition' => 0, 'offset' => 0]);

        self::assertSame([], $this->client->seeks);
    }

    public function testFailedSeekStillHandsBackTheOtherPartitions(): void
    {
        $this->client->append('orders.dlq', 0, self::dead('a'), null);
        $this->client->append('orders.dlq', 1, self::dead('b'), null);
        $a = $this->io->pop('orders.dlq');
        self::assertNotNull($this->io->pop('orders.dlq'));
        self::assertNotNull($a);
        $this->client->failSeekOn = [0];

        try {
            $this->io->rewind($a['handle']);
            self::fail('the failed seek must surface');
        } catch (RuntimeException $e) {
            self::assertSame('seek of partition 0 refused', $e->getMessage());
        }

        self::assertSame([['orders.dlq', 1, 0]], $this->client->seeks);
    }

    public function testSeveralFailedSeeksAreReportedTogether(): void
    {
        $this->client->append('orders.dlq', 0, self::dead('a'), null);
        $this->client->append('orders.dlq', 1, self::dead('b'), null);
        $a = $this->io->pop('orders.dlq');
        $this->io->pop('orders.dlq');
        self::assertNotNull($a);
        $this->client->failSeekOn = [0, 1];

        try {
            $this->io->rewind($a['handle']);
            self::fail('the failed seeks must surface');
        } catch (RedriveRecoveryFailed $e) {
            self::assertCount(2, $e->getErrors());
        }
    }

    public function testFailedRestoreRewindsTheWholeBatchAndLosesNothing(): void
    {
        $this->client->append('orders.dlq', 0, self::dead('p0a'), 'k0');
        $this->client->append('orders.dlq', 0, self::dead('p0b'), null);
        $this->client->append('orders.dlq', 1, self::dead('p1a'), '');
        $this->producer->failTopics = ['orders.dlq'];

        try {
            // Nothing is selected, so every record is restored to the DLQ - and that write is refused.
            Redrive::run($this->io, 'orders.dlq', new RedriveOptions(select: static fn (array $env): bool => false));
            self::fail('the failed restore must surface');
        } catch (RuntimeException $e) {
            self::assertSame('write to orders.dlq refused', $e->getMessage());
        }

        self::assertSame([], $this->client->commits, 'an unrestored record is never acknowledged');
        self::assertEqualsCanonicalizing([['orders.dlq', 0, 0], ['orders.dlq', 1, 0]], $this->client->seeks);

        // A later run (the broker is back) sees the whole batch again, keys intact.
        $this->producer->failTopics = [];
        $result = Redrive::run($this->io, 'orders.dlq', new RedriveOptions(select: static fn (array $env): bool => false));

        self::assertSame(3, $result->skipped);
        self::assertCount(3, $this->producer->records);
        $keys = array_map(static fn (array $r): ?string => $r['key'], $this->producer->records);
        self::assertEqualsCanonicalizing(['k0', null, ''], $keys);
    }

    public function testFailedRedriveWriteRestoresThenRewindsTheRest(): void
    {
        $this->client->append('orders.dlq', 0, self::dead('a'), 'k0');
        $this->client->append('orders.dlq', 0, self::dead('b'), 'k1');
        $this->producer->failTopics = ['orders'];

        try {
            Redrive::run($this->io, 'orders.dlq');
            self::fail('the failed redrive write must surface');
        } catch (RuntimeException $e) {
            self::assertSame('write to orders refused', $e->getMessage());
        }

        // The first record was restored verbatim and acknowledged; the second was only reserved.
        self::assertCount(1, $this->producer->records);
        self::assertSame('orders.dlq', $this->producer->records[0]['topic']);
        self::assertSame(self::dead('a'), $this->producer->records[0]['payload']);
        self::assertSame([['orders.dlq', 0, 0]], $this->client->commits);
        self::assertSame([['orders.dlq', 0, 1]], $this->client->seeks);
    }
}
