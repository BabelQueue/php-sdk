<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Redrive;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\DeadLetter\DeadLetter;
use BabelQueue\Redrive\HeaderRedriveIO;
use BabelQueue\Redrive\KeyedRedriveIO;
use BabelQueue\Redrive\Redrive;
use BabelQueue\Redrive\RedriveIO;
use BabelQueue\Redrive\RedriveOptions;
use BabelQueue\Redrive\ReplayBypass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * In-memory {@see RedriveIO}: pop removes the head (FIFO) and ack is a no-op; publish appends.
 * `failQueue`, when set, makes a publish to that queue throw — to exercise restore-on-failure.
 */
final class FakeIO implements RedriveIO
{
    /** @var array<string, list<string>> */
    public array $queues = [];

    public ?string $failQueue = null;

    public function pop(string $queue): ?array
    {
        if (empty($this->queues[$queue])) {
            return null;
        }

        return ['body' => array_shift($this->queues[$queue]), 'handle' => null];
    }

    public function ack(mixed $handle): void
    {
        // no-op: pop already removed the message
    }

    public function publish(string $queue, string $body): void
    {
        if ($queue === $this->failQueue) {
            throw new RuntimeException('publish refused');
        }
        $this->queues[$queue][] = $body;
    }
}

/**
 * A header-capable {@see HeaderRedriveIO}: same in-memory pop/ack/publish as {@see FakeIO}, plus it
 * records the out-of-band headers each re-published body carried, so a test can assert the
 * `bq-replay-bypass` marker was actually stamped (the seam Go's `Redrive` rides via its
 * `HeaderPublisher` check).
 */
final class HeaderFakeIO implements HeaderRedriveIO
{
    /** @var array<string, list<string>> */
    public array $queues = [];

    /** @var array<string, list<array<string, string>>> queue => per-message header maps */
    public array $headers = [];

    public function pop(string $queue): ?array
    {
        if (empty($this->queues[$queue])) {
            return null;
        }

        return ['body' => array_shift($this->queues[$queue]), 'handle' => null];
    }

    public function ack(mixed $handle): void
    {
        // no-op: pop already removed the message
    }

    public function publish(string $queue, string $body): void
    {
        $this->queues[$queue][] = $body;
    }

    public function publishWithHeaders(string $queue, string $body, array $headers): void
    {
        $this->publish($queue, $body);
        $this->headers[$queue][] = $headers;
    }
}

/**
 * A key-capable {@see KeyedRedriveIO} (the Kafka shape): every queue entry is a `[body, key]` pair, `pop`
 * surfaces the stored key through the optional `key` entry, and `publishKeyed` stores the key it was
 * handed — so a test can assert the exact key (null vs empty vs bytes) each write carried.
 */
final class KeyedFakeIO implements KeyedRedriveIO
{
    /** @var array<string, list<array{0: string, 1: string|null}>> */
    public array $queues = [];

    public function pop(string $queue): ?array
    {
        if (empty($this->queues[$queue])) {
            return null;
        }
        [$body, $key] = array_shift($this->queues[$queue]);

        return ['body' => $body, 'handle' => null, 'key' => $key];
    }

    public function ack(mixed $handle): void
    {
        // no-op: pop already removed the message
    }

    public function publish(string $queue, string $body): void
    {
        $this->publishKeyed($queue, $body, null);
    }

    public function publishKeyed(string $queue, string $body, ?string $key): void
    {
        $this->queues[$queue][] = [$body, $key];
    }
}

final class RedriveTest extends TestCase
{
    /**
     * @param  array<string, mixed>  $data
     * @return array<string, mixed>
     */
    private function deadLettered(string $urn, string $originalQueue, array $data = []): array
    {
        $env = EnvelopeCodec::make($urn, $data, $originalQueue);

        return DeadLetter::annotate($env, 'failed', new RuntimeException('boom'), $originalQueue, 3);
    }

    public function testRedriveToSourceResetsAndPreservesIdentity(): void
    {
        $io = new FakeIO();
        $dl = $this->deadLettered('urn:babel:orders:created', 'orders', ['order_id' => 1]);
        $traceId = $dl['trace_id'];
        $io->publish('orders.dlq', EnvelopeCodec::encode($dl));

        $res = Redrive::run($io, 'orders.dlq');

        $this->assertSame(1, $res->redriven);
        $this->assertSame(0, $res->skipped);
        $this->assertEmpty($io->queues['orders.dlq'] ?? []);
        $this->assertCount(1, $io->queues['orders']);

        $back = EnvelopeCodec::decode($io->queues['orders'][0]);
        $this->assertArrayNotHasKey('dead_letter', $back);
        $this->assertSame(0, $back['attempts']);
        $this->assertSame($traceId, $back['trace_id']);
        $this->assertSame('urn:babel:orders:created', $back['job']);
    }

    public function testRedriveToSandboxLeavesSourceUntouched(): void
    {
        $io = new FakeIO();
        $io->publish('orders.dlq', EnvelopeCodec::encode($this->deadLettered('urn:babel:orders:created', 'orders')));

        $res = Redrive::run($io, 'orders.dlq', new RedriveOptions(toQueue: 'sandbox'));

        $this->assertSame(1, $res->redriven);
        $this->assertEmpty($io->queues['orders'] ?? []);
        $this->assertCount(1, $io->queues['sandbox']);
    }

    public function testDryRunReportsPlanAndLeavesDlqUnchanged(): void
    {
        $io = new FakeIO();
        $io->publish('orders.dlq', EnvelopeCodec::encode($this->deadLettered('urn:babel:orders:created', 'orders')));

        $res = Redrive::run($io, 'orders.dlq', new RedriveOptions(dryRun: true));

        $this->assertSame(0, $res->redriven);
        $this->assertSame(1, $res->skipped);
        $this->assertCount(1, $res->items);
        $this->assertSame('orders', $res->items[0]->to);
        $this->assertFalse($res->items[0]->redriven);
        $this->assertEmpty($io->queues['orders'] ?? []);
        $this->assertCount(1, $io->queues['orders.dlq']);
        $this->assertArrayHasKey('dead_letter', EnvelopeCodec::decode($io->queues['orders.dlq'][0]));
    }

    public function testSelectRedrivesOnlyMatchesAndRestoresTheRest(): void
    {
        $io = new FakeIO();
        $io->publish('dlq', EnvelopeCodec::encode($this->deadLettered('urn:babel:orders:created', 'orders')));
        $io->publish('dlq', EnvelopeCodec::encode($this->deadLettered('urn:babel:emails:welcome', 'emails')));

        $res = Redrive::run($io, 'dlq', new RedriveOptions(
            select: static fn (array $e): bool => EnvelopeCodec::urn($e) === 'urn:babel:orders:created',
        ));

        $this->assertSame(1, $res->redriven);
        $this->assertSame(1, $res->skipped);
        $this->assertCount(1, $io->queues['orders']);
        $this->assertEmpty($io->queues['emails'] ?? []);
        $this->assertCount(1, $io->queues['dlq']); // unselected restored
    }

    public function testMaxCapsHowManyArePulled(): void
    {
        $io = new FakeIO();
        for ($i = 0; $i < 3; $i++) {
            $io->publish('dlq', EnvelopeCodec::encode($this->deadLettered('urn:babel:orders:created', 'orders')));
        }

        $res = Redrive::run($io, 'dlq', new RedriveOptions(max: 2));

        $this->assertSame(2, $res->redriven);
        $this->assertCount(1, $io->queues['dlq']);
    }

    public function testPublishFailureRestoresToDlq(): void
    {
        $io = new FakeIO();
        $io->publish('dlq', EnvelopeCodec::encode($this->deadLettered('urn:babel:orders:created', 'orders')));
        $io->failQueue = 'orders';

        try {
            Redrive::run($io, 'dlq');
            $this->fail('expected the publish error to surface');
        } catch (RuntimeException $e) {
            $this->assertSame('publish refused', $e->getMessage());
        }

        $this->assertCount(1, $io->queues['dlq']); // restored, not lost
        $this->assertEmpty($io->queues['orders'] ?? []);
    }

    public function testUndecodableBodyIsRestored(): void
    {
        $io = new FakeIO();
        $io->publish('dlq', 'not-json{{{');

        $res = Redrive::run($io, 'dlq');

        $this->assertSame(0, $res->redriven);
        $this->assertSame(1, $res->skipped);
        $this->assertCount(1, $io->queues['dlq']);
        $this->assertSame('not-json{{{', $io->queues['dlq'][0]);
    }

    public function testResetIsPureAndStripsDeadLetter(): void
    {
        $dl = $this->deadLettered('urn:babel:orders:created', 'orders', ['x' => 1]);
        $dl['attempts'] = 5;

        $reset = Redrive::reset($dl);

        $this->assertArrayNotHasKey('dead_letter', $reset);
        $this->assertSame(0, $reset['attempts']);
        $this->assertSame($dl['trace_id'], $reset['trace_id']);
        $this->assertArrayHasKey('dead_letter', $dl); // original argument unchanged (pure)
    }

    public function testBypassStampsTheReplayMarkerOnAHeaderCapableIo(): void
    {
        $io = new HeaderFakeIO();
        $io->publish('orders.dlq', EnvelopeCodec::encode($this->deadLettered('urn:babel:orders:created', 'orders')));

        $res = Redrive::run($io, 'orders.dlq', new RedriveOptions(bypass: true));

        $this->assertSame(1, $res->redriven);
        $this->assertTrue($res->items[0]->bypassed, 'the marker was stamped on a HeaderRedriveIO');
        // The redriven message carried exactly the bq-replay-bypass marker beside the envelope.
        $this->assertSame([ReplayBypass::markerHeaders()], $io->headers['orders']);
        // The re-published body is still the reset, frozen envelope (GR-1) — the marker rides beside it.
        $back = EnvelopeCodec::decode($io->queues['orders'][0]);
        $this->assertArrayNotHasKey('dead_letter', $back);
        $this->assertSame(0, $back['attempts']);
    }

    public function testBypassIsANoOpOnAPlainIo(): void
    {
        // A RedriveIO that cannot carry headers: bypass is best-effort, so it degrades to a plain
        // re-publish and the per-item bypassed flag stays false (ADR-0027).
        $io = new FakeIO();
        $io->publish('orders.dlq', EnvelopeCodec::encode($this->deadLettered('urn:babel:orders:created', 'orders')));

        $res = Redrive::run($io, 'orders.dlq', new RedriveOptions(bypass: true));

        $this->assertSame(1, $res->redriven);
        $this->assertFalse($res->items[0]->bypassed, 'a plain RedriveIO cannot carry the marker');
        $this->assertCount(1, $io->queues['orders']);
    }

    public function testWithoutBypassNoMarkerIsStampedEvenOnAHeaderCapableIo(): void
    {
        // A normal redrive (no bypass) re-publishes plainly — no replay marker, byte-identical body.
        $io = new HeaderFakeIO();
        $io->publish('orders.dlq', EnvelopeCodec::encode($this->deadLettered('urn:babel:orders:created', 'orders')));

        $res = Redrive::run($io, 'orders.dlq');

        $this->assertSame(1, $res->redriven);
        $this->assertFalse($res->items[0]->bypassed);
        $this->assertArrayNotHasKey('orders', $io->headers); // publishWithHeaders never called
        $this->assertCount(1, $io->queues['orders']);
    }

    /**
     * @return array<string, array{0: string|null}>
     */
    public static function recordKeys(): array
    {
        return ['bytes' => ["order-1042"], 'empty' => [''], 'null' => [null], 'binary' => ["\x00\xff\x10"]];
    }

    #[DataProvider('recordKeys')]
    public function testRedriveCarriesTheDlqRecordKeyByteForByte(?string $key): void
    {
        $io = new KeyedFakeIO();
        $io->publishKeyed('orders.dlq', EnvelopeCodec::encode($this->deadLettered('urn:babel:orders:created', 'orders')), $key);

        $res = Redrive::run($io, 'orders.dlq');

        $this->assertSame(1, $res->redriven);
        $this->assertCount(1, $io->queues['orders']);
        $this->assertSame($key, $io->queues['orders'][0][1], 'null stays null, empty stays empty');
    }

    #[DataProvider('recordKeys')]
    public function testRestoreToDlqKeepsTheKey(?string $key): void
    {
        $dead = EnvelopeCodec::encode($this->deadLettered('urn:babel:orders:created', 'orders'));

        // dry-run, not-selected and undecodable bodies are all restored to the DLQ with their key.
        foreach ([new RedriveOptions(dryRun: true), new RedriveOptions(select: static fn (array $e): bool => false)] as $options) {
            $io = new KeyedFakeIO();
            $io->publishKeyed('orders.dlq', $dead, $key);
            Redrive::run($io, 'orders.dlq', $options);
            $this->assertSame([[$dead, $key]], $io->queues['orders.dlq']);
        }

        $io = new KeyedFakeIO();
        $io->publishKeyed('orders.dlq', 'not json at all', $key);
        Redrive::run($io, 'orders.dlq');
        $this->assertSame([['not json at all', $key]], $io->queues['orders.dlq']);
    }

    public function testPublishFailureRestoresToDlqWithTheKey(): void
    {
        $keyed = new class () implements KeyedRedriveIO {
            /** @var list<array{0: string, 1: string, 2: string|null}> */
            public array $writes = [];
            public ?array $next = null;

            public function pop(string $queue): ?array
            {
                $n = $this->next;
                $this->next = null;

                return $n;
            }

            public function ack(mixed $handle): void
            {
            }

            public function publish(string $queue, string $body): void
            {
                $this->publishKeyed($queue, $body, null);
            }

            public function publishKeyed(string $queue, string $body, ?string $key): void
            {
                if ($queue === 'orders') {
                    throw new RuntimeException('publish refused');
                }
                $this->writes[] = [$queue, $body, $key];
            }
        };
        $dead = EnvelopeCodec::encode($this->deadLettered('urn:babel:orders:created', 'orders'));
        $keyed->next = ['body' => $dead, 'handle' => null, 'key' => 'k-9'];

        try {
            Redrive::run($keyed, 'orders.dlq');
            $this->fail('expected the publish failure to surface');
        } catch (RuntimeException) {
            $this->assertSame([['orders.dlq', $dead, 'k-9']], $keyed->writes);
        }
    }

    public function testAPlainIoIgnoresAKeyEntryInPopAndRepublishesKeyless(): void
    {
        // A plain RedriveIO whose pop() happens to return a `key` entry: without KeyedRedriveIO the
        // key is ignored and the message is republished through publish(), exactly as before.
        $io = new class () implements RedriveIO {
            /** @var list<array{0: string, 1: string}> */
            public array $published = [];

            /** @var list<string> */
            public array $pending = [];

            public function pop(string $queue): ?array
            {
                $body = array_shift($this->pending);

                return $body === null ? null : ['body' => $body, 'handle' => null, 'key' => 'k-1'];
            }

            public function ack(mixed $handle): void
            {
            }

            public function publish(string $queue, string $body): void
            {
                $this->published[] = [$queue, $body];
            }
        };
        $io->pending[] = EnvelopeCodec::encode($this->deadLettered('urn:babel:orders:created', 'orders'));

        $res = Redrive::run($io, 'orders.dlq');

        $this->assertSame(1, $res->redriven);
        $this->assertCount(1, $io->published);
        $this->assertSame('orders', $io->published[0][0]);
    }

    public function testRedriveReadsDeadLetterWhenTheBodyCarriesEmptyObjects(): void
    {
        $io = new FakeIO();
        $env = EnvelopeCodec::make('urn:babel:orders:created', [], 'orders-original');
        $env['meta']['ext'] = ['acme' => new \stdClass()];
        $dl = DeadLetter::annotate($env, 'failed', new RuntimeException('boom'), 'orders-original', 3);
        $body = EnvelopeCodec::encode($dl);
        $this->assertStringContainsString('"acme":{}', $body);
        $io->publish('orders.dlq', $body);

        $seen = [];
        $res = Redrive::run($io, 'orders.dlq', new RedriveOptions(select: static function (array $e) use (&$seen): bool {
            $seen[] = $e['dead_letter']['reason'];

            return true;
        }));

        $this->assertSame(1, $res->redriven);
        $this->assertSame(['failed'], $seen);
        $this->assertCount(1, $io->queues['orders-original']);
        $this->assertArrayNotHasKey('orders', $io->queues);
        $this->assertStringContainsString('"acme":{}', $io->queues['orders-original'][0]);
    }
}
