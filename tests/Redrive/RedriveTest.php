<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Redrive;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\DeadLetter\DeadLetter;
use BabelQueue\Redrive\HeaderRedriveIO;
use BabelQueue\Redrive\Redrive;
use BabelQueue\Redrive\RedriveIO;
use BabelQueue\Redrive\RedriveOptions;
use BabelQueue\Redrive\ReplayBypass;
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
}
