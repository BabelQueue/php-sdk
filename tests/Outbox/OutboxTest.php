<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Outbox;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\Transport;
use BabelQueue\Outbox\InMemoryOutboxStore;
use BabelQueue\Outbox\Outbox;
use BabelQueue\Outbox\OutboxRelay;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * Records every published body so a test can assert the relay forwarded the stored bytes
 * unchanged. `failUntil` makes the first N publish calls throw, to exercise markFailed + retry.
 */
final class RecordingTransport implements Transport
{
    /** @var list<array{body: string, queue: string|null}> */
    public array $published = [];

    public int $failUntil = 0;

    private int $calls = 0;

    public function publish(string $payload, ?string $queue = null): ?string
    {
        $this->calls++;
        if ($this->calls <= $this->failUntil) {
            throw new RuntimeException('broker unavailable');
        }
        $this->published[] = ['body' => $payload, 'queue' => $queue];

        return 'broker-id-' . $this->calls;
    }
}

/**
 * The transactional outbox helper (ADR-0029): a writer persists the frozen envelope inside the
 * caller's transaction (here, the in-memory reference store) and a relay forwards pending rows
 * through the {@see Transport}, marking them published/failed — at-least-once, trace_id intact,
 * envelope bytes unchanged.
 */
final class OutboxTest extends TestCase
{
    /** A no-op sleeper keeps backoff tests instant. */
    private function noSleep(): callable
    {
        return static function (int $ms): void {
        };
    }

    public function test_save_then_relay_publishes_and_marks_published(): void
    {
        $store = new InMemoryOutboxStore();
        $outbox = new Outbox($store);
        $transport = new RecordingTransport();

        $env = EnvelopeCodec::make('urn:babel:orders:created', ['order_id' => 1042], 'orders');
        $id = $outbox->write($env);

        $this->assertSame(1, $store->pendingCount(), 'row is pending before the relay runs');

        $relay = new OutboxRelay($transport, $store, sleeper: $this->noSleep());
        $result = $relay->flush();

        $this->assertSame(1, $result->published);
        $this->assertSame(0, $result->failed);
        $this->assertSame(0, $store->pendingCount(), 'row is marked published after a successful relay');
        $this->assertCount(1, $transport->published);
        $this->assertSame('orders', $transport->published[0]['queue']);
        $this->assertNotSame('', $id);
    }

    public function test_envelope_bytes_are_identical_before_store_and_after_relay(): void
    {
        $store = new InMemoryOutboxStore();
        $outbox = new Outbox($store);
        $transport = new RecordingTransport();

        $env = EnvelopeCodec::make('urn:babel:orders:created', ['order_id' => 1042, 'note' => 'café ☕'], 'orders');
        $encodedBeforeStore = EnvelopeCodec::encode($env);

        $outbox->write($env);
        (new OutboxRelay($transport, $store, sleeper: $this->noSleep()))->flush();

        $this->assertCount(1, $transport->published);
        $this->assertSame(
            $encodedBeforeStore,
            $transport->published[0]['body'],
            'GR-1/GR-5: the relay must publish the exact bytes that were stored — no decode/rebuild',
        );
    }

    public function test_trace_id_is_preserved_end_to_end(): void
    {
        $store = new InMemoryOutboxStore();
        $outbox = new Outbox($store);
        $transport = new RecordingTransport();

        // An inherited trace continues an existing distributed trace (GR-4).
        $traceId = '11111111-2222-4333-8444-555555555555';
        $env = EnvelopeCodec::make('urn:babel:orders:created', ['order_id' => 7], 'orders', $traceId);

        $outbox->write($env);
        (new OutboxRelay($transport, $store, sleeper: $this->noSleep()))->flush();

        $decoded = EnvelopeCodec::decode($transport->published[0]['body']);
        $this->assertSame($traceId, $decoded['trace_id']);
        $this->assertSame('urn:babel:orders:created', $decoded['job']);
    }

    public function test_publish_failure_marks_failed_and_leaves_row_for_retry(): void
    {
        $store = new InMemoryOutboxStore();
        $outbox = new Outbox($store);
        $transport = new RecordingTransport();
        $transport->failUntil = 1; // the first publish throws

        $id = $outbox->write(EnvelopeCodec::make('urn:babel:orders:created', ['order_id' => 9], 'orders'));

        $relay = new OutboxRelay($transport, $store, sleeper: $this->noSleep());

        $first = $relay->flush();
        $this->assertSame(0, $first->published);
        $this->assertSame(1, $first->failed);
        $this->assertSame(1, $store->pendingCount(), 'a failed publish leaves the row pending');
        $this->assertSame(1, $store->attemptsOf($id), 'the attempt counter is bumped');
        $this->assertStringContainsString('broker unavailable', $store->lastErrorOf($id));
        $this->assertEmpty($transport->published);

        // A later pass — once the broker recovers — publishes it (at-least-once retry).
        $second = $relay->flush();
        $this->assertSame(1, $second->published);
        $this->assertSame(0, $store->pendingCount());
        $this->assertCount(1, $transport->published);
    }

    public function test_one_poison_row_does_not_block_the_rest_of_the_batch(): void
    {
        $store = new InMemoryOutboxStore();
        $outbox = new Outbox($store);
        $transport = new RecordingTransport();
        $transport->failUntil = 1; // only the FIRST publish in the batch throws

        $outbox->write(EnvelopeCodec::make('urn:babel:orders:created', ['order_id' => 1], 'orders'));
        $outbox->write(EnvelopeCodec::make('urn:babel:orders:created', ['order_id' => 2], 'orders'));

        $result = (new OutboxRelay($transport, $store, sleeper: $this->noSleep()))->flush();

        $this->assertSame(1, $result->published, 'the second row publishes despite the first failing');
        $this->assertSame(1, $result->failed);
        $this->assertSame(1, $store->pendingCount(), 'only the poison row stays pending');
    }

    public function test_drain_loops_until_the_outbox_is_empty(): void
    {
        $store = new InMemoryOutboxStore();
        $outbox = new Outbox($store);
        $transport = new RecordingTransport();

        for ($i = 0; $i < 5; $i++) {
            $outbox->write(EnvelopeCodec::make('urn:babel:orders:created', ['order_id' => $i], 'orders'));
        }

        // A tiny batch forces multiple passes.
        $result = (new OutboxRelay($transport, $store, batchSize: 2, sleeper: $this->noSleep()))->drain();

        $this->assertSame(5, $result->published);
        $this->assertSame(0, $store->pendingCount());
        $this->assertCount(5, $transport->published);
    }

    public function test_relay_publishes_to_the_meta_queue_captured_at_write_time(): void
    {
        $store = new InMemoryOutboxStore();
        $outbox = new Outbox($store);
        $transport = new RecordingTransport();

        $outbox->write(EnvelopeCodec::make('urn:babel:emails:welcome', ['to' => 'a@b.test'], 'emails'));
        (new OutboxRelay($transport, $store, sleeper: $this->noSleep()))->flush();

        $this->assertSame('emails', $transport->published[0]['queue']);
    }

    public function test_write_falls_back_to_default_queue_when_meta_queue_is_missing(): void
    {
        $store = new InMemoryOutboxStore();
        $outbox = new Outbox($store);
        $transport = new RecordingTransport();

        // A hand-built envelope with no meta.queue → the writer captures "default".
        $env = ['job' => 'urn:babel:orders:created', 'trace_id' => 't', 'data' => [], 'meta' => [], 'attempts' => 0];
        $outbox->write($env);
        (new OutboxRelay($transport, $store, sleeper: $this->noSleep()))->flush();

        $this->assertSame('default', $transport->published[0]['queue']);
    }

    public function test_repeated_failures_grow_the_attempt_count_and_back_off_within_budget(): void
    {
        $store = new InMemoryOutboxStore();
        $outbox = new Outbox($store);
        $transport = new RecordingTransport();
        $transport->failUntil = 3; // first three publishes throw

        $id = $outbox->write(EnvelopeCodec::make('urn:babel:orders:created', ['order_id' => 1], 'orders'));

        // Real default usleep-based sleeper, but a 1ms step / 2ms cap keeps the test instant
        // while still driving the backoffFor() path off the growing attempt count.
        $relay = new OutboxRelay($transport, $store, batchSize: 10, backoffStepMs: 1, backoffCapMs: 2);

        $relay->flush();
        $this->assertSame(1, $store->attemptsOf($id));
        $relay->flush();
        $this->assertSame(2, $store->attemptsOf($id));
        $relay->flush();
        $this->assertSame(3, $store->attemptsOf($id));

        // The broker has now recovered.
        $result = $relay->flush();
        $this->assertSame(1, $result->published);
        $this->assertSame(0, $store->pendingCount());
    }

    public function test_empty_outbox_relay_is_a_no_op(): void
    {
        $store = new InMemoryOutboxStore();
        $transport = new RecordingTransport();

        $result = (new OutboxRelay($transport, $store, sleeper: $this->noSleep()))->flush();

        $this->assertSame(0, $result->published);
        $this->assertSame(0, $result->failed);
        $this->assertSame(0, $result->attempted());
        $this->assertEmpty($transport->published);
    }

    public function test_write_returns_the_outbox_row_id_for_correlation(): void
    {
        $store = new InMemoryOutboxStore();
        $outbox = new Outbox($store);

        $id1 = $outbox->write(EnvelopeCodec::make('urn:babel:orders:created', [], 'orders'));
        $id2 = $outbox->write(EnvelopeCodec::make('urn:babel:orders:created', [], 'orders'));

        $this->assertNotSame($id1, $id2, 'each row gets a distinct outbox id');
    }
}
