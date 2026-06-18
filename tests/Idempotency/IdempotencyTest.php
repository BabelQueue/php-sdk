<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Idempotency;

use BabelQueue\Contracts\ConsumedMessage;
use BabelQueue\Idempotency\Idempotent;
use BabelQueue\Idempotency\InMemoryStore;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * The optional idempotency helper (ADR-0022): dedupe on `meta.id` under the consume
 * runtime's ack-on-return / redeliver-on-throw contract.
 */
final class IdempotencyTest extends TestCase
{
    public function test_runs_the_handler_on_first_delivery_and_remembers_it(): void
    {
        $store = new InMemoryStore();
        $calls = 0;
        $handler = Idempotent::wrap($store, function (ConsumedMessage $m) use (&$calls): void {
            $calls++;
        });

        $handler($this->message('msg-1'));

        $this->assertSame(1, $calls);
        $this->assertTrue($store->seen('msg-1'));
    }

    public function test_skips_the_handler_on_a_redelivery_of_the_same_id(): void
    {
        $store = new InMemoryStore();
        $calls = 0;
        $handler = Idempotent::wrap($store, function (ConsumedMessage $m) use (&$calls): void {
            $calls++;
        });

        $handler($this->message('msg-1'));
        $handler($this->message('msg-1')); // redelivery → skipped

        $this->assertSame(1, $calls);
    }

    public function test_runs_the_handler_again_for_a_different_id(): void
    {
        $store = new InMemoryStore();
        $calls = 0;
        $handler = Idempotent::wrap($store, function (ConsumedMessage $m) use (&$calls): void {
            $calls++;
        });

        $handler($this->message('msg-1'));
        $handler($this->message('msg-2'));

        $this->assertSame(2, $calls);
    }

    public function test_does_not_remember_an_id_when_the_handler_throws(): void
    {
        $store = new InMemoryStore();
        $calls = 0;
        $handler = Idempotent::wrap($store, function (ConsumedMessage $m) use (&$calls): void {
            $calls++;
            throw new RuntimeException('boom');
        });

        // The throw must propagate so the consumer redelivers (at-least-once).
        try {
            $handler($this->message('msg-1'));
            $this->fail('handler exception should propagate');
        } catch (RuntimeException) {
            // expected
        }
        $this->assertFalse($store->seen('msg-1'));

        // A redelivery now runs the handler again — retry works.
        try {
            $handler($this->message('msg-1'));
        } catch (RuntimeException) {
        }
        $this->assertSame(2, $calls);
    }

    public function test_runs_the_handler_when_the_message_has_no_usable_id(): void
    {
        $store = new InMemoryStore();
        $calls = 0;
        $handler = Idempotent::wrap($store, function (ConsumedMessage $m) use (&$calls): void {
            $calls++;
        });

        $handler($this->message('')); // empty id → cannot dedupe → runs
        $handler($this->message('')); // still runs

        $this->assertSame(2, $calls);
    }

    public function test_forget_removes_a_remembered_id(): void
    {
        $store = new InMemoryStore();
        $store->remember('msg-1');
        $this->assertTrue($store->seen('msg-1'));

        $store->forget('msg-1');
        $this->assertFalse($store->seen('msg-1'));
    }

    private function message(string $id): ConsumedMessage
    {
        return new FakeMessage([
            'job' => 'urn:babel:orders:created',
            'trace_id' => 'trace-1',
            'data' => ['order_id' => 7],
            'meta' => ['id' => $id, 'queue' => 'orders', 'lang' => 'php', 'schema_version' => 1],
            'attempts' => 0,
        ]);
    }
}

final class FakeMessage implements ConsumedMessage
{
    /** @param array<string, mixed> $envelope */
    public function __construct(private array $envelope)
    {
    }

    public function getUrn(): string
    {
        return is_string($this->envelope['job'] ?? null) ? $this->envelope['job'] : '';
    }

    public function getTraceId(): string
    {
        return is_string($this->envelope['trace_id'] ?? null) ? $this->envelope['trace_id'] : '';
    }

    /** @return array<string, mixed> */
    public function getData(): array
    {
        return is_array($this->envelope['data'] ?? null) ? $this->envelope['data'] : [];
    }

    /** @return array<string, mixed> */
    public function getMeta(): array
    {
        return is_array($this->envelope['meta'] ?? null) ? $this->envelope['meta'] : [];
    }

    public function attempts(): int
    {
        return is_int($this->envelope['attempts'] ?? null) ? $this->envelope['attempts'] : 0;
    }

    /** @return array<string, mixed> */
    public function envelope(): array
    {
        return $this->envelope;
    }
}
