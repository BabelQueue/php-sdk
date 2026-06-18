<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Schema;

use BabelQueue\Contracts\ConsumedMessage;
use BabelQueue\Exceptions\InvalidPayloadException;
use BabelQueue\Schema\MapProvider;
use BabelQueue\Schema\SchemaProvider;
use BabelQueue\Schema\SchemaValidated;
use PHPUnit\Framework\TestCase;

/**
 * The optional per-URN payload validation facade (ADR-0024): producer-side check/assert and
 * the consumer-side wrap, under the runtime's redeliver-on-throw contract.
 */
final class SchemaValidatedTest extends TestCase
{
    private function provider(): SchemaProvider
    {
        return MapProvider::fromJson([
            'urn:babel:orders:created' => '{"type":"object","required":["order_id"],"properties":{"order_id":{"type":"integer"}},"additionalProperties":false}',
        ]);
    }

    public function test_check_valid_invalid_and_unregistered(): void
    {
        $p = $this->provider();
        self::assertNull(SchemaValidated::check($p, 'urn:babel:orders:created', ['order_id' => 1]));
        self::assertNull(SchemaValidated::check($p, 'urn:babel:unknown', ['x' => 1])); // opt-in
        self::assertNotNull(SchemaValidated::check($p, 'urn:babel:orders:created', []));
    }

    public function test_assert_throws_on_invalid(): void
    {
        $this->expectException(InvalidPayloadException::class);
        SchemaValidated::assert($this->provider(), 'urn:babel:orders:created', ['order_id' => 'x']);
    }

    public function test_assert_passes_valid_and_unregistered(): void
    {
        $p = $this->provider();
        SchemaValidated::assert($p, 'urn:babel:orders:created', ['order_id' => 1]);
        SchemaValidated::assert($p, 'urn:babel:unknown', ['anything' => true]);
        $this->expectNotToPerformAssertions();
    }

    public function test_wrap_runs_handler_on_valid_data(): void
    {
        $calls = 0;
        $handler = SchemaValidated::wrap($this->provider(), function (ConsumedMessage $m) use (&$calls): void {
            $calls++;
        });

        $handler($this->message('urn:babel:orders:created', ['order_id' => 1]));

        self::assertSame(1, $calls);
    }

    public function test_wrap_throws_and_skips_handler_on_invalid_data(): void
    {
        $calls = 0;
        $handler = SchemaValidated::wrap($this->provider(), function (ConsumedMessage $m) use (&$calls): void {
            $calls++;
        });

        try {
            $handler($this->message('urn:babel:orders:created', [])); // missing order_id
            self::fail('expected an InvalidPayloadException');
        } catch (InvalidPayloadException $e) {
            self::assertSame('urn:babel:orders:created', $e->urn());
        }

        self::assertSame(0, $calls);
    }

    public function test_wrap_runs_handler_for_an_unregistered_urn(): void
    {
        $calls = 0;
        $handler = SchemaValidated::wrap($this->provider(), function (ConsumedMessage $m) use (&$calls): void {
            $calls++;
        });

        $handler($this->message('urn:babel:unknown', ['anything' => true]));

        self::assertSame(1, $calls);
    }

    /**
     * @param  array<string, mixed>  $data
     */
    private function message(string $urn, array $data): ConsumedMessage
    {
        return new FakeMessage([
            'job' => $urn,
            'trace_id' => 'trace-1',
            'data' => $data,
            'meta' => ['id' => 'm1', 'queue' => 'orders', 'lang' => 'php', 'schema_version' => 1],
            'attempts' => 0,
        ]);
    }
}

/**
 * Minimal {@see ConsumedMessage} test double (mirrors the one in the idempotency tests).
 */
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
