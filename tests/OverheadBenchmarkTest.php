<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\PolyglotJob;
use PHPUnit\Framework\TestCase;

/**
 * GR-8 budget: the envelope encode/decode path must add no more than 2% over plain
 * JSON serialization (the baseline a publisher already pays), measured against a
 * conservative broker round-trip. Pure CPU — no broker — so the gate is stable and
 * environment-independent in CI. The same methodology + reference is used by every
 * SDK's equivalent benchmark.
 */
final class OverheadBenchmarkTest extends TestCase
{
    /**
     * Conservative networked broker publish+consume round-trip. Local loopback Redis
     * measures ~300µs; production brokers (networked/persistent, RabbitMQ with
     * confirms) are commonly ≥0.5–2ms, so 750µs is conservative.
     */
    private const REFERENCE_BROKER_ROUNDTRIP_NS = 750_000;

    public function test_codec_overhead_is_within_the_2_percent_budget(): void
    {
        $job = new class implements PolyglotJob {
            public function getBabelUrn(): string
            {
                return 'urn:babel:orders:created';
            }

            /** @return array<string, mixed> */
            public function toPayload(): array
            {
                return ['order_id' => 1042, 'amount' => 99.9, 'currency' => 'USD', 'note' => 'café ☕'];
            }
        };
        $data = $job->toPayload();

        $envelope = static function () use ($job): void {
            $body = EnvelopeCodec::encode(EnvelopeCodec::fromJob($job, 'orders'));
            EnvelopeCodec::decode($body);
        };
        $bare = static function () use ($data): void {
            $body = json_encode($data, JSON_THROW_ON_ERROR);
            json_decode($body, true, 512, JSON_THROW_ON_ERROR);
        };

        $marginal = max(0.0, $this->nsPerOp($envelope) - $this->nsPerOp($bare));
        $overhead = $marginal / self::REFERENCE_BROKER_ROUNDTRIP_NS * 100;

        $this->assertLessThanOrEqual(
            2.0,
            $overhead,
            sprintf('Codec overhead %.2f%% exceeds the 2%% GR-8 budget (marginal %.0f ns).', $overhead, $marginal),
        );
    }

    private function nsPerOp(callable $fn): float
    {
        for ($i = 0; $i < 5_000; $i++) {
            $fn();
        }
        $iterations = 50_000;
        $start = hrtime(true);
        for ($i = 0; $i < $iterations; $i++) {
            $fn();
        }

        return (float) (hrtime(true) - $start) / $iterations;
    }
}
