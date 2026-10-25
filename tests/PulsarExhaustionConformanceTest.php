<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Consume\DeadLetterPublisher;
use BabelQueue\Consume\Dispatcher;
use BabelQueue\Contracts\ConsumedMessage;
use BabelQueue\Contracts\Transport;
use BabelQueue\Tests\Support\ConformanceResults;
use BabelQueue\Transport\PulsarConsumer;
use BabelQueue\Transport\PulsarWebSocketConsumerClient;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Throwable;

/**
 * The Pulsar max-attempts settle decision (ADR-0036 decisions 1-3): the `pulsar_exhaustion` section, run
 * through the real `PulsarConsumer::receive()` (attempts reconciliation) and the real `Consume\Dispatcher`.
 *
 * For the PHP `Dispatcher` "dead_letter: on" means a {@see DeadLetterPublisher} is configured, so `off` means
 * no publisher and nothing can be written; a row that differs there carries `expect.outcome_no_writer`.
 * Observed outcome: `retry` = the Dispatcher rethrows (the consumer loop releases), `dlq` = one record on
 * the transport, `drop` = it returns normally without writing (acknowledge).
 */
final class PulsarExhaustionConformanceTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const URN = 'urn:babel:orders:created';

    protected function setUp(): void
    {
        // The poison bodies make the codec warn through error_log on every decode; keep the run quiet.
        EnvelopeCodec::setWarningHandler(static function (): void {
        });
    }

    protected function tearDown(): void
    {
        EnvelopeCodec::setWarningHandler(null);
    }

    public function test_pulsar_exhaustion_matches_the_golden_cases(): void
    {
        ConformanceResults::each('pulsar_exhaustion', function (array $case): void {
            // A poison case is exercised with two bodies: undecodable, and a JSON object without any identity.
            $bodies = $case['kind'] === 'poison'
                ? ['{not json', '{"trace_id":"t","data":{}}']
                : [(string) json_encode([
                    'job' => self::URN,
                    'trace_id' => 'trace-1',
                    'data' => new \stdClass(),
                    'meta' => ['id' => 'm-1', 'queue' => 'orders', 'lang' => 'php', 'schema_version' => 1, 'created_at' => 1],
                    'attempts' => $case['body_attempts'] ?? 0,
                ])];

            foreach ($bodies as $body) {
                $this->assertOutcome($case, $body);
            }
        });
    }

    /** @param array<string, mixed> $case */
    private function assertOutcome(array $case, string $body): void
    {
        $properties = isset($case['bq_attempts']) ? ['bq-attempts' => (string) $case['bq_attempts']] : [];
        $client = Mockery::mock(PulsarWebSocketConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn([
            'messageId' => 'mid-1',
            'payload' => $body,
            'properties' => $properties,
            'redeliveryCount' => $case['redelivery_count'],
        ]);
        $message = (new PulsarConsumer($client, null, 'orders', subscriptionType: 'Shared'))->receive();
        $this->assertNotNull($message, $case['name']);

        $writer = $case['dead_letter'] === true;
        $published = [];
        $transport = new class($published) implements Transport {
            /** @param list<array{0: string, 1: string}> $published */
            public function __construct(private array &$published)
            {
            }

            public function publish(string $payload, ?string $queue = null): ?string
            {
                $this->published[] = [$payload, (string) $queue];

                return null;
            }
        };

        $dispatcher = new Dispatcher(
            (string) ($case['on_unknown_urn'] ?? 'fail'),
            (int) ($case['max_attempts'] ?? 3),
            $writer ? new DeadLetterPublisher($transport) : null,
            onDrop: static function (): void {
            },
        );
        if ($case['kind'] === 'failed') {
            $dispatcher->on(self::URN, static function (): void {
                throw new RuntimeException('boom');
            });
        }

        $outcome = 'drop';
        try {
            $dispatcher($message);
        } catch (Throwable) {
            $outcome = 'retry';
        }
        if ($published !== []) {
            $outcome = 'dlq';
        }

        $expect = $case['expect'];
        $want = ! $writer && isset($expect['outcome_no_writer']) ? $expect['outcome_no_writer'] : $expect['outcome'];
        $label = $case['name'] . ' <' . substr($body, 0, 20) . '>';
        $this->assertSame($want, $outcome, $label);

        if ($outcome !== 'dlq') {
            return;
        }

        $this->assertCount(1, $published, $label);
        [$payload, $queue] = $published[0];
        $this->assertSame('orders.dlq', $queue, $label);
        $record = json_decode($payload, true, 512, JSON_THROW_ON_ERROR);
        $this->assertSame($expect['reason'], $record['dead_letter']['reason'], $label);
        $this->assertSame('orders', $record['dead_letter']['original_queue'], $label);
        if (isset($expect['dead_letter_attempts'])) {
            $this->assertSame($expect['dead_letter_attempts'], $record['dead_letter']['attempts'], $label);
        }
        if ($case['kind'] === 'poison' && $body === '{not json') {
            $this->assertSame($body, $record['raw'], $label . ': the poison bytes are kept (ADR-0035)');
        }
    }
}
