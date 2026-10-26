<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Consume\DeadLetterPublisher;
use BabelQueue\Consume\Dispatcher;
use BabelQueue\Contracts\Transport;
use BabelQueue\Tests\Support\ConformanceResults;
use BabelQueue\Transport\PulsarConsumer;
use BabelQueue\Transport\PulsarWebSocketClient;
use BabelQueue\Transport\PulsarWebSocketConsumerClient;
use BabelQueue\Transport\SqsTransport;
use BabelQueue\Transport\StompTransport;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * The version-gate settle rule (ADR-0037 decision 1): the `version_gate_<binding>` sections of conformance 1.8.0.
 *
 * `version_gate_pulsar` runs through the real `PulsarConsumer` (receive, gate, consume loop) and the real
 * `Consume\Dispatcher`. For the PHP `Dispatcher` "dead_letter: on" means a {@see DeadLetterPublisher} is
 * configured, so `off` means no publisher and nothing can be written; the `dead_letter` strategy rows with
 * `off` therefore assert `expect.outcome_no_writer`. Observed outcome: `ack` = the message is acknowledged and
 * nothing is written, `dlq_raw` = exactly one record on `<queue>.dlq` whose body equals the incoming body byte
 * for byte and which carries no `dead_letter` block, then the message is acknowledged, `unsettled` = neither
 * acknowledged nor retried (negative acknowledgement). In every row the retry producer is never used.
 *
 * `version_gate_sqs` and `version_gate_artemis` belong to packages with a consumer; the `php-sdk` SQS and STOMP
 * transports only produce, so each of their cases is a `skip` citing ADR-0037. `version_gate_asb` is absent from
 * the results: this package has no Azure Service Bus binding (ADR-0021).
 */
final class VersionGateConformanceTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const QUEUE = 'orders';

    public function test_version_gate_pulsar_matches_the_golden_cases(): void
    {
        $bodies = ConformanceResults::manifest()['version_gate_pulsar']['incoming_bodies'];
        self::assertNotEmpty($bodies);

        ConformanceResults::each('version_gate_pulsar', function (array $case) use ($bodies): void {
            foreach ($bodies as $incoming) {
                $this->runCase($case, (string) $incoming['body'], (string) $incoming['name']);
            }
        });
    }

    public function test_the_producer_only_bindings_skip_with_an_adr(): void
    {
        self::assertFalse(class_exists('BabelQueue\\Transport\\SqsConsumer'), 'a core SQS consumer must run version_gate_sqs instead of skipping it');
        self::assertFalse(method_exists(SqsTransport::class, 'receive'), 'SqsTransport::receive must run version_gate_sqs instead of skipping it');
        self::assertFalse(class_exists('BabelQueue\\Transport\\StompConsumer'), 'a core Artemis consumer must run version_gate_artemis instead of skipping it');
        self::assertFalse(method_exists(StompTransport::class, 'receive'), 'StompTransport::receive must run version_gate_artemis instead of skipping it');

        ConformanceResults::skipAll('version_gate_sqs', 'ADR-0037: the php-sdk SQS transport only produces and has no consumer to settle a version-gated delivery with (ADR-0010); consumption runs in the framework workers');
        ConformanceResults::skipAll('version_gate_artemis', 'ADR-0037: the php-sdk STOMP transport only produces and has no consumer to settle a version-gated delivery with (ADR-0018); consumption runs in the framework workers');

        foreach (['version_gate_sqs', 'version_gate_artemis'] as $section) {
            $ids = array_keys(ConformanceResults::cases($section));
            self::assertNotEmpty($ids);
            foreach ($ids as $id) {
                $recorded = ConformanceResults::recorded()[$section][(string) $id] ?? null;
                self::assertSame('skip', $recorded['status'] ?? null, $section . '/' . $id);
                self::assertMatchesRegularExpression('/ADR-0037/', $recorded['reason'] ?? '', $section . '/' . $id . ' cites ADR-0037');
                self::assertArrayNotHasKey($section . '/' . $id, ConformanceResults::knownGaps(), 'a skipped case is not a known gap');
            }
        }
    }

    /**
     * @param  array<string, mixed>  $case
     */
    private function runCase(array $case, string $body, string $bodyName): void
    {
        $label = $case['name'] . ' / ' . $bodyName;
        $expect = $case['expect'];
        $dlqOn = $case['dead_letter'] === 'on';
        $outcome = $dlqOn ? $expect['outcome'] : ($expect['outcome_no_writer'] ?? $expect['outcome']);
        $strategy = (string) ($case['on_unknown_urn'] ?? 'fail');
        $writeFails = ($case['dlq_write'] ?? 'ok') === 'fails';

        $properties = ['bq-schema-version' => '2', 'bq-job' => 'urn:babel:orders:created'];
        if (isset($case['bq_attempts'])) {
            $properties['bq-attempts'] = (string) $case['bq_attempts'];
        }

        $client = Mockery::mock(PulsarWebSocketConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn(
            ['messageId' => 'mid-1', 'payload' => $body, 'properties' => $properties, 'redeliveryCount' => 0],
        );

        $acked = 0;
        $nacked = 0;
        $client->shouldReceive('acknowledge')->andReturnUsing(static function () use (&$acked): void {
            $acked++;
        });
        $client->shouldReceive('negativeAcknowledge')->andReturnUsing(static function () use (&$nacked): void {
            $nacked++;
        });

        $written = [];
        $publisher = null;
        if ($dlqOn) {
            $transport = Mockery::mock(Transport::class);
            $transport->shouldReceive('publish')->andReturnUsing(static function (string $payload, ?string $queue) use (&$written, $writeFails): ?string {
                $written[] = [$payload, (string) $queue];
                if ($writeFails) {
                    throw new RuntimeException('broker down');
                }

                return null;
            });
            $publisher = new DeadLetterPublisher($transport);
        }

        // Exclusive: a retry would be a re-publish through this producer, which must never happen.
        $retryProducer = Mockery::mock(PulsarWebSocketClient::class);
        $retryProducer->shouldReceive('publish')->never();

        $handled = false;
        $logged = 0;
        $dispatcher = (new Dispatcher($strategy, 3, $publisher, onDrop: static function () use (&$logged): void {
            $logged++;
        }))->on('urn:babel:orders:created', static function () use (&$handled): void {
            $handled = true;
        });

        $stops = 0;
        (new PulsarConsumer(
            $client,
            queue: self::QUEUE,
            retryProducer: $retryProducer,
            tenant: 'public',
            namespace: 'default',
            subscriptionType: 'Exclusive',
        ))->consume($dispatcher, static function () use (&$stops): bool {
            return $stops++ >= 1;
        });

        self::assertFalse($handled, $label . ': a version-gated delivery never reaches a handler');

        switch ($outcome) {
            case 'ack':
                self::assertSame([1, 0, []], [$acked, $nacked, $written], $label);
                // Acknowledged and dropped, logged; the explicit `delete` strategy is the silent ack.
                self::assertSame($strategy === 'delete' ? 0 : 1, $logged, $label . ': the drop is logged unless the strategy is delete');
                break;

            case 'dlq_raw':
                self::assertSame('incoming_body', $expect['record'] ?? null, $label);
                self::assertSame([[$body, self::QUEUE . '.dlq']], $written, $label . ': the raw record, byte for byte, to the consumer\'s queue');
                self::assertSame([1, 0], [$acked, $nacked], $label);
                self::assertSame(0, $logged, $label);
                break;

            case 'unsettled':
                self::assertCount(1, $written, $label . ': the write was attempted once');
                self::assertSame([0, 1], [$acked, $nacked], $label . ': neither acknowledged nor retried');
                break;

            default:
                self::fail($label . ': the PHP Pulsar consumer has no settle for the outcome [' . $outcome . ']');
        }
    }
}
