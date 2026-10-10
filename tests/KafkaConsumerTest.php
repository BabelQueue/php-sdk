<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Consume\DeadLetterPublisher;
use BabelQueue\Consume\Dispatcher;
use BabelQueue\Contracts\Transport;
use BabelQueue\Exceptions\UnknownUrnException;
use BabelQueue\Transport\KafkaConsumer;
use BabelQueue\Transport\KafkaConsumerClient;
use BabelQueue\Transport\KafkaMessage;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * The framework-less Kafka consumer (ext-rdkafka): receive decodes the record and reconciles §6
 * attempts (the `bq-attempts` header is authoritative); the consume loop commits the offset on
 * success (process-then-commit) and leaves it uncommitted when the handler throws. Decoupled from
 * `ext-rdkafka` behind the seam.
 */
final class KafkaConsumerTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const ENVELOPE = '{"job":"urn:babel:orders:created","trace_id":"trace-1",'
        . '"data":{"order_id":1042},"meta":{"id":"msg-1","queue":"orders","lang":"php",'
        . '"schema_version":1,"created_at":1749132727000},"attempts":0}';

    /**
     * @param  array<string, string>  $headers
     * @return array{payload: string, headers: array<string, string>}
     */
    private function raw(array $headers = [], string $payload = self::ENVELOPE): array
    {
        return ['payload' => $payload, 'headers' => $headers];
    }

    public function test_receive_decodes_the_envelope_and_exposes_the_inbound_view(): void
    {
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw(['bq-attempts' => '0']));

        $message = (new KafkaConsumer($client))->receive();

        $this->assertInstanceOf(KafkaMessage::class, $message);
        $this->assertSame('urn:babel:orders:created', $message->getUrn());
        $this->assertSame('trace-1', $message->getTraceId());
        $this->assertSame(['order_id' => 1042], $message->getData());
        $this->assertSame('php', $message->getMeta()['lang']);
        $this->assertSame('urn:babel:orders:created', $message->envelope()['job']);
    }

    public function test_receive_uses_the_authoritative_header_attempts_over_the_body(): void
    {
        // body attempts 0, header 4 → header wins.
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw(['bq-attempts' => '4']));

        $this->assertSame(4, (new KafkaConsumer($client))->receive()->attempts());
    }

    public function test_receive_falls_back_to_the_body_attempts_without_a_header(): void
    {
        $body = '{"job":"urn:babel:x","trace_id":"t","data":{},"meta":{"id":"m","queue":"q",'
            . '"lang":"php","schema_version":1,"created_at":1},"attempts":3}';

        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw([], $body)); // no bq-attempts header

        $this->assertSame(3, (new KafkaConsumer($client))->receive()->attempts());
    }

    public function test_receive_returns_null_when_no_record(): void
    {
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturnNull();

        $this->assertNull((new KafkaConsumer($client))->receive());
    }

    public function test_receive_surfaces_the_raw_bq_headers_for_the_retry_machinery(): void
    {
        // The §6.4/§6.5 retry-topic machinery reads bq-original-topic off the message to route a
        // retry record back to its work topic across hops.
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn(
            $this->raw(['bq-attempts' => '2', 'bq-original-topic' => 'orders']),
        );

        $message = (new KafkaConsumer($client))->receive();

        $this->assertInstanceOf(KafkaMessage::class, $message);
        $this->assertSame('orders', $message->header('bq-original-topic'));
        $this->assertSame('2', $message->headers()['bq-attempts']);
        $this->assertNull($message->header('bq-missing'));
    }

    public function test_consume_commits_after_a_successful_handler(): void
    {
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw(['bq-attempts' => '0']));
        $client->shouldReceive('commit')->once();

        $handled = null;
        $calls = 0;
        (new KafkaConsumer($client))->consume(
            function (KafkaMessage $m) use (&$handled): void {
                $handled = $m->getUrn();
            },
            function () use (&$calls): bool {
                return $calls++ >= 1;
            },
        );

        $this->assertSame('urn:babel:orders:created', $handled);
    }

    public function test_consume_does_not_commit_when_the_handler_throws(): void
    {
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw(['bq-attempts' => '0']));
        $client->shouldNotReceive('commit');

        $calls = 0;
        (new KafkaConsumer($client))->consume(
            function (KafkaMessage $m): void {
                throw new RuntimeException('boom');
            },
            function () use (&$calls): bool {
                return $calls++ >= 1;
            },
        );

        $this->addToAssertionCount(1);
    }

    public function test_consume_skips_idle_polls(): void
    {
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturnNull();
        $client->shouldNotReceive('commit');

        $handlerCalls = 0;
        $calls = 0;
        (new KafkaConsumer($client))->consume(
            function (KafkaMessage $m) use (&$handlerCalls): void {
                $handlerCalls++;
            },
            function () use (&$calls): bool {
                return $calls++ >= 1;
            },
        );

        $this->assertSame(0, $handlerCalls);
    }

    public function test_receive_carries_the_raw_body_for_the_dead_letter_path(): void
    {
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw(['bq-attempts' => '0']));

        $this->assertSame(self::ENVELOPE, (new KafkaConsumer($client))->receive()->rawBody());
    }

    // -- bq-schema-version gate (§6.7) ---------------------------------------

    public function test_a_missing_or_blank_schema_version_decodes_normally(): void
    {
        // blank = empty or only ASCII whitespace (space, \t, \n, \v, \f, \r), identical in every SDK
        $blanks = ['', '  ', "\x0B", "\f", " \t\n\x0B\f\r"];
        $cases = [[]];
        foreach ($blanks as $blank) {
            $cases[] = ['bq-schema-version' => $blank];
        }

        foreach ($cases as $headers) {
            $client = Mockery::mock(KafkaConsumerClient::class);
            $client->shouldReceive('receive')->once()->andReturn($this->raw($headers));

            $message = (new KafkaConsumer($client))->receive();

            $this->assertSame('urn:babel:orders:created', $message->getUrn());
            $this->assertSame('urn:babel:orders:created', $message->envelope()['job']);
        }
    }

    public function test_a_supported_schema_version_is_delivered(): void
    {
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw(['bq-schema-version' => '1', 'bq-attempts' => '3']));

        $message = (new KafkaConsumer($client))->receive();

        $this->assertSame('urn:babel:orders:created', $message->getUrn());
        $this->assertSame(3, $message->attempts());
    }

    /** @return array<string, array{string}> */
    public static function unsupportedSchemaVersions(): array
    {
        return ['2' => ['2'], 'non-numeric' => ['x'], 'leading zero' => ['01'], 'padded' => [' 1'], 'decimal' => ['1.0'], 'negative' => ['-1'],
            // not ASCII whitespace, hence not blank
            'nbsp' => ["\u{00A0}"], 'U+001C' => ["\u{001C}"], 'U+0085' => ["\u{0085}"], 'BOM' => ["\u{FEFF}"], 'ideographic space' => ["\u{3000}"], 'NUL' => ["\0"]];
    }

    #[DataProvider('unsupportedSchemaVersions')]
    public function test_an_unsupported_schema_version_is_gated_before_decode(string $value): void
    {
        // The body is not JSON: had it been decoded the routing URN would be empty (and the envelope
        // empty). The sentinel URN and the untouched raw body prove the gate ran first.
        $garbage = '{not json';

        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn(
            $this->raw(['bq-schema-version' => $value, 'bq-job' => 'urn:babel:orders:created', 'bq-attempts' => '2'], $garbage),
        );

        $message = (new KafkaConsumer($client))->receive();

        $this->assertSame(KafkaConsumer::UNSUPPORTED_SCHEMA_URN_PREFIX . $value, $message->getUrn());
        $this->assertSame($garbage, $message->rawBody());
        $this->assertSame(['raw' => $garbage, 'attempts' => 2], $message->envelope());
        $this->assertSame(2, $message->attempts());
    }

    public function test_a_gated_message_never_reaches_a_handler_and_stays_uncommitted(): void
    {
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn(
            $this->raw(['bq-schema-version' => '2', 'bq-job' => 'urn:babel:orders:created'], '{not json'),
        );
        $client->shouldNotReceive('commit');

        $handled = false;
        $dispatcher = (new Dispatcher())->on('urn:babel:orders:created', function () use (&$handled): void {
            $handled = true;
        });

        $thrown = null;
        $calls = 0;
        (new KafkaConsumer($client))->consume(function (KafkaMessage $m) use ($dispatcher, &$thrown): void {
            try {
                $dispatcher($m);
            } catch (UnknownUrnException $e) {
                $thrown = $e;

                throw $e; // consume() leaves the record uncommitted on a throwing handler
            }
        }, function () use (&$calls): bool {
            return $calls++ >= 1;
        });

        $this->assertFalse($handled);
        $this->assertNotNull($thrown);
        $this->assertStringContainsString('babelqueue:unsupported-schema-version:2', $thrown->getMessage());
    }

    public function test_a_gated_message_is_dead_lettered_with_its_original_bytes(): void
    {
        $client = Mockery::mock(KafkaConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw(['bq-schema-version' => 'x']));
        $message = (new KafkaConsumer($client))->receive();

        $published = [];
        $transport = Mockery::mock(Transport::class);
        $transport->shouldReceive('publish')->once()->andReturnUsing(function (string $payload, string $queue) use (&$published): void {
            $published = [json_decode($payload, true), $queue, $payload];
        });

        (new Dispatcher('dead_letter', 0, new DeadLetterPublisher($transport)))($message);

        $this->assertSame(self::ENVELOPE, $published[0]['raw']);
        $this->assertSame('unknown_urn', $published[0]['dead_letter']['reason']);
        // The sentinel URN is an in-process marker: the dead-letter record never carries it.
        $this->assertStringNotContainsString('"job":"babelqueue:unsupported-schema-version', $published[2]);
        $this->assertArrayNotHasKey('job', $published[0]);
        // The gate does not decode the body, so `meta.queue` is unknown and the record goes to the
        // default dead-letter queue rather than `<queue>.dlq`.
        $this->assertSame('default.dlq', $published[1]);
    }
}
