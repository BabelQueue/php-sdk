<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Consume\DeadLetterPublisher;
use BabelQueue\Consume\Dispatcher;
use BabelQueue\Contracts\Transport;
use BabelQueue\Exceptions\UnknownUrnException;
use BabelQueue\Transport\PulsarConsumer;
use BabelQueue\Transport\PulsarMessage;
use BabelQueue\Transport\PulsarWebSocketConsumerClient;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * The framework-less Pulsar consumer (WebSocket consumer API): receive decodes the envelope and
 * reconciles §5 attempts; the consume loop acknowledges on success and releases (redelivers) when
 * the handler throws. Decoupled from the WebSocket library behind the seam (GR-7 intact).
 */
final class PulsarConsumerTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const ENVELOPE = '{"job":"urn:babel:orders:created","trace_id":"trace-1",'
        . '"data":{"order_id":1042},"meta":{"id":"msg-1","queue":"orders","lang":"php",'
        . '"schema_version":1,"created_at":1749132727000},"attempts":0}';

    /** @return array{messageId: string, payload: string, properties: array<string,string>, redeliveryCount: int} */
    private function raw(string $payload = self::ENVELOPE, int $redelivery = 0, array $properties = []): array
    {
        return ['messageId' => 'mid-1', 'payload' => $payload, 'properties' => $properties, 'redeliveryCount' => $redelivery];
    }

    private function envelopeWith(string $job, int $attempts = 0): string
    {
        return (string) json_encode([
            'job' => $job,
            'trace_id' => 'trace-1',
            'data' => [],
            'meta' => ['id' => 'm', 'queue' => 'orders', 'lang' => 'php', 'schema_version' => 1, 'created_at' => 1],
            'attempts' => $attempts,
        ]);
    }

    /** @param array<string, string> $properties */
    private function receiveWith(string $payload, array $properties, int $redelivery = 0, ?callable $hook = null): ?PulsarMessage
    {
        $client = Mockery::mock(PulsarWebSocketConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw($payload, $redelivery, $properties));

        return (new PulsarConsumer($client, $hook))->receive();
    }

    public function test_receive_decodes_the_envelope_and_exposes_the_inbound_view(): void
    {
        $client = Mockery::mock(PulsarWebSocketConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw());

        $message = (new PulsarConsumer($client))->receive();

        $this->assertInstanceOf(PulsarMessage::class, $message);
        $this->assertSame('urn:babel:orders:created', $message->getUrn());
        $this->assertSame('trace-1', $message->getTraceId());
        $this->assertSame(['order_id' => 1042], $message->getData());
        $this->assertSame('php', $message->getMeta()['lang']);
        $this->assertSame('mid-1', $message->messageId());
        $this->assertSame(0, $message->attempts());
        $this->assertSame('urn:babel:orders:created', $message->envelope()['job']);
    }

    public function test_receive_reconciles_attempts_to_the_redelivery_count(): void
    {
        $client = Mockery::mock(PulsarWebSocketConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw(redelivery: 3));

        $message = (new PulsarConsumer($client))->receive();

        $this->assertSame(3, $message->attempts()); // max(body 0, redelivery 3)
    }

    public function test_receive_returns_null_when_no_message(): void
    {
        $client = Mockery::mock(PulsarWebSocketConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturnNull();

        $this->assertNull((new PulsarConsumer($client))->receive());
    }

    public function test_consume_acknowledges_after_a_successful_handler(): void
    {
        $client = Mockery::mock(PulsarWebSocketConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw());
        $client->shouldReceive('acknowledge')->once()->with('mid-1');

        $handled = null;
        $calls = 0;
        (new PulsarConsumer($client))->consume(
            function (PulsarMessage $m) use (&$handled): void {
                $handled = $m->getUrn();
            },
            function () use (&$calls): bool {
                return $calls++ >= 1; // run exactly one iteration
            },
        );

        $this->assertSame('urn:babel:orders:created', $handled);
    }

    public function test_consume_releases_when_the_handler_throws(): void
    {
        $client = Mockery::mock(PulsarWebSocketConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw());
        $client->shouldReceive('negativeAcknowledge')->once()->with('mid-1');
        $client->shouldNotReceive('acknowledge');

        $calls = 0;
        (new PulsarConsumer($client))->consume(
            function (PulsarMessage $m): void {
                throw new RuntimeException('boom');
            },
            function () use (&$calls): bool {
                return $calls++ >= 1;
            },
        );

        $this->addToAssertionCount(1);
    }

    public function test_consume_skips_idle_receives(): void
    {
        $client = Mockery::mock(PulsarWebSocketConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturnNull();
        $client->shouldNotReceive('acknowledge');

        $handlerCalls = 0;
        $calls = 0;
        (new PulsarConsumer($client))->consume(
            function (PulsarMessage $m) use (&$handlerCalls): void {
                $handlerCalls++;
            },
            function () use (&$calls): bool {
                return $calls++ >= 1;
            },
        );

        $this->assertSame(0, $handlerCalls); // null receive → handler never called
    }

    public function test_receive_carries_the_raw_body_for_the_dead_letter_path(): void
    {
        $client = Mockery::mock(PulsarWebSocketConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw());

        $this->assertSame(self::ENVELOPE, (new PulsarConsumer($client))->receive()->rawBody());
    }

    public function test_bq_job_property_routes_even_when_the_body_names_a_different_urn(): void
    {
        $message = $this->receiveWith(
            $this->envelopeWith('urn:babel:orders:refunded'),
            ['bq-job' => 'urn:babel:orders:created'],
        );

        $this->assertSame('urn:babel:orders:created', $message->getUrn());
        // the body's envelope is untouched
        $this->assertSame('urn:babel:orders:refunded', $message->envelope()['job']);
    }

    public function test_bq_job_property_is_trimmed_and_drives_the_dispatcher(): void
    {
        $client = Mockery::mock(PulsarWebSocketConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn(
            $this->raw($this->envelopeWith('urn:babel:orders:refunded'), 0, ['bq-job' => '  urn:babel:orders:created  ']),
        );
        $client->shouldReceive('acknowledge')->once()->with('mid-1');

        $routed = [];
        $dispatcher = (new Dispatcher())
            ->on('urn:babel:orders:created', function (PulsarMessage $m) use (&$routed): void {
                $routed[] = 'property:' . $m->envelope()['job'];
            })
            ->on('urn:babel:orders:refunded', function () use (&$routed): void {
                $routed[] = 'body';
            });

        $calls = 0;
        (new PulsarConsumer($client))->consume($dispatcher, function () use (&$calls): bool {
            return $calls++ >= 1;
        });

        $this->assertSame(['property:urn:babel:orders:refunded'], $routed);
    }

    public function test_missing_or_blank_bq_job_falls_back_to_the_body_urn(): void
    {
        foreach ([[], ['bq-job' => ''], ['bq-job' => "  \t "]] as $properties) {
            $message = $this->receiveWith(self::ENVELOPE, $properties);

            $this->assertSame('urn:babel:orders:created', $message->getUrn(), json_encode($properties));
        }
    }

    public function test_a_body_without_a_job_is_not_routed_even_when_bq_job_is_set(): void
    {
        $noJob = (string) json_encode([
            'trace_id' => 'trace-1',
            'data' => [],
            'meta' => ['id' => 'm', 'queue' => 'orders', 'lang' => 'php', 'schema_version' => 1, 'created_at' => 1],
            'attempts' => 0,
        ]);

        $mismatches = 0;
        $message = $this->receiveWith($noJob, ['bq-job' => 'urn:babel:orders:created'], 0, function () use (&$mismatches): void {
            $mismatches++;
        });

        $this->assertSame('', $message->getUrn()); // the property cannot stand in for a missing job
        $this->assertSame(0, $mismatches);

        // ... so the dispatcher does not run the property's handler: the message is released.
        $client = Mockery::mock(PulsarWebSocketConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn($this->raw($noJob, 0, ['bq-job' => 'urn:babel:orders:created']));
        $client->shouldReceive('negativeAcknowledge')->once()->with('mid-1');
        $client->shouldNotReceive('acknowledge');

        $handled = 0;
        $dispatcher = (new Dispatcher())->on('urn:babel:orders:created', function () use (&$handled): void {
            $handled++;
        });
        $calls = 0;
        (new PulsarConsumer($client))->consume($dispatcher, function () use (&$calls): bool {
            return $calls++ >= 1;
        });

        $this->assertSame(0, $handled);
    }

    public function test_routing_mismatch_hook_receives_both_urns_and_the_body_envelope(): void
    {
        $seen = [];
        $message = $this->receiveWith(
            $this->envelopeWith('urn:babel:orders:refunded'),
            ['bq-job' => ' urn:babel:orders:created '],
            0,
            function (string $propertyUrn, string $bodyUrn, array $envelope) use (&$seen): void {
                $seen[] = [$propertyUrn, $bodyUrn, $envelope['job']];
            },
        );

        $this->assertNotNull($message);
        $this->assertSame([['urn:babel:orders:created', 'urn:babel:orders:refunded', 'urn:babel:orders:refunded']], $seen);
    }

    public function test_routing_mismatch_hook_sees_the_reconciled_attempts(): void
    {
        $seen = [];
        $this->receiveWith(
            $this->envelopeWith('urn:babel:orders:refunded'),
            ['bq-job' => 'urn:babel:orders:created', 'bq-attempts' => '4'],
            0,
            function (string $propertyUrn, string $bodyUrn, array $envelope) use (&$seen): void {
                $seen[] = $envelope['attempts'];
            },
        );

        $this->assertSame([4], $seen);
    }

    public function test_routing_mismatch_hook_is_not_called_without_a_mismatch(): void
    {
        $called = 0;
        $hook = function () use (&$called): void {
            $called++;
        };

        $this->receiveWith(self::ENVELOPE, ['bq-job' => 'urn:babel:orders:created'], 0, $hook);
        $this->receiveWith(self::ENVELOPE, ['bq-job' => ' urn:babel:orders:created '], 0, $hook);
        $this->receiveWith(self::ENVELOPE, [], 0, $hook);
        $this->receiveWith(self::ENVELOPE, ['bq-job' => '   '], 0, $hook);

        $this->assertSame(0, $called);
    }

    public function test_a_throwing_routing_mismatch_hook_still_routes_and_acknowledges(): void
    {
        $client = Mockery::mock(PulsarWebSocketConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn(
            $this->raw($this->envelopeWith('urn:babel:orders:refunded'), 0, ['bq-job' => 'urn:babel:orders:created']),
        );
        $client->shouldReceive('acknowledge')->once()->with('mid-1');
        $client->shouldNotReceive('negativeAcknowledge');

        $handled = null;
        $consumer = new PulsarConsumer($client, function (): void {
            throw new RuntimeException('hook failure');
        });
        $calls = 0;
        $consumer->consume(
            function (PulsarMessage $m) use (&$handled): void {
                $handled = $m->getUrn();
            },
            function () use (&$calls): bool {
            return $calls++ >= 1;
        },
        );

        $this->assertSame('urn:babel:orders:created', $handled);
    }

    /** @return array<string, array{0: string, 1: int, 2: int, 3: int}> */
    public static function authoritativeAttempts(): array
    {
        return [
            'property replaces body' => ['3', 5, 0, 3],
            'property raises body' => ['4', 1, 0, 4],
            'redelivery raises property' => ['2', 0, 6, 6],
            'property above redelivery' => ['7', 0, 2, 7],
            'leading zeros are digits' => ['007', 0, 0, 7],
            'zero property lowers body' => ['0', 5, 0, 0],
            'int32 max is valid' => ['2147483647', 0, 0, 2147483647],
        ];
    }

    #[DataProvider('authoritativeAttempts')]
    public function test_bq_attempts_is_authoritative(string $property, int $bodyAttempts, int $redelivery, int $expected): void
    {
        $message = $this->receiveWith(
            $this->envelopeWith('urn:babel:orders:created', $bodyAttempts),
            ['bq-attempts' => $property],
            $redelivery,
        );

        $this->assertSame($expected, $message->attempts());
    }

    /** @return array<string, array{0: mixed}> */
    public static function invalidAttempts(): array
    {
        return [
            'empty' => [''],
            'space' => [' '],
            'letters' => ['abc'],
            'negative' => ['-1'],
            'plus sign' => ['+3'],
            'negative zero' => ['-0'],
            'decimal' => ['3.5'],
            'exponent' => ['1e2'],
            'leading space' => [' 3'],
            'trailing space' => ['3 '],
            'trailing newline' => ["3\n"],
            'garbage suffix' => [' 3x'],
            'int32 overflow' => ['2147483648'],
            'huge' => ['99999999999999999999'],
            'not a string' => [3],
        ];
    }

    #[DataProvider('invalidAttempts')]
    public function test_an_invalid_bq_attempts_falls_back_to_the_body(mixed $value): void
    {
        $message = $this->receiveWith(
            $this->envelopeWith('urn:babel:orders:created', 5),
            ['bq-attempts' => $value],
        );

        $this->assertSame(5, $message->attempts());

        // the redelivery floor still applies on top of the fallback
        $message = $this->receiveWith(
            $this->envelopeWith('urn:babel:orders:created', 5),
            ['bq-attempts' => $value],
            9,
        );
        $this->assertSame(9, $message->attempts());
    }

    public function test_a_missing_bq_attempts_falls_back_to_the_body(): void
    {
        $message = $this->receiveWith($this->envelopeWith('urn:babel:orders:created', 5), []);

        $this->assertSame(5, $message->attempts());
    }

    // -- bq-schema-version gate (§5.7 step 3) --------------------------------

    public function test_a_missing_or_blank_schema_version_decodes_normally(): void
    {
        // blank = empty or only ASCII whitespace (space, \t, \n, \v, \f, \r), identical in every SDK
        $blanks = ['', '  ', "\x0B", "\f", " \t\n\x0B\f\r"];
        $cases = [[]];
        foreach ($blanks as $blank) {
            $cases[] = ['bq-schema-version' => $blank];
        }

        foreach ($cases as $properties) {
            $message = $this->receiveWith(self::ENVELOPE, $properties);

            $this->assertSame('urn:babel:orders:created', $message->getUrn());
            $this->assertSame('urn:babel:orders:created', $message->envelope()['job']);
        }
    }

    public function test_a_supported_schema_version_is_delivered(): void
    {
        $message = $this->receiveWith(self::ENVELOPE, ['bq-schema-version' => '1', 'bq-attempts' => '3']);

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

        $message = $this->receiveWith($garbage, ['bq-schema-version' => $value, 'bq-job' => 'urn:babel:orders:created'], 2);

        $this->assertSame(PulsarConsumer::UNSUPPORTED_SCHEMA_URN_PREFIX . $value, $message->getUrn());
        $this->assertSame($garbage, $message->rawBody());
        $this->assertSame(['raw' => $garbage, 'attempts' => 2], $message->envelope());
        $this->assertSame(2, $message->attempts());
        $this->assertSame('mid-1', $message->messageId());
    }

    public function test_a_gated_message_does_not_call_the_routing_mismatch_hook(): void
    {
        $calls = 0;
        $this->receiveWith(
            $this->envelopeWith('urn:babel:orders:refunded'),
            ['bq-schema-version' => '2', 'bq-job' => 'urn:babel:orders:created'],
            0,
            function () use (&$calls): void {
                $calls++;
            },
        );

        $this->assertSame(0, $calls);
    }

    public function test_a_gated_message_never_reaches_a_handler_and_releases_via_the_unknown_urn_path(): void
    {
        $client = Mockery::mock(PulsarWebSocketConsumerClient::class);
        $client->shouldReceive('receive')->once()->andReturn(
            $this->raw('{not json', 0, ['bq-schema-version' => '2', 'bq-job' => 'urn:babel:orders:created']),
        );
        $client->shouldReceive('negativeAcknowledge')->once()->with('mid-1');
        $client->shouldReceive('acknowledge')->never();

        $handled = false;
        $dispatcher = (new Dispatcher())->on('urn:babel:orders:created', function () use (&$handled): void {
            $handled = true;
        });

        $calls = 0;
        (new PulsarConsumer($client))->consume(function (PulsarMessage $m) use ($dispatcher): void {
            try {
                $dispatcher($m);
            } catch (UnknownUrnException $e) {
                $this->assertStringContainsString('babelqueue:unsupported-schema-version:2', $e->getMessage());

                throw $e; // consume() releases on a throwing handler
            }
        }, function () use (&$calls): bool {
            return $calls++ >= 1;
        });

        $this->assertFalse($handled);
    }

    public function test_a_gated_message_is_dead_lettered_with_its_original_bytes(): void
    {
        $body = $this->envelopeWith('urn:babel:orders:created');
        $message = $this->receiveWith($body, ['bq-schema-version' => 'x']);

        $published = [];
        $transport = Mockery::mock(Transport::class);
        $transport->shouldReceive('publish')->once()->andReturnUsing(function (string $payload, string $queue) use (&$published): void {
            $published = [json_decode($payload, true), $queue, $payload];
        });

        (new Dispatcher('dead_letter', 0, new DeadLetterPublisher($transport)))($message);

        $this->assertSame($body, $published[0]['raw']);
        $this->assertSame('unknown_urn', $published[0]['dead_letter']['reason']);
        // The sentinel URN is an in-process marker: the dead-letter record never carries it.
        $this->assertStringNotContainsString('"job":"babelqueue:unsupported-schema-version', $published[2]);
        $this->assertArrayNotHasKey('job', $published[0]);
        // The gate does not decode the body, so `meta.queue` is unknown and the record goes to the
        // default dead-letter queue rather than `<queue>.dlq` (documented in the CHANGELOG).
        $this->assertSame('default.dlq', $published[1]);
    }
}
