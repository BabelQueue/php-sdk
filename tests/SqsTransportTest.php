<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Transport\SqsClient;
use BabelQueue\Transport\SqsTransport;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;

/**
 * The framework-less Amazon SQS producer: the canonical envelope as the message
 * body, projected onto native SQS MessageAttributes (bq-job/bq-trace-id/... + the
 * Number-typed schema-version/created-at) so a non-PHP consumer can route and trace
 * without decoding the body.
 */
final class SqsTransportTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const URL = 'https://sqs.eu-central-1.amazonaws.com/123456789012/orders';

    private const ENVELOPE = '{"job":"urn:babel:orders:created","trace_id":"trace-1",'
        .'"data":{"order_id":1042},"meta":{"id":"msg-1","queue":"orders","lang":"php",'
        .'"schema_version":1,"created_at":1749132727000},"attempts":0}';

    public function test_publish_projects_contract_attributes(): void
    {
        $captured = null;

        $client = Mockery::mock(SqsClient::class);
        $client->shouldReceive('sendMessage')->once()->with(
            Mockery::on(function (array $args) use (&$captured): bool {
                $captured = $args;

                return true;
            }),
        );

        $id = (new SqsTransport($client, self::URL))->publish(self::ENVELOPE);

        $this->assertSame('msg-1', $id);
        $this->assertSame(self::URL, $captured['QueueUrl']);
        $this->assertSame(self::ENVELOPE, $captured['MessageBody']);

        $attrs = $captured['MessageAttributes'];
        $this->assertSame(['DataType' => 'String', 'StringValue' => 'urn:babel:orders:created'], $attrs['bq-job']);
        $this->assertSame(['DataType' => 'String', 'StringValue' => 'trace-1'], $attrs['bq-trace-id']);
        $this->assertSame(['DataType' => 'String', 'StringValue' => 'msg-1'], $attrs['bq-message-id']);
        $this->assertSame(['DataType' => 'Number', 'StringValue' => '1'], $attrs['bq-schema-version']);
        $this->assertSame(['DataType' => 'String', 'StringValue' => 'php'], $attrs['bq-source-lang']);
        $this->assertSame(['DataType' => 'Number', 'StringValue' => '1749132727000'], $attrs['bq-created-at']);
        $this->assertArrayNotHasKey('MessageGroupId', $captured);
    }

    public function test_publish_uses_the_queue_override_url(): void
    {
        $other = 'https://sqs.eu-central-1.amazonaws.com/123456789012/billing';
        $captured = null;

        $client = Mockery::mock(SqsClient::class);
        $client->shouldReceive('sendMessage')->once()->with(
            Mockery::on(function (array $args) use (&$captured): bool {
                $captured = $args;

                return true;
            }),
        );

        (new SqsTransport($client, self::URL))->publish(self::ENVELOPE, $other);

        $this->assertSame($other, $captured['QueueUrl']);
    }

    public function test_fifo_sets_group_and_dedup(): void
    {
        $captured = null;

        $client = Mockery::mock(SqsClient::class);
        $client->shouldReceive('sendMessage')->once()->with(
            Mockery::on(function (array $args) use (&$captured): bool {
                $captured = $args;

                return true;
            }),
        );

        (new SqsTransport($client, self::URL.'.fifo', fifo: true))->publish(self::ENVELOPE);

        $this->assertSame('orders.fifo', $captured['MessageGroupId']);
        $this->assertSame('msg-1', $captured['MessageDeduplicationId']);
    }

    public function test_content_dedup_omits_dedup_id(): void
    {
        $captured = null;

        $client = Mockery::mock(SqsClient::class);
        $client->shouldReceive('sendMessage')->once()->with(
            Mockery::on(function (array $args) use (&$captured): bool {
                $captured = $args;

                return true;
            }),
        );

        (new SqsTransport($client, self::URL.'.fifo', fifo: true, messageGroupId: 'grp', contentDedup: true))
            ->publish(self::ENVELOPE);

        $this->assertSame('grp', $captured['MessageGroupId']);
        $this->assertArrayNotHasKey('MessageDeduplicationId', $captured);
    }

    public function test_publish_with_headers_carries_traceparent_beside_the_contract_attributes(): void
    {
        $captured = $this->captureWithHeaders([
            'traceparent' => '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01',
        ]);

        // Body unchanged (GR-1); the rider rides as an extra String MessageAttribute.
        $this->assertSame(self::ENVELOPE, $captured['MessageBody']);
        $attrs = $captured['MessageAttributes'];
        $this->assertSame(
            ['DataType' => 'String', 'StringValue' => '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'],
            $attrs['traceparent'],
        );
        // The contract bq-* attributes are untouched.
        $this->assertSame(['DataType' => 'String', 'StringValue' => 'trace-1'], $attrs['bq-trace-id']);
        $this->assertSame(['DataType' => 'String', 'StringValue' => 'urn:babel:orders:created'], $attrs['bq-job']);
    }

    public function test_publish_with_headers_never_clobbers_a_contract_attribute(): void
    {
        // A rider keyed bq-trace-id must not overwrite the contract attribute.
        $captured = $this->captureWithHeaders(['bq-trace-id' => 'rider-should-lose', 'traceparent' => '00-abc']);

        $attrs = $captured['MessageAttributes'];
        $this->assertSame(['DataType' => 'String', 'StringValue' => 'trace-1'], $attrs['bq-trace-id']); // contract wins
        $this->assertSame(['DataType' => 'String', 'StringValue' => '00-abc'], $attrs['traceparent']);
    }

    public function test_publish_with_headers_respects_the_ten_attribute_cap(): void
    {
        // The envelope already projects 6 contract attributes; only 4 rider slots remain (cap 10).
        $riders = [];
        for ($i = 1; $i <= 8; $i++) {
            $riders["rider-$i"] = "v$i";
        }

        $captured = $this->captureWithHeaders($riders);
        $attrs = $captured['MessageAttributes'];

        $this->assertCount(10, $attrs);
        // All 6 contract attributes survive (they are seeded first and never dropped).
        foreach (['bq-job', 'bq-trace-id', 'bq-message-id', 'bq-schema-version', 'bq-source-lang', 'bq-created-at'] as $key) {
            $this->assertArrayHasKey($key, $attrs);
        }
        // Exactly 4 riders fit into the remaining headroom.
        $riderKeys = array_filter(array_keys($attrs), static fn (string $k): bool => str_starts_with($k, 'rider-'));
        $this->assertCount(4, $riderKeys);
    }

    /**
     * @param  array<string, string>  $headers
     * @return array<string, mixed>
     */
    private function captureWithHeaders(array $headers): array
    {
        $captured = null;

        $client = Mockery::mock(SqsClient::class);
        $client->shouldReceive('sendMessage')->once()->with(
            Mockery::on(function (array $args) use (&$captured): bool {
                $captured = $args;

                return true;
            }),
        );

        (new SqsTransport($client, self::URL))->publishWithHeaders(self::ENVELOPE, $headers);

        $this->assertIsArray($captured);

        return $captured;
    }
}
