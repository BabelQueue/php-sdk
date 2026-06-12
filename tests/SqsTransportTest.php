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
}
