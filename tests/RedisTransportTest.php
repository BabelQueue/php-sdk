<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Transport\RedisTransport;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;
use Predis\ClientInterface;

/**
 * The framework-less Redis producer: a plain RPUSH onto the shared list, so the
 * envelope lands where every other SDK's reliable-queue consumer reserves it.
 */
final class RedisTransportTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    public function test_publish_rpushes_the_payload_onto_the_named_queue(): void
    {
        $client = Mockery::mock(ClientInterface::class);
        $client->shouldReceive('rpush')->once()->with('orders', ['{"job":"x"}']);

        $transport = new RedisTransport($client, 'default');

        $this->assertNull($transport->publish('{"job":"x"}', 'orders'));
    }

    public function test_publish_falls_back_to_the_default_queue(): void
    {
        $client = Mockery::mock(ClientInterface::class);
        $client->shouldReceive('rpush')->once()->with('default', ['payload']);

        (new RedisTransport($client, 'default'))->publish('payload');
    }
}
