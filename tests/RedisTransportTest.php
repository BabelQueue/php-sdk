<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Exceptions\BabelQueueException;
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

    public function test_keys_names_the_laravel_compatible_structures(): void
    {
        $this->assertSame([
            'ready' => 'queues:emails.high',
            'delayed' => 'queues:emails.high:delayed',
            'reserved' => 'queues:emails.high:reserved',
            'notify' => 'queues:emails.high:notify',
        ], RedisTransport::keys('emails.high'));

        $this->assertSame('app:orders:notify', RedisTransport::keys('orders', 'app:')['notify']);
    }

    public function test_laravel_layout_pushes_the_ready_list_and_the_notify_wakeup_atomically(): void
    {
        $client = Mockery::mock(ClientInterface::class);
        $client->shouldReceive('eval')
            ->once()
            ->with(Mockery::on(static fn (string $script): bool => str_contains($script, "rpush', KEYS[1], ARGV[1]")
                && str_contains($script, "rpush', KEYS[2], 1")), 2, 'queues:orders', 'queues:orders:notify', '{"job":"x"}');
        $client->shouldNotReceive('rpush');

        $transport = new RedisTransport($client, 'default', RedisTransport::LARAVEL_KEY_PREFIX);

        $this->assertNull($transport->publish('{"job":"x"}', 'orders'));
    }

    public function test_laravel_layout_uses_the_default_queue_and_frames_headers(): void
    {
        $client = Mockery::mock(ClientInterface::class);
        $client->shouldReceive('eval')
            ->once()
            ->with(Mockery::type('string'), 2, 'queues:default', 'queues:default:notify', RedisTransport::frameValue('p', ['traceparent' => 'x']));

        (new RedisTransport($client, 'default', 'queues:'))->publishWithHeaders('p', ['traceparent' => 'x']);
    }

    public function test_publish_delayed_zadds_to_the_delayed_set_with_an_integer_score_and_no_notify_token(): void
    {
        $client = Mockery::mock(ClientInterface::class);
        $client->shouldReceive('zadd')->once()->with('queues:orders:delayed', ['{"job":"x"}' => 1700000010]);
        $client->shouldNotReceive('rpush');
        $client->shouldNotReceive('eval');

        $transport = new RedisTransport($client, 'default', RedisTransport::LARAVEL_KEY_PREFIX, static fn (): float => 1700000000.9);

        $this->assertNull($transport->publishDelayed('{"job":"x"}', 9.2, 'orders'));
    }

    public function test_delayed_score_is_floor_now_plus_ceil_delay(): void
    {
        $this->assertSame(1700000011, RedisTransport::delayedScore(1700000000.999, 10.001));
        $this->assertSame(1700000001, RedisTransport::delayedScore(1700000000.5, 0.1));
        $this->assertSame(1700000000, RedisTransport::delayedScore(1700000000.5, 0));
        $this->assertSame(1700000000, RedisTransport::delayedScore(1700000000.5, -3));
    }

    public function test_publish_delayed_needs_the_laravel_layout(): void
    {
        $client = Mockery::mock(ClientInterface::class);
        $client->shouldNotReceive('zadd');

        $this->expectException(BabelQueueException::class);
        (new RedisTransport($client))->publishDelayed('x', 5);
    }
}
