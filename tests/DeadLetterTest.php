<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\DeadLetter\DeadLetter;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class DeadLetterTest extends TestCase
{
    public function test_annotate_preserves_the_original_and_adds_the_block(): void
    {
        $envelope = [
            'job' => 'urn:babel:orders:created',
            'trace_id' => 't1',
            'data' => ['order_id' => 7],
            'meta' => ['id' => 'm1', 'queue' => 'orders'],
            'attempts' => 3,
        ];

        $out = DeadLetter::annotate($envelope, 'failed', new RuntimeException('boom'), 'orders', 3);

        // Original preserved verbatim.
        $this->assertSame('urn:babel:orders:created', $out['job']);
        $this->assertSame('t1', $out['trace_id']);
        $this->assertSame('m1', $out['meta']['id']);
        $this->assertSame(['order_id' => 7], $out['data']);

        // Additive block.
        $this->assertSame('failed', $out['dead_letter']['reason']);
        $this->assertSame('boom', $out['dead_letter']['error']);
        $this->assertSame(RuntimeException::class, $out['dead_letter']['exception']);
        $this->assertSame('orders', $out['dead_letter']['original_queue']);
        $this->assertSame(3, $out['dead_letter']['attempts']);
        $this->assertSame('php', $out['dead_letter']['lang']);
        $this->assertIsInt($out['dead_letter']['failed_at']);
    }

    public function test_annotate_without_exception(): void
    {
        $out = DeadLetter::annotate(['job' => 'u', 'data' => [], 'meta' => []], 'unknown_urn', null, 'orders', 1);

        $this->assertSame('unknown_urn', $out['dead_letter']['reason']);
        $this->assertNull($out['dead_letter']['error']);
        $this->assertNull($out['dead_letter']['exception']);
    }
}
