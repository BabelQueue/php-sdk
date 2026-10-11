<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Redrive;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\DeadLetter\DeadLetter;
use BabelQueue\Redrive\Redrive;
use BabelQueue\Redrive\RedriveIO;
use BabelQueue\Redrive\RedriveOptions;
use BabelQueue\Redrive\RedriveRecoveryFailed;
use BabelQueue\Redrive\RewindingRedriveIO;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * A reserved message whose restore write back to the DLQ failed is never acknowledged and, on a
 * {@see RewindingRedriveIO}, is rewound (Kafka: a later cumulative commit must not skip it).
 */
final class RewindIO implements RewindingRedriveIO
{
    /** @var list<string> */
    public array $pending = [];

    /** @var list<mixed> */
    public array $acked = [];

    /** @var list<mixed> */
    public array $rewound = [];

    public ?RuntimeException $rewindFailure = null;

    public ?RuntimeException $ackFailure = null;

    /** @param  string|null  $failQueue  Writes to this queue throw; null = every write throws. */
    public function __construct(private readonly ?string $failQueue)
    {
    }

    public function pop(string $queue): ?array
    {
        $body = array_shift($this->pending);

        return $body === null ? null : ['body' => $body, 'handle' => 'h:' . md5($body)];
    }

    public function ack(mixed $handle): void
    {
        if ($this->ackFailure !== null) {
            throw $this->ackFailure;
        }
        $this->acked[] = $handle;
    }

    public function publish(string $queue, string $body): void
    {
        if ($this->failQueue === null || $queue === $this->failQueue) {
            throw new RuntimeException("write to {$queue} refused");
        }
    }

    public function rewind(mixed $handle): void
    {
        $this->rewound[] = $handle;
        if ($this->rewindFailure !== null) {
            throw $this->rewindFailure;
        }
    }
}

final class RewindTest extends TestCase
{
    private function deadLetterBody(): string
    {
        $env = EnvelopeCodec::make('urn:babel:orders:created', ['order_id' => 1], 'orders');

        return EnvelopeCodec::encode(DeadLetter::annotate($env, 'failed', new RuntimeException('boom'), 'orders', 3));
    }

    public function testFailedRestoreOfAnUndecodableBodyRewindsAndDoesNotAck(): void
    {
        $io = new RewindIO('orders.dlq');
        $io->pending[] = 'not an envelope';

        try {
            Redrive::run($io, 'orders.dlq');
            $this->fail('expected the restore failure to surface');
        } catch (RuntimeException $e) {
            $this->assertSame('write to orders.dlq refused', $e->getMessage());
        }

        $this->assertSame(['h:' . md5('not an envelope')], $io->rewound);
        $this->assertSame([], $io->acked, 'a message whose restore failed must not be acknowledged');
    }

    public function testFailedRestoreOfAnUnselectedMessageRewindsAndDoesNotAck(): void
    {
        $io = new RewindIO('orders.dlq');
        $io->pending[] = $this->deadLetterBody();

        $this->expectException(RuntimeException::class);

        try {
            Redrive::run($io, 'orders.dlq', new RedriveOptions(select: static fn (array $env): bool => false));
        } finally {
            $this->assertCount(1, $io->rewound);
            $this->assertSame([], $io->acked);
        }
    }

    public function testFailedRestoreOfADryRunMessageRewindsAndDoesNotAck(): void
    {
        $io = new RewindIO('orders.dlq');
        $io->pending[] = $this->deadLetterBody();

        $this->expectException(RuntimeException::class);

        try {
            Redrive::run($io, 'orders.dlq', new RedriveOptions(dryRun: true));
        } finally {
            $this->assertCount(1, $io->rewound);
            $this->assertSame([], $io->acked);
        }
    }

    public function testFailedRedriveWriteFollowedByFailedRestoreRewindsAndReportsBothErrors(): void
    {
        $io = new RewindIO(null); // the work-queue write and the restore write are both refused
        $io->pending[] = $this->deadLetterBody();

        try {
            Redrive::run($io, 'orders.dlq');
            $this->fail('expected the failure to surface');
        } catch (RedriveRecoveryFailed $e) {
            $this->assertSame(
                ['write to orders refused', 'write to orders.dlq refused'],
                array_map(static fn (\Throwable $t): string => $t->getMessage(), $e->getErrors()),
            );
        }

        $this->assertCount(1, $io->rewound);
        $this->assertSame([], $io->acked);
    }

    public function testFailedRewindIsReportedAlongsideTheRestoreError(): void
    {
        $io = new RewindIO('orders.dlq');
        $io->rewindFailure = new RuntimeException('seek failed');
        $io->pending[] = 'not an envelope';

        try {
            Redrive::run($io, 'orders.dlq');
            $this->fail('expected the failure to surface');
        } catch (RedriveRecoveryFailed $e) {
            $errors = $e->getErrors();
            $this->assertCount(2, $errors);
            $this->assertSame('write to orders.dlq refused', $errors[0]->getMessage());
            $this->assertSame($io->rewindFailure, $errors[1]);
            $this->assertSame($errors[0], $e->getPrevious());
        }

        $this->assertSame([], $io->acked, 'never acknowledged, even when the rewind fails too');
    }

    public function testFailedRedriveWriteWithASuccessfulRestoreStillRewindsTheRestOfTheBatch(): void
    {
        $io = new RewindIO('orders'); // the work queue refuses, the DLQ restore succeeds
        $first = $this->deadLetterBody();
        $io->pending = [$first, $this->deadLetterBody()];

        try {
            Redrive::run($io, 'orders.dlq');
            $this->fail('expected the publish failure to surface');
        } catch (RuntimeException $e) {
            $this->assertSame('write to orders refused', $e->getMessage());
            $this->assertNotInstanceOf(RedriveRecoveryFailed::class, $e, 'a lone publish error is thrown as is');
        }

        // The failed message was restored and acknowledged; the run stopped, so the rewind hands back the rest.
        $this->assertSame(['h:' . md5($first)], $io->acked);
        $this->assertSame(['h:' . md5($first)], $io->rewound);
    }

    public function testFailedRewindAfterASuccessfulRestoreIsReportedWithThePublishError(): void
    {
        $io = new RewindIO('orders');
        $io->rewindFailure = new RuntimeException('seek failed');
        $body = $this->deadLetterBody();
        $io->pending[] = $body;

        try {
            Redrive::run($io, 'orders.dlq');
            $this->fail('expected the failure to surface');
        } catch (RedriveRecoveryFailed $e) {
            $this->assertSame(
                ['write to orders refused', 'seek failed'],
                array_map(static fn (\Throwable $t): string => $t->getMessage(), $e->getErrors()),
            );
        }

        $this->assertSame(['h:' . md5($body)], $io->acked, 'the restored message was acknowledged');
    }

    public function testSuccessfulRestoreAndRedriveNeverRewind(): void
    {
        $rewinding = new class () implements RewindingRedriveIO {
            /** @var list<string> */
            public array $pending = [];

            public int $rewinds = 0;

            public int $acks = 0;

            public function pop(string $queue): ?array
            {
                $body = array_shift($this->pending);

                return $body === null ? null : ['body' => $body, 'handle' => null];
            }

            public function ack(mixed $handle): void
            {
                $this->acks++;
            }

            public function publish(string $queue, string $body): void
            {
            }

            public function rewind(mixed $handle): void
            {
                $this->rewinds++;
            }
        };
        $rewinding->pending = [$this->deadLetterBody(), 'not an envelope'];

        $result = Redrive::run($rewinding, 'orders.dlq');

        $this->assertSame(1, $result->redriven);
        $this->assertSame(1, $result->skipped);
        $this->assertSame(0, $rewinding->rewinds);
        $this->assertSame(2, $rewinding->acks);
    }

    public function testAnIoWithoutRewindKeepsLeavingAFailedRestoreUnacknowledged(): void
    {
        $io = new class () implements RedriveIO {
            /** @var list<string> */
            public array $pending = ['not an envelope'];

            public int $acks = 0;

            public function pop(string $queue): ?array
            {
                $body = array_shift($this->pending);

                return $body === null ? null : ['body' => $body, 'handle' => null];
            }

            public function ack(mixed $handle): void
            {
                $this->acks++;
            }

            public function publish(string $queue, string $body): void
            {
                throw new RuntimeException('write refused');
            }
        };

        try {
            Redrive::run($io, 'orders.dlq');
            $this->fail('expected the restore failure to surface');
        } catch (RuntimeException $e) {
            $this->assertSame('write refused', $e->getMessage());
        }

        $this->assertSame(0, $io->acks);
    }

    public function testAnIoWithoutRewindRethrowsTheRestoreErrorAsIsAfterAFailedPublishAndRestore(): void
    {
        $io = new class () implements RedriveIO {
            /** @var list<string> */
            public array $pending = [];

            public function pop(string $queue): ?array
            {
                $body = array_shift($this->pending);

                return $body === null ? null : ['body' => $body, 'handle' => null];
            }

            public function ack(mixed $handle): void
            {
            }

            public function publish(string $queue, string $body): void
            {
                throw new RuntimeException("write to {$queue} refused");
            }
        };
        $io->pending[] = $this->deadLetterBody();

        try {
            Redrive::run($io, 'orders.dlq');
            $this->fail('expected the failure to surface');
        } catch (RuntimeException $e) {
            $this->assertNotInstanceOf(RedriveRecoveryFailed::class, $e, 'the restore error keeps its type');
            $this->assertSame('write to orders.dlq refused', $e->getMessage());
        }
    }

    public function testThrowingSelectPredicateRewindsTheMessageAndTheRestOfTheBatch(): void
    {
        $io = new RewindIO('never-written');
        $first = $this->deadLetterBody();
        $io->pending = [$first, $this->deadLetterBody()];
        $boom = new RuntimeException('select exploded');

        try {
            Redrive::run($io, 'orders.dlq', new RedriveOptions(select: static function (array $env) use ($boom): bool {
                throw $boom;
            }));
            $this->fail('expected the select failure to surface');
        } catch (RuntimeException $e) {
            $this->assertSame($boom, $e);
        }

        $this->assertSame(['h:' . md5($first)], $io->rewound, 'rewound from the message that stopped the run');
        $this->assertSame([], $io->acked);
    }

    public function testFailedAckRewindsTheMessage(): void
    {
        $io = new RewindIO('never-written');
        $io->ackFailure = new RuntimeException('commit timed out');
        $body = $this->deadLetterBody();
        $io->pending[] = $body;

        try {
            Redrive::run($io, 'orders.dlq');
            $this->fail('expected the ack failure to surface');
        } catch (RuntimeException $e) {
            $this->assertSame($io->ackFailure, $e);
        }

        $this->assertSame(['h:' . md5($body)], $io->rewound);
    }

    public function testFailedPublishRestoreAndRewindAreReportedAsOneFlatList(): void
    {
        $io = new RewindIO(null);
        $io->rewindFailure = new RuntimeException('seek failed');
        $io->pending[] = $this->deadLetterBody();

        try {
            Redrive::run($io, 'orders.dlq');
            $this->fail('expected the failure to surface');
        } catch (RedriveRecoveryFailed $e) {
            $this->assertSame(
                ['write to orders refused', 'write to orders.dlq refused', 'seek failed'],
                array_map(static fn (\Throwable $t): string => $t->getMessage(), $e->getErrors()),
            );
        }
    }
}
