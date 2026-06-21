<?php

declare(strict_types=1);

namespace BabelQueue\Redrive;

use BabelQueue\Codec\EnvelopeCodec;
use Throwable;

/**
 * DLQ redrive — safe replay off the dead-letter queue (ADR-0026). The PHP mirror of the Go
 * `babelqueue-go` `Redrive`, Python `babelqueue.redrive` and Node `@babelqueue/core` `redrive`.
 *
 * It reads dead-lettered messages off a DLQ and re-publishes each to its source (its
 * `dead_letter.original_queue`) or a chosen queue, **reset for reprocessing**: the `dead_letter`
 * block is removed and `attempts` reset to 0, while `job`, `trace_id`, `data` and `meta` are
 * preserved verbatim. It is the operator-side counterpart to the runtime's dead-letter routing —
 * the contract leaves redrive to tooling, and this is that tool.
 *
 *     $result = Redrive::run($io, 'orders.dlq', new RedriveOptions(toQueue: 'orders.sandbox'));
 *
 * Because PHP's {@see \BabelQueue\Contracts\Transport} is publish-only, redrive drives a
 * {@see RedriveIO} (reserve / ack / publish) the caller binds to their broker; {@see reset()} is
 * exposed on its own for callers that re-publish through their own machinery. The envelope stays
 * frozen (GR-1) and nothing is added to `require`.
 *
 * Scope: `dryRun` + `select` + a sandbox `toQueue` are the safe-replay primitives. The
 * **Replay-Bypass** guard (ADR-0027) is also wired here: with `RedriveOptions(bypass: true)` and a
 * {@see HeaderRedriveIO}, each redriven message is stamped with the `bq-replay-bypass` transport
 * header so a handler can skip external side-effects that already ran ({@see ReplayBypass}). The
 * marker rides out of band beside the frozen envelope (GR-1); a plain {@see RedriveIO} cannot carry
 * it, so `bypass` is a best-effort no-op there and the per-item `bypassed` flag stays false — the
 * exact best-effort contract Go's `Redrive` uses against its `HeaderPublisher` check (ADR-0027).
 */
final class Redrive
{
    /**
     * Return a copy of $envelope reset for reprocessing: the `dead_letter` block removed and
     * `attempts` set to 0, preserving `job` / `trace_id` / `data` / `meta` verbatim.
     *
     * @param  array<string, mixed>  $envelope
     * @return array<string, mixed>
     */
    public static function reset(array $envelope): array
    {
        unset($envelope['dead_letter']);
        $envelope['attempts'] = 0;

        return $envelope;
    }

    /**
     * Drain the dlq queue and re-publish selected messages, reset, to their source (or
     * $options->toQueue). Messages are drained first and then processed, so restored messages
     * (skipped, dry-run, undecodable) are never re-encountered in the same run. A DLQ message is
     * acknowledged only after its re-publish succeeds; an undecodable body is restored, not
     * dropped. On a publish failure the message is restored to the DLQ and the error re-thrown.
     */
    public static function run(RedriveIO $io, string $dlq, ?RedriveOptions $options = null): RedriveResult
    {
        $options ??= new RedriveOptions();

        /** @var list<array{body: string, handle: mixed, env: array<string, mixed>, decoded: bool}> $batch */
        $batch = [];
        while ($options->max === 0 || count($batch) < $options->max) {
            $reserved = $io->pop($dlq);
            if ($reserved === null) {
                break;
            }
            $env = EnvelopeCodec::decode($reserved['body']);
            $batch[] = [
                'body' => $reserved['body'],
                'handle' => $reserved['handle'],
                'env' => $env,
                'decoded' => $env !== [],
            ];
        }

        $redriven = 0;
        $skipped = 0;
        /** @var list<RedriveItem> $items */
        $items = [];

        foreach ($batch as $p) {
            if (! $p['decoded']) {
                $io->publish($dlq, $p['body']); // restore the poison body; never drop it
                $io->ack($p['handle']);
                $skipped++;
                $items[] = new RedriveItem('', '', '', '', $dlq, '', false);

                continue;
            }

            $env = $p['env'];
            $messageId = self::metaString($env, 'id');
            $traceId = self::stringField($env, 'trace_id');
            $urn = EnvelopeCodec::urn($env);
            $reason = self::deadLetterString($env, 'reason');

            $select = $options->select;
            if ($select !== null && ! $select($env)) {
                $io->publish($dlq, $p['body']); // not selected: restore unchanged
                $io->ack($p['handle']);
                $skipped++;
                $items[] = new RedriveItem($messageId, $traceId, $urn, $reason, $dlq, '', false);

                continue;
            }

            $target = $options->toQueue ?? self::sourceQueueOf($env);

            if ($options->dryRun) {
                $io->publish($dlq, $p['body']); // report the plan; restore unchanged
                $io->ack($p['handle']);
                $skipped++;
                $items[] = new RedriveItem($messageId, $traceId, $urn, $reason, $dlq, $target, false);

                continue;
            }

            $body = EnvelopeCodec::encode(self::reset($env));
            try {
                $bypassed = self::publishRedriven($io, $target, $body, $options->bypass);
            } catch (Throwable $e) {
                $io->publish($dlq, $p['body']); // restore on a publish failure, then surface it
                $io->ack($p['handle']);

                throw $e;
            }
            $io->ack($p['handle']);
            $redriven++;
            $items[] = new RedriveItem($messageId, $traceId, $urn, $reason, $dlq, $target, true, $bypassed);
        }

        return new RedriveResult($redriven, $skipped, $items);
    }

    /**
     * Re-publish a reset message to $queue, stamping the `bq-replay-bypass` marker when $bypass is
     * set AND the IO can carry headers ({@see HeaderRedriveIO}); returns whether the marker was
     * actually stamped. With no bypass, or a plain {@see RedriveIO}, it publishes plainly and
     * returns false. The PHP mirror of Go's `publishRedriven` (ADR-0027).
     */
    private static function publishRedriven(RedriveIO $io, string $queue, string $body, bool $bypass): bool
    {
        if ($bypass && $io instanceof HeaderRedriveIO) {
            $io->publishWithHeaders($queue, $body, ReplayBypass::markerHeaders());

            return true;
        }

        $io->publish($queue, $body);

        return false;
    }

    /**
     * Where a redriven message goes by default: its dead_letter.original_queue, else meta.queue.
     *
     * @param  array<string, mixed>  $env
     */
    private static function sourceQueueOf(array $env): string
    {
        $original = self::deadLetterString($env, 'original_queue');

        return $original !== '' ? $original : self::metaString($env, 'queue');
    }

    /**
     * @param  array<string, mixed>  $env
     */
    private static function stringField(array $env, string $key): string
    {
        return isset($env[$key]) && is_string($env[$key]) ? $env[$key] : '';
    }

    /**
     * @param  array<string, mixed>  $env
     */
    private static function metaString(array $env, string $key): string
    {
        $meta = $env['meta'] ?? null;
        if (is_array($meta) && isset($meta[$key]) && is_string($meta[$key])) {
            return $meta[$key];
        }

        return '';
    }

    /**
     * @param  array<string, mixed>  $env
     */
    private static function deadLetterString(array $env, string $key): string
    {
        $dead = $env['dead_letter'] ?? null;
        if (is_array($dead) && isset($dead[$key]) && is_string($dead[$key])) {
            return $dead[$key];
        }

        return '';
    }
}
