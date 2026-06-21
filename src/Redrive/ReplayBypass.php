<?php

declare(strict_types=1);

namespace BabelQueue\Redrive;

use BabelQueue\Contracts\HasHeaders;

/**
 * Replay-Bypass — the out-of-band side-effect guard for a deliberate DLQ replay (ADR-0027). The PHP
 * mirror of Go's `babelqueue.IsReplay` / `babelqueue.BypassExternalEffects` (`replay.go`) and the
 * `app.go` consume guard.
 *
 * The problem: {@see Redrive} replaying a dead-lettered message back to its *real* source queue
 * re-runs the handler — and the handler's external side-effects re-fire (a second charge, a
 * duplicate email). {@see \BabelQueue\Idempotency\Idempotent} stops an *accidental* duplicate; it
 * does not stop the *intended* reprocess from re-firing effects. This closes that gap.
 *
 * The marker cannot live in the frozen envelope (GR-1), so it rides **out of band** as the transport
 * header {@see self::HEADER} (`bq-replay-bypass`) — the same out-of-band seam OTel's `traceparent`
 * uses (ADR-0028). The producer half is {@see Redrive} with `bypass: true` (it stamps the header via
 * a {@see HeaderRedriveIO}); this is the **consumer half**: a handler reads the marker off the
 * delivered message's headers ({@see \BabelQueue\Contracts\HasHeaders}) and skips the effects that
 * already happened, while the idempotent core still runs.
 *
 *     // The handler stays idempotent at its core, and guards only its external effects:
 *     $dispatch->on('urn:babel:orders:created', static function (ConsumedMessage $m): void {
 *         saveOrder($m);                                   // idempotent core — always runs
 *         ReplayBypass::bypassExternalEffects($m, static function () use ($m): void {
 *             sendConfirmationEmail($m);                   // external effect — skipped on a replay
 *         });
 *     });
 *
 * It is dependency-free and additive: a message produced without the marker (every normal delivery)
 * is not a replay, so {@see self::isReplay()} is false and the effect always runs — a strict,
 * backward-compatible upgrade.
 */
final class ReplayBypass
{
    /**
     * The out-of-band transport header {@see Redrive} stamps (when `bypass` is set) on a replayed
     * message, and that a handler reads via {@see self::isReplay()}. Identical to Go's
     * `babelqueue.HeaderReplayBypass`, so the marker is cross-SDK and a Go-produced replay is
     * recognised by a PHP consumer and vice-versa.
     */
    public const HEADER = 'bq-replay-bypass';

    /** The value the producer stamps; any non-empty value means "this is a replay" (mirrors Go). */
    public const VALUE = '1';

    private function __construct()
    {
    }

    /**
     * Does this delivery carry the replay-bypass marker — i.e. is it a deliberate replay whose
     * already-fired external effects should be skipped? Reads the {@see self::HEADER} transport
     * header off the message when it surfaces headers ({@see HasHeaders}); a message that carries no
     * headers, or whose marker is blank, is **not** a replay. Mirrors Go's `IsReplay(ctx)`, which
     * reads the same header the runtime put on the context.
     */
    public static function isReplay(HasHeaders $message): bool
    {
        return self::markerIsSet($message->headers());
    }

    /**
     * The header-map form of {@see self::isReplay()}, for a caller that already holds the out-of-band
     * header map (e.g. a framework worker that surfaced them itself). True iff {@see self::HEADER} is
     * present and non-empty.
     *
     * @param  array<string, string>  $headers
     */
    public static function isReplayHeaders(array $headers): bool
    {
        return self::markerIsSet($headers);
    }

    /**
     * Run `$effect` unless `$message` is a replay (see {@see self::isReplay()}), in which case it is
     * skipped. Wrap the external, non-idempotent side of a handler — sending an email, charging a
     * card, calling a third party — so a replay re-runs the idempotent core but does not re-fire
     * effects that already happened. The direct mirror of Go's `BypassExternalEffects(ctx, fn)`.
     *
     * @param  callable(): void  $effect
     */
    public static function bypassExternalEffects(HasHeaders $message, callable $effect): void
    {
        if (self::isReplay($message)) {
            return;
        }
        $effect();
    }

    /**
     * Wrap an external-effect handler so a replayed delivery skips it entirely, returning a handler
     * with the same signature — the decorator form, composing with the consume runtime exactly like
     * {@see \BabelQueue\Idempotency\Idempotent::wrap()}. On a normal delivery the inner handler runs;
     * on a replay it is skipped and the wrapper returns (so the loop still acks the message).
     *
     * @param  callable(HasHeaders): void  $effectHandler
     * @return callable(HasHeaders): void
     */
    public static function wrap(callable $effectHandler): callable
    {
        return static function (HasHeaders $message) use ($effectHandler): void {
            if (self::isReplay($message)) {
                return;
            }
            $effectHandler($message);
        };
    }

    /**
     * Build the out-of-band header map a producer stamps to mark a replay — `[HEADER => VALUE]`.
     * {@see Redrive} uses this to stamp a redriven message; exposed so a caller re-publishing
     * through their own {@see \BabelQueue\Contracts\HeaderPublisher} can mark a replay the same way.
     *
     * @return array<string, string>
     */
    public static function markerHeaders(): array
    {
        return [self::HEADER => self::VALUE];
    }

    /**
     * @param  array<string, string>  $headers
     */
    private static function markerIsSet(array $headers): bool
    {
        return isset($headers[self::HEADER]) && $headers[self::HEADER] !== '';
    }
}
