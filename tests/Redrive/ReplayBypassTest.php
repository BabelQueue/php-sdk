<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Redrive;

use BabelQueue\Contracts\ConsumedMessage;
use BabelQueue\Contracts\HasHeaders;
use BabelQueue\Redrive\ReplayBypass;
use PHPUnit\Framework\TestCase;

/**
 * A consumed message that also surfaces out-of-band transport headers ({@see HasHeaders}) — the
 * shape a framework worker builds from a delivery that carried the `bq-replay-bypass` marker. Lets
 * the consume-side replay guard be exercised without a broker.
 */
final class HeaderMessage implements ConsumedMessage, HasHeaders
{
    /**
     * @param  array<string, mixed>  $envelope
     * @param  array<string, string>  $headers
     */
    public function __construct(private array $envelope, private array $headers = [])
    {
    }

    public function getUrn(): string
    {
        return is_string($this->envelope['job'] ?? null) ? $this->envelope['job'] : '';
    }

    public function getTraceId(): string
    {
        return is_string($this->envelope['trace_id'] ?? null) ? $this->envelope['trace_id'] : '';
    }

    /** @return array<string, mixed> */
    public function getData(): array
    {
        return is_array($this->envelope['data'] ?? null) ? $this->envelope['data'] : [];
    }

    /** @return array<string, mixed> */
    public function getMeta(): array
    {
        return is_array($this->envelope['meta'] ?? null) ? $this->envelope['meta'] : [];
    }

    public function attempts(): int
    {
        return is_int($this->envelope['attempts'] ?? null) ? $this->envelope['attempts'] : 0;
    }

    /** @return array<string, mixed> */
    public function envelope(): array
    {
        return $this->envelope;
    }

    /** @return array<string, string> */
    public function headers(): array
    {
        return $this->headers;
    }
}

/**
 * The consume-side replay-bypass guard (ADR-0027): a deliberate replay carries the
 * `bq-replay-bypass` transport header, and a handler skips the external side-effects that already
 * fired — while the idempotent core still runs. The PHP mirror of Go's `IsReplay` /
 * `BypassExternalEffects` + the `app.go` guard.
 */
final class ReplayBypassTest extends TestCase
{
    private function message(bool $replay): HeaderMessage
    {
        $envelope = [
            'job' => 'urn:babel:orders:created',
            'trace_id' => 'trace-1',
            'data' => ['order_id' => 7],
            'meta' => ['id' => 'msg-1', 'queue' => 'orders', 'lang' => 'php', 'schema_version' => 1],
            'attempts' => 0,
        ];

        return new HeaderMessage($envelope, $replay ? ReplayBypass::markerHeaders() : []);
    }

    public function test_a_replay_is_detected_and_a_normal_delivery_is_not(): void
    {
        self::assertTrue(ReplayBypass::isReplay($this->message(true)));
        self::assertFalse(ReplayBypass::isReplay($this->message(false)));
    }

    public function test_bypass_skips_the_effect_on_a_replay(): void
    {
        $fired = 0;
        ReplayBypass::bypassExternalEffects($this->message(true), function () use (&$fired): void {
            $fired++;
        });

        self::assertSame(0, $fired, 'a replay must skip the external side-effect');
    }

    public function test_bypass_runs_the_effect_on_a_normal_delivery(): void
    {
        $fired = 0;
        ReplayBypass::bypassExternalEffects($this->message(false), function () use (&$fired): void {
            $fired++;
        });

        self::assertSame(1, $fired, 'a normal delivery must run the external side-effect');
    }

    public function test_the_wrap_decorator_skips_on_replay_and_runs_otherwise(): void
    {
        $fired = 0;
        $effect = ReplayBypass::wrap(function (HasHeaders $m) use (&$fired): void {
            $fired++;
        });

        $effect($this->message(true));   // replay → skipped
        self::assertSame(0, $fired);

        $effect($this->message(false));  // normal → runs
        self::assertSame(1, $fired);
    }

    public function test_a_blank_marker_value_is_not_a_replay(): void
    {
        // An empty header value must not count as a replay (mirrors Go's non-empty check).
        $blank = new HeaderMessage(['job' => 'x'], [ReplayBypass::HEADER => '']);

        self::assertFalse(ReplayBypass::isReplay($blank));
    }

    public function test_the_header_map_form_matches_the_message_form(): void
    {
        self::assertTrue(ReplayBypass::isReplayHeaders(ReplayBypass::markerHeaders()));
        self::assertFalse(ReplayBypass::isReplayHeaders([]));
        self::assertFalse(ReplayBypass::isReplayHeaders(['x-other' => '1']));
    }

    public function test_the_marker_is_the_canonical_cross_sdk_header(): void
    {
        // Identical to Go's babelqueue.HeaderReplayBypass so a Go-produced replay is recognised here.
        self::assertSame('bq-replay-bypass', ReplayBypass::HEADER);
        self::assertSame(['bq-replay-bypass' => '1'], ReplayBypass::markerHeaders());
    }
}
