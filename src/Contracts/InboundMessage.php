<?php

declare(strict_types=1);

namespace BabelQueue\Contracts;

/**
 * The framework-agnostic, read-only view of a consumed envelope.
 *
 * Framework adapters implement this on their native message/job type (e.g. the
 * Laravel adapter's `PolyglotMessage` also extends Illuminate's `Job` so the
 * worker can ack/release it). Core consume-side helpers depend only on this
 * decoded view and never on a framework's message lifecycle.
 */
interface InboundMessage
{
    /**
     * The message URN — its language-agnostic identity (never a PHP class name).
     */
    public function getUrn(): string;

    /**
     * The cross-service correlation id (trace_id), or '' if absent.
     */
    public function getTraceId(): string;

    /**
     * The decoded "data" block.
     *
     * @return array<string, mixed>
     */
    public function getData(): array;

    /**
     * The decoded "meta" block.
     *
     * @return array<string, mixed>
     */
    public function getMeta(): array;
}
