<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

/**
 * The single STOMP operation the framework-less {@see StompTransport} needs: send a
 * frame to a destination with headers. The `stomp-php/stomp-php` client exposes a
 * matching send — wrap it in a one-line adapter (see the {@see StompTransport} class
 * docs). Keeping the transport behind this tiny seam leaves the core dependency-free
 * and unit-testable with a fake (no `stomp-php`, no broker).
 */
interface StompClient
{
    /**
     * Send a STOMP frame.
     *
     * @param  string  $destination  The STOMP destination (the Artemis anycast address / queue).
     * @param  string  $body  The frame body (the canonical envelope JSON).
     * @param  array<string, string>  $headers  STOMP headers (the §7 projection). Values are plain
     *                                           strings; the underlying client escapes them per
     *                                           STOMP 1.2 (colons → {@code \c}).
     */
    public function send(string $destination, string $body, array $headers): void;
}
