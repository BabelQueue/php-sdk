<?php

declare(strict_types=1);

namespace BabelQueue\Contracts;

/**
 * Gives a message a stable, transport-level identity — a URN — that is fully
 * decoupled from PHP class names.
 *
 * This is the single thing the wire protocol uses to identify *what* a message
 * is. Because it is a plain string under the application's control (never a
 * FQCN), a Go or Java consumer can route on it without sharing any PHP type,
 * and the producing class can be renamed, moved or refactored freely without
 * breaking any consumer.
 *
 * Recommended convention (not enforced, to preserve flexibility):
 *
 *     urn:babel:<bounded-context>:<event-or-command>
 *     e.g. "urn:babel:orders:invoice.requested"
 *
 * The value MUST be stable across deployments and unique per message type.
 */
interface HasBabelUrn
{
    /**
     * The stable URN that identifies this message type on the wire.
     */
    public function getBabelUrn(): string;
}
