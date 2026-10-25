<?php

declare(strict_types=1);

namespace BabelQueue\Codec;

use BabelQueue\Transport\KafkaBodyClassifier;

/**
 * The binding-independent verdict on a raw message body (ADR-0036 decision 4; conformance `identity_verdict`):
 * a body is **poison** when it does not decode into an envelope with a valid identity (undecodable, a
 * wrong-typed known member, or neither a valid `job` nor a valid `urn`). {@see EnvelopeCodec::decode()} is
 * deliberately lenient about member types, so this is the stricter check the consumers use to decide
 * "never hand this to a handler".
 *
 * It delegates to {@see KafkaBodyClassifier}, which first implemented the table for the Kafka `kafka_poison`
 * section; the two sections share one verdict (`no_identity` and `poison` have the same fate), so this name
 * is the one non-Kafka code uses.
 */
final class StrictBodyClassifier
{
    /**
     * True when `$body` is poison (`poison` or `no_identity` in {@see KafkaBodyClassifier} terms).
     */
    public static function isPoison(string $body): bool
    {
        return KafkaBodyClassifier::classify($body) !== KafkaBodyClassifier::OK;
    }
}
