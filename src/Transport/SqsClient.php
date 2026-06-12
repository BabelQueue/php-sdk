<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

/**
 * The single SQS operation the framework-less {@see SqsTransport} needs: send a
 * message. The AWS SDK's {@code Aws\Sqs\SqsClient} exposes a matching
 * {@code sendMessage()} method, so wrap it in a one-line adapter (see the
 * {@see SqsTransport} class docs). Keeping the transport behind this tiny seam
 * leaves the core dependency-free and unit-testable with a fake.
 */
interface SqsClient
{
    /**
     * @param  array<string, mixed>  $args  SQS SendMessage parameters.
     * @return mixed  The AWS result (ignored by the transport).
     */
    public function sendMessage(array $args): mixed;
}
