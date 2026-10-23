<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

/**
 * The SQS operations the framework-less {@see SqsTransport} needs: send a
 * message and change a received message's visibility (the §3.5 release). The AWS SDK's
 * {@code Aws\Sqs\SqsClient} exposes matching {@code sendMessage()} /
 * {@code changeMessageVisibility()} methods, so wrap it in a thin adapter (see the
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

    /**
     * @param  array<string, mixed>  $args  SQS ChangeMessageVisibility parameters
     *                                      (`QueueUrl`, `ReceiptHandle`, `VisibilityTimeout`).
     * @return mixed  The AWS result (ignored by the transport).
     */
    public function changeMessageVisibility(array $args): mixed;
}
