<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\Transport;

/**
 * A framework-less Amazon SQS producer: sends the canonical envelope as the message
 * body and projects the envelope onto the native SQS MessageAttributes every
 * BabelQueue SDK agrees on, so a non-PHP consumer can route on `bq-job` and trace on
 * `bq-trace-id` *without decoding the body first*:
 *
 *  - `bq-job`            ← the job URN
 *  - `bq-trace-id`       ← trace_id
 *  - `bq-message-id`     ← meta.id
 *  - `bq-schema-version` / `bq-source-lang` / `bq-created-at`
 *
 * Implements §3 of the broker-bindings contract. The envelope is unchanged
 * (`schema_version` stays 1); SQS is purely additive.
 *
 * Optional dependency: `aws/aws-sdk-php`. Wrap your client in one line:
 *
 * ```php
 * $sqs = new \Aws\Sqs\SqsClient([...]);
 * $transport = new SqsTransport(
 *     new class ($sqs) implements SqsClient {
 *         public function __construct(private \Aws\Sqs\SqsClient $c) {}
 *         public function sendMessage(array $args): mixed { return $this->c->sendMessage($args); }
 *     },
 *     'https://sqs.eu-central-1.amazonaws.com/123456789012/orders',
 * );
 * ```
 */
final class SqsTransport implements Transport
{
    public function __construct(
        private readonly SqsClient $client,
        private readonly string $queueUrl,
        private readonly bool $fifo = false,
        private readonly ?string $messageGroupId = null,
        private readonly bool $contentDedup = false,
    ) {
    }

    public function publish(string $payload, ?string $queue = null): ?string
    {
        $url = $queue ?? $this->queueUrl;
        $envelope = EnvelopeCodec::decode($payload);
        $meta = is_array($envelope['meta'] ?? null) ? $envelope['meta'] : [];

        $args = [
            'QueueUrl' => $url,
            'MessageBody' => $payload,
            'MessageAttributes' => $this->attributes($envelope, $meta),
        ];

        if ($this->fifo) {
            $args['MessageGroupId'] = $this->messageGroupId ?? $this->queueName($url);
            if (! $this->contentDedup && isset($meta['id']) && is_scalar($meta['id'])) {
                $args['MessageDeduplicationId'] = (string) $meta['id'];
            }
        }

        $this->client->sendMessage($args);

        $id = $meta['id'] ?? null;

        return is_scalar($id) ? (string) $id : null;
    }

    /**
     * @param  array<string, mixed>  $envelope
     * @param  array<string, mixed>  $meta
     * @return array<string, array{DataType: string, StringValue: string}>
     */
    private function attributes(array $envelope, array $meta): array
    {
        $attributes = [];

        $urn = EnvelopeCodec::urn($envelope);
        if ($urn !== '') {
            $attributes['bq-job'] = self::string($urn);
        }

        $traceId = $envelope['trace_id'] ?? null;
        if (is_string($traceId) && $traceId !== '') {
            $attributes['bq-trace-id'] = self::string($traceId);
        }

        if (isset($meta['id']) && is_scalar($meta['id'])) {
            $attributes['bq-message-id'] = self::string((string) $meta['id']);
        }
        if (isset($meta['schema_version']) && is_scalar($meta['schema_version'])) {
            $attributes['bq-schema-version'] = self::number((string) $meta['schema_version']);
        }
        if (isset($meta['lang']) && is_string($meta['lang']) && $meta['lang'] !== '') {
            $attributes['bq-source-lang'] = self::string($meta['lang']);
        }
        if (isset($meta['created_at']) && is_scalar($meta['created_at'])) {
            $attributes['bq-created-at'] = self::number((string) $meta['created_at']);
        }

        return $attributes;
    }

    /**
     * @return array{DataType: string, StringValue: string}
     */
    private static function string(string $value): array
    {
        return ['DataType' => 'String', 'StringValue' => $value];
    }

    /**
     * @return array{DataType: string, StringValue: string}
     */
    private static function number(string $value): array
    {
        return ['DataType' => 'Number', 'StringValue' => $value];
    }

    private function queueName(string $url): string
    {
        $segments = array_values(array_filter(explode('/', $url), static fn (string $s): bool => $s !== ''));

        return $segments === [] ? 'default' : (string) end($segments);
    }
}
