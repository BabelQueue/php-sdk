<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\HeaderPublisher;
use BabelQueue\Support\Headers;

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
 * **Out-of-band headers (ADR-0028).** It also implements the optional {@see HeaderPublisher}
 * capability: {@see self::publishWithHeaders()} carries out-of-band transport headers (e.g. a W3C
 * `traceparent` for cross-hop span linkage) as additional String `MessageAttributes`, merged
 * **beside** the contract `bq-*` attributes where `bq-trace-id` already rides — the contract wins a
 * key collision, and the merged set is bounded by SQS's **10-attribute limit** (contract attributes
 * are seeded first, so a rider is only added while headroom remains). A plain {@see self::publish()}
 * stays byte-identical to before. GR-1: the wire envelope body is never touched.
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
final class SqsTransport implements HeaderPublisher
{
    /** SQS allows at most 10 message attributes per message. */
    private const MAX_ATTRIBUTES = 10;

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
        return $this->send($payload, $queue, []);
    }

    /**
     * Publish the envelope together with out-of-band `$headers` ({@see HeaderPublisher}, ADR-0028).
     * The headers ride as additional String `MessageAttributes` beside the contract `bq-*`
     * attributes, which win a key collision; the merged set is capped at SQS's 10-attribute limit
     * (the contract attributes are kept; riders fill any remaining slots). An empty/blank map
     * degrades to a plain {@see self::publish()}.
     *
     * @param  array<string, string>  $headers
     */
    public function publishWithHeaders(string $payload, array $headers, ?string $queue = null): ?string
    {
        return $this->send($payload, $queue, $headers);
    }

    /**
     * @param  array<string, string>  $extraHeaders  out-of-band headers to carry beside the contract `bq-*`
     */
    private function send(string $payload, ?string $queue, array $extraHeaders): ?string
    {
        $url = $queue ?? $this->queueUrl;
        $envelope = EnvelopeCodec::decode($payload);
        $meta = is_array($envelope['meta'] ?? null) ? $envelope['meta'] : [];

        $args = [
            'QueueUrl' => $url,
            'MessageBody' => $payload,
            'MessageAttributes' => $this->attributes($envelope, $meta, $extraHeaders),
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
     * @param  array<string, string>  $extraHeaders
     * @return array<string, array{DataType: string, StringValue: string}>
     */
    private function attributes(array $envelope, array $meta, array $extraHeaders): array
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

        // Fold in the out-of-band riders, but never clobber a contract attribute and never exceed
        // the 10-attribute SQS cap (contract attributes are seeded above, so they always win and a
        // rider only lands while headroom remains).
        foreach (Headers::sanitize($extraHeaders) as $name => $value) {
            if (isset($attributes[$name])) {
                continue;
            }
            if (count($attributes) >= self::MAX_ATTRIBUTES) {
                break;
            }
            $attributes[$name] = self::string($value);
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
