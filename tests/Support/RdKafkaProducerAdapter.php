<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Support;

use BabelQueue\Transport\KeyedKafkaProducer;
use RdKafka\Conf;
use RdKafka\Producer;
use RuntimeException;

/**
 * The one-line `ext-rdkafka` adapter behind the {@see KeyedKafkaProducer} seam, for the live Kafka tests
 * only (the SDK core never loads `RdKafka\*`). Every produce is flushed and a failed delivery throws,
 * so a test sees a broken write where it happens.
 */
final class RdKafkaProducerAdapter implements KeyedKafkaProducer
{
    private readonly Producer $producer;

    /** @var list<string> */
    private array $deliveryErrors = [];

    public function __construct(string $brokers)
    {
        $conf = new Conf();
        $conf->set('metadata.broker.list', $brokers);
        $conf->setDrMsgCb(function (Producer $producer, \RdKafka\Message $message): void {
            if ($message->err !== RD_KAFKA_RESP_ERR_NO_ERROR) {
                $this->deliveryErrors[] = $message->errstr();
            }
        });
        $this->producer = new Producer($conf);
    }

    public function produce(string $topic, string $payload, array $headers, ?int $timestampMs = null): void
    {
        $this->send($topic, $payload, $headers, $timestampMs, null);
    }

    public function produceKeyed(string $topic, string $payload, array $headers, ?int $timestampMs, ?string $key): void
    {
        $this->send($topic, $payload, $headers, $timestampMs, $key);
    }

    /** @param  array<string, string>  $headers */
    private function send(string $topic, string $payload, array $headers, ?int $timestampMs, ?string $key): void
    {
        $this->producer->newTopic($topic)->producev(
            RD_KAFKA_PARTITION_UA,
            0,
            $payload,
            $key,
            $headers === [] ? null : $headers,
            $timestampMs,
        );
        $this->producer->poll(0);

        $result = $this->producer->flush(15000);
        if ($result !== RD_KAFKA_RESP_ERR_NO_ERROR) {
            throw new RuntimeException("Kafka flush to {$topic} failed (librdkafka error {$result})");
        }
        if ($this->deliveryErrors !== []) {
            $errors = implode('; ', $this->deliveryErrors);
            $this->deliveryErrors = [];

            throw new RuntimeException("Kafka delivery to {$topic} failed: {$errors}");
        }
    }
}
