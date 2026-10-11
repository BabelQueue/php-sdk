<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Support;

use BabelQueue\Redrive\KafkaRedriveClient;
use BabelQueue\Transport\KafkaConsumerClient;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\TopicPartition;
use RuntimeException;

/**
 * The `ext-rdkafka` adapter behind {@see KafkaConsumerClient} and {@see KafkaRedriveClient}, for the live
 * Kafka tests only. One consumer reads one topic through a **manual assignment** of all its partitions
 * (no group rebalance to wait for), auto-commit off, `auto.offset.reset=earliest`. `seek()` is recorded
 * and applied as a re-assignment before the next poll (the high-level consumer has no seek call): the
 * sought partitions restart at the given offset, every other partition at its committed offset.
 */
final class RdKafkaConsumerAdapter implements KafkaConsumerClient, KafkaRedriveClient
{
    private readonly KafkaConsumer $consumer;

    private ?string $assignedTopic = null;

    /** @var list<int> */
    private array $partitions = [];

    /** @var array<int, int> */
    private array $pendingSeeks = [];

    private ?\RdKafka\Message $last = null;

    public function __construct(
        string $brokers,
        string $groupId,
        private readonly ?string $topic = null,
        private readonly int $pollTimeoutMs = 5000,
    ) {
        $conf = new Conf();
        $conf->set('metadata.broker.list', $brokers);
        $conf->set('group.id', $groupId);
        $conf->set('enable.auto.commit', 'false');
        $conf->set('auto.offset.reset', 'earliest');
        $this->consumer = new KafkaConsumer($conf);
    }

    public function receive(): ?array
    {
        if ($this->topic === null) {
            throw new RuntimeException('RdKafkaConsumerAdapter::receive() needs a topic in the constructor');
        }
        $record = $this->poll($this->topic);

        return $record === null ? null : [
            'payload' => $record['payload'],
            'headers' => $record['headers'],
            'key' => $record['key'] ?? null,
        ];
    }

    public function commit(): void
    {
        if ($this->last !== null) {
            $this->consumer->commit($this->last);
        }
    }

    public function poll(string $topic): ?array
    {
        $this->assign($topic);

        $message = $this->consumer->consume($this->pollTimeoutMs);
        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                break;
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                return null;
            default:
                throw new RuntimeException('Kafka consume failed: ' . $message->errstr());
        }

        $this->last = $message;
        $headers = [];
        foreach ($message->headers ?? [] as $name => $value) {
            $headers[(string) $name] = (string) $value;
        }

        return [
            'topic' => (string) $message->topic_name,
            'partition' => $message->partition,
            'offset' => $message->offset,
            'payload' => (string) $message->payload,
            'headers' => $headers,
            'key' => $message->key,
        ];
    }

    public function commitOffset(string $topic, int $partition, int $offset): void
    {
        $this->consumer->commit([new TopicPartition($topic, $partition, $offset + 1)]);
    }

    public function seek(string $topic, int $partition, int $offset): void
    {
        $this->pendingSeeks[$partition] = $offset;
    }

    /** Assign every partition of $topic on first use and apply the seeks recorded since the last poll. */
    private function assign(string $topic): void
    {
        if ($this->assignedTopic === null) {
            $this->assignedTopic = $topic;
            $metadata = $this->consumer->getMetadata(false, $this->consumer->newTopic($topic), 15000);
            foreach ($metadata->getTopics() as $t) {
                foreach ($t->getPartitions() as $p) {
                    $this->partitions[] = $p->getId();
                }
            }
            if ($this->partitions === []) {
                throw new RuntimeException("Kafka topic {$topic} has no partitions (does it exist?)");
            }
            $this->applyAssignment();

            return;
        }
        if ($topic !== $this->assignedTopic) {
            throw new RuntimeException("RdKafkaConsumerAdapter reads one topic ({$this->assignedTopic}), not {$topic}");
        }
        if ($this->pendingSeeks !== []) {
            $this->applyAssignment();
            $this->pendingSeeks = [];
        }
    }

    private function applyAssignment(): void
    {
        $assignment = [];
        foreach ($this->partitions as $id) {
            $assignment[] = new TopicPartition(
                (string) $this->assignedTopic,
                $id,
                $this->pendingSeeks[$id] ?? RD_KAFKA_OFFSET_STORED,
            );
        }
        $this->consumer->assign($assignment);
    }
}
