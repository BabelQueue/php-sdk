<?php

declare(strict_types=1);

namespace BabelQueue\Transport;

/**
 * An optional capability of a {@see KafkaConsumerClient} adapter: the topics its consumer group is subscribed to.
 * When a record frame carries no `topic`, {@see KafkaConsumer} takes the queue from here, but only when the
 * subscription is a single topic (the queue is then unambiguous); with several topics (or a pattern) it cannot
 * tell which one a record came from and degrades as if it did not know (ADR-0036 decision 3, section 6.5).
 */
interface KafkaSubscriptionDescriptor
{
    /**
     * The subscribed topics; an empty list means the adapter does not know them.
     *
     * @return list<string>
     */
    public function subscribedTopics(): array;
}
