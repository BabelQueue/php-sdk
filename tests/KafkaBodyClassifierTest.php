<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\Transport\KafkaBodyClassifier;
use PHPUnit\Framework\TestCase;

/**
 * Spot checks for the `kafka_poison` classification (the full 92-case matrix runs in
 * BehaviourConformanceTest against the vendored conformance suite).
 */
final class KafkaBodyClassifierTest extends TestCase
{
    public function test_valid_envelope_is_ok(): void
    {
        $this->assertSame(KafkaBodyClassifier::OK, KafkaBodyClassifier::classify('{"job":"urn:babel:a:b","data":{},"attempts":0}'));
    }

    public function test_property_name_starting_with_nul_is_not_poison(): void
    {
        $this->assertSame(KafkaBodyClassifier::OK, KafkaBodyClassifier::classify('{"job":"urn:babel:a:b","attempts":0,"data":{"\\u0000x":1}}'));
    }

    public function test_urn_alias_is_a_valid_identity(): void
    {
        $this->assertSame(KafkaBodyClassifier::OK, KafkaBodyClassifier::classify('{"urn":"urn:babel:a:b"}'));
    }

    public function test_object_without_identity_and_literal_null_are_no_identity(): void
    {
        $this->assertSame(KafkaBodyClassifier::NO_IDENTITY, KafkaBodyClassifier::classify('{"data":{}}'));
        $this->assertSame(KafkaBodyClassifier::NO_IDENTITY, KafkaBodyClassifier::classify('null'));
    }

    public function test_undecodable_bodies_are_poison(): void
    {
        foreach (['', '{', '[]', '"x"', '{"job":"urn:babel:a:b"} x', "\xEF\xBB\xBF{\"job\":\"urn:babel:a:b\"}", '{"job":"urn:babel:a:b","attempts":"3"}', '{"job":"urn:babel:a:b","attempts":1.5}', '{"job":"urn:babel:a:b","data":1e999}'] as $body) {
            $this->assertSame(KafkaBodyClassifier::POISON, KafkaBodyClassifier::classify($body), $body);
        }
    }

    public function test_nested_member_of_the_wrong_type_is_poison(): void
    {
        $this->assertSame(KafkaBodyClassifier::POISON, KafkaBodyClassifier::classify('{"job":"urn:babel:a:b","meta":{"schema_version":"1"}}'));
    }
}
