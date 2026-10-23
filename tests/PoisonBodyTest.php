<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\DeadLetter\PoisonBody;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class PoisonBodyTest extends TestCase
{
    public function test_an_object_body_is_spliced_byte_for_byte(): void
    {
        $raw = '{"job":"","data":{},"x":[]}';

        $out = PoisonBody::annotate($raw, 'poison', null, 'orders', 2);

        $this->assertStringStartsWith(substr($raw, 0, -1) . ',"dead_letter":{', $out);
        $decoded = json_decode($out, false);
        $this->assertSame('poison', $decoded->dead_letter->reason);
        $this->assertSame('orders', $decoded->dead_letter->original_queue);
        $this->assertSame(2, $decoded->dead_letter->attempts);
        $this->assertEquals(new \stdClass(), $decoded->data);
    }

    public function test_an_empty_object_and_surrounding_whitespace_are_handled(): void
    {
        $out = PoisonBody::annotate(" \n{ }\t\n", 'poison', null, 'q', 0);

        $this->assertStringStartsWith(" \n{\"dead_letter\":{", $out);
        $this->assertSame('poison', json_decode($out, false)->dead_letter->reason);
    }

    /** @return array<string, array{0: string}> */
    public static function rawBodies(): array
    {
        return [
            'malformed' => ['{broken'],
            'list' => ['[1,2]'],
            'scalar' => ['"x"'],
            'empty' => [''],
            'invalid UTF-8' => ["\xff\xfe\x00{bad"],
            'object with a previous dead_letter key' => ['{"dead_letter":{"reason":"old"},"a":1}'],
        ];
    }

    #[DataProvider('rawBodies')]
    public function test_every_other_body_is_wrapped_without_losing_a_byte(string $raw): void
    {
        $out = PoisonBody::annotate($raw, 'poison', null, 'q', 1);

        $decoded = json_decode($out, true);
        $this->assertIsArray($decoded);
        $this->assertSame('poison', $decoded['dead_letter']['reason']);

        $carried = ($decoded['raw_encoding'] ?? null) === 'base64' ? base64_decode($decoded['raw'], true) : $decoded['raw'];
        $this->assertSame($raw, $carried);
    }

    public function test_valid_utf8_is_carried_as_plain_raw_without_an_encoding_marker(): void
    {
        $decoded = json_decode(PoisonBody::annotate('{broken ü', 'poison', null, 'q', 0), true);

        $this->assertSame('{broken ü', $decoded['raw']);
        $this->assertArrayNotHasKey('raw_encoding', $decoded);
    }

    public function test_an_overflowing_number_is_carried_verbatim(): void
    {
        $out = PoisonBody::annotate('{"n":1e400}', 'poison', null, 'q', 0);

        $this->assertStringContainsString('"n":1e400', $out);
    }

    public function test_queue_hint_reads_meta_queue_of_an_object_only(): void
    {
        $this->assertSame('billing', PoisonBody::queueHint('{"meta":{"queue":"billing"}}'));
        $this->assertNull(PoisonBody::queueHint('{"meta":{"queue":""}}'));
        $this->assertNull(PoisonBody::queueHint('{"meta":[]}'));
        $this->assertNull(PoisonBody::queueHint('{}'));
        $this->assertNull(PoisonBody::queueHint("\xff"));
        $this->assertNull(PoisonBody::queueHint('[1]'));
    }
}
