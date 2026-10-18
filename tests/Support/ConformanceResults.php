<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Support;

use PHPUnit\Framework\Assert;
use PHPUnit\Framework\AssertionFailedError;
use PHPUnit\Framework\IncompleteTest;
use PHPUnit\Framework\SkippedTest;
use PHPUnit\Framework\SkippedWithMessageException;

/**
 * Case enumeration, per-case recording and the `results.json` writer of the cross-SDK conformance
 * suite (ADR-0032 section 2).
 *
 * Runners iterate a manifest section through {@see each()}, which names every case by the suite's
 * case-id rules, records `pass` / `fail` / `skip` for each, and fails the test once at the end if any
 * case failed. When the env var `BQ_CONFORMANCE_RESULTS` is set, a shutdown function writes the
 * collected results to that path in the shape of `conformance/schema/results.schema.json`; when it is
 * unset nothing is written and nothing is checked.
 *
 * `bindings` is derived from the transport modules in `src/` (never hand-listed). Before writing, the
 * run is checked for completeness: a transport module this class cannot map, a binding whose
 * sections are missing (or whose case set is incomplete), or a section of a binding the package does
 * not carry all make the process exit non-zero, so CI fails (ADR-0032 Compliance).
 *
 * Known-gaps gate: a repo-root `conformance-known-gaps.json` (`{"gaps": [{"section", "case", "reason"}]}`)
 * lists the MUST cases this package does not implement yet. Every listed case is still **run** through
 * {@see each()} against the real code, never recorded blind: an *assertion* failure on a listed case is
 * the expected gap and is recorded as an honest `fail` (never a `skip`), whereas any other throwable
 * (a TypeError, a missing method, a Mockery error) is a broken runner and breaks the test even when
 * every case of the section is a gap. A `fail` passes the gate only when it is listed; a listed case
 * that no longer fails is a "stale gap" and breaks the run, and an unlisted `fail` always does. Every
 * gap reason must point at an ADR or at a `.ssot/plans/` entry that tracks the follow-up.
 */
final class ConformanceResults
{
    public const SDK = 'babelqueue/php-sdk';

    /** The SDK family named in the manifest `exceptions` list (ADR-0021: PHP x asb). */
    private const FAMILY = 'php';

    /** `src/Transport/<Name>Transport.php` -> results.json binding. */
    private const TRANSPORT_BINDINGS = [
        'Redis' => 'redis',
        'Amqp' => 'amqp',
        'Sqs' => 'sqs',
        'Pulsar' => 'pulsar',
        'Kafka' => 'kafka',
        'Stomp' => 'artemis',
    ];

    /** A gap reason must point at the decision or the plan that tracks it. */
    public const GAP_REFERENCE = '/ADR-\d{4}|\.ssot\/plans\//';

    /** @var array<string, array<string, array{status: string, reason?: string}>> */
    private static array $results = [];

    private static bool $registered = false;

    /** @var list<string> skip reasons that do not cite an ADR (ADR-0032: a skip is only valid with one) */
    private static array $skipProblems = [];

    /** @var array<string, mixed>|null */
    private static ?array $assoc = null;

    /** @var array<string, string>|null `section/case` => reason of the known-gaps register */
    private static ?array $gaps = null;

    private static ?\stdClass $objects = null;

    /**
     * Run `$fn($case, $id)` for every case of a manifest section (or of one sub-key of it),
     * recording each outcome. Skips a case when `$fn` calls {@see skipCase()}; throws once at the end
     * if any case failed.
     *
     * @param  callable(mixed, string): void  $fn
     * @param  bool  $objects  hand `$fn` the case with JSON objects kept as stdClass
     */
    public static function each(string $section, callable $fn, ?string $sub = null, bool $objects = false): void
    {
        $cases = self::cases($section, $sub, $objects);
        if ($cases === []) {
            Assert::fail('manifest section ' . $section . ($sub !== null ? '.' . $sub : '') . ' has no cases');
        }

        $failures = [];
        foreach ($cases as $id => $case) {
            $id = (string) $id;
            $listed = self::isKnownGap($section, $id);
            try {
                $fn($case, $id);
                self::record($section, $id, 'pass');
                if ($listed) {
                    $failures[] = self::staleGapMessage($section, $id, 'passes');
                }
            } catch (SkippedCase $e) {
                self::record($section, $id, 'skip', $e->getMessage());
                if ($listed) {
                    $failures[] = self::staleGapMessage($section, $id, 'is skipped');
                }
            } catch (SkippedTest | IncompleteTest $e) {
                // PHPUnit's own skip/incomplete signals extend AssertionFailedError, so they must be caught
                // first: they are neither an assertion about the code nor an ADR-cited SkippedCase, and
                // letting them through would record an un-cited skip as a "listed gap".
                self::record($section, $id, 'fail', $e::class . ': ' . $e->getMessage());
                $failures[] = $section . '/' . $id . ': ' . ($listed ? 'unexpected ' : '') . $e::class . ': ' . $e->getMessage()
                    . ' (use ConformanceResults::skipCase() with an ADR to skip a case)';
            } catch (AssertionFailedError $e) {
                self::record($section, $id, 'fail', $e->getMessage() !== '' ? $e->getMessage() : $e::class);
                // A listed known gap is the expected failure (the gate in finish() judges it); anything else breaks the run.
                if (! $listed) {
                    $failures[] = $section . '/' . $id . ': ' . $e->getMessage();
                }
            } catch (\Throwable $e) {
                // Not an assertion: the runner itself is broken (or the code crashed), which is never a known gap.
                self::record($section, $id, 'fail', $e::class . ': ' . $e->getMessage());
                $failures[] = $section . '/' . $id . ': ' . ($listed ? 'unexpected ' : '') . $e::class . ': ' . $e->getMessage();
            }
        }

        if ($failures !== []) {
            Assert::fail(implode("\n", $failures));
        }
    }

    /**
     * Record every case of a section (or sub-key) as skipped with an ADR-cited reason.
     */
    public static function skipAll(string $section, string $reason, ?string $sub = null): void
    {
        self::assertCitesAdr($reason);
        foreach (array_keys(self::cases($section, $sub)) as $id) {
            self::record($section, (string) $id, 'skip', $reason);
        }
    }

    /**
     * Everything recorded so far: `section` => `case id` => `{status, reason?}`.
     *
     * @return array<string, array<string, array{status: string, reason?: string}>>
     */
    public static function recorded(): array
    {
        return self::$results;
    }

    /**
     * Run `$fn` against a scratch results book and a stand-in known-gaps register, then restore the real
     * state. This is how the mechanism is tested without polluting the `results.json` of the run: the
     * returned `results` are what `$fn` recorded, `error` is what it threw (null when it returned).
     *
     * @param  array<string, string>  $gaps  `section/case` => reason, replacing the real register
     * @return array{results: array<string, array<string, array{status: string, reason?: string}>>, error: ?\Throwable, skipProblems: list<string>}
     */
    public static function isolated(array $gaps, callable $fn): array
    {
        $saved = [self::$results, self::$skipProblems, self::$gaps];
        self::$results = [];
        self::$skipProblems = [];
        self::$gaps = $gaps;
        $error = null;

        try {
            $fn();
        } catch (\Throwable $e) {
            $error = $e;
        } finally {
            $captured = ['results' => self::$results, 'error' => $error, 'skipProblems' => self::$skipProblems];
            [self::$results, self::$skipProblems, self::$gaps] = $saved;
        }

        return $captured;
    }

    /**
     * The known-gaps register (`conformance-known-gaps.json` at the repo root): `section/case` => reason.
     *
     * @return array<string, string>
     */
    public static function knownGaps(): array
    {
        if (self::$gaps !== null) {
            return self::$gaps;
        }

        $path = dirname(__DIR__, 2) . '/conformance-known-gaps.json';
        $gaps = [];
        if (is_file($path)) {
            /** @var array{gaps?: mixed} $decoded */
            $decoded = json_decode((string) file_get_contents($path), true, 512, JSON_THROW_ON_ERROR);
            foreach ((array) ($decoded['gaps'] ?? []) as $gap) {
                $section = is_array($gap) ? ($gap['section'] ?? null) : null;
                $case = is_array($gap) ? ($gap['case'] ?? null) : null;
                $reason = is_array($gap) ? ($gap['reason'] ?? null) : null;
                if (! is_string($section) || $section === '' || ! is_string($case) || $case === '' || ! is_string($reason) || $reason === '') {
                    throw new \RuntimeException('conformance-known-gaps.json: every gap needs a non-empty section, case and reason');
                }
                $key = $section . '/' . $case;
                if (isset($gaps[$key])) {
                    throw new \RuntimeException('conformance-known-gaps.json: duplicate gap ' . $key);
                }
                if (preg_match(self::GAP_REFERENCE, $reason) !== 1) {
                    throw new \RuntimeException('conformance-known-gaps.json: gap ' . $key . ' must reference an ADR-NNNN or a .ssot/plans/ entry in its reason');
                }
                $gaps[$key] = $reason;
            }
        }

        return self::$gaps = $gaps;
    }

    private static function isKnownGap(string $section, string $id): bool
    {
        return isset(self::knownGaps()[$section . '/' . $id]);
    }

    private static function staleGapMessage(string $section, string $id, string $what): string
    {
        return 'stale gap: ' . $section . '/' . $id . ' ' . $what . ' but is still listed in conformance-known-gaps.json; remove the entry';
    }

    /**
     * The known-gaps gate over the collected results: every `fail` must be listed, every listed gap
     * must still `fail` (else it is stale) and must name a case that exists in the results.
     *
     * @return list<string>
     */
    private static function gateProblems(): array
    {
        $problems = [];
        $gaps = self::knownGaps();

        foreach (self::$results as $section => $cases) {
            foreach ($cases as $id => $result) {
                $key = $section . '/' . $id;
                if ($result['status'] === 'fail' && ! isset($gaps[$key])) {
                    $problems[] = 'fail of ' . $key . ' is not listed in conformance-known-gaps.json';
                }
                if ($result['status'] !== 'fail' && isset($gaps[$key])) {
                    $problems[] = self::staleGapMessage((string) $section, (string) $id, 'has status ' . $result['status']);
                }
            }
        }

        foreach (array_keys($gaps) as $key) {
            [$section, $id] = explode('/', $key, 2);
            if (! isset(self::$results[$section][$id])) {
                $problems[] = 'conformance-known-gaps.json lists ' . $key . ' but the run has no such case';
            }
        }

        return $problems;
    }

    /** Inside an {@see each()} callback: skip this case (the reason must cite an `ADR-NNNN`). */
    public static function skipCase(string $reason): never
    {
        self::assertCitesAdr($reason);
        throw new SkippedCase($reason);
    }

    /**
     * The cases of a section as `id => case`. Case ids follow the suite rules: a `cases` array gives
     * `name` else `<section>#<index>`; a sub-key with its own `cases` array gives `<sub>/<name|index>`;
     * any other object sub-key is one case keyed by the sub-key.
     *
     * @return array<string, mixed>
     */
    public static function cases(string $section, ?string $sub = null, bool $objects = false): array
    {
        $root = $objects ? self::objectManifest() : self::assocManifest();
        $body = self::prop($root, $section);
        if ($body === null) {
            throw new SkippedWithMessageException('manifest has no ' . $section . ' section');
        }

        $out = [];
        foreach (self::enumerate($section, $body) as $id => $entry) {
            if ($entry['sub'] === $sub) {
                $out[$id] = $entry['case'];
            }
        }

        return $out;
    }

    public static function record(string $section, string $id, string $status, ?string $reason = null): void
    {
        self::register();
        if ($status === 'skip' && preg_match('/ADR-\d{4}/', (string) $reason) !== 1) {
            self::$skipProblems[] = 'skip of ' . $section . '/' . $id . ' does not cite an ADR-NNNN in its reason';
        }
        self::$results[$section][$id] = $reason === null ? ['status' => $status] : ['status' => $status, 'reason' => $reason];
    }

    /** A skip is valid only with a reason that cites an `ADR-NNNN` (ADR-0032); fail at the call site otherwise. */
    private static function assertCitesAdr(string $reason): void
    {
        if (preg_match('/ADR-\d{4}/', $reason) !== 1) {
            Assert::fail('a skip reason must cite an ADR-NNNN (ADR-0032), got: ' . $reason);
        }
    }

    /** The manifest as an associative array. */
    public static function manifest(): array
    {
        return self::assocManifest();
    }

    /**
     * The shutdown hook: write results.json (and enforce completeness) when `BQ_CONFORMANCE_RESULTS` is set.
     */
    public static function finish(): void
    {
        $path = getenv('BQ_CONFORMANCE_RESULTS');
        if ($path === false || $path === '') {
            return;
        }

        $problems = self::$skipProblems;
        $bindings = self::bindings($problems);
        $problems = [...$problems, ...self::completenessProblems($bindings), ...self::gateProblems()];

        $sections = self::$results;
        ksort($sections);

        $document = [
            'sdk' => self::SDK,
            'version' => self::sdkVersion(),
            'conformance_version' => self::conformanceVersion(),
            'generated_at' => gmdate('Y-m-d\TH:i:s\Z'),
            'bindings' => $bindings,
            'sections' => (object) array_map(static fn (array $cases): object => (object) $cases, $sections),
        ];

        $json = json_encode($document, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        if ($json === false || file_put_contents($path, $json . "\n") === false) {
            $problems[] = 'could not write ' . $path;
        }

        if ($problems !== []) {
            fwrite(STDERR, "\nconformance results incomplete (ADR-0032):\n - " . implode("\n - ", $problems) . "\n");
            exit(1);
        }
    }

    /**
     * Bindings derived from the transport modules of this package.
     *
     * @param  list<string>  $problems
     * @return list<string>
     */
    public static function bindings(array &$problems = []): array
    {
        $bindings = [];
        $src = dirname(__DIR__, 2) . '/src';

        foreach (glob($src . '/Transport/*Transport.php') ?: [] as $file) {
            $name = substr(basename($file), 0, -strlen('Transport.php'));
            if (! isset(self::TRANSPORT_BINDINGS[$name])) {
                $problems[] = 'transport module ' . basename($file) . ' has no binding mapping; add it to ConformanceResults::TRANSPORT_BINDINGS';

                continue;
            }
            $bindings[] = self::TRANSPORT_BINDINGS[$name];
        }

        // The W3C traceparent extract/inject code of ADR-0028 lives in src/Otel.
        if (is_file($src . '/Otel/Tracing.php')) {
            $bindings[] = 'otel';
        }

        $bindings = array_values(array_unique($bindings));
        sort($bindings);

        return $bindings;
    }

    /**
     * @param  list<string>  $bindings
     * @return list<string>
     */
    private static function completenessProblems(array $bindings): array
    {
        $manifest = self::assocManifest();
        $excluded = [];
        foreach ((array) ($manifest['exceptions'] ?? []) as $exception) {
            if (is_array($exception) && ($exception['family'] ?? null) === self::FAMILY && is_string($exception['binding'] ?? null)) {
                $excluded[] = $exception['binding'];
            }
        }

        $problems = [];
        $expected = [];
        foreach ((array) ($manifest['sections'] ?? []) as $name => $meta) {
            $scope = is_array($meta) ? (string) ($meta['scope'] ?? '') : '';
            $inScope = $scope === 'core' || (in_array($scope, $bindings, true) && ! in_array($scope, $excluded, true));
            if (! $inScope) {
                if (isset(self::$results[$name])) {
                    $problems[] = 'section ' . $name . ' (scope ' . $scope . ') must be absent: the package does not carry that binding';
                }

                continue;
            }

            $expected[$name] = true;
            $ids = array_keys(self::enumerate((string) $name, self::prop($manifest, (string) $name)));
            $recorded = array_keys(self::$results[$name] ?? []);
            foreach (array_diff($ids, $recorded) as $missing) {
                $problems[] = 'section ' . $name . ' is missing case ' . $missing;
            }
            foreach (array_diff($recorded, $ids) as $extra) {
                $problems[] = 'section ' . $name . ' has an unknown case ' . $extra;
            }
        }

        foreach (array_keys(self::$results) as $name) {
            if (! isset($expected[$name]) && ! isset($manifest['sections'][$name])) {
                $problems[] = 'section ' . $name . ' is not in the manifest sections map';
            }
        }

        return $problems;
    }

    /**
     * Enumerate `id => {sub, case}` for a section body (see {@see cases()} for the id rules). `sub` is
     * null for the section's own `cases` array and the sub-key name for everything under a sub-key.
     *
     * @return array<string, array{sub: ?string, case: mixed}>
     */
    private static function enumerate(string $section, mixed $body): array
    {
        $out = [];

        if (self::isList($body)) {
            foreach (self::items($body) as $index => $case) {
                $out[self::caseId($section, $case, $index)] = ['sub' => null, 'case' => $case];
            }

            return $out;
        }

        foreach (self::items(self::prop($body, 'cases')) as $index => $case) {
            $out[self::caseId($section, $case, $index)] = ['sub' => null, 'case' => $case];
        }

        foreach (self::keys($body) as $key) {
            if (in_array($key, ['description', 'schema', 'cases'], true)) {
                continue;
            }
            $node = self::prop($body, $key);
            if (! self::isObject($node)) {
                continue;
            }
            $nested = self::prop($node, 'cases');
            if (self::isList($nested)) {
                foreach (self::items($nested) as $index => $case) {
                    $out[$key . '/' . self::caseId('', $case, $index, true)] = ['sub' => $key, 'case' => $case];
                }
            } else {
                $out[$key] = ['sub' => $key, 'case' => $node];
            }
        }

        return $out;
    }

    private static function caseId(string $section, mixed $case, int $index, bool $bare = false): string
    {
        $name = self::prop($case, 'name');
        if (is_string($name) && $name !== '') {
            return $name;
        }

        return $bare ? (string) $index : $section . '#' . $index;
    }

    private static function prop(mixed $node, string $key): mixed
    {
        if ($node instanceof \stdClass) {
            return $node->{$key} ?? null;
        }

        return is_array($node) ? ($node[$key] ?? null) : null;
    }

    /** @return list<string> */
    private static function keys(mixed $node): array
    {
        $keys = $node instanceof \stdClass ? array_keys(get_object_vars($node)) : (is_array($node) ? array_keys($node) : []);

        return array_map('strval', $keys);
    }

    private static function isObject(mixed $node): bool
    {
        return $node instanceof \stdClass || (is_array($node) && $node !== [] && ! array_is_list($node));
    }

    private static function isList(mixed $node): bool
    {
        return is_array($node) && array_is_list($node);
    }

    /** @return array<int, mixed> */
    private static function items(mixed $list): array
    {
        return is_array($list) ? array_values($list) : [];
    }

    /** @return array<string, mixed> */
    private static function assocManifest(): array
    {
        if (self::$assoc === null) {
            /** @var array<string, mixed> $decoded */
            $decoded = json_decode(self::manifestJson(), true, 512, JSON_THROW_ON_ERROR);
            self::$assoc = $decoded;
        }

        return self::$assoc;
    }

    private static function objectManifest(): \stdClass
    {
        if (self::$objects === null) {
            /** @var \stdClass $decoded */
            $decoded = json_decode(self::manifestJson(), false, 512, JSON_THROW_ON_ERROR);
            self::$objects = $decoded;
        }

        return self::$objects;
    }

    private static function manifestJson(): string
    {
        $path = dirname(__DIR__) . '/conformance/manifest.json';
        if (! is_file($path)) {
            throw new SkippedWithMessageException('vendored conformance suite not present');
        }

        return (string) file_get_contents($path);
    }

    private static function conformanceVersion(): string
    {
        return trim((string) file_get_contents(dirname(__DIR__) . '/conformance/CONFORMANCE_VERSION'));
    }

    /** The package's own version source: the newest released heading of CHANGELOG.md. */
    private static function sdkVersion(): string
    {
        $changelog = (string) @file_get_contents(dirname(__DIR__, 2) . '/CHANGELOG.md');
        if (preg_match('/^## \[(\d+\.\d+\.\d+)\]/m', $changelog, $m) === 1) {
            return $m[1];
        }

        throw new \RuntimeException('no released version heading found in CHANGELOG.md');
    }

    private static function register(): void
    {
        if (! self::$registered) {
            self::$registered = true;
            register_shutdown_function([self::class, 'finish']);
        }
    }
}
