#!/usr/bin/env php
<?php

declare(strict_types=1);

/**
 * Minimal, dependency-free line-coverage gate for CI.
 *
 *   php bin/check-coverage.php <clover.xml> <min-percent>
 *
 * Reads a Clover report's project-level metrics and exits non-zero when line
 * coverage is below the gate. Keeps the core free of a coverage-tooling dep.
 */

$clover = $argv[1] ?? '';
$min = (float) ($argv[2] ?? '0');

if (! is_file($clover)) {
    fwrite(STDERR, "coverage file not found: {$clover}\n");
    exit(2);
}

$xml = simplexml_load_file($clover);
$metrics = $xml->project->metrics ?? null;

if ($metrics === null) {
    fwrite(STDERR, "no <project><metrics> in {$clover}\n");
    exit(2);
}

$total = (int) $metrics['statements'];
$covered = (int) $metrics['coveredstatements'];
$pct = $total > 0 ? ($covered / $total) * 100 : 100.0;

printf("Line coverage: %.2f%% (%d/%d) — gate: %.2f%%\n", $pct, $covered, $total, $min);

if ($pct + 1e-9 < $min) {
    fwrite(STDERR, sprintf("FAIL: line coverage %.2f%% is below the %.2f%% gate.\n", $pct, $min));
    exit(1);
}

echo "OK: coverage gate met.\n";
