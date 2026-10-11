<?php

declare(strict_types=1);

namespace BabelQueue\Redrive;

/**
 * An optional {@see RedriveIO} capability: hand a reserved message back to the broker so it is
 * redelivered — the PHP mirror of Go's `Rewinder`.
 *
 * {@see Redrive} calls {@see rewind()} whenever a message stops the run: its restore write failed (it must
 * not be lost and is **never acknowledged**), its redrive write failed (it was restored and acknowledged),
 * the `select` predicate threw, or the acknowledgement failed. The run stops there, so everything reserved
 * after it would never be processed. For an offset-committing broker (Kafka) a later commit is cumulative
 * and would silently skip a reserved-but-unprocessed record, so the IO must seek its consumer back to the
 * first unacknowledged record of every affected partition — a batch is reserved before it is processed, so
 * that covers everything reserved and still unacknowledged, not only `$handle`.
 *
 * `$handle` itself may already be acknowledged (a restored message after a failed redrive write). It MUST
 * NOT be redelivered then (that would duplicate the restored record); only the other unacknowledged
 * messages are.
 *
 * The IO is handed the same `handle` its {@see RedriveIO::pop()} returned. It MUST NOT delete or
 * acknowledge the message and MUST NOT alter its body. A {@see RedriveIO} that does not implement
 * this keeps the previous behaviour (the failed message is left unacknowledged). If the rewind
 * itself fails, {@see Redrive} throws a {@see RedriveRecoveryFailed} carrying every error.
 */
interface RewindingRedriveIO extends RedriveIO
{
    /**
     * Rewind the consumer so every reserved, unacknowledged message — `$handle` included unless it was
     * acknowledged already — is delivered again.
     */
    public function rewind(mixed $handle): void;
}
