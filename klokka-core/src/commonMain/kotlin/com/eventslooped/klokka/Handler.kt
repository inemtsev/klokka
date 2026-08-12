@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka

import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

/**
 * Execution context available to a handler as its receiver.
 *
 * Klokka is at-least-once: a claimed job whose worker dies will run again. Use [attempt],
 * [scheduledFor], and your own idempotency keys to make re-execution safe.
 */
public interface JobContext {
    public val jobId: JobId
    public val kind: String
    public val queue: QueueName

    /** 1-based attempt number. 1 means this is not a retry. */
    public val attempt: Int

    public val enqueuedAt: Instant

    /**
     * The intended fire time, not the actual start time. For a schedule firing at 09:00
     * this is 09:00 even if execution starts at 09:00:02. Use it to answer "which window
     * of data does this run cover" and to derive idempotency keys.
     */
    public val scheduledFor: Instant

    /** Reports progress in `0.0..1.0` for the dashboard. Best-effort, cheap to call. */
    public suspend fun progress(fraction: Double)

    /**
     * Extends this run's lease for long jobs. The runtime heartbeats automatically;
     * call this only when a single operation may outlive the lease and cannot checkpoint.
     */
    public suspend fun extendLease(by: Duration)
}

/**
 * A job handler. The simple registration path wraps a lambda in this interface, so
 * graduating to a class (for constructor-injected dependencies and direct unit testing)
 * changes no enqueue site and no registration semantics.
 *
 * Handlers must be reentrant. Never swallow exceptions to "handle" errors: rethrow or
 * let them propagate, because a caught exception means Klokka never learns the job failed.
 */
public interface JobHandler<T> {
    public suspend fun JobContext.execute(payload: T)
}
