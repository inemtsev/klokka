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
 *
 * On the JVM every log line written while a handler runs carries the MDC entries
 * `klokka.jobId`, `klokka.kind`, `klokka.queue`, `klokka.attempt` and, for schedule-emitted
 * runs, `klokka.scheduleId`. They are propagated across suspension points, so handlers use
 * their own logger and need no Klokka-specific one.
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

    /**
     * Id of the recurring schedule that emitted this run (also set by `triggerNow`);
     * null for directly enqueued jobs.
     */
    public val scheduleId: String?

    /** Reports progress in `0.0..1.0` for the dashboard. Best-effort, cheap to call. */
    public suspend fun progress(fraction: Double)

    /**
     * Renews this run's lease so that it now lasts [by] from the store's current time.
     *
     * The lease only tells other workers that this one is alive: the runtime renews it
     * automatically every heartbeat interval for as long as this attempt is in flight, and
     * handler duration is bounded only by the per-kind timeout (see `JobRegistry.handle`),
     * never by the lease. Call this ahead of a single blocking operation that could starve
     * the heartbeater or outlast a lease by more than one heartbeat interval, passing a
     * value that covers the whole operation.
     *
     * Per the store contract, a lease that has already expired cannot be renewed, not even
     * by its former holder: another worker may already have revived the job with a higher
     * fence, and this attempt's completion write will then be rejected. That is the
     * documented at-least-once case, not an error.
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

/**
 * The failure recorded when a handler exceeds its per-kind timeout. The attempt is
 * cancelled cooperatively and then treated as an ordinary failure: the retry policy sees
 * this exception and decides whether to book another attempt. Match on it to retry
 * timeouts differently from other errors.
 */
public class JobTimeoutException(
    public val kind: String,
    public val timeout: Duration,
) : RuntimeException("Job kind '$kind' exceeded its timeout of $timeout")
