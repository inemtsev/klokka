@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka

import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

/**
 * The lifecycle of a job run.
 *
 * ```
 * Scheduled -> Enqueued -> Running -> Succeeded
 *                                   | Failed (a retry is booked for retryAt)
 *                                   | DeadLettered
 *                                   | Cancelled
 * ```
 *
 * [com.eventslooped.klokka.spi.JobStore.transition] performs a compare-and-set on these values:
 * a transition only applies if the stored state still equals the expected `from` state.
 */
public sealed interface JobState {
    /** True when no further execution will happen for this run. */
    public val terminal: Boolean
        get() = false

    /** Waiting for its run-at time. Delayed jobs and schedule-emitted runs start here. */
    public data object Scheduled : JobState

    /** Due and visible to workers. */
    public data object Enqueued : JobState

    /** Claimed under a lease and executing. */
    public data object Running : JobState

    public data object Succeeded : JobState {
        override val terminal: Boolean get() = true
    }

    /** The last attempt failed and a retry is booked. Not terminal. */
    public data class Failed(val retryAt: Instant) : JobState

    /** Retries exhausted or non-retryable failure. Kept for triage, never silently deleted. */
    public data object DeadLettered : JobState {
        override val terminal: Boolean get() = true
    }

    public data object Cancelled : JobState {
        override val terminal: Boolean get() = true
    }
}

/**
 * In-process lifecycle events, emitted by the runtime as a hot [kotlinx.coroutines.flow.SharedFlow].
 * This is the only observability primitive in the core: metrics and tracing modules are built on it,
 * and applications may subscribe directly for custom alerting.
 */
public sealed interface JobEvent {
    public val jobId: JobId
    public val kind: String
    public val at: Instant

    public data class Enqueued(
        override val jobId: JobId,
        override val kind: String,
        override val at: Instant,
        val queue: QueueName,
        val scheduledFor: Instant,
    ) : JobEvent

    public data class Started(
        override val jobId: JobId,
        override val kind: String,
        override val at: Instant,
        val attempt: Int,
        /** Time spent between becoming due and starting. Queue wait is an outage signal on its own. */
        val queueWait: Duration,
    ) : JobEvent

    public data class Succeeded(
        override val jobId: JobId,
        override val kind: String,
        override val at: Instant,
        val attempt: Int,
        val runDuration: Duration,
        /** True when the run started later than its misfire threshold allowed. */
        val late: Boolean,
    ) : JobEvent

    public data class FailedAttempt(
        override val jobId: JobId,
        override val kind: String,
        override val at: Instant,
        val attempt: Int,
        val error: Throwable,
        /** Null when this failure dead-letters the job. */
        val retryAt: Instant?,
    ) : JobEvent

    public data class DeadLettered(
        override val jobId: JobId,
        override val kind: String,
        override val at: Instant,
        val attempts: Int,
        val error: Throwable,
    ) : JobEvent

    public data class Cancelled(
        override val jobId: JobId,
        override val kind: String,
        override val at: Instant,
    ) : JobEvent

    public data class ScheduleMisfired(
        override val jobId: JobId,
        override val kind: String,
        override val at: Instant,
        val scheduleId: String,
        val missedFires: Int,
        val policy: MisfirePolicy,
    ) : JobEvent
}
