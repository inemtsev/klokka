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
 * The kind of a [JobState], without per-state data. What queries filter by and dashboards
 * group by: [JobState.Failed] carries its retry time, but "show me failed jobs" is about
 * the kind of state, not one specific value of it. Names match [JobState] one to one.
 */
public enum class JobStatus {
    Scheduled,
    Enqueued,
    Running,
    Succeeded,
    Failed,
    DeadLettered,
    Cancelled,
}

/** The [JobStatus] classifying this state. */
public val JobState.status: JobStatus
    get() =
        when (this) {
            is JobState.Scheduled -> JobStatus.Scheduled
            is JobState.Enqueued -> JobStatus.Enqueued
            is JobState.Running -> JobStatus.Running
            is JobState.Succeeded -> JobStatus.Succeeded
            is JobState.Failed -> JobStatus.Failed
            is JobState.DeadLettered -> JobStatus.DeadLettered
            is JobState.Cancelled -> JobStatus.Cancelled
        }

/**
 * In-process lifecycle events, emitted by the runtime as a hot [kotlinx.coroutines.flow.SharedFlow].
 * This is the only observability primitive in the core: metrics and tracing modules are built on it,
 * and applications may subscribe directly for custom alerting.
 */
public sealed interface JobEvent {
    public val kind: String
    public val at: Instant

    /** Id of the recurring schedule this event belongs to; null for directly enqueued jobs. */
    public val scheduleId: String?

    public data class Enqueued(
        val jobId: JobId,
        override val kind: String,
        override val at: Instant,
        val queue: QueueName,
        val scheduledFor: Instant,
        override val scheduleId: String? = null,
    ) : JobEvent

    public data class Started(
        val jobId: JobId,
        override val kind: String,
        override val at: Instant,
        val attempt: Int,
        /** Time spent between becoming due and starting. Queue wait is an outage signal on its own. */
        val queueWait: Duration,
        override val scheduleId: String? = null,
    ) : JobEvent

    public data class Succeeded(
        val jobId: JobId,
        override val kind: String,
        override val at: Instant,
        val attempt: Int,
        val runDuration: Duration,
        /** True when the run started later than its misfire threshold allowed. */
        val late: Boolean,
        override val scheduleId: String? = null,
    ) : JobEvent

    public data class FailedAttempt(
        val jobId: JobId,
        override val kind: String,
        override val at: Instant,
        val attempt: Int,
        val error: Throwable,
        /** Null when this failure dead-letters the job. */
        val retryAt: Instant?,
        override val scheduleId: String? = null,
    ) : JobEvent

    public data class DeadLettered(
        val jobId: JobId,
        override val kind: String,
        override val at: Instant,
        val attempts: Int,
        val error: Throwable,
        override val scheduleId: String? = null,
    ) : JobEvent

    public data class Cancelled(
        val jobId: JobId,
        override val kind: String,
        override val at: Instant,
        override val scheduleId: String? = null,
    ) : JobEvent

    /**
     * A recurring schedule came due later than its misfire threshold allowed. There is no
     * per-run job id here because a misfire is a schedule-level fact: with
     * [MisfirePolicy.Skip] no run exists at all. [missedFires] counts the fire times
     * inside the missed window; [emitted] is how many runs the policy actually produced
     * (0 for Skip, 1 for FireOnce, up to `atMost` for CatchUp).
     */
    public data class ScheduleMisfired(
        override val scheduleId: String,
        override val kind: String,
        override val at: Instant,
        val missedFires: Int,
        val policy: MisfirePolicy,
        val emitted: Int,
    ) : JobEvent
}
