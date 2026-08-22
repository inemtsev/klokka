@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.spi

import com.eventslooped.klokka.JobId
import com.eventslooped.klokka.JobState
import com.eventslooped.klokka.MisfirePolicy
import com.eventslooped.klokka.QueueName
import com.eventslooped.klokka.RetentionPolicy
import com.eventslooped.klokka.WorkerId
import kotlinx.coroutines.flow.Flow
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

/**
 * A job to persist. [payload] is the serialized form (JSON by default); [payloadVersion]
 * is the envelope version, present from v1 so wire-format evolution never needs a
 * data migration.
 */
public data class NewJob(
    val kind: String,
    val payload: String,
    val payloadVersion: Int = 1,
    val queue: QueueName = QueueName.DEFAULT,
    /** When the job becomes due. Now or past means immediately eligible. */
    val runAt: Instant,
    /** See [com.eventslooped.klokka.JobOptions.uniqueKey] for the dedup contract. */
    val uniqueKey: String? = null,
)

/** A job claimed under a lease, with everything a worker needs to execute it. */
public data class ClaimedJob(
    val id: JobId,
    val kind: String,
    val payload: String,
    val payloadVersion: Int,
    val queue: QueueName,
    /** 1-based attempt number of the execution about to happen. */
    val attempt: Int,
    val enqueuedAt: Instant,
    val scheduledFor: Instant,
    val leaseUntil: Instant,
    /**
     * Fencing token: increases every time this job is claimed. Stores must reject
     * writes (transition, heartbeat) carrying a stale fence, so a zombie worker
     * resurrected after a pause cannot overwrite newer state.
     */
    val fence: Long,
)

/** A recurring schedule that is due to emit a job run. */
public data class ScheduleFire(
    val scheduleId: String,
    val kind: String,
    val payload: String?,
    val payloadVersion: Int,
    val scheduledFor: Instant,
    val misfire: MisfirePolicy,
    /** True when scheduledFor is further in the past than the schedule's misfire threshold. */
    val misfired: Boolean,
)

/**
 * The storage contract. Implementations plus the conformance kit (klokka-tck) ARE the
 * portability story: any store passing the TCK is a valid backend.
 *
 * Contract highlights, enforced by the TCK:
 * - All time comparisons use the STORE's clock (database time), never the caller's.
 * - [claim] must be atomic across concurrent callers: a row is claimed by exactly one
 *   worker per lease window. In SQL this is a single statement (SKIP LOCKED or CAS),
 *   never an in-process check.
 * - [claim] scans queues in the given list order: declared order is priority, uniformly
 *   across all stores.
 * - [claim] returns only rows whose kind is in the caller's `kinds`; rows of any other kind
 *   are left untouched, so a worker never claims a job it cannot run.
 * - [enqueue] treats a uniqueKey conflict with a non-terminal existing job as idempotent
 *   success and returns the existing job's id.
 * - [transition] is a compare-and-set: it returns false and changes nothing when the
 *   stored state does not equal [from] or the fence is stale.
 * - Expired leases make jobs claimable again (at-least-once semantics).
 */
public interface JobStore {
    /**
     * Persists [jobs] and returns their ids in the same order. A job whose [NewJob.runAt] is
     * not in the future by the store's clock is stored as [JobState.Enqueued] (due, visible
     * to workers); a future [NewJob.runAt] is stored as [JobState.Scheduled] (waiting for its
     * time). Promoting Scheduled to Enqueued when that time arrives is the store's concern;
     * the runtime never depends on it, because [claim] decides due-ness by time, not state.
     * A uniqueKey conflict with a non-terminal job returns that job's id (see interface KDoc).
     */
    public suspend fun enqueue(jobs: List<NewJob>): List<JobId>

    /**
     * Claims up to [limit] due jobs for [worker] under a lease of [lease], scanning [queues]
     * in list order (declared order is priority) and returning only jobs whose kind is in
     * [kinds]. Rows of any other kind are left untouched: not claimed, not transitioned, not
     * counted against [limit]. An empty [kinds] returns an empty list. The filter is what lets
     * a worker that does not bind a kind (an older version mid-rollout, a fleet that drains a
     * shared queue for a subset of kinds) leave that job for a worker that does.
     *
     * Atomic across concurrent callers: a row is claimed by exactly one worker per lease
     * window, in a single statement (SKIP LOCKED or CAS), never an in-process check. Each
     * claim bumps the row's attempt and fence. Due-ness is decided by the store's clock.
     */
    public suspend fun claim(
        /** Declared order is priority: earlier queues are drained first. */
        queues: List<QueueName>,
        /** The kinds this worker can run; only rows with one of these kinds are returned. */
        kinds: Set<String>,
        limit: Int,
        lease: Duration,
        worker: WorkerId,
    ): List<ClaimedJob>

    /**
     * Extends the lease of running jobs. Ignores ids whose lease this worker no longer
     * holds, including the worker's OWN ids whose lease has already expired: an expired
     * lease is not held, even by its former owner. Reviving an expired job goes through
     * [claim], never through heartbeat.
     */
    public suspend fun heartbeat(ids: List<JobId>, worker: WorkerId, extend: Duration)

    /**
     * Compare-and-set state transition. See interface KDoc for fence semantics.
     *
     * Stores enforce CAS only, never state-machine edge legality: whether [to] is a
     * legal successor of [from] is the runtime's responsibility, mirroring a SQL
     * `UPDATE ... WHERE state = ?` with no CHECK constraint on the target state.
     *
     * [fence] is optional because callers that never claimed the job (a producer
     * cancelling a Scheduled run) have no fence to pass. Transitions out of
     * [JobState.Running] MUST carry the fence from the worker's [ClaimedJob]:
     * omitting it there silently disables zombie-worker protection.
     * On success, stores clear the lease and holder when [to] is terminal AND when
     * [to] is [JobState.Failed], which is not terminal but is no longer running.
     */
    public suspend fun transition(id: JobId, from: JobState, to: JobState, fence: Long? = null): Boolean

    /**
     * Returns schedules that are due by the STORE's own clock, marking each as fired so
     * no other node emits the same run. Deliberately takes no `now` parameter: no SPI
     * method accepts caller-supplied time for comparisons, because the caller's clock
     * is never authoritative.
     */
    public suspend fun dueSchedules(limit: Int): List<ScheduleFire>

    /** Deletes terminal runs past their retention. Runs as a built-in recurring job. */
    public suspend fun sweep(retention: RetentionPolicy)
}

/**
 * Optional capability: the store can push "new work may be available" signals
 * (Postgres LISTEN/NOTIFY, an in-process channel for the in-memory store), so the
 * runtime does not depend on polling for latency. Emissions are wake-up hints and
 * carry no payload; spurious emissions are allowed.
 */
public interface PushCapableStore : JobStore {
    public fun wakeups(): Flow<Unit>
}

/**
 * Optional capability: enqueue participating in a caller-supplied transaction of type [TX].
 * The job is visible to workers if and only if the transaction commits. The transaction is
 * passed explicitly because thread-local transaction state does not survive coroutine
 * thread hops.
 */
public interface TransactionalStore<TX> : JobStore {
    public suspend fun enqueue(tx: TX, jobs: List<NewJob>): List<JobId>
}
