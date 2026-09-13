@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.spi

import com.eventslooped.klokka.JobId
import com.eventslooped.klokka.JobState
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
    /** Id of the recurring schedule that emitted this run; null for directly enqueued jobs. */
    val scheduleId: String? = null,
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
    /** Id of the recurring schedule that emitted this run; null for directly enqueued jobs. */
    val scheduleId: String? = null,
)

/**
 * A recurring schedule's persisted identity and timing state, as the runtime registers it.
 * The store never sees or evaluates a schedule expression: all schedule math (cron, DSL,
 * misfire policy, payloads) lives in the runtime, and the store only keeps the fields
 * below plus per-row lease state. [fingerprint] is an opaque string the runtime derives
 * from the full definition; stores compare it for equality and never parse it.
 */
public data class ScheduleSpec(
    /** Stable schedule id, chosen by the user. The row's identity. */
    val id: String,
    /** The job kind this schedule emits. Display and ops metadata, not used by the store. */
    val kind: String,
    /** Opaque definition fingerprint; a change means "the definition changed". */
    val fingerprint: String,
    /** Human-readable definition (for tooling and the dashboard), e.g. `cron '0 9 * * 1-5' UTC`. */
    val description: String,
    /**
     * The next fire time to store when this upsert inserts the row or replaces a changed
     * fingerprint. Null means the schedule currently has no future fire (dormant).
     */
    val fireAt: Instant?,
)

/** A due schedule claimed under a lease, with what the runtime needs to compute the fire. */
public data class ScheduleFire(
    val scheduleId: String,
    /** The stored next-fire time that came due. */
    val scheduledFor: Instant,
    /**
     * The store's clock at claim time. The runtime uses it to judge misfires and to compute
     * the fire time that follows a missed window, so no node clock enters schedule math.
     */
    val now: Instant,
    /**
     * Fencing token: increases every time this schedule is claimed by [JobStore.dueSchedules].
     * [JobStore.completeFire] carrying a stale fence is rejected.
     */
    val fence: Long,
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
 * - Schedule rows follow the same pattern as job rows: [dueSchedules] is an atomic
 *   fenced claim by the store's clock, filtered to the ids the caller holds definitions
 *   for, and [completeFire] is a fenced compare-and-set that applies a fire (runs plus
 *   next fire time) in one transaction. Stores never evaluate schedule expressions.
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
     * [to] is [JobState.Failed], which is not terminal but is no longer running. Stores
     * stamp the terminal timestamp when [to] is terminal and CLEAR it when [to] is not:
     * a requeued row is live again, and must neither read as terminal to queries nor
     * inherit a stale timestamp on its next terminal transition.
     */
    public suspend fun transition(id: JobId, from: JobState, to: JobState, fence: Long? = null): Boolean

    /**
     * Registers or reconciles code-defined schedules, once per runtime start. Per spec:
     * an unknown [ScheduleSpec.id] inserts the row with `nextFireAt = fireAt`; a known id
     * with an EQUAL fingerprint updates nothing (timing state is preserved across
     * restarts); a known id with a DIFFERENT fingerprint overwrites kind, fingerprint and
     * description, sets `nextFireAt = fireAt`, clears any lease, AND bumps the fence:
     * a claim taken under the old definition must not be completable, because its runs
     * and next fire time were computed from a definition that no longer exists. Last writer wins;
     * concurrent upserts from nodes running different code versions are expected during
     * rolling deploys. Upsert never deletes: a schedule removed from code keeps its row
     * and simply stops being fired, because no node passes its id to [dueSchedules].
     */
    public suspend fun upsertSchedules(schedules: List<ScheduleSpec>)

    /**
     * Claims up to [limit] due schedules for [worker] under a lease of [lease], soonest
     * next-fire first. A schedule is due when its stored `nextFireAt` is non-null and not
     * in the future by the STORE's own clock; no SPI method accepts caller-supplied time
     * for comparisons, because the caller's clock is never authoritative. Only rows whose
     * id is in [ids] are returned; other rows are left untouched, so a node only ever
     * fires schedules it holds a definition for (the schedule analogue of [claim]'s kind
     * filter). An empty [ids] returns an empty list.
     *
     * Atomic across concurrent callers: a due schedule is claimed by exactly one caller
     * per lease window (SKIP LOCKED or CAS, never an in-process check). Each claim bumps
     * the row's fence. A claim whose lease expires before [completeFire] makes the row
     * claimable again with a fresh fence; the previous claim's completeFire is then stale
     * and rejected, so a fire is never applied twice.
     */
    public suspend fun dueSchedules(
        ids: Set<String>,
        limit: Int,
        lease: Duration,
        worker: WorkerId,
    ): List<ScheduleFire>

    /**
     * Applies one claimed fire ATOMICALLY: inserts [runs] with [enqueue] semantics
     * (including the uniqueKey rule), advances the schedule's `nextFireAt` to
     * [nextFireAt] (null makes the schedule dormant), sets `lastFiredAt` to the store's
     * clock, and releases the lease, all in one transaction. Compare-and-set on [fence]:
     * returns null and changes NOTHING, inserts no runs, when the stored fence differs.
     * On success returns the run ids in [runs] order, existing ids on uniqueKey dedup,
     * exactly like [enqueue]. An empty [runs] is a legal completion (a misfire handled
     * with Skip, for example) and returns an empty list.
     */
    public suspend fun completeFire(
        scheduleId: String,
        fence: Long,
        runs: List<NewJob>,
        nextFireAt: Instant?,
    ): List<JobId>?

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
