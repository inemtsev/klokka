@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.store

import com.eventslooped.klokka.JobId
import com.eventslooped.klokka.JobState
import com.eventslooped.klokka.JobStatus
import com.eventslooped.klokka.QueueName
import com.eventslooped.klokka.RetentionPolicy
import com.eventslooped.klokka.WorkerId
import com.eventslooped.klokka.spi.ClaimedJob
import com.eventslooped.klokka.spi.JobDetails
import com.eventslooped.klokka.spi.JobQuery
import com.eventslooped.klokka.spi.JobStore
import com.eventslooped.klokka.spi.JobSummary
import com.eventslooped.klokka.spi.NewJob
import com.eventslooped.klokka.spi.PushCapableStore
import com.eventslooped.klokka.spi.QueryableStore
import com.eventslooped.klokka.status
import com.eventslooped.klokka.spi.ScheduleFire
import com.eventslooped.klokka.spi.ScheduleSpec
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

/**
 * A single stored job run, mutable in place. All access is guarded by
 * [InMemoryJobStore]'s mutex; nothing here is safe to read or write unlocked.
 */
private class JobRecord(
    val id: JobId,
    val kind: String,
    val payload: String,
    val payloadVersion: Int,
    val queue: QueueName,
    /** Fixed at creation. Doubles as [ClaimedJob.scheduledFor]: it never changes across retries. */
    val runAt: Instant,
    val uniqueKey: String?,
    val enqueuedAt: Instant,
    val scheduleId: String?,
) {
    var state: JobState = JobState.Scheduled
    var attempts: Int = 0
    var fence: Long = 0
    var leaseUntil: Instant? = null
    var holder: WorkerId? = null

    /** Set when [state] becomes terminal. Drives [InMemoryJobStore.sweep]. */
    var terminalAt: Instant? = null
}

/**
 * One registered recurring schedule's timing state. Guarded by [InMemoryJobStore]'s
 * mutex, like [JobRecord].
 */
private class ScheduleRecord(
    val id: String,
    var kind: String,
    var fingerprint: String,
    var description: String,
    var nextFireAt: Instant?,
) {
    var lastFiredAt: Instant? = null
    var fence: Long = 0
    var leaseUntil: Instant? = null
    var holder: WorkerId? = null
}

/**
 * Snapshot of a schedule's mutable fields, for tests to assert on without reflection.
 */
public data class InMemoryScheduleSnapshot(
    val kind: String,
    val fingerprint: String,
    val description: String,
    val nextFireAt: Instant?,
    val lastFiredAt: Instant?,
    val fence: Long,
)

/**
 * Snapshot of a stored job's mutable fields, for tests to assert on without reflection.
 */
public data class InMemoryJobSnapshot(
    val state: JobState,
    val attempts: Int,
    val fence: Long,
    val queue: QueueName,
)

/**
 * An in-process, non-persistent [JobStore]. Backs single-process usage and the test suite;
 * all state is lost on restart. All time comparisons use the injected [clock], which plays
 * the role database time plays for a real backend, so tests can drive time deterministically.
 */
public class InMemoryJobStore(private val clock: Clock = Clock.System) : JobStore, PushCapableStore, QueryableStore {
    private val mutex = Mutex()
    private val records = linkedMapOf<JobId, JobRecord>()
    private val schedules = linkedMapOf<String, ScheduleRecord>()
    private var counter: Long = 0

    private val wakeupFlow =
        MutableSharedFlow<Unit>(
            replay = 0,
            extraBufferCapacity = 1,
            onBufferOverflow = BufferOverflow.DROP_OLDEST,
        )

    override suspend fun enqueue(jobs: List<NewJob>): List<JobId> {
        val result = mutex.withLock { enqueueLocked(jobs, clock.now()) }
        maybeWakeup(jobs)
        return result
    }

    /** Shared by [enqueue] and [completeFire]. The caller must hold [mutex]. */
    private fun enqueueLocked(jobs: List<NewJob>, now: Instant): List<JobId> {
        val result = ArrayList<JobId>(jobs.size)
        for (job in jobs) {
            val existing =
                job.uniqueKey?.let { key ->
                    records.values.firstOrNull { it.uniqueKey == key && !it.state.terminal }
                }
            if (existing != null) {
                result.add(existing.id)
                continue
            }
            counter += 1
            val id = JobId("job-$counter")
            val record =
                JobRecord(
                    id = id,
                    kind = job.kind,
                    payload = job.payload,
                    payloadVersion = job.payloadVersion,
                    queue = job.queue,
                    runAt = job.runAt,
                    uniqueKey = job.uniqueKey,
                    enqueuedAt = now,
                    scheduleId = job.scheduleId,
                )
            // Due by the store's clock at insert means Enqueued (visible to workers);
            // a future runAt means Scheduled (waiting for its time). See JobStore.enqueue.
            record.state = if (job.runAt <= now) JobState.Enqueued else JobState.Scheduled
            records[id] = record
            result.add(id)
        }
        return result
    }

    /** Spurious wakeups are allowed by the contract, so this checks the inputs, not the inserts. */
    private fun maybeWakeup(jobs: List<NewJob>) {
        if (jobs.any { it.runAt <= clock.now() }) {
            wakeupFlow.tryEmit(Unit)
        }
    }

    override suspend fun claim(
        queues: List<QueueName>,
        kinds: Set<String>,
        limit: Int,
        lease: Duration,
        worker: WorkerId,
    ): List<ClaimedJob> {
        if (limit <= 0 || kinds.isEmpty()) return emptyList()
        val claimed = ArrayList<ClaimedJob>(limit)
        mutex.withLock {
            val now = clock.now()
            for (queue in queues) {
                if (claimed.size >= limit) break
                // Kind filter first: rows of other kinds are never touched, per the SPI contract.
                val eligible =
                    records.values
                        .filter { it.queue == queue && it.kind in kinds && isClaimable(it, now) }
                        .sortedBy { claimSortKey(it) }
                for (record in eligible) {
                    if (claimed.size >= limit) break
                    record.attempts += 1
                    record.fence += 1
                    record.state = JobState.Running
                    val newLeaseUntil = now + lease
                    record.leaseUntil = newLeaseUntil
                    record.holder = worker
                    claimed.add(
                        ClaimedJob(
                            id = record.id,
                            kind = record.kind,
                            payload = record.payload,
                            payloadVersion = record.payloadVersion,
                            queue = record.queue,
                            attempt = record.attempts,
                            enqueuedAt = record.enqueuedAt,
                            scheduledFor = record.runAt,
                            leaseUntil = newLeaseUntil,
                            fence = record.fence,
                            scheduleId = record.scheduleId,
                        ),
                    )
                }
            }
        }
        return claimed
    }

    private fun isClaimable(record: JobRecord, now: Instant): Boolean =
        when (val state = record.state) {
            is JobState.Scheduled, is JobState.Enqueued -> record.runAt <= now
            is JobState.Failed -> state.retryAt <= now
            is JobState.Running -> {
                val leaseUntil = record.leaseUntil
                leaseUntil != null && leaseUntil <= now
            }
            else -> false
        }

    private fun claimSortKey(record: JobRecord): Instant =
        when (val state = record.state) {
            is JobState.Failed -> state.retryAt
            else -> record.runAt
        }

    override suspend fun heartbeat(ids: List<JobId>, worker: WorkerId, extend: Duration) {
        mutex.withLock {
            val now = clock.now()
            for (id in ids) {
                val record = records[id] ?: continue
                val leaseUntil = record.leaseUntil
                if (record.state is JobState.Running && record.holder == worker && leaseUntil != null && leaseUntil > now) {
                    record.leaseUntil = now + extend
                }
            }
        }
    }

    override suspend fun transition(id: JobId, from: JobState, to: JobState, fence: Long?): Boolean =
        mutex.withLock {
            val record = records[id] ?: return@withLock false
            if (record.state != from) return@withLock false
            if (fence != null && record.fence != fence) return@withLock false

            record.state = to
            if (to.terminal || to is JobState.Failed) {
                record.holder = null
                record.leaseUntil = null
            }
            record.terminalAt = if (to.terminal) clock.now() else null
            true
        }

    override suspend fun upsertSchedules(schedules: List<ScheduleSpec>) {
        mutex.withLock {
            for (spec in schedules) {
                val existing = this.schedules[spec.id]
                if (existing == null) {
                    this.schedules[spec.id] =
                        ScheduleRecord(
                            id = spec.id,
                            kind = spec.kind,
                            fingerprint = spec.fingerprint,
                            description = spec.description,
                            nextFireAt = spec.fireAt,
                        )
                } else if (existing.fingerprint != spec.fingerprint) {
                    existing.kind = spec.kind
                    existing.fingerprint = spec.fingerprint
                    existing.description = spec.description
                    existing.nextFireAt = spec.fireAt
                    existing.leaseUntil = null
                    existing.holder = null
                    // A claim taken under the old definition must not be completable.
                    existing.fence += 1
                }
                // Equal fingerprint: keep all timing state, per the SPI contract.
            }
        }
    }

    override suspend fun dueSchedules(
        ids: Set<String>,
        limit: Int,
        lease: Duration,
        worker: WorkerId,
    ): List<ScheduleFire> {
        if (limit <= 0 || ids.isEmpty()) return emptyList()
        return mutex.withLock {
            val now = clock.now()
            schedules.values
                .filter { record ->
                    val next = record.nextFireAt
                    val leaseUntil = record.leaseUntil
                    record.id in ids && next != null && next <= now && (leaseUntil == null || leaseUntil <= now)
                }
                .sortedBy { it.nextFireAt }
                .take(limit)
                .map { record ->
                    record.fence += 1
                    record.leaseUntil = now + lease
                    record.holder = worker
                    ScheduleFire(
                        scheduleId = record.id,
                        scheduledFor = record.nextFireAt ?: error("filtered on nextFireAt != null"),
                        now = now,
                        fence = record.fence,
                    )
                }
        }
    }

    override suspend fun completeFire(
        scheduleId: String,
        fence: Long,
        runs: List<NewJob>,
        nextFireAt: Instant?,
    ): List<JobId>? {
        val result =
            mutex.withLock {
                val record = schedules[scheduleId] ?: return@withLock null
                if (record.fence != fence) return@withLock null
                val now = clock.now()
                val ids = enqueueLocked(runs, now)
                record.nextFireAt = nextFireAt
                record.lastFiredAt = now
                record.leaseUntil = null
                record.holder = null
                ids
            }
        if (result != null) {
            maybeWakeup(runs)
        }
        return result
    }

    override suspend fun sweep(retention: RetentionPolicy) {
        mutex.withLock {
            val now = clock.now()
            val toRemove = ArrayList<JobId>()
            for (record in records.values) {
                val terminalAt = record.terminalAt ?: continue
                val expired =
                    when (record.state) {
                        is JobState.Succeeded -> now - terminalAt > retention.succeededFor
                        is JobState.Cancelled -> now - terminalAt > retention.cancelledFor
                        is JobState.DeadLettered -> {
                            val deadLetteredFor = retention.deadLetteredFor
                            deadLetteredFor != null && now - terminalAt > deadLetteredFor
                        }
                        else -> false
                    }
                if (expired) toRemove.add(record.id)
            }
            toRemove.forEach { records.remove(it) }
        }
    }

    override fun wakeups(): Flow<Unit> = wakeupFlow.asSharedFlow()

    // ---- QueryableStore -----------------------------------------------------------------

    override suspend fun countsByStatus(): Map<JobStatus, Long> =
        mutex.withLock {
            records.values.groupingBy { it.state.status }.eachCount().mapValues { it.value.toLong() }
        }

    override suspend fun listJobs(query: JobQuery): List<JobSummary> =
        mutex.withLock {
            records.values
                .reversed() // insertion order reversed: most recently persisted first
                .asSequence()
                .filter { query.status == null || it.state.status == query.status }
                .filter { query.kind == null || it.kind == query.kind }
                .filter { query.queue == null || it.queue == query.queue }
                .drop(query.offset)
                .take(query.limit)
                .map { it.toSummary() }
                .toList()
        }

    override suspend fun getJob(id: JobId): JobDetails? =
        mutex.withLock {
            val record = records[id] ?: return@withLock null
            JobDetails(
                summary = record.toSummary(),
                payload = record.payload,
                payloadVersion = record.payloadVersion,
                uniqueKey = record.uniqueKey,
                fence = record.fence,
                leaseUntil = if (record.state is JobState.Running) record.leaseUntil else null,
                holder = if (record.state is JobState.Running) record.holder else null,
            )
        }

    private fun JobRecord.toSummary(): JobSummary =
        JobSummary(
            id = id,
            kind = kind,
            queue = queue,
            status = state.status,
            attempt = attempts,
            runAt = runAt,
            enqueuedAt = enqueuedAt,
            scheduleId = scheduleId,
            retryAt = (state as? JobState.Failed)?.retryAt,
            terminalAt = terminalAt,
        )

    /** Test hook: reads a schedule's mutable fields without reflection. Null when [id] is unknown. */
    public suspend fun scheduleSnapshot(id: String): InMemoryScheduleSnapshot? =
        mutex.withLock {
            val record = schedules[id] ?: return@withLock null
            InMemoryScheduleSnapshot(
                kind = record.kind,
                fingerprint = record.fingerprint,
                description = record.description,
                nextFireAt = record.nextFireAt,
                lastFiredAt = record.lastFiredAt,
                fence = record.fence,
            )
        }

    /** Test hook: reads a job's mutable fields without reflection. Null when [id] is unknown. */
    public suspend fun snapshot(id: JobId): InMemoryJobSnapshot? =
        mutex.withLock {
            val record = records[id] ?: return@withLock null
            InMemoryJobSnapshot(
                state = record.state,
                attempts = record.attempts,
                fence = record.fence,
                queue = record.queue,
            )
        }
}
