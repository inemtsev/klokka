@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.store

import com.eventslooped.klokka.JobId
import com.eventslooped.klokka.JobState
import com.eventslooped.klokka.QueueName
import com.eventslooped.klokka.RetentionPolicy
import com.eventslooped.klokka.WorkerId
import com.eventslooped.klokka.spi.ClaimedJob
import com.eventslooped.klokka.spi.JobStore
import com.eventslooped.klokka.spi.NewJob
import com.eventslooped.klokka.spi.PushCapableStore
import com.eventslooped.klokka.spi.ScheduleFire
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
public class InMemoryJobStore(private val clock: Clock = Clock.System) : JobStore, PushCapableStore {
    private val mutex = Mutex()
    private val records = linkedMapOf<JobId, JobRecord>()
    private var counter: Long = 0

    private val wakeupFlow =
        MutableSharedFlow<Unit>(
            replay = 0,
            extraBufferCapacity = 1,
            onBufferOverflow = BufferOverflow.DROP_OLDEST,
        )

    override suspend fun enqueue(jobs: List<NewJob>): List<JobId> {
        val result = ArrayList<JobId>(jobs.size)
        val insertedRunAts = ArrayList<Instant>()
        mutex.withLock {
            val now = clock.now()
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
                    )
                // Due by the store's clock at insert means Enqueued (visible to workers);
                // a future runAt means Scheduled (waiting for its time). See JobStore.enqueue.
                record.state = if (job.runAt <= now) JobState.Enqueued else JobState.Scheduled
                records[id] = record
                result.add(id)
                insertedRunAts.add(job.runAt)
            }
        }
        if (insertedRunAts.any { it <= clock.now() }) {
            wakeupFlow.tryEmit(Unit)
        }
        return result
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
            if (to.terminal) {
                record.terminalAt = clock.now()
            }
            true
        }

    /**
     * Always returns an empty list: schedule registration and the recurring-jobs machinery
     * that would populate it arrive with issue #11.
     */
    override suspend fun dueSchedules(limit: Int): List<ScheduleFire> = emptyList()

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
