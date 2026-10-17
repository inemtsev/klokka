@file:OptIn(ExperimentalTime::class, KlokkaBridgeApi::class)

package com.eventslooped.klokka.jdbc

import com.eventslooped.klokka.JobEvent
import com.eventslooped.klokka.JobId
import com.eventslooped.klokka.JobOptions
import com.eventslooped.klokka.JobType
import com.eventslooped.klokka.KlokkaBridgeApi
import com.eventslooped.klokka.runtime.KlokkaRuntime
import com.eventslooped.klokka.spi.JdbcTransactionalStore
import java.sql.Connection
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

/**
 * Enqueues a job due immediately inside the JDBC transaction running on [tx]: the job
 * exists if and only if that transaction commits, and no worker sees it before then.
 * Queue resolution and the uniqueKey rule are as in [KlokkaRuntime.enqueue].
 *
 * Emits no [JobEvent.Enqueued]: a plain connection offers no way to observe the commit,
 * and an Enqueued event always means the job exists. The job appears in [KlokkaRuntime.events]
 * from Started onward. Bridges that can observe the commit (klokka-exposed) emit it then.
 *
 * Writes on the caller's coroutine and thread, like the caller's own statements on [tx].
 * [tx] must reach the database the configured store claims from.
 *
 * @throws IllegalStateException when the configured store cannot join a JDBC transaction
 *   (the in-memory store), or when [tx] has auto-commit on, which means it is not in a
 *   transaction and the job would be committed immediately.
 */
public suspend fun <T> KlokkaRuntime.enqueue(
    type: JobType<T>,
    payload: T,
    tx: Connection,
    options: JobOptions = JobOptions(),
): JobId = enqueueWithin(type, payload, at = null, options, tx).id

/**
 * Schedules a job due at [at] inside the JDBC transaction running on [tx]. Same rules as
 * the transactional [enqueue]: the job exists if and only if the transaction commits.
 *
 * @throws IllegalStateException as for the transactional [enqueue].
 */
public suspend fun <T> KlokkaRuntime.schedule(
    type: JobType<T>,
    payload: T,
    at: Instant,
    tx: Connection,
    options: JobOptions = JobOptions(),
): JobId = enqueueWithin(type, payload, at, options, tx).id

/**
 * Writes a job through [tx] like the transactional [enqueue], and hands the Enqueued event
 * back instead of emitting it, for a bridge to [publish][PendingEnqueue.publish] once it has
 * observed the commit. [at] null means due now by the runtime clock.
 */
@KlokkaBridgeApi
public suspend fun <T> KlokkaRuntime.enqueueWithin(
    type: JobType<T>,
    payload: T,
    at: Instant?,
    options: JobOptions,
    tx: Connection,
): PendingEnqueue {
    val store =
        jobStore as? JdbcTransactionalStore
            ?: throw IllegalStateException(
                "Transactional enqueue needs a store that can join a JDBC transaction, but the configured " +
                    "store is ${jobStore::class.simpleName}. The in-memory store cannot honor \"the job exists " +
                    "if and only if the transaction commits\"; configure PostgresJobStore (klokka-postgres).",
            )
    check(!tx.autoCommit) {
        "Transactional enqueue was given a connection with auto-commit on, so it is not in a transaction and " +
            "the job would be committed immediately. Call setAutoCommit(false) and commit yourself, or use " +
            "enqueue without tx."
    }
    val job = newJob(type, payload, at ?: now(), options)
    val id = store.enqueue(tx, listOf(job)).single()
    return PendingEnqueue(id, { enqueuedEvent(id, job) }, ::emit)
}

private const val PENDING = 0
private const val PUBLISHED = 1
private const val DISCARDED = 2

/**
 * A job written inside a caller's transaction whose Enqueued event has not been emitted yet.
 * A bridge calls [publish] after the transaction commits, or [discard] after it rolls back;
 * the first of the two wins and later calls do nothing, so a rollback can never be followed
 * by an event and the event is never emitted twice.
 */
@KlokkaBridgeApi
public class PendingEnqueue internal constructor(
    /** The written job's id, or the existing job's id on uniqueKey dedup. */
    public val id: JobId,
    private val event: () -> JobEvent.Enqueued,
    private val emit: (JobEvent) -> Unit,
) {
    private val state = AtomicInteger(PENDING)

    /**
     * Emits the Enqueued event, stamped with the time of this call: the job came into
     * existence at the commit, not at the write. Call after the caller's transaction has committed.
     */
    public fun publish() {
        if (state.compareAndSet(PENDING, PUBLISHED)) emit(event())
    }

    /** Drops the event for good. Call after the caller's transaction has rolled back. */
    public fun discard() {
        state.compareAndSet(PENDING, DISCARDED)
    }
}
