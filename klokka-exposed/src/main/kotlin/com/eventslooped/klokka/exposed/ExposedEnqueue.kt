@file:OptIn(ExperimentalTime::class, KlokkaBridgeApi::class)

package com.eventslooped.klokka.exposed

import com.eventslooped.klokka.JobEvent
import com.eventslooped.klokka.JobId
import com.eventslooped.klokka.JobOptions
import com.eventslooped.klokka.JobType
import com.eventslooped.klokka.KlokkaBridgeApi
import com.eventslooped.klokka.jdbc.PendingEnqueue
import com.eventslooped.klokka.jdbc.enqueueWithin
import com.eventslooped.klokka.runtime.KlokkaRuntime
import org.jetbrains.exposed.v1.core.Transaction
import org.jetbrains.exposed.v1.core.statements.StatementInterceptor
import org.jetbrains.exposed.v1.jdbc.JdbcTransaction
import java.sql.Connection
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

/**
 * Enqueues a job due immediately inside the Exposed transaction [tx]: the job exists if and
 * only if [tx] commits, and no worker sees it before then. Inside `suspendTransaction` or
 * `transaction`, pass the receiver: `klokka.enqueue(SendReceipt, receipt, tx = this)`.
 * Queue resolution and the uniqueKey rule are as in [KlokkaRuntime.enqueue].
 *
 * [JobEvent.Enqueued] is emitted after the commit and never on rollback. In a nested
 * transaction (a savepoint, with `useNestedTransactions`), the event waits for the
 * outermost transaction to commit and is dropped if the savepoint or the outermost
 * transaction rolls back.
 *
 * @throws IllegalStateException when the configured store cannot join a JDBC transaction
 *   (the in-memory store).
 */
public suspend fun <T> KlokkaRuntime.enqueue(
    type: JobType<T>,
    payload: T,
    tx: JdbcTransaction,
    options: JobOptions = JobOptions(),
): JobId = enqueueIn(tx, type, payload, at = null, options)

/**
 * Schedules a job due at [at] inside the Exposed transaction [tx]. Same rules as the
 * transactional [enqueue]: the job exists if and only if [tx] commits.
 *
 * @throws IllegalStateException as for the transactional [enqueue].
 */
public suspend fun <T> KlokkaRuntime.schedule(
    type: JobType<T>,
    payload: T,
    at: Instant,
    tx: JdbcTransaction,
    options: JobOptions = JobOptions(),
): JobId = enqueueIn(tx, type, payload, at, options)

private suspend fun <T> KlokkaRuntime.enqueueIn(
    tx: JdbcTransaction,
    type: JobType<T>,
    payload: T,
    at: Instant?,
    options: JobOptions,
): JobId {
    val pending = enqueueWithin(type, payload, at, options, tx.connection.connection as Connection)
    val outermost = generateSequence(tx) { it.outerTransaction }.last()
    // Publish only when the outermost transaction commits: a savepoint "commit" is just a
    // release and the job can still roll back with the outer transaction.
    outermost.registerInterceptor(EnqueueEventInterceptor(pending, publishOnCommit = true))
    // A savepoint rollback removes the job even if the outer transaction later commits.
    if (tx !== outermost) tx.registerInterceptor(EnqueueEventInterceptor(pending, publishOnCommit = false))
    return pending.id
}

private class EnqueueEventInterceptor(
    private val pending: PendingEnqueue,
    private val publishOnCommit: Boolean,
) : StatementInterceptor {
    override fun afterCommit(transaction: Transaction) {
        if (publishOnCommit) pending.publish()
    }

    override fun afterRollback(transaction: Transaction) {
        pending.discard()
    }
}
