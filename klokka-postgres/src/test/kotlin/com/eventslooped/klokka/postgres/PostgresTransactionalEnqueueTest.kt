@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.postgres

import com.eventslooped.klokka.JobId
import com.eventslooped.klokka.JobStatus
import com.eventslooped.klokka.QueueName
import com.eventslooped.klokka.WorkerId
import com.eventslooped.klokka.spi.JobQuery
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import java.sql.Connection
import java.sql.SQLException
import java.time.OffsetDateTime
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime

private val WORKER = WorkerId("tx-worker")
private val KINDS = setOf("test.kind")

/**
 * Store-level tests of [PostgresJobStore.enqueue] on a caller's JDBC transaction, against a
 * real container. The runtime is not involved: each test opens its own auto-commit-off
 * connection, as the runtime's guard requires, and ends it with a commit or rollback.
 */
internal class PostgresTransactionalEnqueueTest {
    private fun openTx(): Connection =
        PostgresTestSupport.dataSource.connection.apply { autoCommit = false }

    private fun count(sql: String): Long =
        PostgresTestSupport.dataSource.connection.use { connection ->
            connection.createStatement().use { st ->
                st.executeQuery(sql).use { rs ->
                    rs.next()
                    rs.getLong(1)
                }
            }
        }

    private fun Connection.scalar(sql: String): String =
        createStatement().use { st ->
            st.executeQuery(sql).use { rs ->
                rs.next()
                rs.getString(1)
            }
        }

    private suspend fun PostgresJobStore.claimAll(): List<JobId> =
        claim(listOf(QueueName.DEFAULT), KINDS, limit = 10, lease = 1.minutes, worker = WORKER).map { it.id }

    @Test
    fun commitMakesTheJobExistAndClaimable() =
        runBlocking {
            val store = PostgresTestSupport.freshStore()
            val id =
                openTx().use { tx ->
                    val id = store.enqueue(tx, listOf(PostgresTestSupport.newJob())).single()
                    tx.commit()
                    id
                }
            assertEquals("Enqueued", PostgresTestSupport.jobRow(id)?.state)
            assertEquals(listOf(id), store.claimAll())
        }

    @Test
    fun rollbackLeavesNothing() =
        runBlocking {
            val store = PostgresTestSupport.freshStore()
            val id =
                openTx().use { tx ->
                    val id = store.enqueue(tx, listOf(PostgresTestSupport.newJob(), PostgresTestSupport.newJob())).first()
                    tx.rollback()
                    id
                }
            assertNull(PostgresTestSupport.jobRow(id))
            assertEquals(0L, count("SELECT count(*) FROM klokka_jobs"))
            assertEquals(emptyList(), store.claimAll())
        }

    @Test
    fun theJobIsInvisibleToOtherSessionsUntilCommit() =
        runBlocking {
            val store = PostgresTestSupport.freshStore()
            openTx().use { tx ->
                val id = store.enqueue(tx, listOf(PostgresTestSupport.newJob())).single()

                assertEquals(emptyList(), store.claimAll())
                assertEquals(emptyList(), store.listJobs(JobQuery()))
                assertEquals(emptyMap(), store.countsByStatus())
                assertNull(PostgresTestSupport.jobRow(id))

                tx.commit()

                assertEquals(listOf(id), store.listJobs(JobQuery()).map { it.id })
                assertEquals(mapOf(JobStatus.Enqueued to 1L), store.countsByStatus())
                assertEquals(listOf(id), store.claimAll())
            }
        }

    @Test
    fun wakeupsArriveOnCommitOnlyAndNeverOnRollback() =
        runBlocking {
            val store = PostgresTestSupport.freshStore()
            val hints = Channel<Unit>(Channel.UNLIMITED)
            val listener = launch(Dispatchers.Default) { store.wakeups().collect { hints.send(Unit) } }

            suspend fun awaitHint() =
                assertNotNull(withTimeoutOrNull(5.seconds) { hints.receive() }, "expected a wake-up hint within 5s")

            suspend fun assertQuiet(window: Duration) =
                assertNull(withTimeoutOrNull(window) { hints.receive() }, "expected NO wake-up hint within $window")

            try {
                awaitHint() // the subscription hint, emitted once LISTEN is live
                while (hints.tryReceive().isSuccess) { /* drain */ }

                openTx().use { tx ->
                    store.enqueue(tx, listOf(PostgresTestSupport.newJob()))
                    assertQuiet(1.seconds)
                    tx.commit()
                }
                awaitHint()
                while (hints.tryReceive().isSuccess) { /* drain */ }

                openTx().use { tx ->
                    store.enqueue(tx, listOf(PostgresTestSupport.newJob()))
                    tx.rollback()
                }
                assertQuiet(1.seconds)
            } finally {
                listener.cancelAndJoin()
            }
        }

    @Test
    fun uniqueKeyDedupesAgainstACommittedRowFromInsideATransaction() =
        runBlocking {
            val store = PostgresTestSupport.freshStore()
            val existing = store.enqueue(listOf(PostgresTestSupport.newJob(uniqueKey = "k1"))).single()

            openTx().use { tx ->
                val again = store.enqueue(tx, listOf(PostgresTestSupport.newJob(uniqueKey = "k1"))).single()
                assertEquals(existing, again)
                tx.commit()
            }
            assertEquals(1L, count("SELECT count(*) FROM klokka_jobs"))
        }

    @Test
    fun uniqueKeyWaitsForAnUncommittedConflictThenDedupsWhenItCommits() =
        runBlocking {
            val store = PostgresTestSupport.freshStore()
            openTx().use { a ->
                openTx().use { b ->
                    val idA = store.enqueue(a, listOf(PostgresTestSupport.newJob(uniqueKey = "race"))).single()
                    val blocked = startEnqueue(store, b, "race")

                    delay(750.milliseconds)
                    assertFalse(blocked.isCompleted, "B must wait for A's uncommitted row")

                    a.commit()
                    val idB = withTimeout(10.seconds) { blocked.await() }
                    assertEquals(idA, idB)
                    b.commit()
                }
            }
            assertEquals(1L, count("SELECT count(*) FROM klokka_jobs"))
        }

    @Test
    fun uniqueKeyWaitsForAnUncommittedConflictThenInsertsWhenItRollsBack() =
        runBlocking {
            val store = PostgresTestSupport.freshStore()
            openTx().use { a ->
                openTx().use { b ->
                    val idA = store.enqueue(a, listOf(PostgresTestSupport.newJob(uniqueKey = "race"))).single()
                    val blocked = startEnqueue(store, b, "race")

                    delay(750.milliseconds)
                    assertFalse(blocked.isCompleted, "B must wait for A's uncommitted row")

                    a.rollback()
                    val idB = withTimeout(10.seconds) { blocked.await() }
                    assertNotEquals(idA, idB)
                    b.commit()
                    assertEquals("Enqueued", PostgresTestSupport.jobRow(idB)?.state)
                }
            }
            assertEquals(1L, count("SELECT count(*) FROM klokka_jobs"))
        }

    /** Runs the enqueue on its own thread: it blocks inside the database until the other tx ends. */
    private fun CoroutineScope.startEnqueue(store: PostgresJobStore, tx: Connection, key: String): Deferred<JobId> =
        async(Dispatchers.IO) { store.enqueue(tx, listOf(PostgresTestSupport.newJob(uniqueKey = key))).single() }

    @Test
    fun futureRunAtIsScheduledAndPastRunAtIsEnqueued() =
        runBlocking {
            val store = PostgresTestSupport.freshStore()
            val (future, past) =
                openTx().use { tx ->
                    val ids =
                        store.enqueue(
                            tx,
                            listOf(
                                PostgresTestSupport.newJob(runAt = PostgresTestSupport.dbNow() + 5.minutes),
                                PostgresTestSupport.newJob(runAt = PostgresTestSupport.dbNow() - 1.minutes),
                            ),
                        )
                    tx.commit()
                    ids
                }
            assertEquals("Scheduled", PostgresTestSupport.jobRow(future)?.state)
            assertEquals("Enqueued", PostgresTestSupport.jobRow(past)?.state)
            assertEquals(listOf(past), store.claimAll())
        }

    @Test
    fun enqueuedAtIsStatementTimeNotTransactionStart() =
        runBlocking {
            val store = PostgresTestSupport.freshStore()
            val (txNow, id) =
                openTx().use { tx ->
                    val txNow =
                        tx.createStatement().use { st ->
                            st.executeQuery("SELECT now()").use { rs ->
                                rs.next()
                                rs.getObject(1, OffsetDateTime::class.java)
                            }
                        }
                    delay(200.milliseconds)
                    val id = store.enqueue(tx, listOf(PostgresTestSupport.newJob())).single()
                    tx.commit()
                    txNow to id
                }
            val enqueuedAt =
                PostgresTestSupport.dataSource.connection.use { connection ->
                    connection.prepareStatement("SELECT enqueued_at FROM klokka_jobs WHERE id = ?").use { ps ->
                        ps.setLong(1, id.value.toLong())
                        ps.executeQuery().use { rs ->
                            rs.next()
                            rs.getObject(1, OffsetDateTime::class.java)
                        }
                    }
                }
            val lagMillis = java.time.Duration.between(txNow, enqueuedAt).toMillis()
            assertTrue(lagMillis >= 150, "enqueued_at must be statement time, was only ${lagMillis}ms after the tx started")
        }

    @Test
    fun aConfiguredSchemaIsQualifiedInTheSqlAndTheCallersSessionIsUntouched() =
        runBlocking {
            PostgresTestSupport.freshStore()
            val schemaStore = PostgresJobStore(PostgresTestSupport.dataSource, PostgresStoreConfig(schema = "klokka_tx"))
            schemaStore.migrate()
            PostgresTestSupport.dataSource.connection.use { connection ->
                connection.createStatement().use { it.execute("TRUNCATE klokka_tx.klokka_jobs RESTART IDENTITY") }
            }

            val job = PostgresTestSupport.newJob(kind = "tx.schema.kind")
            openTx().use { tx ->
                val before = tx.scalar("SHOW search_path")
                schemaStore.enqueue(tx, listOf(job))
                assertEquals(before, tx.scalar("SHOW search_path"))
                tx.commit()
            }

            assertEquals(1L, count("SELECT count(*) FROM klokka_tx.klokka_jobs WHERE kind = 'tx.schema.kind'"))
            assertEquals(0L, count("SELECT count(*) FROM public.klokka_jobs WHERE kind = 'tx.schema.kind'"))
        }

    @Test
    fun theCallersAutoCommitAndTransactionStayUsable() =
        runBlocking {
            val store = PostgresTestSupport.freshStore()
            val txJob = PostgresTestSupport.newJob(kind = "tx.first")
            val secondJob = PostgresTestSupport.newJob(kind = "tx.second")
            openTx().use { tx ->
                store.enqueue(tx, listOf(txJob))
                assertFalse(tx.autoCommit)
                assertFalse(tx.isClosed)

                // The same transaction keeps working: a statement of the caller's own, then a second enqueue.
                tx.createStatement().use { it.execute("UPDATE klokka_jobs SET payload = '{\"touched\":true}'") }
                store.enqueue(tx, listOf(secondJob))
                assertFalse(tx.autoCommit)
                tx.commit()
            }
            assertEquals(2L, count("SELECT count(*) FROM klokka_jobs"))
            assertEquals(1L, count("SELECT count(*) FROM klokka_jobs WHERE payload = '{\"touched\":true}'"))
        }

    @Test
    fun repeatableReadSurfacesTheSerializationFailureOfAUniqueKeyRace() =
        runBlocking {
            val store = PostgresTestSupport.freshStore()
            openTx().use { tx ->
                tx.transactionIsolation = Connection.TRANSACTION_REPEATABLE_READ
                tx.scalar("SELECT count(*) FROM klokka_jobs") // takes the snapshot

                // Another session commits the key after the caller's snapshot.
                store.enqueue(listOf(PostgresTestSupport.newJob(uniqueKey = "rr")))

                val failure =
                    assertFailsWith<SQLException> {
                        store.enqueue(tx, listOf(PostgresTestSupport.newJob(uniqueKey = "rr")))
                    }
                assertEquals("40001", failure.sqlState)
                tx.rollback()
            }
            assertEquals(1L, count("SELECT count(*) FROM klokka_jobs"))
        }
}
