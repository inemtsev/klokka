@file:OptIn(ExperimentalTime::class, KlokkaBridgeApi::class)

package com.eventslooped.klokka.jdbc

import com.eventslooped.klokka.JobEvent
import com.eventslooped.klokka.JobId
import com.eventslooped.klokka.JobOptions
import com.eventslooped.klokka.JobRegistry
import com.eventslooped.klokka.KlokkaBridgeApi
import com.eventslooped.klokka.QueueName
import com.eventslooped.klokka.TestClock
import com.eventslooped.klokka.WorkerId
import com.eventslooped.klokka.jobType
import com.eventslooped.klokka.runtime.KlokkaRole
import com.eventslooped.klokka.runtime.KlokkaRuntime
import com.eventslooped.klokka.runtime.KlokkaSettings
import com.eventslooped.klokka.spi.JdbcTransactionalStore
import com.eventslooped.klokka.spi.JobStore
import com.eventslooped.klokka.spi.NewJob
import com.eventslooped.klokka.store.InMemoryJobStore
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.Serializable
import java.lang.reflect.Proxy
import java.sql.Connection
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.hours
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

@Serializable
private data class TxPayload(val value: String = "x")

private val EPOCH: Instant = Instant.fromEpochMilliseconds(1_000_000)

private val TX_TYPE = jobType<TxPayload>("tx.test")
private val ORDERS_TYPE = jobType<TxPayload>("tx.orders", queue = QueueName("orders"))

/** Method names that must never be invoked on a caller-owned connection. */
private val FORBIDDEN_CALLS = setOf("commit", "rollback", "setAutoCommit", "setTransactionIsolation", "close")

/** A [Connection] proxy that records every invoked method name and answers [autoCommit] reads. */
private class FakeConnection(val autoCommit: Boolean) {
    val calls: MutableList<String> = mutableListOf()

    val connection: Connection =
        Proxy.newProxyInstance(
            Connection::class.java.classLoader,
            arrayOf(Connection::class.java),
        ) { proxy, method, args ->
            when (method.name) {
                "hashCode" -> return@newProxyInstance System.identityHashCode(proxy)
                "equals" -> return@newProxyInstance proxy === args?.firstOrNull()
                "toString" -> return@newProxyInstance "FakeConnection"
            }
            calls.add(method.name)
            when {
                method.name == "getAutoCommit" -> autoCommit
                method.returnType == Boolean::class.javaPrimitiveType -> false
                method.returnType == Int::class.javaPrimitiveType -> 0
                method.returnType == Long::class.javaPrimitiveType -> 0L
                method.returnType == Short::class.javaPrimitiveType -> 0.toShort()
                method.returnType == Byte::class.javaPrimitiveType -> 0.toByte()
                method.returnType == Float::class.javaPrimitiveType -> 0f
                method.returnType == Double::class.javaPrimitiveType -> 0.0
                else -> null
            }
        } as Connection

    fun forbiddenCalls(): List<String> = calls.filter { it in FORBIDDEN_CALLS }
}

/** An in-memory store that also claims to join JDBC transactions, recording what it is handed. */
private class RecordingJdbcStore(
    private val delegate: InMemoryJobStore,
) : JobStore by delegate,
    JdbcTransactionalStore {
    class Call(val tx: Connection, val jobs: List<NewJob>)

    val calls: MutableList<Call> = mutableListOf()
    val returnedIds: MutableList<JobId> = mutableListOf()

    override suspend fun enqueue(tx: Connection, jobs: List<NewJob>): List<JobId> {
        calls.add(Call(tx, jobs))
        return delegate.enqueue(jobs).also { returnedIds.addAll(it) }
    }
}

private class Fixture(val clock: TestClock, val store: RecordingJdbcStore, val runtime: KlokkaRuntime)

private fun runtimeOver(store: JobStore, clock: TestClock): KlokkaRuntime =
    KlokkaRuntime(
        store,
        JobRegistry(),
        KlokkaSettings(role = KlokkaRole.Producer, clock = clock),
    )

private fun fixture(): Fixture {
    val clock = TestClock(EPOCH)
    val store = RecordingJdbcStore(InMemoryJobStore(clock))
    return Fixture(clock, store, runtimeOver(store, clock))
}

/** Subscribes to [runtime]'s events before returning, so nothing emitted afterward is missed. */
private fun TestScope.collectEvents(runtime: KlokkaRuntime): MutableList<JobEvent> {
    val events = mutableListOf<JobEvent>()
    backgroundScope.launch(start = CoroutineStart.UNDISPATCHED) {
        runtime.events.collect { events.add(it) }
    }
    return events
}

public class TransactionalEnqueueTest {
    @Test
    public fun inMemoryStoreRejectsConnectionEnqueue() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val runtime = runtimeOver(store, clock)
            val conn = FakeConnection(autoCommit = false)

            val failure =
                assertFailsWith<IllegalStateException> {
                    runtime.enqueue(TX_TYPE, TxPayload(), conn.connection)
                }

            val message = failure.message.orEmpty()
            assertTrue("InMemoryJobStore" in message, message)
            assertTrue("PostgresJobStore" in message, message)
            assertEquals(emptyList(), conn.calls)
            val claimed = store.claim(listOf(QueueName.DEFAULT), setOf(TX_TYPE.kind), 10, 1.hours, WorkerId("w"))
            assertEquals(emptyList(), claimed)
        }

    @Test
    public fun autoCommitConnectionIsRejectedBeforeTheStoreIsCalled() =
        runTest {
            val f = fixture()
            val conn = FakeConnection(autoCommit = true)

            val failure =
                assertFailsWith<IllegalStateException> {
                    f.runtime.enqueue(TX_TYPE, TxPayload(), conn.connection)
                }
            val scheduled =
                assertFailsWith<IllegalStateException> {
                    f.runtime.schedule(TX_TYPE, TxPayload(), EPOCH, conn.connection)
                }

            assertTrue("auto-commit" in failure.message.orEmpty(), failure.message)
            assertTrue("auto-commit" in scheduled.message.orEmpty(), scheduled.message)
            assertEquals(emptyList(), f.store.calls.map { it.jobs })
            assertEquals(emptyList(), conn.forbiddenCalls())
        }

    @Test
    public fun enqueueHandsTheCallersConnectionAndAJobDueNowToTheStore() =
        runTest {
            val f = fixture()
            val conn = FakeConnection(autoCommit = false)

            val id = f.runtime.enqueue(TX_TYPE, TxPayload("hello"), conn.connection)

            val call = f.store.calls.single()
            assertSame(conn.connection, call.tx)
            val job = call.jobs.single()
            assertEquals("tx.test", job.kind)
            assertEquals("""{"value":"hello"}""", job.payload.replace(" ", ""))
            assertEquals(QueueName.DEFAULT, job.queue)
            assertEquals(null, job.uniqueKey)
            assertEquals(f.clock.now(), job.runAt)
            assertEquals(f.store.returnedIds.single(), id)
        }

    @Test
    public fun queueResolutionFollowsTypeThenOptions() =
        runTest {
            val f = fixture()
            val conn = FakeConnection(autoCommit = false)

            f.runtime.enqueue(ORDERS_TYPE, TxPayload(), conn.connection)
            f.runtime.enqueue(ORDERS_TYPE, TxPayload(), conn.connection, JobOptions(queue = QueueName("override")))

            assertEquals(QueueName("orders"), f.store.calls[0].jobs.single().queue)
            assertEquals(QueueName("override"), f.store.calls[1].jobs.single().queue)
        }

    @Test
    public fun uniqueKeyComesFromOptionsAndDedupReturnsTheExistingId() =
        runTest {
            val f = fixture()
            val conn = FakeConnection(autoCommit = false)
            val options = JobOptions(uniqueKey = "receipt-42")

            val first = f.runtime.enqueue(TX_TYPE, TxPayload(), conn.connection, options)
            val second = f.runtime.enqueue(TX_TYPE, TxPayload(), conn.connection, options)

            assertEquals("receipt-42", f.store.calls[0].jobs.single().uniqueKey)
            assertEquals("receipt-42", f.store.calls[1].jobs.single().uniqueKey)
            assertEquals(first, second)
        }

    @Test
    public fun scheduleUsesTheGivenInstantAsRunAt() =
        runTest {
            val f = fixture()
            val conn = FakeConnection(autoCommit = false)
            val at = EPOCH + 2.hours

            val id = f.runtime.schedule(TX_TYPE, TxPayload(), at, conn.connection)

            val call = f.store.calls.single()
            assertSame(conn.connection, call.tx)
            assertEquals(at, call.jobs.single().runAt)
            assertEquals(f.store.returnedIds.single(), id)
        }

    @Test
    public fun connectionOverloadsEmitNoEnqueuedEvent() =
        runTest {
            val f = fixture()
            val conn = FakeConnection(autoCommit = false)
            val events = collectEvents(f.runtime)

            f.runtime.enqueue(TX_TYPE, TxPayload(), conn.connection)
            f.runtime.schedule(TX_TYPE, TxPayload(), EPOCH + 1.hours, conn.connection)
            runCurrent()

            assertEquals(2, f.store.calls.size)
            assertEquals(emptyList(), events)
        }

    @Test
    public fun runtimeNeverEndsTheCallersTransaction() =
        runTest {
            val f = fixture()
            val conn = FakeConnection(autoCommit = false)

            f.runtime.enqueue(TX_TYPE, TxPayload(), conn.connection)
            f.runtime.schedule(TX_TYPE, TxPayload(), EPOCH + 1.hours, conn.connection)
            f.runtime.enqueueWithin(TX_TYPE, TxPayload(), null, JobOptions(), conn.connection).publish()

            assertEquals(emptyList(), conn.forbiddenCalls())
            assertTrue(conn.calls.all { it == "getAutoCommit" }, conn.calls.toString())
        }

    @Test
    public fun publishEmitsExactlyOneEnqueuedEvent() =
        runTest {
            val f = fixture()
            val conn = FakeConnection(autoCommit = false)
            val at = EPOCH + 3.hours
            val events = collectEvents(f.runtime)

            val pending =
                f.runtime.enqueueWithin(ORDERS_TYPE, TxPayload(), at, JobOptions(), conn.connection)
            runCurrent()
            assertEquals(emptyList(), events)

            pending.publish()
            runCurrent()
            pending.publish()
            runCurrent()

            val event = events.single() as JobEvent.Enqueued
            assertEquals(pending.id, event.jobId)
            assertEquals("tx.orders", event.kind)
            assertEquals(QueueName("orders"), event.queue)
            assertEquals(at, event.scheduledFor)
        }

    @Test
    public fun discardThenPublishEmitsNothing() =
        runTest {
            val f = fixture()
            val conn = FakeConnection(autoCommit = false)
            val events = collectEvents(f.runtime)

            val pending = f.runtime.enqueueWithin(TX_TYPE, TxPayload(), null, JobOptions(), conn.connection)
            pending.discard()
            pending.publish()
            runCurrent()

            assertEquals(emptyList(), events)
        }

    @Test
    public fun publishThenDiscardKeepsTheSingleEvent() =
        runTest {
            val f = fixture()
            val conn = FakeConnection(autoCommit = false)
            val events = collectEvents(f.runtime)

            val pending = f.runtime.enqueueWithin(TX_TYPE, TxPayload(), null, JobOptions(), conn.connection)
            pending.publish()
            pending.discard()
            runCurrent()

            assertEquals(1, events.size)
            assertEquals(pending.id, (events.single() as JobEvent.Enqueued).jobId)
        }

    @Test
    public fun nullAtMeansDueAtTheRuntimeClocksNow() =
        runTest {
            val f = fixture()
            val conn = FakeConnection(autoCommit = false)
            f.clock.advanceBy(5.hours)
            val events = collectEvents(f.runtime)

            val pending = f.runtime.enqueueWithin(TX_TYPE, TxPayload(), null, JobOptions(), conn.connection)
            pending.publish()
            runCurrent()

            assertEquals(f.clock.now(), f.store.calls.single().jobs.single().runAt)
            assertEquals(f.clock.now(), (events.single() as JobEvent.Enqueued).scheduledFor)
        }
}
