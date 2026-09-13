@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.postgres

import com.eventslooped.klokka.JobRegistry
import com.eventslooped.klokka.JobState
import com.eventslooped.klokka.QueueName
import com.eventslooped.klokka.WorkerId
import com.eventslooped.klokka.jobType
import com.eventslooped.klokka.runtime.KlokkaRuntime
import com.eventslooped.klokka.runtime.KlokkaSettings
import com.eventslooped.klokka.spi.ScheduleSpec
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime

private val E2E = jobType<String>("wakeup.e2e")

/** Collects [PostgresJobStore.wakeups] in the background and exposes hints and failure. */
private class Listener(scope: CoroutineScope, store: PostgresJobStore) {
    val hints = Channel<Unit>(Channel.UNLIMITED)
    val failure = CompletableDeferred<Throwable>()
    val job =
        scope.launch(Dispatchers.Default) {
            try {
                store.wakeups().collect { hints.send(Unit) }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                failure.complete(e)
            }
        }

    suspend fun awaitHint(timeout: Duration = 5.seconds) {
        assertNotNull(withTimeoutOrNull(timeout) { hints.receive() }, "expected a wake-up hint within $timeout")
    }

    suspend fun assertQuiet(window: Duration) {
        assertEquals(null, withTimeoutOrNull(window) { hints.receive() }, "expected NO wake-up hint within $window")
    }

    fun drain() {
        while (hints.tryReceive().isSuccess) { /* discard */ }
    }

    suspend fun stop() = job.cancelAndJoin()
}

internal class PostgresWakeupTest {
    @Test
    fun dueEnqueueProducesAWakeupAndFutureEnqueueStaysSilent() =
        runBlocking {
            val store = PostgresTestSupport.freshStore()
            val listener = Listener(this, store)
            try {
                // The subscription hint: emitted once LISTEN is live, covering the startup gap.
                listener.awaitHint()
                listener.drain()

                store.enqueue(listOf(PostgresTestSupport.newJob()))
                listener.awaitHint()
                listener.drain()

                // A Scheduled row is not claimable, so it must not wake anyone; its due
                // moment is a time arriving, which the poll fallback owns.
                store.enqueue(listOf(PostgresTestSupport.newJob(runAt = PostgresTestSupport.dbNow() + 5.minutes)))
                listener.assertQuiet(500.milliseconds)
            } finally {
                listener.stop()
            }
        }

    @Test
    fun requeueTransitionProducesAWakeup() =
        runBlocking {
            val store = PostgresTestSupport.freshStore()
            val listener = Listener(this, store)
            try {
                listener.awaitHint()
                val id = store.enqueue(listOf(PostgresTestSupport.newJob())).single()
                val claim = store.claim(listOf(QueueName.DEFAULT), setOf("test.kind"), 1, 5.minutes, WorkerId("w")).single()
                assertEquals(id, claim.id)
                listener.drain()

                // The drain path: Running back to Enqueued. The claim itself (to Running) must not notify.
                assertTrue(store.transition(id, JobState.Running, JobState.Enqueued, claim.fence))
                listener.awaitHint()
            } finally {
                listener.stop()
            }
        }

    @Test
    fun completeFireRunsProduceAWakeupOnCommit() =
        runBlocking {
            val store = PostgresTestSupport.freshStore()
            val listener = Listener(this, store)
            try {
                listener.awaitHint()
                val now = PostgresTestSupport.dbNow()
                store.upsertSchedules(listOf(ScheduleSpec("s", "test.kind", "fp", "test", now - 1.minutes)))
                listener.drain()

                val fire = store.dueSchedules(setOf("s"), 1, 5.minutes, WorkerId("w")).single()
                val ids = store.completeFire("s", fire.fence, listOf(PostgresTestSupport.newJob(scheduleId = "s")), null)
                assertEquals(1, ids?.size)
                listener.awaitHint()
            } finally {
                listener.stop()
            }
        }

    @Test
    fun aDeadListenerConnectionFailsTheFlowAndACollectionRestartsCleanly() =
        runBlocking {
            val store = PostgresTestSupport.freshStore()
            val first = Listener(this, store)
            first.awaitHint()

            // Simulate a failover: kill the listener's backend from another connection.
            PostgresTestSupport.dataSource.connection.use { connection ->
                connection.createStatement().use {
                    it.execute(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity " +
                            "WHERE query = 'LISTEN klokka_wakeup' AND pid <> pg_backend_pid()",
                    )
                }
            }
            val failure = withTimeout(5.seconds) { first.failure.await() }
            assertTrue(failure is java.sql.SQLException, "expected an SQLException, got $failure")

            // The caller's restart loop is the reconnect: a fresh collection works at once.
            val second = Listener(this, store)
            try {
                second.awaitHint()
                store.enqueue(listOf(PostgresTestSupport.newJob()))
                second.awaitHint()
            } finally {
                second.stop()
            }
        }

    @Test
    fun endToEndAJobStartsViaPushWhileThePollIsFarAway() =
        runBlocking {
            val store = PostgresTestSupport.freshStore()
            val registry = JobRegistry()
            val done = CompletableDeferred<String>()
            registry.handle(E2E) { payload -> done.complete(payload) }
            // The poll can never fire inside this test; only push can start the job.
            val settings = KlokkaSettings(pollInterval = 10.minutes, workerId = WorkerId("e2e-worker"))
            val runtime = KlokkaRuntime(store, registry, settings)
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            try {
                runtime.start(scope)
                runtime.enqueue(E2E, "pushed")
                // Either ordering is covered: enqueue before LISTEN is caught by the
                // subscription hint's claim round, enqueue after LISTEN by the NOTIFY.
                assertEquals("pushed", withTimeout(5.seconds) { done.await() })
            } finally {
                runtime.drain()
                scope.cancel()
            }
        }
}
