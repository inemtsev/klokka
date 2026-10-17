@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.exposed

import com.eventslooped.klokka.JobId
import com.eventslooped.klokka.JobOptions
import com.eventslooped.klokka.JobRegistry
import com.eventslooped.klokka.QueueName
import com.eventslooped.klokka.runtime.KlokkaRuntime
import com.eventslooped.klokka.runtime.KlokkaSettings
import com.eventslooped.klokka.store.InMemoryJobStore
import kotlinx.coroutines.runBlocking
import org.jetbrains.exposed.v1.jdbc.transactions.suspendTransaction
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration.Companion.hours
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

private class Boom : RuntimeException("boom")

/**
 * The Exposed bridge against a real Postgres. The contract under test: a job has an
 * Enqueued event if and only if it exists, and the event arrives after the commit that
 * made it exist. Every assertion is written to that contract, not to how Exposed happens
 * to deliver its commit and rollback callbacks.
 */
internal class ExposedEnqueueTest {
    private lateinit var probe: EventProbe
    private val pg = ExposedTestDatabase

    @BeforeTest
    fun setUp() {
        pg.reset()
        probe = EventProbe()
    }

    @AfterTest
    fun tearDown() = probe.close()

    @Test
    fun commitCreatesTheRowAndEmitsOneEventAfterTheCommit() =
        runBlocking {
            var eventsInsideTx = -1
            val id =
                suspendTransaction(db = pg.plain) {
                    val options = JobOptions(queue = QueueName("mail"))
                    val id = probe.runtime.enqueue(Ping, "a", tx = this, options = options)
                    probe.settle()
                    eventsInsideTx = probe.enqueued.size
                    id
                }
            probe.awaitControl()

            assertEquals(0, eventsInsideTx, "no event may be observed before the commit")
            assertEquals(1, pg.jobCount(id))
            val event = probe.enqueued.filter { it.jobId == id }.single()
            assertEquals(Ping.kind, event.kind)
            assertEquals(QueueName("mail"), event.queue)
            assertEquals(1, probe.enqueued.count { it.jobId == id })
        }

    @Test
    fun rollbackByExceptionLeavesNoRowAndNoEvent() =
        runBlocking {
            var id = JobId("unset")
            assertFailsWith<Boom> {
                suspendTransaction(db = pg.plain) {
                    id = probe.runtime.enqueue(Ping, "a", tx = this)
                    throw Boom()
                }
            }
            val control = probe.awaitControl()

            assertEquals(0, pg.jobCount(id))
            assertEquals(listOf(control), probe.ids(), "only the control job may have an event")
        }

    @Test
    fun explicitRollbackDropsTheFirstJobAndLaterWorkStillCommitsWithItsEvent() =
        runBlocking {
            var first = JobId("unset")
            var second = JobId("unset")
            suspendTransaction(db = pg.plain) {
                first = probe.runtime.enqueue(Ping, "first", tx = this)
                rollback()
                second = probe.runtime.enqueue(Ping, "second", tx = this)
            }
            val control = probe.awaitControl()

            assertEquals(0, pg.jobCount(first), "the rolled-back job has no row")
            assertEquals(1, pg.jobCount(second), "the job enqueued after the rollback committed")
            assertEquals(listOf(second, control), probe.ids(), "no event for the first job, one for the second")
        }

    @Test
    fun explicitCommitMidBlockKeepsTheCommittedJobEvenIfTheRestRollsBack() =
        runBlocking {
            var first = JobId("unset")
            var second = JobId("unset")
            assertFailsWith<Boom> {
                suspendTransaction(db = pg.plain) {
                    first = probe.runtime.enqueue(Ping, "first", tx = this)
                    commit()
                    second = probe.runtime.enqueue(Ping, "second", tx = this)
                    throw Boom()
                }
            }
            val control = probe.awaitControl()

            assertEquals(1, pg.jobCount(first), "the job committed mid-block exists")
            assertEquals(0, pg.jobCount(second), "the job written after the commit was rolled back")
            assertEquals(listOf(first, control), probe.ids(), "one event for the committed job, none for the other")
        }

    @Test
    fun scheduleWritesAScheduledRowAndTheEventCarriesTheDueTime() =
        runBlocking {
            // Whole microseconds: the store's precision, so equality is exact on both sides.
            val now = Clock.System.now()
            val at = Instant.fromEpochSeconds((now + 1.hours).epochSeconds, 123_456_000)
            val id =
                suspendTransaction(db = pg.plain) {
                    probe.runtime.schedule(Ping, "later", at, tx = this)
                }
            probe.awaitControl()

            assertEquals("Scheduled", pg.jobState(id))
            assertEquals(at, pg.jobRunAt(id))
            assertEquals(at, probe.enqueued.single { it.jobId == id }.scheduledFor)
        }

    @Test
    fun uniqueKeyDedupAgainstACommittedJobReturnsTheExistingId() =
        runBlocking {
            val options = JobOptions(uniqueKey = "dedup-1")
            val existing = probe.runtime.enqueue(Ping, "original", options)
            probe.awaitControl()
            val before = probe.enqueued.count { it.jobId == existing }

            val returned =
                suspendTransaction(db = pg.plain) {
                    probe.runtime.enqueue(Ping, "duplicate", tx = this, options = options)
                }
            probe.awaitControl()

            assertEquals(existing, returned, "dedup returns the existing job's id")
            assertEquals(1, pg.count("SELECT count(*) FROM klokka_jobs WHERE unique_key = 'dedup-1'"))
            // Observed behavior, tracked in issue #36: the dedup path is not distinguished from
            // a fresh write, so the commit emits a second Enqueued event for the existing id.
            // The job does exist, so the "event implies the job exists" contract holds; what
            // #36 is about is that the event does not say this call created nothing. If #36
            // makes dedup silent, this assertion should become `before` instead of `before + 1`.
            assertEquals(before + 1, probe.enqueued.count { it.jobId == existing })
        }

    @Test
    fun inMemoryStoreIsRejectedAndTheSurroundingTransactionRollsBack() =
        runBlocking {
            val memory = KlokkaRuntime(InMemoryJobStore(), JobRegistry(), KlokkaSettings())

            assertFailsWith<IllegalStateException> {
                suspendTransaction(db = pg.plain) {
                    insertScratch("must-not-survive")
                    memory.enqueue(Ping, "a", tx = this)
                }
            }
            probe.awaitControl()

            assertEquals(0, pg.count("SELECT count(*) FROM scratch"), "the transaction rolled back cleanly")
            assertEquals(1, pg.count("SELECT count(*) FROM klokka_jobs"), "only the control job exists")
        }

    @Test
    fun nestedSavepointEnqueueEmitsAfterTheOuterCommitNotAtTheInnerEnd() =
        runBlocking {
            var eventsAfterInner = -1
            var id = JobId("unset")
            suspendTransaction(db = pg.nested) {
                suspendTransaction(db = pg.nested) {
                    assertTrue(outerTransaction != null, "useNestedTransactions makes the inner block a savepoint")
                    id = probe.runtime.enqueue(Ping, "inner", tx = this)
                }
                probe.settle()
                eventsAfterInner = probe.enqueued.size
            }
            probe.awaitControl()

            assertEquals(0, eventsAfterInner, "the inner savepoint release is not a commit")
            assertEquals(1, pg.jobCount(id))
            assertEquals(1, probe.enqueued.count { it.jobId == id })
        }

    @Test
    fun nestedSavepointRollbackDropsTheRowAndTheEventEvenWhenTheOuterCommits() =
        runBlocking {
            var id = JobId("unset")
            suspendTransaction(db = pg.nested) {
                insertScratch("outer")
                runCatching {
                    suspendTransaction(db = pg.nested) {
                        assertTrue(outerTransaction != null, "the inner block is a savepoint")
                        id = probe.runtime.enqueue(Ping, "inner", tx = this)
                        throw Boom()
                    }
                }.also { assertTrue(it.exceptionOrNull() is Boom) }
            }
            val control = probe.awaitControl()

            assertEquals(1, pg.count("SELECT count(*) FROM scratch"), "the outer transaction committed")
            assertEquals(0, pg.jobCount(id), "the savepoint rolled the job back")
            assertEquals(listOf(control), probe.ids(), "no event for a job that does not exist")
        }

    @Test
    fun nestedEnqueueThenOuterFailureLeavesNoRowAndNoEvent() =
        runBlocking {
            var id = JobId("unset")
            assertFailsWith<Boom> {
                suspendTransaction(db = pg.nested) {
                    suspendTransaction(db = pg.nested) {
                        id = probe.runtime.enqueue(Ping, "inner", tx = this)
                    }
                    throw Boom()
                }
            }
            val control = probe.awaitControl()

            assertEquals(0, pg.jobCount(id))
            assertEquals(listOf(control), probe.ids())
        }

    @Test
    fun withoutNestedTransactionsTheInnerBlockReusesTheOuterAndEmitsOneEvent() =
        runBlocking {
            var id = JobId("unset")
            suspendTransaction(db = pg.plain) {
                val outer = this
                suspendTransaction(db = pg.plain) {
                    assertTrue(this === outer, "without useNestedTransactions the inner block reuses the outer")
                    id = probe.runtime.enqueue(Ping, "inner", tx = this)
                }
            }
            probe.awaitControl()

            assertEquals(1, pg.jobCount(id))
            assertEquals(1, probe.enqueued.count { it.jobId == id })
        }
}
