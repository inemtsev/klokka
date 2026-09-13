@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka

import com.eventslooped.klokka.spi.JobQuery
import com.eventslooped.klokka.spi.NewJob
import com.eventslooped.klokka.store.InMemoryJobStore
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

private val EPOCH: Instant = Instant.fromEpochMilliseconds(0)
private val WORKER = WorkerId("query-test-worker")

private fun job(kind: String = "orders.confirm", queue: String = "default", runAt: Instant = EPOCH): NewJob =
    NewJob(kind = kind, payload = """{"n":1}""", queue = QueueName(queue), runAt = runAt)

public class InMemoryQueryStoreTest {
    @Test
    public fun countsGroupByStatusAndOmitNothingThatExists() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            store.enqueue(listOf(job(), job(), job(runAt = EPOCH + 5.minutes)))
            store.enqueue(listOf(job()))
            store.claim(listOf(QueueName.DEFAULT), setOf("orders.confirm"), 1, 1.minutes, WORKER).single()

            val counts = store.countsByStatus()
            assertEquals(1L, counts[JobStatus.Scheduled])
            assertEquals(2L, counts[JobStatus.Enqueued])
            assertEquals(1L, counts[JobStatus.Running])
            assertNull(counts[JobStatus.DeadLettered], "zero statuses may be omitted")
        }

    @Test
    public fun listingFiltersConjunctivelyAndOrdersNewestFirst() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val first = store.enqueue(listOf(job(kind = "a.one"))).single()
            val second = store.enqueue(listOf(job(kind = "a.one", queue = "email"))).single()
            val third = store.enqueue(listOf(job(kind = "b.two"))).single()

            assertEquals(listOf(third, second, first), store.listJobs(JobQuery()).map { it.id })
            assertEquals(listOf(second, first), store.listJobs(JobQuery(kind = "a.one")).map { it.id })
            assertEquals(
                listOf(second),
                store.listJobs(JobQuery(kind = "a.one", queue = QueueName("email"))).map { it.id },
            )
            assertEquals(emptyList(), store.listJobs(JobQuery(kind = "b.two", queue = QueueName("email"))))
        }

    @Test
    public fun listingPagesThroughTheFilteredSet() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val ids = (1..7).map { store.enqueue(listOf(job())).single() }.reversed()

            assertEquals(ids.take(3), store.listJobs(JobQuery(limit = 3)).map { it.id })
            assertEquals(ids.drop(3).take(3), store.listJobs(JobQuery(limit = 3, offset = 3)).map { it.id })
            assertEquals(ids.drop(6), store.listJobs(JobQuery(limit = 3, offset = 6)).map { it.id })
            assertFailsWith<IllegalArgumentException> { JobQuery(limit = 0) }
            assertFailsWith<IllegalArgumentException> { JobQuery(offset = -1) }
        }

    @Test
    public fun summariesCarryStatusSpecificFields() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val id = store.enqueue(listOf(job())).single()
            val claim = store.claim(listOf(QueueName.DEFAULT), setOf("orders.confirm"), 1, 5.minutes, WORKER).single()

            val running = store.listJobs(JobQuery(status = JobStatus.Running)).single()
            assertEquals(1, running.attempt)
            assertNull(running.retryAt)
            assertNull(running.terminalAt)

            val retryAt = EPOCH + 2.minutes
            store.transition(id, JobState.Running, JobState.Failed(retryAt), claim.fence)
            val failed = store.listJobs(JobQuery(status = JobStatus.Failed)).single()
            assertEquals(retryAt, failed.retryAt)
            assertNull(failed.terminalAt)

            clock.advanceBy(3.minutes)
            val second = store.claim(listOf(QueueName.DEFAULT), setOf("orders.confirm"), 1, 5.minutes, WORKER).single()
            store.transition(id, JobState.Running, JobState.Succeeded, second.fence)
            val done = store.listJobs(JobQuery(status = JobStatus.Succeeded)).single()
            assertNull(done.retryAt)
            assertEquals(clock.now(), done.terminalAt)
            assertEquals(2, done.attempt)
        }

    @Test
    public fun detailExposesPayloadAndLeaseOnlyWhileRunning() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val id = store.enqueue(listOf(job())).single()

            val idle = assertNotNull(store.getJob(id))
            assertEquals("""{"n":1}""", idle.payload)
            assertNull(idle.leaseUntil)
            assertNull(idle.holder)

            val claim = store.claim(listOf(QueueName.DEFAULT), setOf("orders.confirm"), 1, 5.minutes, WORKER).single()
            val running = assertNotNull(store.getJob(id))
            assertEquals(EPOCH + 5.minutes, running.leaseUntil)
            assertEquals(WORKER, running.holder)
            assertEquals(claim.fence, running.fence)

            assertNull(store.getJob(JobId("job-999")), "unknown ids are null, not an error")
            assertNull(store.getJob(JobId("not-even-shaped-right")))
        }

    @Test
    public fun requeueIsAPlainCasAndTheBudgetResumes() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val id = store.enqueue(listOf(job())).single()
            val claim = store.claim(listOf(QueueName.DEFAULT), setOf("orders.confirm"), 1, 5.minutes, WORKER).single()
            store.transition(id, JobState.Running, JobState.DeadLettered, claim.fence)

            // The dashboard's requeue: no new SPI, one CAS, terminal rows carry no fence.
            assertTrue(store.transition(id, JobState.DeadLettered, JobState.Enqueued))
            val summary = store.listJobs(JobQuery(status = JobStatus.Enqueued)).single()
            assertEquals(id, summary.id)
            assertEquals(1, summary.attempt, "attempt history survives a requeue; the budget resumes, not resets")
            assertNull(summary.terminalAt, "a requeued row is live again: terminalAt must clear")

            val revived = store.claim(listOf(QueueName.DEFAULT), setOf("orders.confirm"), 1, 5.minutes, WORKER).single()
            assertEquals(2, revived.attempt)
        }
}
