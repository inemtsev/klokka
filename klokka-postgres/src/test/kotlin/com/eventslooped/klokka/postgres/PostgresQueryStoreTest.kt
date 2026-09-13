@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.postgres

import com.eventslooped.klokka.JobId
import com.eventslooped.klokka.JobState
import com.eventslooped.klokka.JobStatus
import com.eventslooped.klokka.QueueName
import com.eventslooped.klokka.WorkerId
import com.eventslooped.klokka.spi.JobQuery
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes
import kotlin.time.ExperimentalTime

private val WORKER = WorkerId("query-test-worker")

/** Covers every kind the fixtures in this file enqueue by default. */
private val DEFAULT_KINDS = setOf("test.kind")

/**
 * Conformance tests for [PostgresJobStore]'s [com.eventslooped.klokka.spi.QueryableStore]
 * side, run against a real container via [PostgresTestSupport]. Ports every case from
 * `InMemoryQueryStoreTest`: same names, same assertions, adapted to database time. As in
 * [PostgresJobStoreContractTest] there is no injectable clock; due jobs and booked retries
 * are made due through past `runAt`/`retry_at` values read back from [PostgresTestSupport.dbNow].
 */
public class PostgresQueryStoreTest {
    @Test
    public fun countsGroupByStatusAndOmitNothingThatExists() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            store.enqueue(
                listOf(
                    PostgresTestSupport.newJob(),
                    PostgresTestSupport.newJob(),
                    PostgresTestSupport.newJob(runAt = PostgresTestSupport.dbNow() + 5.minutes),
                ),
            )
            store.enqueue(listOf(PostgresTestSupport.newJob()))
            store.claim(listOf(QueueName.DEFAULT), DEFAULT_KINDS, 1, 1.minutes, WORKER).single()

            val counts = store.countsByStatus()
            assertEquals(1L, counts[JobStatus.Scheduled])
            assertEquals(2L, counts[JobStatus.Enqueued])
            assertEquals(1L, counts[JobStatus.Running])
            assertNull(counts[JobStatus.DeadLettered], "zero statuses may be omitted")
        }

    @Test
    public fun listingFiltersConjunctivelyAndOrdersNewestFirst() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val first = store.enqueue(listOf(PostgresTestSupport.newJob(kind = "a.one"))).single()
            val second =
                store.enqueue(listOf(PostgresTestSupport.newJob(kind = "a.one", queue = QueueName("email")))).single()
            val third = store.enqueue(listOf(PostgresTestSupport.newJob(kind = "b.two"))).single()

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
            val store = PostgresTestSupport.freshStore()
            val ids = (1..7).map { store.enqueue(listOf(PostgresTestSupport.newJob())).single() }.reversed()

            assertEquals(ids.take(3), store.listJobs(JobQuery(limit = 3)).map { it.id })
            assertEquals(ids.drop(3).take(3), store.listJobs(JobQuery(limit = 3, offset = 3)).map { it.id })
            assertEquals(ids.drop(6), store.listJobs(JobQuery(limit = 3, offset = 6)).map { it.id })
            assertFailsWith<IllegalArgumentException> { JobQuery(limit = 0) }
            assertFailsWith<IllegalArgumentException> { JobQuery(offset = -1) }
        }

    @Test
    public fun summariesCarryStatusSpecificFields() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val id = store.enqueue(listOf(PostgresTestSupport.newJob())).single()
            val claim = store.claim(listOf(QueueName.DEFAULT), DEFAULT_KINDS, 1, 5.minutes, WORKER).single()

            val running = store.listJobs(JobQuery(status = JobStatus.Running)).single()
            assertEquals(1, running.attempt)
            assertNull(running.retryAt)
            assertNull(running.terminalAt)

            // dbNow() already carries the database's microsecond precision, so the round trip
            // through retry_at preserves equality exactly. Past, so the row is due again at once.
            val retryAt = PostgresTestSupport.dbNow() - 2.minutes
            store.transition(id, JobState.Running, JobState.Failed(retryAt), claim.fence)
            val failed = store.listJobs(JobQuery(status = JobStatus.Failed)).single()
            assertEquals(retryAt, failed.retryAt)
            assertNull(failed.terminalAt)

            val second = store.claim(listOf(QueueName.DEFAULT), DEFAULT_KINDS, 1, 5.minutes, WORKER).single()
            val beforeTerminal = PostgresTestSupport.dbNow()
            store.transition(id, JobState.Running, JobState.Succeeded, second.fence)
            val done = store.listJobs(JobQuery(status = JobStatus.Succeeded)).single()
            assertNull(done.retryAt)
            val terminalAt = assertNotNull(done.terminalAt)
            assertTrue(terminalAt >= beforeTerminal, "terminalAt must land at or after the pre-transition clock reading")
            assertEquals(2, done.attempt)
        }

    @Test
    public fun detailExposesPayloadAndLeaseOnlyWhileRunning() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val id = store.enqueue(listOf(PostgresTestSupport.newJob(payload = """{"n":1}"""))).single()

            val idle = assertNotNull(store.getJob(id))
            assertEquals("""{"n":1}""", idle.payload)
            assertNull(idle.leaseUntil)
            assertNull(idle.holder)

            val claim = store.claim(listOf(QueueName.DEFAULT), DEFAULT_KINDS, 1, 5.minutes, WORKER).single()
            val running = assertNotNull(store.getJob(id))
            assertNotNull(running.leaseUntil)
            assertEquals(WORKER, running.holder)
            assertEquals(claim.fence, running.fence)

            assertNull(store.getJob(JobId("999999")), "unknown ids are null, not an error")
            assertNull(store.getJob(JobId("not-even-shaped-right")))
        }

    @Test
    public fun requeueIsAPlainCasAndTheBudgetResumes() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val id = store.enqueue(listOf(PostgresTestSupport.newJob())).single()
            val claim = store.claim(listOf(QueueName.DEFAULT), DEFAULT_KINDS, 1, 5.minutes, WORKER).single()
            store.transition(id, JobState.Running, JobState.DeadLettered, claim.fence)

            // The dashboard's requeue: no new SPI, one CAS, terminal rows carry no fence.
            assertTrue(store.transition(id, JobState.DeadLettered, JobState.Enqueued))
            val summary = store.listJobs(JobQuery(status = JobStatus.Enqueued)).single()
            assertEquals(id, summary.id)
            assertEquals(1, summary.attempt, "attempt history survives a requeue; the budget resumes, not resets")
            assertNull(summary.terminalAt, "a requeued row is live again: terminalAt must clear")

            val revived = store.claim(listOf(QueueName.DEFAULT), DEFAULT_KINDS, 1, 5.minutes, WORKER).single()
            assertEquals(2, revived.attempt)
        }

    @Test
    public fun listJobsOrdersByNumericIdNotStringEvenPastTenRows() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val ids = (1..12).map { store.enqueue(listOf(PostgresTestSupport.newJob())).single() }

            // String order would place id "10" before "9"; numeric identity ids must not.
            val listed = store.listJobs(JobQuery(limit = 12)).map { it.id }
            assertEquals(ids.reversed(), listed, "ids must sort numerically descending, not lexicographically")
        }
}
