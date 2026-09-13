@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.postgres

import com.eventslooped.klokka.JobId
import com.eventslooped.klokka.JobState
import com.eventslooped.klokka.QueueName
import com.eventslooped.klokka.WorkerId
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime

/**
 * The reason SKIP LOCKED exists: claims must be atomic across concurrent callers. These
 * tests hammer one store from many coroutines on real connections and assert the
 * exactly-one-claimer-per-lease-window guarantee that the multi-node suite (#16) will
 * later re-verify across processes.
 */
internal class PostgresClaimConcurrencyTest {
    @Test
    fun concurrentClaimersNeverDoubleClaimAJob() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val jobs = 200
            val claimers = 8
            store.enqueue((1..jobs).map { PostgresTestSupport.newJob() })

            val claimed: List<List<JobId>> =
                coroutineScope {
                    (1..claimers).map { n ->
                        async(Dispatchers.IO) {
                            val mine = mutableListOf<JobId>()
                            while (true) {
                                val batch =
                                    store.claim(
                                        queues = listOf(QueueName.DEFAULT),
                                        kinds = setOf("test.kind"),
                                        limit = 7,
                                        lease = 5.minutes,
                                        worker = WorkerId("claimer-$n"),
                                    )
                                if (batch.isEmpty()) break
                                mine.addAll(batch.map { it.id })
                            }
                            mine
                        }
                    }.awaitAll()
                }

            val all = claimed.flatten()
            assertEquals(jobs, all.size, "every job claimed exactly once across $claimers concurrent claimers")
            assertEquals(jobs, all.toSet().size, "no job claimed twice")
        }

    @Test
    fun concurrentDueSchedulesClaimEachScheduleOnce() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val now = PostgresTestSupport.dbNow()
            val specs =
                (1..40).map { n ->
                    com.eventslooped.klokka.spi.ScheduleSpec(
                        id = "sched-$n",
                        kind = "test.kind",
                        fingerprint = "fp",
                        description = "test",
                        fireAt = now - 1.minutes,
                    )
                }
            store.upsertSchedules(specs)
            val ids = specs.map { it.id }.toSet()

            val fires =
                coroutineScope {
                    (1..8).map { n ->
                        async(Dispatchers.IO) {
                            store.dueSchedules(ids, limit = 40, lease = 5.minutes, worker = WorkerId("node-$n"))
                        }
                    }.awaitAll()
                }.flatten()

            assertEquals(40, fires.size, "each due schedule claimed exactly once across concurrent callers")
            assertEquals(40, fires.map { it.scheduleId }.toSet().size)
        }

    @Test
    fun concurrentUniqueKeyEnqueuesConvergeOnOneRow() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val ids =
                coroutineScope {
                    (1..12).map {
                        async(Dispatchers.IO) {
                            store.enqueue(listOf(PostgresTestSupport.newJob(uniqueKey = "the-one"))).single()
                        }
                    }.awaitAll()
                }
            assertEquals(1, ids.toSet().size, "all concurrent enqueues under one uniqueKey must return the same id")
        }

    @Test
    fun staleFenceLosesTheCompleteFireRace() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val now = PostgresTestSupport.dbNow()
            store.upsertSchedules(
                listOf(
                    com.eventslooped.klokka.spi.ScheduleSpec("s", "test.kind", "fp", "test", now - 1.minutes),
                ),
            )
            // First claim expires instantly (zero lease); second claim revives with a higher fence.
            val stale = store.dueSchedules(setOf("s"), 1, 0.seconds, WorkerId("a")).single()
            val fresh = store.dueSchedules(setOf("s"), 1, 5.minutes, WorkerId("b")).single()
            assertTrue(fresh.fence > stale.fence)

            val freshResult = store.completeFire("s", fresh.fence, listOf(PostgresTestSupport.newJob(scheduleId = "s")), now + 5.minutes)
            assertEquals(1, freshResult?.size)
            val staleResult = store.completeFire("s", stale.fence, listOf(PostgresTestSupport.newJob(scheduleId = "s")), now + 9.minutes)
            assertEquals(null, staleResult, "the zombie's completion must be rejected")

            val row = PostgresTestSupport.scheduleRow("s")
            assertEquals(now.epochSeconds + 300, row?.nextFireAt?.epochSeconds, "the winner's nextFireAt stands")
        }

    @Test
    fun zombieWorkerCannotOverwriteARevivedJob() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val id = store.enqueue(listOf(PostgresTestSupport.newJob())).single()
            val zombie = store.claim(listOf(QueueName.DEFAULT), setOf("test.kind"), 1, 0.seconds, WorkerId("zombie")).single()
            val revived = store.claim(listOf(QueueName.DEFAULT), setOf("test.kind"), 1, 5.minutes, WorkerId("alive")).single()
            assertEquals(id, revived.id)
            assertTrue(revived.fence > zombie.fence)

            assertEquals(false, store.transition(id, JobState.Running, JobState.Succeeded, zombie.fence))
            assertEquals("Running", PostgresTestSupport.jobRow(id)?.state, "the zombie's write must change nothing")
            assertEquals(true, store.transition(id, JobState.Running, JobState.Succeeded, revived.fence))
        }
}
