@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.postgres

import com.eventslooped.klokka.JobId
import com.eventslooped.klokka.JobState
import com.eventslooped.klokka.QueueName
import com.eventslooped.klokka.RetentionPolicy
import com.eventslooped.klokka.WorkerId
import com.eventslooped.klokka.spi.ScheduleSpec
import kotlinx.coroutines.test.runTest
import java.time.OffsetDateTime
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.ExperimentalTime

private val WORKER_A = WorkerId("worker-a")
private val WORKER_B = WorkerId("worker-b")

/** Covers every kind the fixtures in this file enqueue by default. */
private val DEFAULT_KINDS = setOf("test.kind")

/**
 * Conformance tests for [PostgresJobStore], run against a real container via
 * [PostgresTestSupport]. There is no injectable clock here: rows become due through past
 * `runAt`/`fireAt` values and leases expire through zero or negative lease [Duration]s,
 * exactly as [PostgresTestSupport]'s KDoc directs.
 */
public class PostgresJobStoreContractTest {
    @Test
    public fun enqueuePastRunAtIsClaimableFutureRunAtIsNotAndIdsPreserveInsertOrder() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val pastJob = PostgresTestSupport.newJob(runAt = PostgresTestSupport.dbNow() - 1.minutes)
            val futureJob = PostgresTestSupport.newJob(runAt = PostgresTestSupport.dbNow() + 5.minutes)

            val ids = store.enqueue(listOf(pastJob, futureJob))
            assertEquals(2, ids.size)
            assertTrue(ids[0].value.toLong() < ids[1].value.toLong(), "ids must be returned in insert order")

            val claimed = store.claim(listOf(QueueName.DEFAULT), DEFAULT_KINDS, limit = 10, lease = 1.minutes, worker = WORKER_A)
            assertEquals(listOf(ids[0]), claimed.map { it.id })
        }

    @Test
    public fun uniqueKeyDedupesUntilTerminalThenAllowsANewRow() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val key = "receipt-42"

            val firstId = store.enqueue(listOf(PostgresTestSupport.newJob(uniqueKey = key))).single()
            val secondId = store.enqueue(listOf(PostgresTestSupport.newJob(uniqueKey = key))).single()
            assertEquals(firstId, secondId)
            assertEquals(1, countByUniqueKey(key), "a conflicting non-terminal row must insert no second row")

            val claim = store.claim(listOf(QueueName.DEFAULT), DEFAULT_KINDS, limit = 10, lease = 1.minutes, worker = WORKER_A).single()
            assertTrue(store.transition(firstId, JobState.Running, JobState.Succeeded, fence = claim.fence))

            val thirdId = store.enqueue(listOf(PostgresTestSupport.newJob(uniqueKey = key))).single()
            assertTrue(thirdId != firstId, "the key is free again once the owning row is terminal")
            assertEquals(2, countByUniqueKey(key))
        }

    @Test
    public fun claimFiltersByKindLeavingAnUnboundKindUntouchedAndEmptyKindsReturnsNothing() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val boundId = store.enqueue(listOf(PostgresTestSupport.newJob(kind = "bound"))).single()
            val unboundId = store.enqueue(listOf(PostgresTestSupport.newJob(kind = "unbound"))).single()

            val claimed = store.claim(listOf(QueueName.DEFAULT), setOf("bound"), limit = 10, lease = 1.minutes, worker = WORKER_A)
            assertEquals(listOf(boundId), claimed.map { it.id })

            val untouched = assertNotNull(PostgresTestSupport.jobRow(unboundId))
            assertEquals("Enqueued", untouched.state)
            assertEquals(0, untouched.attempt)
            assertEquals(0L, untouched.fence)

            val empty = store.claim(listOf(QueueName.DEFAULT), emptySet(), limit = 10, lease = 1.minutes, worker = WORKER_A)
            assertTrue(empty.isEmpty())
        }

    @Test
    public fun claimDrainsQueuesInDeclaredOrder() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val critical = QueueName("critical")
            store.enqueue(listOf(PostgresTestSupport.newJob(queue = QueueName.DEFAULT)))
            val criticalId = store.enqueue(listOf(PostgresTestSupport.newJob(queue = critical))).single()

            val claimed =
                store.claim(listOf(critical, QueueName.DEFAULT), DEFAULT_KINDS, limit = 1, lease = 1.minutes, worker = WORKER_A)

            assertEquals(listOf(criticalId), claimed.map { it.id })
        }

    @Test
    public fun claimBumpsAttemptAndFenceAndSetsRunningWithHolderAndLease() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val id = store.enqueue(listOf(PostgresTestSupport.newJob())).single()

            val claimed =
                store.claim(listOf(QueueName.DEFAULT), DEFAULT_KINDS, limit = 10, lease = 5.minutes, worker = WORKER_A).single()
            assertEquals(id, claimed.id)
            assertEquals(1, claimed.attempt)
            assertEquals(1L, claimed.fence)

            val row = assertNotNull(PostgresTestSupport.jobRow(id))
            assertEquals("Running", row.state)
            assertEquals(1, row.attempt)
            assertEquals(1L, row.fence)
            assertEquals("worker-a", row.holder)
            assertNotNull(row.leaseUntil)
        }

    @Test
    public fun expiredLeaseIsRevivedByAnotherWorkerWithBumpedAttemptAndFence() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val id = store.enqueue(listOf(PostgresTestSupport.newJob())).single()

            val first =
                store.claim(listOf(QueueName.DEFAULT), DEFAULT_KINDS, limit = 10, lease = Duration.ZERO, worker = WORKER_A).single()
            assertEquals(1, first.attempt)
            assertEquals(1L, first.fence)

            val second =
                store.claim(listOf(QueueName.DEFAULT), DEFAULT_KINDS, limit = 10, lease = 1.minutes, worker = WORKER_B).single()
            assertEquals(id, second.id)
            assertEquals(2, second.attempt)
            assertEquals(2L, second.fence)
        }

    @Test
    public fun heartbeatExtendsALiveLeaseButIgnoresAnExpiredOrWrongHolderLease() =
        runTest {
            val store = PostgresTestSupport.freshStore()

            // A live lease under the claiming worker is extended.
            val liveId = store.enqueue(listOf(PostgresTestSupport.newJob())).single()
            store.claim(listOf(QueueName.DEFAULT), DEFAULT_KINDS, limit = 10, lease = 1.minutes, worker = WORKER_A)
            val before = leaseUntilOf(liveId)
            store.heartbeat(listOf(liveId), WORKER_A, extend = 5.minutes)
            val after = leaseUntilOf(liveId)
            assertTrue(after.isAfter(before), "heartbeat must move lease_until later")

            // An expired lease is not held, not even by its former owner.
            val expiredId = store.enqueue(listOf(PostgresTestSupport.newJob())).single()
            store.claim(listOf(QueueName.DEFAULT), DEFAULT_KINDS, limit = 10, lease = Duration.ZERO, worker = WORKER_A)
            store.heartbeat(listOf(expiredId), WORKER_A, extend = 5.minutes)
            val revived =
                store.claim(listOf(QueueName.DEFAULT), DEFAULT_KINDS, limit = 10, lease = 1.minutes, worker = WORKER_B)
            assertEquals(listOf(expiredId), revived.map { it.id }, "an expired lease must remain claimable despite the heartbeat")

            // A different worker's heartbeat is ignored.
            val heldId = store.enqueue(listOf(PostgresTestSupport.newJob())).single()
            store.claim(listOf(QueueName.DEFAULT), DEFAULT_KINDS, limit = 10, lease = 5.minutes, worker = WORKER_A)
            val heldBefore = leaseUntilOf(heldId)
            store.heartbeat(listOf(heldId), WORKER_B, extend = 5.minutes)
            val heldAfter = leaseUntilOf(heldId)
            assertEquals(heldBefore, heldAfter, "a different worker's heartbeat must not extend the lease")
        }

    @Test
    public fun transitionWithCorrectFenceSucceedsClearsLeaseAndSetsTerminalAt() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val id = store.enqueue(listOf(PostgresTestSupport.newJob())).single()
            val claim =
                store.claim(listOf(QueueName.DEFAULT), DEFAULT_KINDS, limit = 10, lease = 1.minutes, worker = WORKER_A).single()

            assertTrue(store.transition(id, JobState.Running, JobState.Succeeded, fence = claim.fence))

            val row = assertNotNull(PostgresTestSupport.jobRow(id))
            assertEquals("Succeeded", row.state)
            assertNotNull(row.terminalAt)
            assertNull(row.leaseUntil)
            assertNull(row.holder)
        }

    @Test
    public fun transitionWithWrongFenceFailsAndChangesNothing() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val id = store.enqueue(listOf(PostgresTestSupport.newJob())).single()
            val claim =
                store.claim(listOf(QueueName.DEFAULT), DEFAULT_KINDS, limit = 10, lease = 1.minutes, worker = WORKER_A).single()

            assertFalse(store.transition(id, JobState.Running, JobState.Succeeded, fence = claim.fence + 1))

            val row = assertNotNull(PostgresTestSupport.jobRow(id))
            assertEquals("Running", row.state)
            assertNotNull(row.leaseUntil)
            assertEquals("worker-a", row.holder)
        }

    @Test
    public fun transitionWithWrongFromStateFailsAndChangesNothing() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val id = store.enqueue(listOf(PostgresTestSupport.newJob())).single()
            store.claim(listOf(QueueName.DEFAULT), DEFAULT_KINDS, limit = 10, lease = 1.minutes, worker = WORKER_A)

            assertFalse(store.transition(id, JobState.Scheduled, JobState.Succeeded))

            val row = assertNotNull(PostgresTestSupport.jobRow(id))
            assertEquals("Running", row.state)
        }

    @Test
    public fun transitionWithoutFenceWorksForANonRunningRow() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val id = store.enqueue(listOf(PostgresTestSupport.newJob())).single()

            assertTrue(store.transition(id, JobState.Enqueued, JobState.Cancelled))

            val row = assertNotNull(PostgresTestSupport.jobRow(id))
            assertEquals("Cancelled", row.state)
            assertNotNull(row.terminalAt)
        }

    @Test
    public fun transitionToFailedWithPastRetryAtMakesTheJobClaimableAgainOrderedByRetryAt() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val laterId = store.enqueue(listOf(PostgresTestSupport.newJob())).single()
            val soonerId = store.enqueue(listOf(PostgresTestSupport.newJob())).single()
            val claims = store.claim(listOf(QueueName.DEFAULT), DEFAULT_KINDS, limit = 10, lease = 1.minutes, worker = WORKER_A)
            val laterClaim = claims.single { it.id == laterId }
            val soonerClaim = claims.single { it.id == soonerId }

            val now = PostgresTestSupport.dbNow()
            assertTrue(store.transition(laterId, JobState.Running, JobState.Failed(now - 1.minutes), fence = laterClaim.fence))
            assertTrue(store.transition(soonerId, JobState.Running, JobState.Failed(now - 5.minutes), fence = soonerClaim.fence))

            val laterRow = assertNotNull(PostgresTestSupport.jobRow(laterId))
            assertEquals("Failed", laterRow.state)
            assertNull(laterRow.leaseUntil)
            assertNull(laterRow.holder)

            val retried = store.claim(listOf(QueueName.DEFAULT), DEFAULT_KINDS, limit = 1, lease = 1.minutes, worker = WORKER_B)
            assertEquals(listOf(soonerId), retried.map { it.id }, "the retry claim orders by retry_at, earliest first")
        }

    @Test
    public fun transitionFromFailedWithTheWrongRetryAtFails() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val id = store.enqueue(listOf(PostgresTestSupport.newJob())).single()
            val claim =
                store.claim(listOf(QueueName.DEFAULT), DEFAULT_KINDS, limit = 10, lease = 1.minutes, worker = WORKER_A).single()
            val retryAt = PostgresTestSupport.dbNow() - 1.minutes
            assertTrue(store.transition(id, JobState.Running, JobState.Failed(retryAt), fence = claim.fence))

            assertFalse(store.transition(id, JobState.Failed(retryAt + 1.minutes), JobState.Cancelled))

            val row = assertNotNull(PostgresTestSupport.jobRow(id))
            assertEquals("Failed", row.state)
        }

    @Test
    public fun transitionFromFailedWithTheExactRetryAtSucceeds() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val id = store.enqueue(listOf(PostgresTestSupport.newJob())).single()
            val claim =
                store.claim(listOf(QueueName.DEFAULT), DEFAULT_KINDS, limit = 10, lease = 1.minutes, worker = WORKER_A).single()
            // dbNow() already carries the database's microsecond precision.
            val retryAt = PostgresTestSupport.dbNow() - 1.minutes
            assertTrue(store.transition(id, JobState.Running, JobState.Failed(retryAt), fence = claim.fence))

            assertTrue(store.transition(id, JobState.Failed(retryAt), JobState.Cancelled))

            val row = assertNotNull(PostgresTestSupport.jobRow(id))
            assertEquals("Cancelled", row.state)
        }

    @Test
    public fun sweepDeletesTerminalRowsPastRetentionAndNeverTouchesNonTerminalRows() =
        runTest {
            val store = PostgresTestSupport.freshStore()

            val succeededId = store.enqueue(listOf(PostgresTestSupport.newJob())).single()
            val succeededClaim =
                store.claim(listOf(QueueName.DEFAULT), DEFAULT_KINDS, limit = 10, lease = 1.minutes, worker = WORKER_A).single()
            store.transition(succeededId, JobState.Running, JobState.Succeeded, fence = succeededClaim.fence)

            val deadLetteredId = store.enqueue(listOf(PostgresTestSupport.newJob())).single()
            val deadLetteredClaim =
                store.claim(listOf(QueueName.DEFAULT), DEFAULT_KINDS, limit = 10, lease = 1.minutes, worker = WORKER_A).single()
            store.transition(deadLetteredId, JobState.Running, JobState.DeadLettered, fence = deadLetteredClaim.fence)

            val runningId = store.enqueue(listOf(PostgresTestSupport.newJob())).single()
            store.claim(listOf(QueueName.DEFAULT), DEFAULT_KINDS, limit = 10, lease = 1.minutes, worker = WORKER_A)

            store.sweep(RetentionPolicy(succeededFor = Duration.ZERO, cancelledFor = Duration.ZERO, deadLetteredFor = null))
            assertNull(PostgresTestSupport.jobRow(succeededId), "a succeeded row past retention must be swept")
            assertNotNull(PostgresTestSupport.jobRow(deadLetteredId), "dead-lettered rows are kept forever by default")
            assertNotNull(PostgresTestSupport.jobRow(runningId), "non-terminal rows are never swept")

            store.sweep(RetentionPolicy(deadLetteredFor = Duration.ZERO))
            assertNull(PostgresTestSupport.jobRow(deadLetteredId), "an explicit zero retention sweeps dead-lettered rows too")
            assertNotNull(PostgresTestSupport.jobRow(runningId), "non-terminal rows are still never swept")
        }

    @Test
    public fun upsertSchedulesInsertsANewScheduleWithTheGivenFireTime() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val fireAt = PostgresTestSupport.dbNow() + 5.minutes
            store.upsertSchedules(
                listOf(ScheduleSpec(id = "digest", kind = "reports.digest", fingerprint = "fp-1", description = "every 5m", fireAt = fireAt)),
            )

            val row = assertNotNull(PostgresTestSupport.scheduleRow("digest"))
            assertEquals("reports.digest", row.kind)
            assertEquals("fp-1", row.fingerprint)
            assertEquals("every 5m", row.description)
            assertEquals(fireAt, row.nextFireAt)
            assertNull(row.lastFiredAt)
        }

    @Test
    public fun upsertWithAnEqualFingerprintPreservesTimingStateAfterAFire() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val fireAt = PostgresTestSupport.dbNow() - 1.minutes
            store.upsertSchedules(
                listOf(ScheduleSpec(id = "digest", kind = "reports.digest", fingerprint = "fp-1", description = "every 5m", fireAt = fireAt)),
            )

            val fire = store.dueSchedules(setOf("digest"), 10, 1.minutes, WORKER_A).single()
            val nextFireAt = PostgresTestSupport.dbNow() + 10.minutes
            assertNotNull(store.completeFire("digest", fire.fence, emptyList(), nextFireAt))

            // A restart re-upserts the same definition with a freshly computed fireAt; the
            // unchanged fingerprint must keep the timing state the fire just wrote.
            store.upsertSchedules(
                listOf(
                    ScheduleSpec(
                        id = "digest",
                        kind = "reports.digest",
                        fingerprint = "fp-1",
                        description = "every 5m",
                        fireAt = PostgresTestSupport.dbNow() + 99.minutes,
                    ),
                ),
            )

            val row = assertNotNull(PostgresTestSupport.scheduleRow("digest"))
            assertEquals(nextFireAt, row.nextFireAt, "an unchanged fingerprint must keep timing state")
            assertNotNull(row.lastFiredAt)
        }

    @Test
    public fun upsertWithAChangedFingerprintResetsTheFireTimeAndClearsTheLease() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val fireAt = PostgresTestSupport.dbNow() - 1.minutes
            store.upsertSchedules(
                listOf(ScheduleSpec(id = "digest", kind = "reports.digest", fingerprint = "fp-1", description = "every 5m", fireAt = fireAt)),
            )
            val stale = store.dueSchedules(setOf("digest"), 10, 10.minutes, WORKER_A).single()

            val newFireAt = PostgresTestSupport.dbNow() + 6.minutes
            store.upsertSchedules(
                listOf(ScheduleSpec(id = "digest", kind = "reports.other", fingerprint = "fp-2", description = "changed", fireAt = newFireAt)),
            )

            val row = assertNotNull(PostgresTestSupport.scheduleRow("digest"))
            assertEquals("fp-2", row.fingerprint)
            assertEquals("reports.other", row.kind)
            assertEquals("changed", row.description)
            assertEquals(newFireAt, row.nextFireAt)
            assertNull(row.holder)
            assertNull(row.leaseUntil)

            // The pre-change claim's lease was cleared, so its completeFire is now stale.
            assertNull(store.completeFire("digest", stale.fence, emptyList(), PostgresTestSupport.dbNow() + 20.minutes))
        }

    @Test
    public fun dueSchedulesFiltersByIdsSkipsDormantAndNotYetDueOrdersSoonestFirst() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val now = PostgresTestSupport.dbNow()
            store.upsertSchedules(
                listOf(
                    ScheduleSpec(id = "third", kind = "k", fingerprint = "fp", description = "d", fireAt = now - 1.minutes),
                    ScheduleSpec(id = "first", kind = "k", fingerprint = "fp", description = "d", fireAt = now - 3.minutes),
                    ScheduleSpec(id = "second", kind = "k", fingerprint = "fp", description = "d", fireAt = now - 2.minutes),
                    ScheduleSpec(id = "unknown-here", kind = "k", fingerprint = "fp", description = "d", fireAt = now - 1.minutes),
                    ScheduleSpec(id = "dormant", kind = "k", fingerprint = "fp", description = "d", fireAt = null),
                    ScheduleSpec(id = "future", kind = "k", fingerprint = "fp", description = "d", fireAt = now + 30.minutes),
                ),
            )

            val ids = setOf("first", "second", "third", "dormant", "future")
            val fires = store.dueSchedules(ids, 10, 1.minutes, WORKER_A)
            assertEquals(listOf("first", "second", "third"), fires.map { it.scheduleId }, "only due ids from the given set, soonest first")

            val nowDiff = (fires.first().now - PostgresTestSupport.dbNow()).absoluteValue
            assertTrue(nowDiff < 1.minutes, "now must be close to the store's clock at claim")

            assertEquals(emptyList(), store.dueSchedules(emptySet(), 10, 1.minutes, WORKER_A))
        }

    @Test
    public fun dueSchedulesHonorsTheLimit() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val now = PostgresTestSupport.dbNow()
            store.upsertSchedules(
                listOf(
                    ScheduleSpec(id = "third", kind = "k", fingerprint = "fp", description = "d", fireAt = now - 1.minutes),
                    ScheduleSpec(id = "first", kind = "k", fingerprint = "fp", description = "d", fireAt = now - 3.minutes),
                    ScheduleSpec(id = "second", kind = "k", fingerprint = "fp", description = "d", fireAt = now - 2.minutes),
                ),
            )

            val fires = store.dueSchedules(setOf("first", "second", "third"), 2, 1.minutes, WORKER_A)
            assertEquals(listOf("first", "second"), fires.map { it.scheduleId })
        }

    @Test
    public fun dueSchedulesHidesALeasedRowFromOtherCallers() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            store.upsertSchedules(
                listOf(
                    ScheduleSpec(
                        id = "digest",
                        kind = "reports.digest",
                        fingerprint = "fp-1",
                        description = "every 5m",
                        fireAt = PostgresTestSupport.dbNow() - 1.minutes,
                    ),
                ),
            )

            store.dueSchedules(setOf("digest"), 10, 5.minutes, WORKER_A)
            assertEquals(emptyList(), store.dueSchedules(setOf("digest"), 10, 1.minutes, WORKER_B), "a live lease hides the row")
        }

    @Test
    public fun dueSchedulesReclaimsAnExpiredLeaseWithABumpedFence() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            store.upsertSchedules(
                listOf(
                    ScheduleSpec(
                        id = "digest",
                        kind = "reports.digest",
                        fingerprint = "fp-1",
                        description = "every 5m",
                        fireAt = PostgresTestSupport.dbNow() - 1.minutes,
                    ),
                ),
            )

            val first = store.dueSchedules(setOf("digest"), 10, Duration.ZERO, WORKER_A).single()
            val second = store.dueSchedules(setOf("digest"), 10, 1.minutes, WORKER_B).single()
            assertTrue(second.fence > first.fence, "an expired-lease reclaim must bump the fence")
        }

    @Test
    public fun completeFireAppliesRunsNextFireAtAndLastFiredAtAtomicallyAndReleasesTheLease() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val fireAt = PostgresTestSupport.dbNow() - 5.minutes
            store.upsertSchedules(
                listOf(ScheduleSpec(id = "digest", kind = "reports.digest", fingerprint = "fp-1", description = "every 5m", fireAt = fireAt)),
            )
            val fire = store.dueSchedules(setOf("digest"), 10, 1.minutes, WORKER_A).single()

            // Already due again, so a successful reclaim below proves the lease was released.
            val nextFireAt = PostgresTestSupport.dbNow() - 1.minutes
            val run = PostgresTestSupport.newJob(kind = "reports.digest", runAt = fire.scheduledFor, scheduleId = "digest")
            val ids = assertNotNull(store.completeFire("digest", fire.fence, listOf(run), nextFireAt))
            assertEquals(1, ids.size)

            val scheduleRow = assertNotNull(PostgresTestSupport.scheduleRow("digest"))
            assertEquals(nextFireAt, scheduleRow.nextFireAt)
            assertNotNull(scheduleRow.lastFiredAt)
            assertNull(scheduleRow.holder)
            assertNull(scheduleRow.leaseUntil)

            val jobRow = assertNotNull(PostgresTestSupport.jobRow(ids.single()))
            assertEquals("digest", jobRow.scheduleId)

            val reclaimed = store.dueSchedules(setOf("digest"), 10, 1.minutes, WORKER_B)
            assertEquals(1, reclaimed.size, "the lease must be released so the schedule is claimable once due again")
        }

    @Test
    public fun completeFireWithAStaleFenceReturnsNullAndInsertsNoRuns() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            store.upsertSchedules(
                listOf(
                    ScheduleSpec(
                        id = "digest",
                        kind = "reports.digest",
                        fingerprint = "fp-1",
                        description = "every 5m",
                        fireAt = PostgresTestSupport.dbNow() - 1.minutes,
                    ),
                ),
            )
            val fire = store.dueSchedules(setOf("digest"), 10, Duration.ZERO, WORKER_A).single()
            // The zero-lease claim above already expired; reclaiming bumps the fence and makes `fire` stale.
            store.dueSchedules(setOf("digest"), 10, 1.minutes, WORKER_B)

            val run = PostgresTestSupport.newJob(kind = "reports.digest", scheduleId = "digest")
            val result = store.completeFire("digest", fire.fence, listOf(run), PostgresTestSupport.dbNow() + 10.minutes)
            assertNull(result)
            assertEquals(0, countJobsOfKind("reports.digest"))
        }

    @Test
    public fun completeFireWithEmptyRunsIsLegal() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            store.upsertSchedules(
                listOf(
                    ScheduleSpec(
                        id = "digest",
                        kind = "reports.digest",
                        fingerprint = "fp-1",
                        description = "every 5m",
                        fireAt = PostgresTestSupport.dbNow() - 1.minutes,
                    ),
                ),
            )
            val fire = store.dueSchedules(setOf("digest"), 10, 1.minutes, WORKER_A).single()

            val result = store.completeFire("digest", fire.fence, emptyList(), null)
            assertEquals(emptyList(), result)

            val row = assertNotNull(PostgresTestSupport.scheduleRow("digest"))
            assertNull(row.nextFireAt, "a null nextFireAt makes the schedule dormant")
        }

    @Test
    public fun completeFireRunsRespectTheUniqueKeyRule() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            store.upsertSchedules(
                listOf(
                    ScheduleSpec(
                        id = "digest",
                        kind = "reports.digest",
                        fingerprint = "fp-1",
                        description = "every 5m",
                        fireAt = PostgresTestSupport.dbNow() - 1.minutes,
                    ),
                ),
            )
            val key = "klokka:schedule:digest"
            val existing = store.enqueue(listOf(PostgresTestSupport.newJob(kind = "reports.digest", uniqueKey = key))).single()

            val fire = store.dueSchedules(setOf("digest"), 10, 1.minutes, WORKER_A).single()
            val run = PostgresTestSupport.newJob(kind = "reports.digest", runAt = fire.scheduledFor, uniqueKey = key, scheduleId = "digest")
            val ids = assertNotNull(store.completeFire("digest", fire.fence, listOf(run), null))
            assertEquals(listOf(existing), ids, "a non-terminal run under the same key dedups to the existing id")
        }

    @Test
    public fun completeFireRunWithAPastRunAtIsClaimableImmediately() =
        runTest {
            val store = PostgresTestSupport.freshStore()
            val fireAt = PostgresTestSupport.dbNow() - 1.minutes
            store.upsertSchedules(
                listOf(ScheduleSpec(id = "digest", kind = "reports.digest", fingerprint = "fp-1", description = "every 5m", fireAt = fireAt)),
            )
            val fire = store.dueSchedules(setOf("digest"), 10, 1.minutes, WORKER_A).single()
            val run = PostgresTestSupport.newJob(kind = "reports.digest", runAt = fire.scheduledFor, scheduleId = "digest")
            val ids = assertNotNull(store.completeFire("digest", fire.fence, listOf(run), null))

            val claimed = store.claim(listOf(QueueName.DEFAULT), setOf("reports.digest"), limit = 10, lease = 1.minutes, worker = WORKER_A)
            assertEquals(ids, claimed.map { it.id })
        }
}

private fun leaseUntilOf(id: JobId): OffsetDateTime {
    val row = PostgresTestSupport.jobRow(id)
    val leaseUntil = row?.leaseUntil
    checkNotNull(leaseUntil) { "expected a lease_until for job $id" }
    return OffsetDateTime.parse(leaseUntil)
}

private fun countByUniqueKey(key: String): Int =
    PostgresTestSupport.dataSource.connection.use { connection ->
        connection.prepareStatement("SELECT count(*) FROM klokka_jobs WHERE unique_key = ?").use { ps ->
            ps.setString(1, key)
            ps.executeQuery().use { rs ->
                rs.next()
                rs.getInt(1)
            }
        }
    }

private fun countJobsOfKind(kind: String): Int =
    PostgresTestSupport.dataSource.connection.use { connection ->
        connection.prepareStatement("SELECT count(*) FROM klokka_jobs WHERE kind = ?").use { ps ->
            ps.setString(1, kind)
            ps.executeQuery().use { rs ->
                rs.next()
                rs.getInt(1)
            }
        }
    }
