@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka

import com.eventslooped.klokka.spi.NewJob
import com.eventslooped.klokka.store.InMemoryJobStore
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

private val EPOCH: Instant = Instant.fromEpochMilliseconds(0)
private val WORKER_A = WorkerId("worker-a")
private val WORKER_B = WorkerId("worker-b")

/** Covers every kind the fixtures in this file enqueue by default; pass explicitly to [claim] so existing assertions keep their meaning. */
private val ALL_KINDS = setOf("test.kind")

private fun newJob(
    runAt: Instant,
    queue: QueueName = QueueName.DEFAULT,
    uniqueKey: String? = null,
    kind: String = "test.kind",
) = NewJob(kind = kind, payload = "{}", runAt = runAt, queue = queue, uniqueKey = uniqueKey)

public class InMemoryJobStoreTest {
    @Test
    public fun enqueueThenClaimSetsAttemptOneRunningFenceOne() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val id = store.enqueue(listOf(newJob(runAt = EPOCH))).single()

            val claimed = store.claim(listOf(QueueName.DEFAULT), ALL_KINDS, limit = 10, lease = 1.minutes, worker = WORKER_A)
            val job = claimed.single()

            assertEquals(id, job.id)
            assertEquals(1, job.attempt)
            assertEquals(1L, job.fence)

            val snapshot = store.snapshot(id)
            assertNotNull(snapshot)
            assertEquals(JobState.Running, snapshot.state)
            assertEquals(1, snapshot.attempts)
            assertEquals(1L, snapshot.fence)
        }

    @Test
    public fun claimDrainsQueuesInDeclaredOrder() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val criticalQueue = QueueName("critical")

            store.enqueue(listOf(newJob(runAt = EPOCH, queue = QueueName.DEFAULT)))
            val criticalId = store.enqueue(listOf(newJob(runAt = EPOCH, queue = criticalQueue))).single()

            val claimed =
                store.claim(
                    queues = listOf(criticalQueue, QueueName.DEFAULT),
                    kinds = ALL_KINDS,
                    limit = 10,
                    lease = 1.minutes,
                    worker = WORKER_A,
                )

            assertEquals(2, claimed.size)
            assertEquals(criticalId, claimed.first().id)
        }

    @Test
    public fun enqueueDedupesByUniqueKeyUntilTerminalThenAllowsNewId() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val key = "receipt-42"

            val firstId = store.enqueue(listOf(newJob(runAt = EPOCH, uniqueKey = key))).single()
            val secondId = store.enqueue(listOf(newJob(runAt = EPOCH, uniqueKey = key))).single()
            assertEquals(firstId, secondId)

            val claim =
                store.claim(listOf(QueueName.DEFAULT), ALL_KINDS, limit = 10, lease = 1.minutes, worker = WORKER_A).single()
            assertTrue(store.transition(firstId, JobState.Running, JobState.Succeeded, fence = claim.fence))

            val thirdId = store.enqueue(listOf(newJob(runAt = EPOCH, uniqueKey = key))).single()
            assertTrue(thirdId != firstId)
        }

    @Test
    public fun jobNotClaimableBeforeRunAtThenClaimableAfterAdvancing() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val runAt = EPOCH + 5.minutes
            val id = store.enqueue(listOf(newJob(runAt = runAt))).single()

            val tooEarly = store.claim(listOf(QueueName.DEFAULT), ALL_KINDS, limit = 10, lease = 1.minutes, worker = WORKER_A)
            assertTrue(tooEarly.isEmpty())

            clock.advanceBy(5.minutes)

            val onTime = store.claim(listOf(QueueName.DEFAULT), ALL_KINDS, limit = 10, lease = 1.minutes, worker = WORKER_A)
            assertEquals(listOf(id), onTime.map { it.id })
        }

    @Test
    public fun expiredLeaseIsRevivedWithBumpedAttemptAndFence() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val id = store.enqueue(listOf(newJob(runAt = EPOCH))).single()

            val firstClaim =
                store.claim(listOf(QueueName.DEFAULT), ALL_KINDS, limit = 10, lease = 30.seconds, worker = WORKER_A).single()
            assertEquals(1, firstClaim.attempt)
            assertEquals(1L, firstClaim.fence)

            clock.advanceBy(31.seconds)

            val secondClaim =
                store.claim(listOf(QueueName.DEFAULT), ALL_KINDS, limit = 10, lease = 30.seconds, worker = WORKER_B).single()
            assertEquals(id, secondClaim.id)
            assertEquals(2, secondClaim.attempt)
            assertEquals(2L, secondClaim.fence)
        }

    @Test
    public fun transitionRejectsStaleFenceAfterRevival() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val id = store.enqueue(listOf(newJob(runAt = EPOCH))).single()

            store.claim(listOf(QueueName.DEFAULT), ALL_KINDS, limit = 10, lease = 30.seconds, worker = WORKER_A)
            clock.advanceBy(31.seconds)
            store.claim(listOf(QueueName.DEFAULT), ALL_KINDS, limit = 10, lease = 30.seconds, worker = WORKER_B)

            assertFalse(store.transition(id, JobState.Running, JobState.Succeeded, fence = 1L))
            assertTrue(store.transition(id, JobState.Running, JobState.Succeeded, fence = 2L))
        }

    @Test
    public fun transitionRejectsWrongFromState() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val id = store.enqueue(listOf(newJob(runAt = EPOCH))).single()
            store.claim(listOf(QueueName.DEFAULT), ALL_KINDS, limit = 10, lease = 1.minutes, worker = WORKER_A)

            assertFalse(store.transition(id, JobState.Scheduled, JobState.Succeeded))

            val snapshot = store.snapshot(id)
            assertEquals(JobState.Running, snapshot?.state)
        }

    @Test
    public fun failedJobBecomesClaimableAfterRetryAt() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val id = store.enqueue(listOf(newJob(runAt = EPOCH))).single()
            val firstClaim =
                store.claim(listOf(QueueName.DEFAULT), ALL_KINDS, limit = 10, lease = 1.minutes, worker = WORKER_A).single()

            val retryAt = clock.now() + 5.minutes
            assertTrue(store.transition(id, JobState.Running, JobState.Failed(retryAt), fence = firstClaim.fence))

            val tooEarly = store.claim(listOf(QueueName.DEFAULT), ALL_KINDS, limit = 10, lease = 1.minutes, worker = WORKER_A)
            assertTrue(tooEarly.isEmpty())

            clock.advanceBy(5.minutes)

            val secondClaim =
                store.claim(listOf(QueueName.DEFAULT), ALL_KINDS, limit = 10, lease = 1.minutes, worker = WORKER_A).single()
            assertEquals(id, secondClaim.id)
            assertEquals(2, secondClaim.attempt)
        }

    @Test
    public fun heartbeatExtendsLeaseOnlyForTheHoldingWorker() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val id = store.enqueue(listOf(newJob(runAt = EPOCH))).single()
            store.claim(listOf(QueueName.DEFAULT), ALL_KINDS, limit = 10, lease = 30.seconds, worker = WORKER_A)

            // A different worker's heartbeat must be silently ignored.
            store.heartbeat(listOf(id), WORKER_B, extend = 5.minutes)
            clock.advanceBy(31.seconds)
            val afterWrongHeartbeat =
                store.claim(listOf(QueueName.DEFAULT), ALL_KINDS, limit = 10, lease = 30.seconds, worker = WORKER_B)
            assertEquals(1, afterWrongHeartbeat.size, "worker B's heartbeat must not have extended A's lease")

            // The claim above revived the job for worker B; B's own heartbeat should now extend its lease.
            store.heartbeat(listOf(id), WORKER_B, extend = 5.minutes)
            clock.advanceBy(31.seconds)
            val afterOwnHeartbeat =
                store.claim(listOf(QueueName.DEFAULT), ALL_KINDS, limit = 10, lease = 30.seconds, worker = WORKER_A)
            assertTrue(afterOwnHeartbeat.isEmpty(), "worker B's own heartbeat should have kept the lease valid")
        }

    @Test
    public fun sweepRemovesExpiredTerminalJobsPerRetentionPolicy() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)

            val succeededId = store.enqueue(listOf(newJob(runAt = EPOCH))).single()
            val succeededClaim =
                store.claim(listOf(QueueName.DEFAULT), ALL_KINDS, limit = 10, lease = 1.minutes, worker = WORKER_A).single()
            store.transition(succeededId, JobState.Running, JobState.Succeeded, fence = succeededClaim.fence)

            val deadLetteredId = store.enqueue(listOf(newJob(runAt = EPOCH))).single()
            val deadLetteredClaim =
                store.claim(listOf(QueueName.DEFAULT), ALL_KINDS, limit = 10, lease = 1.minutes, worker = WORKER_A).single()
            store.transition(deadLetteredId, JobState.Running, JobState.DeadLettered, fence = deadLetteredClaim.fence)

            clock.advanceBy(25.hours)

            // Default policy: succeededFor = 24h (exceeded, swept); deadLetteredFor = null (kept forever).
            store.sweep(RetentionPolicy())
            assertNull(store.snapshot(succeededId))
            assertNotNull(store.snapshot(deadLetteredId))

            // An explicit, exceeded deadLetteredFor sweeps it too.
            store.sweep(RetentionPolicy(deadLetteredFor = 1.hours))
            assertNull(store.snapshot(deadLetteredId))
        }

    @Test
    public fun wakeupsEmitsWhenADueJobIsEnqueued() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val received = CompletableDeferred<Unit>()

            val collector =
                launch(start = CoroutineStart.UNDISPATCHED) {
                    store.wakeups().first()
                    received.complete(Unit)
                }

            store.enqueue(listOf(newJob(runAt = clock.now())))

            withTimeout(5.seconds) { received.await() }
            collector.cancel()
        }

    @Test
    public fun enqueueStoresDueJobsAsEnqueuedAndFutureJobsAsScheduled() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val dueId = store.enqueue(listOf(newJob(runAt = EPOCH))).single()
            val laterId = store.enqueue(listOf(newJob(runAt = EPOCH + 1.minutes))).single()

            assertEquals(JobState.Enqueued, store.snapshot(dueId)?.state)
            assertEquals(JobState.Scheduled, store.snapshot(laterId)?.state)
        }

    @Test
    public fun claimReturnsOnlyRequestedKinds() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val aId = store.enqueue(listOf(newJob(runAt = EPOCH, kind = "a"))).single()
            val bId = store.enqueue(listOf(newJob(runAt = EPOCH, kind = "b"))).single()

            val claimed =
                store.claim(listOf(QueueName.DEFAULT), setOf("a"), limit = 10, lease = 1.minutes, worker = WORKER_A)

            assertEquals(listOf(aId), claimed.map { it.id })

            val bSnapshot = store.snapshot(bId)
            assertNotNull(bSnapshot)
            assertEquals(JobState.Enqueued, bSnapshot.state)
            assertEquals(0, bSnapshot.attempts)
            assertEquals(0L, bSnapshot.fence)
        }

    @Test
    public fun claimWithEmptyKindsReturnsNothingAndTouchesNothing() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val aId = store.enqueue(listOf(newJob(runAt = EPOCH, kind = "a"))).single()
            val bId = store.enqueue(listOf(newJob(runAt = EPOCH, kind = "b"))).single()

            val claimed =
                store.claim(listOf(QueueName.DEFAULT), kinds = emptySet(), limit = 10, lease = 1.minutes, worker = WORKER_A)

            assertTrue(claimed.isEmpty())

            val aSnapshot = store.snapshot(aId)
            val bSnapshot = store.snapshot(bId)
            assertNotNull(aSnapshot)
            assertNotNull(bSnapshot)
            assertEquals(JobState.Enqueued, aSnapshot.state)
            assertEquals(JobState.Enqueued, bSnapshot.state)
            assertEquals(0, aSnapshot.attempts)
            assertEquals(0, bSnapshot.attempts)
            assertEquals(0L, aSnapshot.fence)
            assertEquals(0L, bSnapshot.fence)
        }

    @Test
    public fun twoWorkersWithDisjointKindsOnOneQueueEachGetOnlyTheirOwn() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val aId = store.enqueue(listOf(newJob(runAt = EPOCH, kind = "a"))).single()
            val bId = store.enqueue(listOf(newJob(runAt = EPOCH, kind = "b"))).single()

            val claimedByA =
                store.claim(listOf(QueueName.DEFAULT), setOf("a"), limit = 10, lease = 1.minutes, worker = WORKER_A)
            val claimedByB =
                store.claim(listOf(QueueName.DEFAULT), setOf("b"), limit = 10, lease = 1.minutes, worker = WORKER_B)

            assertEquals(listOf(aId), claimedByA.map { it.id })
            assertEquals(listOf(bId), claimedByB.map { it.id })

            val thirdClaim =
                store.claim(
                    listOf(QueueName.DEFAULT),
                    setOf("a", "b"),
                    limit = 10,
                    lease = 1.minutes,
                    worker = WORKER_A,
                )
            assertTrue(thirdClaim.isEmpty())
        }
}
