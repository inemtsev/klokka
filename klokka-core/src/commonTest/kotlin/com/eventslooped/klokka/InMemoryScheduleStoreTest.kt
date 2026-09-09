@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka

import com.eventslooped.klokka.spi.NewJob
import com.eventslooped.klokka.spi.ScheduleSpec
import com.eventslooped.klokka.store.InMemoryJobStore
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

private val EPOCH: Instant = Instant.fromEpochMilliseconds(0)
private val WORKER = WorkerId("test-worker")
private val OTHER_WORKER = WorkerId("other-worker")

private fun spec(
    id: String = "digest",
    fingerprint: String = "fp-1",
    fireAt: Instant? = EPOCH + 5.minutes,
): ScheduleSpec =
    ScheduleSpec(id = id, kind = "reports.digest", fingerprint = fingerprint, description = "every 5m", fireAt = fireAt)

private fun run(runAt: Instant, uniqueKey: String? = null): NewJob =
    NewJob(kind = "reports.digest", payload = "{}", runAt = runAt, uniqueKey = uniqueKey, scheduleId = "digest")

public class InMemoryScheduleStoreTest {
    @Test
    public fun upsertInsertsNewSchedulesWithTheGivenFireTime() =
        runTest {
            val store = InMemoryJobStore(TestClock(EPOCH))
            store.upsertSchedules(listOf(spec()))
            val snapshot = assertNotNull(store.scheduleSnapshot("digest"))
            assertEquals(EPOCH + 5.minutes, snapshot.nextFireAt)
            assertEquals("fp-1", snapshot.fingerprint)
            assertNull(snapshot.lastFiredAt)
        }

    @Test
    public fun upsertWithAnEqualFingerprintPreservesTimingState() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            store.upsertSchedules(listOf(spec()))
            clock.advanceBy(5.minutes)
            val fire = store.dueSchedules(setOf("digest"), 10, 1.minutes, WORKER).single()
            assertNotNull(store.completeFire("digest", fire.fence, emptyList(), EPOCH + 10.minutes))

            // A restart re-upserts the same definition with a freshly computed fireAt.
            store.upsertSchedules(listOf(spec(fireAt = EPOCH + 99.minutes)))
            val snapshot = assertNotNull(store.scheduleSnapshot("digest"))
            assertEquals(EPOCH + 10.minutes, snapshot.nextFireAt, "unchanged fingerprint must keep timing state")
            assertEquals(EPOCH + 5.minutes, snapshot.lastFiredAt)
        }

    @Test
    public fun upsertWithAChangedFingerprintResetsTheFireTimeAndClearsTheLease() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            store.upsertSchedules(listOf(spec()))
            clock.advanceBy(5.minutes)
            val stale = store.dueSchedules(setOf("digest"), 10, 10.minutes, WORKER).single()

            store.upsertSchedules(listOf(spec(fingerprint = "fp-2", fireAt = EPOCH + 6.minutes)))
            val snapshot = assertNotNull(store.scheduleSnapshot("digest"))
            assertEquals("fp-2", snapshot.fingerprint)
            assertEquals(EPOCH + 6.minutes, snapshot.nextFireAt)

            // The pre-change claim's lease was cleared; its completeFire must not apply either,
            clock.advanceBy(1.minutes)
            val fresh = store.dueSchedules(setOf("digest"), 10, 1.minutes, WORKER).single()
            assertTrue(fresh.fence > stale.fence)
            assertNull(store.completeFire("digest", stale.fence, emptyList(), EPOCH + 20.minutes))
        }

    @Test
    public fun dueSchedulesReturnsOnlyDueRowsAmongTheGivenIds() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            store.upsertSchedules(
                listOf(
                    spec(id = "due", fireAt = EPOCH + 1.minutes),
                    spec(id = "later", fireAt = EPOCH + 30.minutes),
                    spec(id = "unknown-here", fireAt = EPOCH + 1.minutes),
                    spec(id = "dormant", fireAt = null),
                ),
            )
            clock.advanceBy(2.minutes)
            val fires = store.dueSchedules(setOf("due", "later", "dormant"), 10, 1.minutes, WORKER)
            assertEquals(listOf("due"), fires.map { it.scheduleId })
            val fire = fires.single()
            assertEquals(EPOCH + 1.minutes, fire.scheduledFor)
            assertEquals(EPOCH + 2.minutes, fire.now, "now must be the store's clock at claim")
            assertEquals(emptyList(), store.dueSchedules(emptySet(), 10, 1.minutes, WORKER))
        }

    @Test
    public fun dueSchedulesOrdersSoonestFirstAndHonorsTheLimit() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            store.upsertSchedules(
                listOf(
                    spec(id = "third", fireAt = EPOCH + 3.minutes),
                    spec(id = "first", fireAt = EPOCH + 1.minutes),
                    spec(id = "second", fireAt = EPOCH + 2.minutes),
                ),
            )
            clock.advanceBy(10.minutes)
            val ids = setOf("first", "second", "third")
            val fires = store.dueSchedules(ids, 2, 1.minutes, WORKER)
            assertEquals(listOf("first", "second"), fires.map { it.scheduleId })
        }

    @Test
    public fun aClaimedScheduleIsInvisibleUntilItsLeaseExpires() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            store.upsertSchedules(listOf(spec()))
            clock.advanceBy(5.minutes)
            val first = store.dueSchedules(setOf("digest"), 10, 1.minutes, WORKER).single()
            assertEquals(emptyList(), store.dueSchedules(setOf("digest"), 10, 1.minutes, OTHER_WORKER))

            clock.advanceBy(1.minutes)
            val second = store.dueSchedules(setOf("digest"), 10, 1.minutes, OTHER_WORKER).single()
            assertTrue(second.fence > first.fence, "an expired-lease reclaim must bump the fence")

            // The zombie's completion is stale and must change nothing.
            assertNull(store.completeFire("digest", first.fence, listOf(run(EPOCH)), EPOCH + 10.minutes))
            assertEquals(emptyList(), store.claim(listOf(QueueName.DEFAULT), setOf("reports.digest"), 10, 1.minutes, WORKER))
        }

    @Test
    public fun completeFireAppliesRunsAndTimingAtomically() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            store.upsertSchedules(listOf(spec()))
            clock.advanceBy(5.minutes)
            val fire = store.dueSchedules(setOf("digest"), 10, 1.minutes, WORKER).single()

            val ids = assertNotNull(store.completeFire("digest", fire.fence, listOf(run(fire.scheduledFor)), EPOCH + 10.minutes))
            assertEquals(1, ids.size)
            val snapshot = assertNotNull(store.scheduleSnapshot("digest"))
            assertEquals(EPOCH + 10.minutes, snapshot.nextFireAt)
            assertEquals(EPOCH + 5.minutes, snapshot.lastFiredAt)

            val claimed = store.claim(listOf(QueueName.DEFAULT), setOf("reports.digest"), 10, 1.minutes, WORKER).single()
            assertEquals(ids.single(), claimed.id)
            assertEquals("digest", claimed.scheduleId)
            assertEquals(fire.scheduledFor, claimed.scheduledFor)

            // The lease is released: after the next fire time arrives the schedule is claimable again.
            clock.advanceBy(5.minutes)
            assertEquals(1, store.dueSchedules(setOf("digest"), 10, 1.minutes, WORKER).size)
        }

    @Test
    public fun completeFireRunsRespectTheUniqueKeyRule() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            store.upsertSchedules(listOf(spec()))
            val existing = store.enqueue(listOf(run(EPOCH, uniqueKey = "klokka:schedule:digest"))).single()

            clock.advanceBy(5.minutes)
            val fire = store.dueSchedules(setOf("digest"), 10, 1.minutes, WORKER).single()
            val ids =
                assertNotNull(
                    store.completeFire(
                        "digest",
                        fire.fence,
                        listOf(run(fire.scheduledFor, uniqueKey = "klokka:schedule:digest")),
                        EPOCH + 10.minutes,
                    ),
                )
            assertEquals(listOf(existing), ids, "a non-terminal run under the same key dedups to the existing id")
        }

    @Test
    public fun secondsPrecisionLeaseBoundary() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            store.upsertSchedules(listOf(spec(fireAt = EPOCH)))
            store.dueSchedules(setOf("digest"), 10, 30.seconds, WORKER)
            clock.advanceBy(30.seconds)
            // A lease lasting exactly `lease` is expired AT the boundary, matching job leases.
            assertEquals(1, store.dueSchedules(setOf("digest"), 10, 30.seconds, WORKER).size)
        }
}
