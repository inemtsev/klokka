@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.runtime

import com.eventslooped.klokka.JobEvent
import com.eventslooped.klokka.JobRegistry
import com.eventslooped.klokka.MisfirePolicy
import com.eventslooped.klokka.OverlapPolicy
import com.eventslooped.klokka.QueueName
import com.eventslooped.klokka.RetryPolicy
import com.eventslooped.klokka.TestClock
import com.eventslooped.klokka.WorkerId
import com.eventslooped.klokka.dailyAt
import com.eventslooped.klokka.every
import com.eventslooped.klokka.jobType
import com.eventslooped.klokka.store.InMemoryJobStore
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.datetime.TimeZone
import kotlinx.serialization.Serializable
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

private val EPOCH: Instant = Instant.fromEpochMilliseconds(0)

@Serializable
private data class Digest(val name: String = "daily")

private val DIGEST = jobType<Digest>("reports.digest")

private fun settings(clock: TestClock, lease: Duration = 1.minutes): KlokkaSettings =
    KlokkaSettings(
        queues = listOf(QueueConfig(QueueName.DEFAULT)),
        defaultRetry = RetryPolicy.None,
        clock = clock,
        lease = lease,
        heartbeatInterval = 5.seconds,
        pollInterval = 20.milliseconds,
        sweepInterval = 1.hours,
        lateThreshold = 1.minutes,
        workerId = WorkerId("test-worker"),
    )

private fun TestScope.collectEvents(runtime: KlokkaRuntime): MutableList<JobEvent> {
    val events = mutableListOf<JobEvent>()
    backgroundScope.launch(start = CoroutineStart.UNDISPATCHED) {
        runtime.events.collect { events.add(it) }
    }
    return events
}

private suspend fun TestScope.settle(pollInterval: Duration) {
    runCurrent()
    advanceTimeBy(pollInterval)
    runCurrent()
}

public class RecurringRuntimeTest {
    @Test
    public fun firesOnScheduleAndRunsTheHandlerWithScheduleContext() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val registry = JobRegistry()
            val seen = mutableListOf<Pair<Instant, String?>>()
            registry.handle(DIGEST) { _ -> seen.add(scheduledFor to scheduleId) }
            val cfg = settings(clock)
            val runtime = KlokkaRuntime(store, registry, cfg)
            runtime.recurring("digest", DIGEST, Digest(), schedule = every(5.minutes))
            val events = collectEvents(runtime)

            runtime.start(backgroundScope)
            settle(cfg.pollInterval)
            assertEquals(emptyList(), seen, "nothing is due yet")
            assertEquals(EPOCH + 5.minutes, store.scheduleSnapshot("digest")?.nextFireAt)

            clock.advanceBy(5.minutes)
            settle(cfg.pollInterval)
            settle(cfg.pollInterval)
            assertEquals(listOf<Pair<Instant, String?>>(EPOCH + 5.minutes to "digest"), seen)
            assertEquals(EPOCH + 10.minutes, store.scheduleSnapshot("digest")?.nextFireAt)

            clock.advanceBy(5.minutes)
            settle(cfg.pollInterval)
            settle(cfg.pollInterval)
            assertEquals(2, seen.size)
            assertEquals(EPOCH + 10.minutes to "digest", seen[1])

            val enqueued = events.filterIsInstance<JobEvent.Enqueued>()
            assertEquals(listOf("digest", "digest"), enqueued.map { it.scheduleId })
            val succeeded = events.filterIsInstance<JobEvent.Succeeded>()
            assertEquals(listOf("digest", "digest"), succeeded.map { it.scheduleId })
        }

    @Test
    public fun fireOnceMisfireRunsTheLatestMissedFireAndEmitsTheMisfireEvent() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val registry = JobRegistry()
            val seen = mutableListOf<Instant>()
            registry.handle(DIGEST) { _ -> seen.add(scheduledFor) }
            val cfg = settings(clock)
            val runtime = KlokkaRuntime(store, registry, cfg)
            runtime.recurring("digest", DIGEST, Digest(), schedule = every(5.minutes), misfire = MisfirePolicy.FireOnce)
            val events = collectEvents(runtime)

            runtime.start(backgroundScope)
            settle(cfg.pollInterval)

            // Down for 22 minutes: fires at 5, 10, 15, 20 are all missed.
            clock.advanceBy(22.minutes)
            settle(cfg.pollInterval)
            settle(cfg.pollInterval)

            assertEquals(listOf(EPOCH + 20.minutes), seen, "FireOnce runs only the latest missed fire")
            val misfire = events.filterIsInstance<JobEvent.ScheduleMisfired>().single()
            assertEquals("digest", misfire.scheduleId)
            assertEquals(4, misfire.missedFires)
            assertEquals(1, misfire.emitted)
            assertEquals(EPOCH + 27.minutes, store.scheduleSnapshot("digest")?.nextFireAt, "cadence resumes from now")
        }

    @Test
    public fun skipMisfireEmitsNothingButStillReportsIt() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val registry = JobRegistry()
            var ran = 0
            registry.handle(DIGEST) { _ -> ran += 1 }
            val cfg = settings(clock)
            val runtime = KlokkaRuntime(store, registry, cfg)
            runtime.recurring("digest", DIGEST, Digest(), schedule = every(5.minutes), misfire = MisfirePolicy.Skip)
            val events = collectEvents(runtime)

            runtime.start(backgroundScope)
            settle(cfg.pollInterval)
            clock.advanceBy(22.minutes)
            settle(cfg.pollInterval)
            settle(cfg.pollInterval)

            assertEquals(0, ran)
            val misfire = events.filterIsInstance<JobEvent.ScheduleMisfired>().single()
            assertEquals(0, misfire.emitted)
            assertEquals(4, misfire.missedFires)
            assertTrue(events.filterIsInstance<JobEvent.Enqueued>().isEmpty())
        }

    @Test
    public fun skipIfRunningNeverStacksASecondRunWhileOneIsInFlight() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val registry = JobRegistry()
            val release = CompletableDeferred<Unit>()
            var started = 0
            registry.handle(DIGEST) { _ ->
                started += 1
                release.await()
            }
            // A long lease: the TestClock jumps minutes at a time and the in-flight run must
            // not be revived as an expired lease mid-test (that is claim's job, tested elsewhere).
            val cfg = settings(clock, lease = 30.minutes)
            val runtime = KlokkaRuntime(store, registry, cfg)
            runtime.recurring(
                "digest",
                DIGEST,
                Digest(),
                schedule = every(5.minutes),
                misfire = MisfirePolicy.Skip,
                overlap = OverlapPolicy.SkipIfRunning,
            )
            collectEvents(runtime)

            runtime.start(backgroundScope)
            settle(cfg.pollInterval)
            clock.advanceBy(5.minutes)
            settle(cfg.pollInterval)
            settle(cfg.pollInterval)
            assertEquals(1, started)

            // The next fire comes due while the first run is still executing.
            clock.advanceBy(5.minutes)
            settle(cfg.pollInterval)
            settle(cfg.pollInterval)
            assertEquals(1, started, "SkipIfRunning must not start a second run")
            assertEquals(EPOCH + 15.minutes, store.scheduleSnapshot("digest")?.nextFireAt, "the schedule itself advances")

            release.complete(Unit)
            settle(cfg.pollInterval)
            clock.advanceBy(5.minutes)
            settle(cfg.pollInterval)
            settle(cfg.pollInterval)
            assertEquals(2, started, "after completion the following fire runs again")
        }

    @Test
    public fun triggerNowRunsImmediatelyWithoutShiftingTheSchedule() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val registry = JobRegistry()
            val seen = mutableListOf<Pair<Instant, String?>>()
            registry.handle(DIGEST) { _ -> seen.add(scheduledFor to scheduleId) }
            val cfg = settings(clock)
            val runtime = KlokkaRuntime(store, registry, cfg)
            runtime.recurring("digest", DIGEST, Digest(), schedule = dailyAt(9, 0, TimeZone.UTC))
            collectEvents(runtime)

            runtime.start(backgroundScope)
            settle(cfg.pollInterval)
            val before = assertNotNull(store.scheduleSnapshot("digest")).nextFireAt

            clock.advanceBy(1.minutes)
            runtime.triggerNow("digest")
            settle(cfg.pollInterval)

            assertEquals(listOf<Pair<Instant, String?>>(EPOCH + 1.minutes to "digest"), seen)
            assertEquals(before, store.scheduleSnapshot("digest")?.nextFireAt, "triggerNow must not shift the cadence")

            assertFailsWith<IllegalArgumentException> { runtime.triggerNow("unknown") }
        }

    @Test
    public fun recurringMustBeDeclaredBeforeStart() =
        runTest {
            val clock = TestClock(EPOCH)
            val runtime = KlokkaRuntime(InMemoryJobStore(clock), JobRegistry(), settings(clock))
            runtime.start(backgroundScope)
            assertFailsWith<IllegalStateException> {
                runtime.recurring("late", DIGEST, Digest(), schedule = every(5.minutes))
            }
        }
}
