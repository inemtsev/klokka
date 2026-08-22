@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.runtime

import com.eventslooped.klokka.JobEvent
import com.eventslooped.klokka.JobRegistry
import com.eventslooped.klokka.JobState
import com.eventslooped.klokka.NonRetryable
import com.eventslooped.klokka.QueueName
import com.eventslooped.klokka.RetryPolicy
import com.eventslooped.klokka.TestClock
import com.eventslooped.klokka.WorkerId
import com.eventslooped.klokka.jobType
import com.eventslooped.klokka.options
import com.eventslooped.klokka.spi.NewJob
import com.eventslooped.klokka.store.InMemoryJobStore
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.Serializable
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
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
private data class Payload(val value: String = "x")

private val TYPE = jobType<Payload>("runtime.test")

private class BoomException(message: String) : RuntimeException(message), NonRetryable

private fun testSettings(
    clock: TestClock,
    queues: List<QueueConfig> = listOf(QueueConfig(QueueName.DEFAULT)),
    role: KlokkaRole = KlokkaRole.Both,
    defaultRetry: RetryPolicy = RetryPolicy.None,
    drainTimeout: Duration = 5.seconds,
): KlokkaSettings =
    KlokkaSettings(
        queues = queues,
        role = role,
        defaultRetry = defaultRetry,
        clock = clock,
        lease = 1.minutes,
        heartbeatInterval = 5.seconds,
        pollInterval = 20.milliseconds,
        claimBatch = 16,
        sweepInterval = 1.hours,
        drainTimeout = drainTimeout,
        lateThreshold = 1.minutes,
        workerId = WorkerId("test-worker"),
    )

/** Subscribes to [runtime]'s events before returning, so nothing emitted afterward is missed. */
private fun TestScope.collectEvents(runtime: KlokkaRuntime): MutableList<JobEvent> {
    val events = mutableListOf<JobEvent>()
    backgroundScope.launch(start = CoroutineStart.UNDISPATCHED) {
        runtime.events.collect { events.add(it) }
    }
    return events
}

/** Flushes whatever is immediately runnable, then lets one poll tick fire and flushes again. */
private suspend fun TestScope.settle(pollInterval: Duration) {
    runCurrent()
    advanceTimeBy(pollInterval)
    runCurrent()
}

public class KlokkaRuntimeTest {
    @Test
    public fun successEndToEndEmitsSucceededAndPersistsState() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val registry = JobRegistry()
            var seenAttempt = -1
            var seenPayload: Payload? = null
            registry.handle(TYPE) { payload ->
                seenAttempt = attempt
                seenPayload = payload
            }
            val cfg = testSettings(clock)
            val runtime = KlokkaRuntime(store, registry, cfg)
            val events = collectEvents(runtime)

            runtime.start(backgroundScope)
            val id = runtime.enqueue(TYPE, Payload("hi"))
            settle(cfg.pollInterval)

            assertEquals(1, seenAttempt)
            assertEquals(Payload("hi"), seenPayload)
            val succeeded = events.filterIsInstance<JobEvent.Succeeded>().singleOrNull()
            assertNotNull(succeeded, "expected a single Succeeded event, got: $events")
            assertEquals(1, succeeded.attempt)
            assertEquals(JobState.Succeeded, store.snapshot(id)?.state)

            runtime.drain()
        }

    @Test
    public fun retryThenSuccessEmitsFailedAttemptThenSucceeded() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val registry = JobRegistry()
            var calls = 0
            registry.handle(TYPE, retry = RetryPolicy.intervals(listOf(1.seconds))) { _ ->
                calls += 1
                if (calls == 1) error("transient failure")
            }
            val cfg = testSettings(clock)
            val runtime = KlokkaRuntime(store, registry, cfg)
            val events = collectEvents(runtime)

            val id = runtime.enqueue(TYPE, Payload())
            runtime.start(backgroundScope)
            settle(cfg.pollInterval)

            assertEquals(1, events.filterIsInstance<JobEvent.FailedAttempt>().size)
            assertEquals(0, events.filterIsInstance<JobEvent.Succeeded>().size)

            clock.advanceBy(1.seconds)
            advanceTimeBy(cfg.pollInterval * 3)
            runCurrent()

            assertEquals(1, events.filterIsInstance<JobEvent.FailedAttempt>().size)
            assertEquals(1, events.filterIsInstance<JobEvent.Succeeded>().size)
            assertEquals(2, calls)
            val snapshot = store.snapshot(id)
            assertEquals(JobState.Succeeded, snapshot?.state)
            assertEquals(2, snapshot?.attempts)

            runtime.drain()
        }

    @Test
    public fun retriesExhaustedDeadLettersAfterFailedAttempt() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val registry = JobRegistry()
            registry.handle(TYPE, retry = RetryPolicy.intervals(listOf(1.seconds))) { _ ->
                error("always fails")
            }
            val cfg = testSettings(clock)
            val runtime = KlokkaRuntime(store, registry, cfg)
            val events = collectEvents(runtime)

            val id = runtime.enqueue(TYPE, Payload())
            runtime.start(backgroundScope)
            settle(cfg.pollInterval)

            assertEquals(1, events.filterIsInstance<JobEvent.FailedAttempt>().size)

            clock.advanceBy(1.seconds)
            advanceTimeBy(cfg.pollInterval * 3)
            runCurrent()

            assertEquals(1, events.filterIsInstance<JobEvent.FailedAttempt>().size)
            assertEquals(1, events.filterIsInstance<JobEvent.DeadLettered>().size)
            assertEquals(0, events.filterIsInstance<JobEvent.Succeeded>().size)
            assertEquals(JobState.DeadLettered, store.snapshot(id)?.state)

            runtime.drain()
        }

    @Test
    public fun nonRetryableFailureDeadLettersImmediately() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val registry = JobRegistry()
            registry.handle(TYPE, retry = RetryPolicy.intervals(listOf(1.seconds))) { _ ->
                throw BoomException("nope")
            }
            val cfg = testSettings(clock)
            val runtime = KlokkaRuntime(store, registry, cfg)
            val events = collectEvents(runtime)

            val id = runtime.enqueue(TYPE, Payload())
            runtime.start(backgroundScope)
            settle(cfg.pollInterval)

            assertEquals(0, events.filterIsInstance<JobEvent.FailedAttempt>().size)
            assertEquals(1, events.filterIsInstance<JobEvent.DeadLettered>().size)
            assertEquals(JobState.DeadLettered, store.snapshot(id)?.state)

            runtime.drain()
        }

    @Test
    public fun decodeFailureDeadLettersWithoutInvokingHandler() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val registry = JobRegistry()
            var invoked = false
            registry.handle(TYPE) { _ -> invoked = true }
            val cfg = testSettings(clock)
            val runtime = KlokkaRuntime(store, registry, cfg)
            val events = collectEvents(runtime)

            val id = store.enqueue(listOf(NewJob(kind = TYPE.kind, payload = "not-json", runAt = clock.now()))).single()
            runtime.start(backgroundScope)
            settle(cfg.pollInterval)

            assertFalse(invoked)
            assertEquals(1, events.filterIsInstance<JobEvent.DeadLettered>().size)
            assertEquals(JobState.DeadLettered, store.snapshot(id)?.state)

            runtime.drain()
        }

    @Test
    public fun workerWithoutTheKindBoundLeavesTheJobEnqueued() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val registry = JobRegistry()
            val boundType = jobType<Payload>("runtime.test.bound")
            val unboundType = jobType<Payload>("runtime.test.unbound")
            registry.handle(boundType) { _ -> }
            val cfg = testSettings(clock)
            val runtime = KlokkaRuntime(store, registry, cfg)
            val events = collectEvents(runtime)

            val id = runtime.enqueue(unboundType, Payload())
            runtime.start(backgroundScope)
            settle(cfg.pollInterval)
            settle(cfg.pollInterval)
            settle(cfg.pollInterval)

            val snapshot = store.snapshot(id)
            assertEquals(JobState.Enqueued, snapshot?.state)
            assertEquals(0, snapshot?.attempts)
            assertTrue(events.none { it is JobEvent.Started && it.kind == unboundType.kind })
            assertTrue(events.none { it is JobEvent.DeadLettered && it.kind == unboundType.kind })

            runtime.drain()
        }

    @Test
    public fun queuePriorityDrainsHigherPriorityQueueFirst() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val registry = JobRegistry()
            val order = mutableListOf<QueueName>()
            registry.handle(TYPE) { _ -> order.add(queue) }
            val criticalQueue = QueueName("critical")
            val cfg = testSettings(clock, queues = listOf(QueueConfig(criticalQueue, concurrency = 1), QueueConfig(QueueName.DEFAULT, concurrency = 1)))
            val runtime = KlokkaRuntime(store, registry, cfg)

            store.enqueue(listOf(NewJob(kind = TYPE.kind, payload = "{}", queue = QueueName.DEFAULT, runAt = clock.now())))
            store.enqueue(listOf(NewJob(kind = TYPE.kind, payload = "{}", queue = criticalQueue, runAt = clock.now())))

            runtime.start(backgroundScope)
            settle(cfg.pollInterval)

            assertEquals(listOf(criticalQueue, QueueName.DEFAULT), order)

            runtime.drain()
        }

    @Test
    public fun enqueueUsesTheJobTypeDefaultQueue() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val registry = JobRegistry()
            val emailQueue = QueueName("email")
            val type = jobType<Payload>("runtime.test.email-default", queue = emailQueue)
            val cfg = testSettings(clock, role = KlokkaRole.Producer)
            val runtime = KlokkaRuntime(store, registry, cfg)
            val events = collectEvents(runtime)

            val id = runtime.enqueue(type, Payload())
            runCurrent()

            assertEquals(emailQueue, store.snapshot(id)?.queue)
            val enqueued = events.filterIsInstance<JobEvent.Enqueued>().singleOrNull()
            assertNotNull(enqueued, "expected a single Enqueued event, got: $events")
            assertEquals(emailQueue, enqueued.queue)

            runtime.drain()
        }

    @Test
    public fun enqueueOptionsQueueOverridesTheJobTypeDefault() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val registry = JobRegistry()
            val emailQueue = QueueName("email")
            val criticalQueue = QueueName("critical")
            val type = jobType<Payload>("runtime.test.email-override", queue = emailQueue)
            val cfg = testSettings(clock, role = KlokkaRole.Producer)
            val runtime = KlokkaRuntime(store, registry, cfg)

            val id = runtime.enqueue(type, Payload(), options { queue = criticalQueue })
            runCurrent()

            assertEquals(criticalQueue, store.snapshot(id)?.queue)

            runtime.drain()
        }

    @Test
    public fun concurrencyBoundLimitsSimultaneousExecutions() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val registry = JobRegistry()
            val gate = CompletableDeferred<Unit>()
            var started = 0
            var succeeded = 0
            registry.handle(TYPE) { _ ->
                started += 1
                gate.await()
                succeeded += 1
            }
            val cfg = testSettings(clock, queues = listOf(QueueConfig(QueueName.DEFAULT, concurrency = 2)))
            val runtime = KlokkaRuntime(store, registry, cfg)
            val events = collectEvents(runtime)

            repeat(4) { store.enqueue(listOf(NewJob(kind = TYPE.kind, payload = "{}", runAt = clock.now()))) }

            runtime.start(backgroundScope)
            settle(cfg.pollInterval)

            assertEquals(2, started)
            assertEquals(2, events.filterIsInstance<JobEvent.Started>().size)

            gate.complete(Unit)
            settle(cfg.pollInterval)
            settle(cfg.pollInterval)

            assertEquals(4, started)
            assertEquals(4, succeeded)
            assertEquals(4, events.filterIsInstance<JobEvent.Succeeded>().size)

            runtime.drain()
        }

    @Test
    public fun drainRequeuesInFlightJobs() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val registry = JobRegistry()
            registry.handle(TYPE) { _ -> awaitCancellation() }
            val cfg = testSettings(clock, drainTimeout = 50.milliseconds)
            val runtime = KlokkaRuntime(store, registry, cfg)
            val events = collectEvents(runtime)

            val id = runtime.enqueue(TYPE, Payload())
            runtime.start(backgroundScope)
            settle(cfg.pollInterval)

            assertTrue(events.any { it is JobEvent.Started })

            runtime.drain()

            val snapshot = store.snapshot(id)
            assertEquals(JobState.Enqueued, snapshot?.state)
            assertEquals(1, snapshot?.attempts)
        }

    @Test
    public fun perKindRetryOverridesDefaultNone() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val registry = JobRegistry()
            var calls = 0
            registry.handle(TYPE, retry = RetryPolicy.intervals(listOf(1.seconds))) { _ ->
                calls += 1
                if (calls == 1) error("transient")
            }
            val cfg = testSettings(clock, defaultRetry = RetryPolicy.None)
            val runtime = KlokkaRuntime(store, registry, cfg)
            val events = collectEvents(runtime)

            val id = runtime.enqueue(TYPE, Payload())
            runtime.start(backgroundScope)
            settle(cfg.pollInterval)

            assertEquals(1, events.filterIsInstance<JobEvent.FailedAttempt>().size)
            assertEquals(0, events.filterIsInstance<JobEvent.DeadLettered>().size)

            clock.advanceBy(1.seconds)
            advanceTimeBy(cfg.pollInterval * 3)
            runCurrent()

            assertEquals(1, events.filterIsInstance<JobEvent.Succeeded>().size)
            assertEquals(0, events.filterIsInstance<JobEvent.DeadLettered>().size)
            assertEquals(JobState.Succeeded, store.snapshot(id)?.state)

            runtime.drain()
        }

    @Test
    public fun producerRoleLaunchesNoWorkers() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val registry = JobRegistry()
            registry.handle(TYPE) { _ -> }
            val cfg = testSettings(clock, role = KlokkaRole.Producer)
            val runtime = KlokkaRuntime(store, registry, cfg)

            val id = runtime.enqueue(TYPE, Payload())
            runtime.start(backgroundScope)

            clock.advanceBy(1.hours)
            advanceTimeBy(1.hours)
            runCurrent()

            val snapshot = store.snapshot(id)
            assertEquals(JobState.Enqueued, snapshot?.state)
            assertEquals(0, snapshot?.attempts)

            runtime.drain()
        }

    @Test
    public fun enqueuedEventCarriesScheduledFor() =
        runTest {
            val clock = TestClock(EPOCH)
            val store = InMemoryJobStore(clock)
            val registry = JobRegistry()
            val cfg = testSettings(clock)
            val runtime = KlokkaRuntime(store, registry, cfg)
            val events = collectEvents(runtime)

            val at = EPOCH + 90.minutes
            runtime.schedule(TYPE, Payload(), at = at)
            runCurrent()

            val enqueued = events.filterIsInstance<JobEvent.Enqueued>().singleOrNull()
            assertNotNull(enqueued, "expected a single Enqueued event, got: $events")
            assertEquals(at, enqueued.scheduledFor)
        }
}
