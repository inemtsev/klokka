@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.runtime

import com.eventslooped.klokka.JobRegistry
import com.eventslooped.klokka.QueueName
import com.eventslooped.klokka.RetryPolicy
import com.eventslooped.klokka.TestClock
import com.eventslooped.klokka.WorkerId
import com.eventslooped.klokka.jobType
import com.eventslooped.klokka.store.InMemoryJobStore
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.Serializable
import org.slf4j.MDC
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

@Serializable
private data class MdcPayload(val value: String = "x")

private val MDC_TYPE = jobType<MdcPayload>("mdc.test")

private fun mdcSettings(clock: TestClock): KlokkaSettings =
    KlokkaSettings(
        queues = listOf(QueueConfig(QueueName.DEFAULT)),
        role = KlokkaRole.Both,
        defaultRetry = RetryPolicy.None,
        defaultTimeout = null,
        clock = clock,
        lease = 1.minutes,
        heartbeatInterval = 5.seconds,
        pollInterval = 20.milliseconds,
        claimBatch = 16,
        sweepInterval = 1.hours,
        drainTimeout = 5.seconds,
        lateThreshold = 1.minutes,
        workerId = WorkerId("test-worker"),
    )

public class WorkerMdcTest {
    @Test
    public fun handlerSeesJobIdentityInMdc() =
        runTest {
            val clock = TestClock(Instant.fromEpochMilliseconds(0))
            val store = InMemoryJobStore(clock)
            val registry = JobRegistry()
            var jobId: String? = null
            var kind: String? = null
            var attempt: String? = null
            var scheduleId: String? = "unset"
            registry.handle(MDC_TYPE) { _ ->
                jobId = MDC.get("klokka.jobId")
                kind = MDC.get("klokka.kind")
                attempt = MDC.get("klokka.attempt")
                scheduleId = MDC.get("klokka.scheduleId")
            }
            val cfg = mdcSettings(clock)
            val runtime = KlokkaRuntime(store, registry, cfg)

            runtime.start(backgroundScope)
            val id = runtime.enqueue(MDC_TYPE, MdcPayload())
            runCurrent()
            advanceTimeBy(cfg.pollInterval)
            runCurrent()

            assertEquals(id.value, jobId)
            assertEquals("mdc.test", kind)
            assertEquals("1", attempt)
            assertNull(scheduleId)
            assertNull(MDC.get("klokka.jobId"))

            runtime.drain()
        }
}
