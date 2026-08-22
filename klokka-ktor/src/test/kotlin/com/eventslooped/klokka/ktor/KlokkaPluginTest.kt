@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.ktor

import com.eventslooped.klokka.JobEvent
import com.eventslooped.klokka.QueueName
import com.eventslooped.klokka.WorkerId
import com.eventslooped.klokka.jobType
import com.eventslooped.klokka.options
import com.eventslooped.klokka.spi.ClaimedJob
import com.eventslooped.klokka.spi.JobStore
import com.eventslooped.klokka.store.InMemoryJobStore
import io.ktor.client.request.get
import io.ktor.server.application.Application
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import kotlinx.serialization.Serializable
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime

@Serializable
private data class Greeting(val message: String)

private val GREETING_JOB = jobType<Greeting>("ktor-test.greeting")
private val ORDER_JOB = jobType<String>("ktor-test.order")

/**
 * Wraps the in-memory store and records the worker id the runtime hands to [claim], so a
 * test can prove the configured `workerId` reaches the runtime rather than being replaced
 * by the settings default. Interface delegation drops `PushCapableStore`, so the claim
 * loop runs on the poll interval; the engine kicks one claim round immediately on start,
 * which is enough to observe the id without enqueueing anything.
 */
private class ClaimRecordingStore(
    private val delegate: JobStore = InMemoryJobStore(),
) : JobStore by delegate {
    val firstClaimWorker = CompletableDeferred<WorkerId>()

    override suspend fun claim(
        queues: List<QueueName>,
        kinds: Set<String>,
        limit: Int,
        lease: Duration,
        worker: WorkerId,
    ): List<ClaimedJob> {
        firstClaimWorker.complete(worker)
        return delegate.claim(queues, kinds, limit, lease, worker)
    }
}

public class KlokkaPluginTest {
    @Test
    public fun configuredWorkerIdReachesTheRuntime() =
        testApplication {
            val recordingStore = ClaimRecordingStore()

            install(Klokka) {
                pollInterval = 20.milliseconds
                lease = 5.seconds
                heartbeatInterval = 5.seconds
                drainTimeout = 2.seconds
                store = recordingStore
                workerId = WorkerId("web-1")
                handle(GREETING_JOB) { }
            }
            application {
                routing {
                    get("/") { call.respondText("ok") }
                }
            }

            // The first request starts the application, which starts the runtime's claim loop.
            client.get("/")

            val worker = withTimeout(5.seconds) { recordingStore.firstClaimWorker.await() }
            assertEquals(WorkerId("web-1"), worker)
        }

    @Test
    public fun endToEndEnqueueThroughRouteCompletesAndEmitsSucceeded() =
        testApplication {
            val received = CompletableDeferred<Greeting>()
            val events = mutableListOf<JobEvent>()

            // Installed at the testApplication top level, not inside application { }: from
            // inside application { }, both the Application receiver and the outer
            // TestApplicationBuilder receiver offer an "install" that Kotlin cannot
            // disambiguate implicitly, so TestApplicationBuilder's own install() convenience
            // is used directly here instead.
            install(Klokka) {
                // Shrink intervals so the test does not depend on production-sane defaults.
                pollInterval = 20.milliseconds
                lease = 5.seconds
                heartbeatInterval = 5.seconds
                drainTimeout = 2.seconds
                handle(GREETING_JOB) { payload -> received.complete(payload) }
            }
            application {
                // Subscribe before any request is made, so nothing emitted afterward is missed.
                launch(start = CoroutineStart.UNDISPATCHED) {
                    klokka.events.collect { events.add(it) }
                }
                routing {
                    get("/enqueue") {
                        call.application.klokka.enqueue(GREETING_JOB, Greeting("hello"))
                        call.respondText("ok")
                    }
                }
            }

            client.get("/enqueue")

            val payload = withTimeout(5.seconds) { received.await() }
            assertEquals(Greeting("hello"), payload)

            withTimeout(5.seconds) {
                while (events.none { it is JobEvent.Succeeded }) yield()
            }
            assertTrue(events.any { it is JobEvent.Succeeded })
        }

    @Test
    public fun applicationKlokkaThrowsWhenPluginNotInstalled() =
        testApplication {
            lateinit var app: Application

            application {
                routing {
                    get("/") {
                        app = call.application
                        call.respondText("ok")
                    }
                }
            }

            client.get("/")

            assertFailsWith<IllegalStateException> { app.klokka }
        }

    /**
     * `KlokkaConfig` does not expose its built queue list, so declared order cannot be
     * asserted by introspection. Cross-queue execution order also cannot be asserted
     * reliably here: unlike `KlokkaRuntimeTest.queuePriorityDrainsHigherPriorityQueueFirst`,
     * which runs under kotlinx-coroutines-test's single-threaded virtual time, testApplication
     * runs on real dispatchers where two independently-concurrent queues can interleave.
     * The pragmatic check: declare "critical" before "default" and confirm a job enqueued
     * onto the first-declared, non-default queue is actually claimed and completed end to
     * end through the plugin, proving `queue()` entries reach the runtime rather than being
     * silently dropped in favor of the default queue.
     */
    @Test
    public fun queueDeclaredFirstIsWiredIntoTheRuntime() =
        testApplication {
            val succeeded = CompletableDeferred<Unit>()

            install(Klokka) {
                pollInterval = 20.milliseconds
                lease = 5.seconds
                heartbeatInterval = 5.seconds
                drainTimeout = 2.seconds
                queue("critical", concurrency = 1)
                queue("default", concurrency = 1)
                handle(ORDER_JOB) { succeeded.complete(Unit) }
            }
            application {
                routing {
                    get("/enqueue") {
                        call.application.klokka.enqueue(
                            ORDER_JOB,
                            "critical-job",
                            options { queue = QueueName("critical") },
                        )
                        call.respondText("ok")
                    }
                }
            }

            client.get("/enqueue")

            withTimeout(5.seconds) { succeeded.await() }
        }

    @Test
    public fun boundKindsOnUndrainedQueuesReportsMismatches() {
        val config =
            KlokkaConfig().apply {
                queue("critical")
                handle(jobType<String>("ktor-test.emailish", queue = QueueName("email"))) { }
                handle(jobType<String>("ktor-test.crit", queue = QueueName("critical"))) { }
            }

        assertEquals(mapOf(QueueName("email") to listOf("ktor-test.emailish")), config.boundKindsOnUndrainedQueues())

        val allDrained =
            KlokkaConfig().apply {
                handle(jobType<String>("ktor-test.defaulted")) { }
            }

        assertTrue(allDrained.boundKindsOnUndrainedQueues().isEmpty())
    }

    @Test
    public fun jobTypeDefaultQueueRoutesThroughThePlugin() =
        testApplication {
            val succeeded = CompletableDeferred<Unit>()
            val emailJob = jobType<String>("ktor-test.email-default", queue = QueueName("email"))

            install(Klokka) {
                pollInterval = 20.milliseconds
                lease = 5.seconds
                heartbeatInterval = 5.seconds
                drainTimeout = 2.seconds
                queue("email", concurrency = 1)
                handle(emailJob) { succeeded.complete(Unit) }
            }
            application {
                routing {
                    get("/enqueue") {
                        call.application.klokka.enqueue(emailJob, "no-queue-override")
                        call.respondText("ok")
                    }
                }
            }

            client.get("/enqueue")

            withTimeout(5.seconds) { succeeded.await() }
        }
}
