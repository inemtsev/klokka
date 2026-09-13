@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.dashboard

import com.eventslooped.klokka.JobId
import com.eventslooped.klokka.JobState
import com.eventslooped.klokka.QueueName
import com.eventslooped.klokka.WorkerId
import com.eventslooped.klokka.spi.JobStore
import com.eventslooped.klokka.spi.NewJob
import com.eventslooped.klokka.store.InMemoryJobStore
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration.Companion.minutes
import kotlin.time.ExperimentalTime

private val WORKER = WorkerId("dashboard-test")

private fun newJob(kind: String = "orders.confirm", payload: String = """{"orderId":42}""") =
    NewJob(kind = kind, payload = payload, runAt = Clock.System.now() - 1.minutes)

private suspend fun InMemoryJobStore.deadLettered(kind: String = "orders.confirm"): JobId {
    val id = enqueue(listOf(newJob(kind))).single()
    val claim = claim(listOf(QueueName.DEFAULT), setOf(kind), 1, 5.minutes, WORKER).single()
    transition(id, JobState.Running, JobState.DeadLettered, claim.fence)
    return id
}

internal class KlokkaDashboardTest {
    @Test
    fun mountingOutsideAuthenticateFailsAtStartup() =
        testApplication {
            application {
                routing { klokkaDashboard(InMemoryJobStore()) }
            }
            val failure = assertFailsWith<IllegalStateException> { startApplication() }
            assertTrue("authenticate" in failure.message.orEmpty(), failure.message ?: "")
        }

    @Test
    fun aStoreWithoutTheQueryCapabilityIsRejectedAtMount() =
        testApplication {
            // Interface delegation keeps JobStore and drops QueryableStore: a valid store
            // that simply cannot serve a dashboard.
            val blindStore = object : JobStore by InMemoryJobStore() {}
            application {
                routing { klokkaDashboard(blindStore) { allowAnonymous = true } }
            }
            val failure = assertFailsWith<IllegalStateException> { startApplication() }
            assertTrue("QueryableStore" in failure.message.orEmpty(), failure.message ?: "")
        }

    @Test
    fun overviewListsCountsJobsAndHonorsStatusFilter() =
        testApplication {
            val store = InMemoryJobStore()
            val liveId = store.enqueue(listOf(newJob(kind = "orders.confirm"))).single()
            val deadId = store.deadLettered(kind = "billing.invoice")
            application {
                routing { klokkaDashboard(store) { allowAnonymous = true } }
            }

            val overview = client.get("/klokka")
            assertEquals(HttpStatusCode.OK, overview.status)
            val body = overview.bodyAsText()
            assertTrue("orders.confirm" in body)
            assertTrue("billing.invoice" in body)
            assertTrue(liveId.value in body && deadId.value in body)

            val filtered = client.get("/klokka?status=DeadLettered").bodyAsText()
            assertTrue(deadId.value in filtered)
            assertFalse(liveId.value in filtered)
        }

    @Test
    fun detailShowsPayloadAndUnknownIdsAre404() =
        testApplication {
            val store = InMemoryJobStore()
            val id = store.enqueue(listOf(newJob(payload = """{"secret":"visible-to-operators"}"""))).single()
            application {
                routing { klokkaDashboard(store) { allowAnonymous = true } }
            }

            val detail = client.get("/klokka/jobs/${id.value}")
            assertEquals(HttpStatusCode.OK, detail.status)
            assertTrue("visible-to-operators" in detail.bodyAsText())

            assertEquals(HttpStatusCode.NotFound, client.get("/klokka/jobs/job-999").status)
        }

    @Test
    fun requeueTransitionsTheJobAndRedirectsToItsDetail() =
        testApplication {
            val store = InMemoryJobStore()
            val id = store.deadLettered()
            application {
                routing { klokkaDashboard(store) { allowAnonymous = true } }
            }

            assertTrue("Requeue" in client.get("/klokka/jobs/${id.value}").bodyAsText())
            val response = client.post("/klokka/jobs/${id.value}/requeue")
            assertEquals(HttpStatusCode.Found, response.status)
            val location = response.headers["Location"]
            assertEquals("/klokka/jobs/${id.value}", location, "redirect back to the detail page")
            val body = client.get(location!!).bodyAsText()
            assertTrue("Enqueued" in body)
            assertFalse("Requeue<" in body, "the button must disappear once the job is live again")
        }

    @Test
    fun readOnlyModeHidesTheButtonAndRejectsThePost() =
        testApplication {
            val store = InMemoryJobStore()
            val id = store.deadLettered()
            application {
                routing {
                    klokkaDashboard(store) {
                        allowAnonymous = true
                        readOnly = true
                    }
                }
            }

            assertFalse("Requeue" in client.get("/klokka/jobs/${id.value}").bodyAsText())
            assertEquals(HttpStatusCode.Forbidden, client.post("/klokka/jobs/${id.value}/requeue").status)
            assertEquals(JobState.DeadLettered, store.snapshot(id)?.state, "read-only must leave the row untouched")
        }
}
