@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.dashboard

import com.eventslooped.klokka.jobType
import com.eventslooped.klokka.ktor.Klokka
import com.eventslooped.klokka.ktor.klokka
import com.eventslooped.klokka.runtime.KlokkaRuntime
import com.eventslooped.klokka.store.InMemoryJobStore
import io.ktor.client.request.basicAuth
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.auth.Authentication
import io.ktor.server.auth.UserIdPrincipal
import io.ktor.server.auth.authenticate
import io.ktor.server.auth.basic
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import kotlinx.serialization.Serializable
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.ExperimentalTime

// =========================================================================================
// Sample: "mount the Klokka dashboard behind the auth my app already has".
//
// The module below is what an application developer writes: one Authentication provider,
// Klokka installed as usual, and the dashboard mounted INSIDE the authenticate block, so
// it inherits the app's own guard. Klokka ships no login system on purpose.
// =========================================================================================

@Serializable
data class Ping(val n: Int)

val PingJob = jobType<Ping>("sample.ping")

fun Application.opsModule(store: InMemoryJobStore) {
    install(Authentication) {
        basic("ops") {
            validate { credentials ->
                if (credentials.name == "ops" && credentials.password == "s3cret") {
                    UserIdPrincipal(credentials.name)
                } else {
                    null
                }
            }
        }
    }
    install(Klokka) {
        this.store = store
        handle(PingJob) { _ -> }
    }
    routing {
        authenticate("ops") {
            // The dashboard's pages hang under this guard node: no credentials, no pages.
            klokkaDashboard(store)
        }
    }
}

class DashboardSampleTest {
    @Test
    fun theDashboardInheritsTheAppsOwnAuthentication() =
        testApplication {
            val store = InMemoryJobStore()
            application { opsModule(store) }

            // No credentials: the guard answers before any dashboard code runs.
            assertEquals(HttpStatusCode.Unauthorized, client.get("/klokka").status)

            // The app's own credentials open it; Klokka added no auth concepts of its own.
            val page = client.get("/klokka") { basicAuth("ops", "s3cret") }
            assertEquals(HttpStatusCode.OK, page.status)
            assertTrue("Klokka" in page.bodyAsText())
        }

    @Test
    fun enqueuedWorkShowsUpForAuthenticatedOperators() =
        testApplication {
            val store = InMemoryJobStore()
            var runtime: KlokkaRuntime? = null
            application {
                opsModule(store)
                runtime = klokka
            }
            startApplication()

            val id = runtime!!.enqueue(PingJob, Ping(n = 7))

            val page = client.get("/klokka") { basicAuth("ops", "s3cret") }
            val body = page.bodyAsText()
            assertTrue("sample.ping" in body, "the enqueued kind appears in the listing")
            assertTrue(id.value in body, "the job's id appears in the listing")

            val detail = client.get("/klokka/jobs/${id.value}") { basicAuth("ops", "s3cret") }
            assertEquals(HttpStatusCode.OK, detail.status)
            assertTrue("sample.ping" in detail.bodyAsText())
        }
}
