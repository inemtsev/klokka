@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.dashboard

import com.eventslooped.klokka.JobId
import com.eventslooped.klokka.JobState
import com.eventslooped.klokka.JobStatus
import com.eventslooped.klokka.QueueName
import com.eventslooped.klokka.spi.JobQuery
import com.eventslooped.klokka.spi.JobStore
import com.eventslooped.klokka.spi.QueryableStore
import io.ktor.http.HttpStatusCode
import io.ktor.server.auth.AuthenticationRouteSelector
import io.ktor.server.html.respondHtml
import io.ktor.server.response.respond
import io.ktor.server.response.respondRedirect
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.route
import kotlin.time.ExperimentalTime

/** Configuration for [klokkaDashboard]. */
public class KlokkaDashboardConfig {
    /**
     * Serve without any authentication in effect. False by default and deliberately
     * explicit: an unwrapped dashboard shows job payloads to anyone who finds the URL.
     * Development-mode convenience only.
     */
    public var allowAnonymous: Boolean = false

    /** Hide and reject the mutating actions (requeue). The pages become pure observation. */
    public var readOnly: Boolean = false

    /** Jobs per listing page. */
    public var pageSize: Int = 50
}

/**
 * Mounts the Klokka dashboard under this route at [path]: a states overview with
 * filterable job listing, a per-job detail page, and a requeue action for dead-lettered
 * jobs. Server-rendered HTML, no JavaScript.
 *
 * Authentication is inherited, never implemented here: mount inside your existing
 * `authenticate { }` block and the dashboard is guarded by whatever provider that block
 * names. FAIL-CLOSED: mounting outside any authenticate block throws at startup unless
 * [KlokkaDashboardConfig.allowAnonymous] is set explicitly, so a forgotten wrapper is a
 * boot failure, not a public dashboard.
 *
 * [store] must implement [QueryableStore] (both bundled stores do). Requeue is the
 * plain CAS `DeadLettered -> Enqueued`: the retry budget resumes where it left off, so a
 * requeued job that fails again dead-letters again rather than gaining a fresh budget.
 */
public fun Route.klokkaDashboard(
    store: JobStore,
    path: String = "klokka",
    configure: KlokkaDashboardConfig.() -> Unit = {},
) {
    val config = KlokkaDashboardConfig().apply(configure)
    val queryable =
        store as? QueryableStore
            ?: error(
                "klokkaDashboard needs a store implementing QueryableStore; " +
                    "${store::class.simpleName} does not, so there is nothing to display.",
            )
    if (!config.allowAnonymous && !isInsideAuthenticateBlock()) {
        error(
            "klokkaDashboard is mounted outside any authenticate { } block and would be publicly " +
                "reachable, payloads included. Wrap the mount: authenticate(...) { klokkaDashboard(store) }. " +
                "To serve it deliberately unauthenticated (development only), set allowAnonymous = true.",
        )
    }

    route(path) {
        get {
            val params = call.request.queryParameters
            val status = params["status"]?.let { name -> JobStatus.entries.firstOrNull { it.name == name } }
            val kind = params["kind"]?.takeUnless { it.isBlank() }
            val queue = params["queue"]?.takeUnless { it.isBlank() }?.let(::QueueName)
            val page = (params["page"]?.toIntOrNull() ?: 0).coerceAtLeast(0)
            val query =
                JobQuery(
                    status = status,
                    kind = kind,
                    queue = queue,
                    limit = config.pageSize,
                    offset = page * config.pageSize,
                )
            val counts = queryable.countsByStatus()
            val jobs = queryable.listJobs(query)
            call.respondHtml { overviewPage(counts, jobs, query, page) }
        }

        get("jobs/{id}") {
            val id = JobId(call.parameters.getOrFail("id"))
            val details = queryable.getJob(id)
            if (details == null) {
                call.respond(HttpStatusCode.NotFound)
            } else {
                call.respondHtml { detailPage(details, readOnly = config.readOnly) }
            }
        }

        post("jobs/{id}/requeue") {
            if (config.readOnly) {
                call.respond(HttpStatusCode.Forbidden)
                return@post
            }
            val id = JobId(call.parameters.getOrFail("id"))
            // The whole action is one CAS; a job that is not DeadLettered anymore is a no-op.
            store.transition(id, JobState.DeadLettered, JobState.Enqueued)
            call.respondRedirect(call.request.local.uri.removeSuffix("/requeue"))
        }
    }
}

/**
 * True when an `authenticate { }` block encloses this mount point. Ktor's authenticate
 * wrapper is a child route carrying an [AuthenticationRouteSelector], so walking the
 * parent chain at mount time answers "is anyone guarding this door".
 */
private fun Route.isInsideAuthenticateBlock(): Boolean =
    generateSequence(this) { it.parent }.any { it.selector is AuthenticationRouteSelector }

private fun io.ktor.http.Parameters.getOrFail(name: String): String =
    checkNotNull(get(name)) { "missing route parameter '$name'" }
