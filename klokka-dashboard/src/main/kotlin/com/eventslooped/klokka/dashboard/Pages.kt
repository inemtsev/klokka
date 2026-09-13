@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.dashboard

import com.eventslooped.klokka.JobStatus
import com.eventslooped.klokka.spi.JobDetails
import com.eventslooped.klokka.spi.JobQuery
import com.eventslooped.klokka.spi.JobSummary
import kotlinx.html.BODY
import kotlinx.html.HTML
import kotlinx.html.a
import kotlinx.html.body
import kotlinx.html.dd
import kotlinx.html.div
import kotlinx.html.dl
import kotlinx.html.dt
import kotlinx.html.form
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.head
import kotlinx.html.input
import kotlinx.html.option
import kotlinx.html.p
import kotlinx.html.pre
import kotlinx.html.select
import kotlinx.html.span
import kotlinx.html.style
import kotlinx.html.submitInput
import kotlinx.html.table
import kotlinx.html.tbody
import kotlinx.html.td
import kotlinx.html.th
import kotlinx.html.thead
import kotlinx.html.title
import kotlinx.html.tr
import kotlinx.html.unsafe
import kotlinx.html.FormMethod
import kotlinx.html.InputType
import kotlin.time.ExperimentalTime

private const val STYLE = """
    body { font-family: system-ui, sans-serif; margin: 2rem; color: #1a1a1a; }
    table { border-collapse: collapse; width: 100%; margin-top: 1rem; }
    th, td { text-align: left; padding: .4rem .8rem; border-bottom: 1px solid #ddd; }
    th { border-bottom: 2px solid #999; }
    .counts { display: flex; gap: 1rem; flex-wrap: wrap; margin: 1rem 0; }
    .counts a { text-decoration: none; color: inherit; border: 1px solid #ccc; border-radius: 6px; padding: .5rem 1rem; }
    .counts .n { font-size: 1.4rem; font-weight: 600; display: block; }
    form.filters { display: flex; gap: .5rem; align-items: center; }
    pre { background: #f6f6f6; padding: 1rem; border-radius: 6px; overflow-x: auto; }
    .pager { margin-top: 1rem; display: flex; gap: 1rem; }
    .muted { color: #777; }
"""

internal fun HTML.overviewPage(counts: Map<JobStatus, Long>, jobs: List<JobSummary>, query: JobQuery, page: Int) {
    head {
        title { +"Klokka" }
        style { unsafe { +STYLE } }
    }
    body {
        h1 { +"Klokka" }
        div(classes = "counts") {
            for (status in JobStatus.entries) {
                a(href = "?status=${status.name}") {
                    span(classes = "n") { +"${counts[status] ?: 0}" }
                    +status.name
                }
            }
        }
        form(classes = "filters", method = FormMethod.get) {
            select {
                name = "status"
                option {
                    value = ""
                    +"any status"
                }
                for (status in JobStatus.entries) {
                    option {
                        value = status.name
                        selected = query.status == status
                        +status.name
                    }
                }
            }
            input(type = InputType.text) {
                name = "kind"
                placeholder = "kind"
                value = query.kind ?: ""
            }
            input(type = InputType.text) {
                name = "queue"
                placeholder = "queue"
                value = query.queue?.value ?: ""
            }
            submitInput { value = "Filter" }
        }
        table {
            thead {
                tr {
                    th { +"id" }
                    th { +"kind" }
                    th { +"queue" }
                    th { +"status" }
                    th { +"attempt" }
                    th { +"run at" }
                    th { +"schedule" }
                }
            }
            tbody {
                for (job in jobs) {
                    tr {
                        td { a(href = "jobs/${job.id.value}") { +job.id.value } }
                        td { +job.kind }
                        td { +job.queue.value }
                        td { +job.status.name }
                        td { +"${job.attempt}" }
                        td { +job.runAt.toString() }
                        td { job.scheduleId?.let { +it } ?: span(classes = "muted") { +"-" } }
                    }
                }
            }
        }
        if (jobs.isEmpty()) p(classes = "muted") { +"No jobs match." }
        div(classes = "pager") {
            if (page > 0) a(href = pageLink(query, page - 1)) { +"Newer" }
            if (jobs.size == query.limit) a(href = pageLink(query, page + 1)) { +"Older" }
        }
    }
}

internal fun HTML.detailPage(details: JobDetails, readOnly: Boolean) {
    val job = details.summary
    head {
        title { +"Klokka: ${job.id.value}" }
        style { unsafe { +STYLE } }
    }
    body {
        h1 { +"Job ${job.id.value}" }
        p { a(href = "../") { +"Back to overview" } }
        dl {
            dt { +"kind" }
            dd { +job.kind }
            dt { +"status" }
            dd { +job.status.name }
            dt { +"queue" }
            dd { +job.queue.value }
            dt { +"attempt" }
            dd { +"${job.attempt}" }
            dt { +"fence" }
            dd { +"${details.fence}" }
            dt { +"run at" }
            dd { +job.runAt.toString() }
            dt { +"enqueued at" }
            dd { +job.enqueuedAt.toString() }
            job.retryAt?.let {
                dt { +"retry at" }
                dd { +it.toString() }
            }
            job.terminalAt?.let {
                dt { +"terminal at" }
                dd { +it.toString() }
            }
            details.leaseUntil?.let {
                dt { +"lease until" }
                dd { +it.toString() }
            }
            details.holder?.let {
                dt { +"held by" }
                dd { +it.value }
            }
            job.scheduleId?.let {
                dt { +"schedule" }
                dd { +it }
            }
            details.uniqueKey?.let {
                dt { +"unique key" }
                dd { +it }
            }
        }
        h2 { +"Payload (v${details.payloadVersion})" }
        pre { +details.payload }
        if (job.status == JobStatus.DeadLettered && !readOnly) {
            form(action = "${job.id.value}/requeue", method = FormMethod.post) {
                submitInput { value = "Requeue" }
            }
            p(classes = "muted") {
                +"Requeue resumes the retry budget where it left off: one more failure dead-letters again."
            }
        }
    }
}

private fun pageLink(query: JobQuery, page: Int): String {
    val params = mutableListOf("page=$page")
    query.status?.let { params.add("status=${it.name}") }
    query.kind?.let { params.add("kind=$it") }
    query.queue?.let { params.add("queue=${it.value}") }
    return "?" + params.joinToString("&")
}
