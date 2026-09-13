@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.spi

import com.eventslooped.klokka.JobId
import com.eventslooped.klokka.JobStatus
import com.eventslooped.klokka.QueueName
import com.eventslooped.klokka.WorkerId
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

/**
 * Filter and page for [QueryableStore.listJobs]. Null filter fields mean "any".
 * [limit]/[offset] page through the filtered set; stores order results most recently
 * persisted first, so page 0 is the newest work.
 */
public data class JobQuery(
    val status: JobStatus? = null,
    val kind: String? = null,
    val queue: QueueName? = null,
    val limit: Int = 50,
    val offset: Int = 0,
) {
    init {
        require(limit >= 1) { "limit must be at least 1, was $limit" }
        require(offset >= 0) { "offset must not be negative, was $offset" }
    }
}

/** One job row as a listing sees it: everything but the payload and lease internals. */
public data class JobSummary(
    val id: JobId,
    val kind: String,
    val queue: QueueName,
    val status: JobStatus,
    /** Executions so far; 0 for a job never claimed. */
    val attempt: Int,
    val runAt: Instant,
    val enqueuedAt: Instant,
    /** Non-null when this run was emitted by a recurring schedule or triggerNow. */
    val scheduleId: String?,
    /** Non-null exactly when [status] is [JobStatus.Failed]: when the booked retry is due. */
    val retryAt: Instant?,
    /** Non-null exactly when [status] is terminal: when the run reached that state. */
    val terminalAt: Instant?,
)

/**
 * One job in full, for a detail view. [payload] is the serialized form as stored;
 * anything mounted on top of this (the dashboard) shows it to operators, which is worth
 * remembering when payloads carry sensitive data: the dashboard fails closed on
 * authentication for exactly that reason.
 */
public data class JobDetails(
    val summary: JobSummary,
    val payload: String,
    val payloadVersion: Int,
    val uniqueKey: String?,
    val fence: Long,
    /** Live lease expiry while Running; null otherwise. */
    val leaseUntil: Instant?,
    /** The worker holding the lease while Running; null otherwise. */
    val holder: WorkerId?,
)

/**
 * Optional capability: read access for dashboards and tooling. Kept OUT of [JobStore]
 * deliberately: the core contract is the minimal thing a store must get right to run
 * jobs safely, and observation is additive. A store without this capability is still a
 * valid backend; it just cannot serve the dashboard.
 *
 * Contract:
 * - All methods are read-only snapshots. Two calls, or one call's count and another's
 *   listing, need not be transactionally consistent with each other.
 * - [listJobs] orders results most recently persisted first and applies filters
 *   conjunctively.
 * - [countsByStatus] may omit statuses with zero jobs.
 * - [getJob] returns null for an unknown id, including ids that could never belong to
 *   this store (a malformed id is not an error; the caller often took it from a URL).
 */
public interface QueryableStore : JobStore {
    public suspend fun countsByStatus(): Map<JobStatus, Long>

    public suspend fun listJobs(query: JobQuery): List<JobSummary>

    public suspend fun getJob(id: JobId): JobDetails?
}
