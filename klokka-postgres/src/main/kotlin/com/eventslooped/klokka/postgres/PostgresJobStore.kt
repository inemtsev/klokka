@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.postgres

import com.eventslooped.klokka.JobId
import com.eventslooped.klokka.JobState
import com.eventslooped.klokka.JobStatus
import com.eventslooped.klokka.QueueName
import com.eventslooped.klokka.RetentionPolicy
import com.eventslooped.klokka.WorkerId
import com.eventslooped.klokka.spi.ClaimedJob
import com.eventslooped.klokka.spi.JobDetails
import com.eventslooped.klokka.spi.JobQuery
import com.eventslooped.klokka.spi.JobStore
import com.eventslooped.klokka.spi.JobSummary
import com.eventslooped.klokka.spi.NewJob
import com.eventslooped.klokka.spi.PushCapableStore
import com.eventslooped.klokka.spi.QueryableStore
import com.eventslooped.klokka.spi.ScheduleFire
import com.eventslooped.klokka.spi.ScheduleSpec
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.withContext
import org.postgresql.PGConnection
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.time.OffsetDateTime
import java.time.ZoneOffset
import javax.sql.DataSource
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.ExperimentalTime
import kotlin.time.Instant
import kotlin.time.toKotlinInstant

private val TERMINAL_STATES = listOf("Succeeded", "DeadLettered", "Cancelled")
private val TERMINAL_STATES_SQL = TERMINAL_STATES.joinToString(", ") { "'$it'" }

/** How often the uniqueKey insert/select race (a conflicting row turning terminal in between) is retried. */
private const val UNIQUE_KEY_RETRIES = 5

/** The NOTIFY channel the V002 triggers publish on. */
private const val WAKEUP_CHANNEL = "klokka_wakeup"

/**
 * Ceiling on one blocking getNotifications call. Coroutine cancellation cannot interrupt
 * a blocked socket read, so this bounds how long an unsubscribe or shutdown waits.
 */
private const val NOTIFICATION_WAIT_MILLIS = 500

/**
 * Tuning for [PostgresJobStore]. Everything has a production-sane default.
 *
 * [schema] names the Postgres schema holding Klokka's tables; null uses the connection's
 * default search_path. When set, the store creates it if missing and pins search_path on
 * every borrowed connection. Must be a plain identifier (letters, digits, underscore).
 *
 * [dispatcher] is where every blocking JDBC call runs; the caller's coroutine is never
 * blocked on a connection. Size it alongside the [DataSource] pool.
 *
 * [autoMigrate] applies the bundled schema migrations lazily before the first statement.
 * Set false when migrations are operated externally (Flyway against the bundled resource
 * directory, or a DBA-run script) and call [PostgresJobStore.migrate] yourself if needed.
 */
public class PostgresStoreConfig(
    public val schema: String? = null,
    public val dispatcher: CoroutineDispatcher = Dispatchers.IO,
    public val autoMigrate: Boolean = true,
) {
    init {
        require(schema == null || schema.matches(Regex("[A-Za-z_][A-Za-z0-9_]*"))) {
            "schema must be a plain identifier, was '$schema'"
        }
    }
}

/**
 * The reference [JobStore]: Postgres on plain JDBC, no driver classes and no ORM. Every
 * contract rule is enforced by single SQL statements against database time:
 *
 * - [claim] and [dueSchedules] are one `FOR UPDATE SKIP LOCKED` claim each: atomic across
 *   concurrent callers, due-ness decided by `now()`, fences bumped in the same statement.
 * - [transition] and [completeFire] are compare-and-set `UPDATE`s; a stale state or fence
 *   changes nothing.
 * - The uniqueKey rule is a partial unique index, not application logic.
 * - [sweep] compiles the declarative [RetentionPolicy] into bulk `DELETE`s.
 *
 * Instants are stored with Postgres's microsecond precision; sub-microsecond components
 * of caller-supplied times are truncated on write and in comparisons.
 */
public class PostgresJobStore(
    private val dataSource: DataSource,
    private val config: PostgresStoreConfig = PostgresStoreConfig(),
) : JobStore, PushCapableStore, QueryableStore {
    @Volatile
    private var migrated = false
    private val migrationLock = Any()

    /**
     * Applies the bundled migrations now, regardless of [PostgresStoreConfig.autoMigrate].
     * Idempotent and safe under concurrent callers (advisory-locked). With autoMigrate on,
     * the store calls this lazily before its first statement.
     */
    public suspend fun migrate(): Unit =
        withContext(config.dispatcher) {
            runMigrations()
        }

    // ---- enqueue --------------------------------------------------------------------------

    override suspend fun enqueue(jobs: List<NewJob>): List<JobId> =
        withConnection { connection -> enqueueOn(connection, jobs) }

    private fun enqueueOn(connection: Connection, jobs: List<NewJob>): List<JobId> {
        val plainSql = """
            INSERT INTO klokka_jobs (kind, payload, payload_version, queue, state, run_at, unique_key, schedule_id)
            VALUES (?, ?, ?, ?, CASE WHEN ?::timestamptz <= now() THEN 'Enqueued' ELSE 'Scheduled' END, ?, ?, ?)
            RETURNING id
        """
        val uniqueSql = """
            INSERT INTO klokka_jobs (kind, payload, payload_version, queue, state, run_at, unique_key, schedule_id)
            VALUES (?, ?, ?, ?, CASE WHEN ?::timestamptz <= now() THEN 'Enqueued' ELSE 'Scheduled' END, ?, ?, ?)
            ON CONFLICT (unique_key) WHERE unique_key IS NOT NULL AND state NOT IN ($TERMINAL_STATES_SQL) DO NOTHING
            RETURNING id
        """
        val result = ArrayList<JobId>(jobs.size)
        for (job in jobs) {
            if (job.uniqueKey == null) {
                connection.prepareStatement(plainSql).use { ps ->
                    bindNewJob(ps, job)
                    result.add(ps.executeQuery().use { rs -> rs.singleId() })
                }
            } else {
                result.add(enqueueUnique(connection, uniqueSql, job))
            }
        }
        return result
    }

    private fun enqueueUnique(connection: Connection, uniqueSql: String, job: NewJob): JobId {
        repeat(UNIQUE_KEY_RETRIES) {
            connection.prepareStatement(uniqueSql).use { ps ->
                bindNewJob(ps, job)
                ps.executeQuery().use { rs -> if (rs.next()) return JobId(rs.getLong(1).toString()) }
            }
            // Conflict: an existing non-terminal row owns the key. Idempotent success returns
            // its id. If it went terminal between the two statements, insert again.
            connection.prepareStatement(
                "SELECT id FROM klokka_jobs WHERE unique_key = ? AND state NOT IN ($TERMINAL_STATES_SQL) LIMIT 1",
            ).use { ps ->
                ps.setString(1, job.uniqueKey)
                ps.executeQuery().use { rs -> if (rs.next()) return JobId(rs.getLong(1).toString()) }
            }
        }
        error("uniqueKey '${job.uniqueKey}': could not insert or find a non-terminal owner after $UNIQUE_KEY_RETRIES attempts")
    }

    private fun bindNewJob(ps: PreparedStatement, job: NewJob) {
        ps.setString(1, job.kind)
        ps.setString(2, job.payload)
        ps.setInt(3, job.payloadVersion)
        ps.setString(4, job.queue.value)
        ps.setObject(5, job.runAt.toDb())
        ps.setObject(6, job.runAt.toDb())
        ps.setString(7, job.uniqueKey)
        ps.setString(8, job.scheduleId)
    }

    // ---- claim / heartbeat / transition ---------------------------------------------------

    override suspend fun claim(
        queues: List<QueueName>,
        kinds: Set<String>,
        limit: Int,
        lease: Duration,
        worker: WorkerId,
    ): List<ClaimedJob> {
        if (limit <= 0 || kinds.isEmpty() || queues.isEmpty()) return emptyList()
        val sql = """
            WITH c AS (
                SELECT id FROM klokka_jobs
                WHERE queue = ?
                  AND kind = ANY(?)
                  AND ((state IN ('Scheduled', 'Enqueued') AND run_at <= now())
                    OR (state = 'Failed' AND retry_at <= now())
                    OR (state = 'Running' AND lease_until <= now()))
                ORDER BY CASE WHEN state = 'Failed' THEN retry_at ELSE run_at END
                LIMIT ?
                FOR UPDATE SKIP LOCKED
            )
            UPDATE klokka_jobs j
            SET state = 'Running', attempt = j.attempt + 1, fence = j.fence + 1,
                lease_until = now() + make_interval(secs => ?), holder = ?, retry_at = NULL
            FROM c
            WHERE j.id = c.id
            RETURNING j.id, j.kind, j.payload, j.payload_version, j.queue, j.attempt,
                      j.enqueued_at, j.run_at, j.lease_until, j.fence, j.schedule_id
        """
        return withConnection { connection ->
            val claimed = ArrayList<ClaimedJob>(limit)
            val kindsArray = connection.createArrayOf("text", kinds.toTypedArray())
            // Declared order is priority: earlier queues are drained before later ones see a claim.
            for (queue in queues) {
                if (claimed.size >= limit) break
                connection.prepareStatement(sql).use { ps ->
                    ps.setString(1, queue.value)
                    ps.setArray(2, kindsArray)
                    ps.setInt(3, limit - claimed.size)
                    ps.setDouble(4, lease.toDouble(DurationUnit.SECONDS))
                    ps.setString(5, worker.value)
                    ps.executeQuery().use { rs ->
                        while (rs.next()) {
                            claimed.add(
                                ClaimedJob(
                                    id = JobId(rs.getLong(1).toString()),
                                    kind = rs.getString(2),
                                    payload = rs.getString(3),
                                    payloadVersion = rs.getInt(4),
                                    queue = QueueName(rs.getString(5)),
                                    attempt = rs.getInt(6),
                                    enqueuedAt = rs.instant(7),
                                    scheduledFor = rs.instant(8),
                                    leaseUntil = rs.instant(9),
                                    fence = rs.getLong(10),
                                    scheduleId = rs.getString(11),
                                ),
                            )
                        }
                    }
                }
            }
            claimed
        }
    }

    override suspend fun heartbeat(ids: List<JobId>, worker: WorkerId, extend: Duration) {
        val numeric = ids.mapNotNull { it.value.toLongOrNull() }
        if (numeric.isEmpty()) return
        withConnection { connection ->
            connection.prepareStatement(
                """
                UPDATE klokka_jobs SET lease_until = now() + make_interval(secs => ?)
                WHERE id = ANY(?) AND holder = ? AND state = 'Running' AND lease_until > now()
                """,
            ).use { ps ->
                ps.setDouble(1, extend.toDouble(DurationUnit.SECONDS))
                ps.setArray(2, connection.createArrayOf("bigint", numeric.toTypedArray()))
                ps.setString(3, worker.value)
                ps.executeUpdate()
            }
        }
    }

    override suspend fun transition(id: JobId, from: JobState, to: JobState, fence: Long?): Boolean {
        val numericId = id.value.toLongOrNull() ?: return false
        val clearLease = to.terminal || to is JobState.Failed
        val sql = """
            UPDATE klokka_jobs
            SET state = ?, retry_at = ?,
                lease_until = CASE WHEN ? THEN NULL ELSE lease_until END,
                holder      = CASE WHEN ? THEN NULL ELSE holder END,
                terminal_at = CASE WHEN ? THEN now() ELSE NULL END
            WHERE id = ? AND state = ?
              AND (? OR retry_at = ?)
              AND (? OR fence = ?)
        """
        return withConnection { connection ->
            connection.prepareStatement(sql).use { ps ->
                ps.setString(1, to.dbName())
                ps.setObject(2, (to as? JobState.Failed)?.retryAt?.toDb())
                ps.setBoolean(3, clearLease)
                ps.setBoolean(4, clearLease)
                ps.setBoolean(5, to.terminal)
                ps.setLong(6, numericId)
                ps.setString(7, from.dbName())
                ps.setBoolean(8, from !is JobState.Failed)
                ps.setObject(9, (from as? JobState.Failed)?.retryAt?.toDb())
                ps.setBoolean(10, fence == null)
                if (fence == null) ps.setNull(11, java.sql.Types.BIGINT) else ps.setLong(11, fence)
                ps.executeUpdate() == 1
            }
        }
    }

    // ---- schedules ------------------------------------------------------------------------

    override suspend fun upsertSchedules(schedules: List<ScheduleSpec>) {
        if (schedules.isEmpty()) return
        val sql = """
            INSERT INTO klokka_schedules (id, kind, fingerprint, description, next_fire_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE
            SET kind = EXCLUDED.kind, fingerprint = EXCLUDED.fingerprint, description = EXCLUDED.description,
                next_fire_at = EXCLUDED.next_fire_at, lease_until = NULL, holder = NULL,
                fence = klokka_schedules.fence + 1
            WHERE klokka_schedules.fingerprint IS DISTINCT FROM EXCLUDED.fingerprint
        """
        withConnection { connection ->
            connection.prepareStatement(sql).use { ps ->
                for (spec in schedules) {
                    ps.setString(1, spec.id)
                    ps.setString(2, spec.kind)
                    ps.setString(3, spec.fingerprint)
                    ps.setString(4, spec.description)
                    ps.setObject(5, spec.fireAt?.toDb())
                    ps.executeUpdate()
                }
            }
        }
    }

    override suspend fun dueSchedules(
        ids: Set<String>,
        limit: Int,
        lease: Duration,
        worker: WorkerId,
    ): List<ScheduleFire> {
        if (limit <= 0 || ids.isEmpty()) return emptyList()
        val sql = """
            WITH c AS (
                SELECT id FROM klokka_schedules
                WHERE id = ANY(?) AND next_fire_at IS NOT NULL AND next_fire_at <= now()
                  AND (lease_until IS NULL OR lease_until <= now())
                ORDER BY next_fire_at
                LIMIT ?
                FOR UPDATE SKIP LOCKED
            )
            UPDATE klokka_schedules s
            SET fence = s.fence + 1, lease_until = now() + make_interval(secs => ?), holder = ?
            FROM c
            WHERE s.id = c.id
            RETURNING s.id, s.next_fire_at, now(), s.fence
        """
        return withConnection { connection ->
            connection.prepareStatement(sql).use { ps ->
                ps.setArray(1, connection.createArrayOf("text", ids.toTypedArray()))
                ps.setInt(2, limit)
                ps.setDouble(3, lease.toDouble(DurationUnit.SECONDS))
                ps.setString(4, worker.value)
                ps.executeQuery().use { rs ->
                    val fires = ArrayList<ScheduleFire>()
                    while (rs.next()) {
                        fires.add(
                            ScheduleFire(
                                scheduleId = rs.getString(1),
                                scheduledFor = rs.instant(2),
                                now = rs.instant(3),
                                fence = rs.getLong(4),
                            ),
                        )
                    }
                    fires
                }
            }
        }
    }

    override suspend fun completeFire(
        scheduleId: String,
        fence: Long,
        runs: List<NewJob>,
        nextFireAt: Instant?,
    ): List<JobId>? =
        withConnection { connection ->
            connection.autoCommit = false
            try {
                val advanced =
                    connection.prepareStatement(
                        """
                        UPDATE klokka_schedules
                        SET next_fire_at = ?, last_fired_at = now(), lease_until = NULL, holder = NULL
                        WHERE id = ? AND fence = ?
                        """,
                    ).use { ps ->
                        ps.setObject(1, nextFireAt?.toDb())
                        ps.setString(2, scheduleId)
                        ps.setLong(3, fence)
                        ps.executeUpdate() == 1
                    }
                if (!advanced) {
                    connection.rollback()
                    null
                } else {
                    val ids = enqueueOn(connection, runs)
                    connection.commit()
                    ids
                }
            } catch (e: Throwable) {
                connection.rollback()
                throw e
            } finally {
                connection.autoCommit = true
            }
        }

    // ---- wakeups --------------------------------------------------------------------------

    /**
     * LISTEN/NOTIFY push wake-ups, fed by the V002 schema triggers on every write that
     * makes a job claimable. Delivery is at COMMIT, so a wake-up never precedes the row
     * it announces; batch inserts collapse to one notification.
     *
     * While collected, this flow holds ONE dedicated connection from the [DataSource]
     * (LISTEN is connection-scoped) and occupies one [PostgresStoreConfig.dispatcher]
     * thread. The connection must reach Postgres directly or through a session-mode
     * pooler; PgBouncer transaction pooling breaks LISTEN.
     *
     * One hint is emitted immediately after LISTEN is established, so work enqueued
     * while no listener existed (startup, a reconnect gap) is claimed at once instead of
     * at the next poll. A dead connection makes the flow throw: reconnecting is the
     * caller's loop (the runtime restarts collection with backoff, and its poll ticker
     * covers the gap), so the worst failure mode of push is poll latency.
     */
    override fun wakeups(): Flow<Unit> =
        flow {
            if (config.autoMigrate) runMigrations()
            dataSource.connection.use { connection ->
                config.schema?.let { schema ->
                    connection.createStatement().use { it.execute("SET search_path TO $schema") }
                }
                connection.createStatement().use { it.execute("LISTEN $WAKEUP_CHANNEL") }
                val pg = connection.unwrap(PGConnection::class.java)
                emit(Unit) // the listener is live from here; cover anything that arrived before it
                while (true) {
                    val notifications = pg.getNotifications(NOTIFICATION_WAIT_MILLIS)
                    if (!notifications.isNullOrEmpty()) {
                        emit(Unit)
                    }
                    currentCoroutineContext().ensureActive()
                }
            }
        }.flowOn(config.dispatcher)

    // ---- queries --------------------------------------------------------------------------

    override suspend fun countsByStatus(): Map<JobStatus, Long> =
        withConnection { connection ->
            connection.prepareStatement("SELECT state, count(*) FROM klokka_jobs GROUP BY state").use { ps ->
                ps.executeQuery().use { rs ->
                    val counts = LinkedHashMap<JobStatus, Long>()
                    while (rs.next()) {
                        counts[JobStatus.valueOf(rs.getString(1))] = rs.getLong(2)
                    }
                    counts
                }
            }
        }

    override suspend fun listJobs(query: JobQuery): List<JobSummary> {
        // Dynamic WHERE from the non-null filters, conditions and params kept in step.
        val conditions = mutableListOf<String>()
        val params = mutableListOf<String>()
        query.status?.let {
            conditions += "state = ?"
            params += it.name
        }
        query.kind?.let {
            conditions += "kind = ?"
            params += it
        }
        query.queue?.let {
            conditions += "queue = ?"
            params += it.value
        }
        val where = if (conditions.isEmpty()) "" else " WHERE " + conditions.joinToString(" AND ")
        val sql = """
            SELECT id, kind, queue, state, attempt, run_at, enqueued_at, schedule_id, retry_at, terminal_at
            FROM klokka_jobs$where
            ORDER BY id DESC
            LIMIT ? OFFSET ?
        """
        return withConnection { connection ->
            connection.prepareStatement(sql).use { ps ->
                params.forEachIndexed { index, value -> ps.setString(index + 1, value) }
                ps.setInt(params.size + 1, query.limit)
                ps.setInt(params.size + 2, query.offset)
                ps.executeQuery().use { rs ->
                    val result = ArrayList<JobSummary>()
                    while (rs.next()) {
                        result += rs.toSummary()
                    }
                    result
                }
            }
        }
    }

    override suspend fun getJob(id: JobId): JobDetails? {
        val numericId = id.value.toLongOrNull() ?: return null
        val sql = """
            SELECT id, kind, queue, state, attempt, run_at, enqueued_at, schedule_id, retry_at, terminal_at,
                   payload, payload_version, unique_key, fence, lease_until, holder
            FROM klokka_jobs
            WHERE id = ?
        """
        return withConnection { connection ->
            connection.prepareStatement(sql).use { ps ->
                ps.setLong(1, numericId)
                ps.executeQuery().use { rs ->
                    if (!rs.next()) {
                        null
                    } else {
                        val summary = rs.toSummary()
                        val running = summary.status == JobStatus.Running
                        JobDetails(
                            summary = summary,
                            payload = rs.getString(11),
                            payloadVersion = rs.getInt(12),
                            uniqueKey = rs.getString(13),
                            fence = rs.getLong(14),
                            leaseUntil = if (running) rs.instantOrNull(15) else null,
                            holder = if (running) rs.getString(16)?.let { WorkerId(it) } else null,
                        )
                    }
                }
            }
        }
    }

    // ---- sweep ----------------------------------------------------------------------------

    override suspend fun sweep(retention: RetentionPolicy) {
        withConnection { connection ->
            fun delete(state: String, keepFor: Duration) {
                connection.prepareStatement(
                    "DELETE FROM klokka_jobs WHERE state = ? AND terminal_at < now() - make_interval(secs => ?)",
                ).use { ps ->
                    ps.setString(1, state)
                    ps.setDouble(2, keepFor.toDouble(DurationUnit.SECONDS))
                    ps.executeUpdate()
                }
            }
            delete("Succeeded", retention.succeededFor)
            delete("Cancelled", retention.cancelledFor)
            retention.deadLetteredFor?.let { delete("DeadLettered", it) }
        }
    }

    // ---- plumbing -------------------------------------------------------------------------

    private suspend fun <T> withConnection(block: (Connection) -> T): T =
        withContext(config.dispatcher) {
            if (config.autoMigrate) runMigrations()
            dataSource.connection.use { connection ->
                config.schema?.let { schema ->
                    connection.createStatement().use { it.execute("SET search_path TO $schema") }
                }
                block(connection)
            }
        }

    private fun runMigrations() {
        if (migrated) return
        synchronized(migrationLock) {
            if (migrated) return
            dataSource.connection.use { Migrations.apply(it, config.schema) }
            migrated = true
        }
    }
}

// ---- mapping helpers ----------------------------------------------------------------------

private fun JobState.dbName(): String =
    when (this) {
        is JobState.Scheduled -> "Scheduled"
        is JobState.Enqueued -> "Enqueued"
        is JobState.Running -> "Running"
        is JobState.Succeeded -> "Succeeded"
        is JobState.Failed -> "Failed"
        is JobState.DeadLettered -> "DeadLettered"
        is JobState.Cancelled -> "Cancelled"
    }

/** Truncates to Postgres's microsecond precision so stored and compared values agree. */
private fun Instant.toDb(): OffsetDateTime {
    val micros = nanosecondsOfSecond / 1_000L * 1_000L
    return OffsetDateTime.ofInstant(java.time.Instant.ofEpochSecond(epochSeconds, micros), ZoneOffset.UTC)
}

private fun ResultSet.instant(column: Int): Instant =
    getObject(column, OffsetDateTime::class.java).toInstant().toKotlinInstant()

private fun ResultSet.instantOrNull(column: Int): Instant? =
    getObject(column, OffsetDateTime::class.java)?.toInstant()?.toKotlinInstant()

private fun ResultSet.singleId(): JobId {
    check(next()) { "INSERT ... RETURNING produced no row" }
    return JobId(getLong(1).toString())
}

/**
 * Maps the first ten columns of a queries-section SELECT (id, kind, queue, state, attempt,
 * run_at, enqueued_at, schedule_id, retry_at, terminal_at) to a [JobSummary]. [listJobs] and
 * [getJob][PostgresJobStore.getJob] both select exactly this prefix, in this order, so their
 * result sets share this mapping. retryAt and terminalAt are read unconditionally: by
 * construction retry_at is NULL outside JobState.Failed and terminal_at is NULL outside a
 * terminal state, so no state check is needed here.
 */
private fun ResultSet.toSummary(): JobSummary =
    JobSummary(
        id = JobId(getLong(1).toString()),
        kind = getString(2),
        queue = QueueName(getString(3)),
        status = JobStatus.valueOf(getString(4)),
        attempt = getInt(5),
        runAt = instant(6),
        enqueuedAt = instant(7),
        scheduleId = getString(8),
        retryAt = instantOrNull(9),
        terminalAt = instantOrNull(10),
    )
