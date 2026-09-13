@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.postgres

import com.eventslooped.klokka.JobId
import com.eventslooped.klokka.QueueName
import com.eventslooped.klokka.spi.NewJob
import kotlinx.coroutines.runBlocking
import org.postgresql.ds.PGSimpleDataSource
import org.testcontainers.containers.PostgreSQLContainer
import javax.sql.DataSource
import kotlin.time.Duration.Companion.minutes
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

/**
 * One shared Postgres container and store for the whole test run (Testcontainers keeps it
 * alive until the JVM exits). Tests call [freshStore] to get the store with truncated
 * tables; migrations run once, lazily, through the store's own autoMigrate path.
 *
 * There is deliberately no injectable clock here: database time is authoritative, exactly
 * as in production. Tests make rows due by inserting past `runAt` values and make leases
 * expire by using zero or negative lease durations.
 */
internal object PostgresTestSupport {
    private val container: PostgreSQLContainer<*> by lazy {
        PostgreSQLContainer("postgres:16-alpine").also { it.start() }
    }

    val dataSource: DataSource by lazy {
        PGSimpleDataSource().apply {
            setURL(container.jdbcUrl)
            user = container.username
            password = container.password
        }
    }

    private val store: PostgresJobStore by lazy {
        PostgresJobStore(dataSource).also { runBlocking { it.migrate() } }
    }

    fun freshStore(): PostgresJobStore {
        store // force migration before truncating
        dataSource.connection.use { connection ->
            connection.createStatement().use {
                it.execute("TRUNCATE klokka_jobs RESTART IDENTITY")
                it.execute("TRUNCATE klokka_schedules")
            }
        }
        return store
    }

    /** now() from the database, the only clock these tests trust. */
    fun dbNow(): Instant =
        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery("SELECT now()").use { rs ->
                    rs.next()
                    rs.getObject(1, java.time.OffsetDateTime::class.java).toInstant().let {
                        Instant.fromEpochSeconds(it.epochSecond, it.nano)
                    }
                }
            }
        }

    /** A due-now job; override [runAt] with a future instant for a Scheduled one. */
    fun newJob(
        kind: String = "test.kind",
        queue: QueueName = QueueName.DEFAULT,
        runAt: Instant = dbNow() - 1.minutes,
        uniqueKey: String? = null,
        scheduleId: String? = null,
        payload: String = "{}",
    ): NewJob =
        NewJob(
            kind = kind,
            payload = payload,
            payloadVersion = 1,
            queue = queue,
            runAt = runAt,
            uniqueKey = uniqueKey,
            scheduleId = scheduleId,
        )

    /** Reads one job row's mutable fields for assertions. Null when the id is unknown. */
    fun jobRow(id: JobId): JobRow? =
        dataSource.connection.use { connection ->
            connection.prepareStatement(
                "SELECT state, attempt, fence, queue, retry_at, lease_until, holder, terminal_at, schedule_id " +
                    "FROM klokka_jobs WHERE id = ?",
            ).use { ps ->
                ps.setLong(1, id.value.toLong())
                ps.executeQuery().use { rs ->
                    if (!rs.next()) return null
                    JobRow(
                        state = rs.getString(1),
                        attempt = rs.getInt(2),
                        fence = rs.getLong(3),
                        queue = rs.getString(4),
                        retryAt = rs.getObject(5, java.time.OffsetDateTime::class.java)?.toString(),
                        leaseUntil = rs.getObject(6, java.time.OffsetDateTime::class.java)?.toString(),
                        holder = rs.getString(7),
                        terminalAt = rs.getObject(8, java.time.OffsetDateTime::class.java)?.toString(),
                        scheduleId = rs.getString(9),
                    )
                }
            }
        }

    data class JobRow(
        val state: String,
        val attempt: Int,
        val fence: Long,
        val queue: String,
        val retryAt: String?,
        val leaseUntil: String?,
        val holder: String?,
        val terminalAt: String?,
        val scheduleId: String?,
    )

    /** Reads one schedule row for assertions. Null when the id is unknown. */
    fun scheduleRow(id: String): ScheduleRow? =
        dataSource.connection.use { connection ->
            connection.prepareStatement(
                "SELECT kind, fingerprint, description, next_fire_at, last_fired_at, fence, lease_until, holder " +
                    "FROM klokka_schedules WHERE id = ?",
            ).use { ps ->
                ps.setString(1, id)
                ps.executeQuery().use { rs ->
                    if (!rs.next()) return null
                    ScheduleRow(
                        kind = rs.getString(1),
                        fingerprint = rs.getString(2),
                        description = rs.getString(3),
                        nextFireAt = rs.getObject(4, java.time.OffsetDateTime::class.java)?.toInstant()?.let {
                            Instant.fromEpochSeconds(it.epochSecond, it.nano)
                        },
                        lastFiredAt = rs.getObject(5, java.time.OffsetDateTime::class.java)?.toInstant()?.let {
                            Instant.fromEpochSeconds(it.epochSecond, it.nano)
                        },
                        fence = rs.getLong(6),
                        leaseUntil = rs.getObject(7, java.time.OffsetDateTime::class.java)?.toString(),
                        holder = rs.getString(8),
                    )
                }
            }
        }

    data class ScheduleRow(
        val kind: String,
        val fingerprint: String,
        val description: String,
        val nextFireAt: Instant?,
        val lastFiredAt: Instant?,
        val fence: Long,
        val leaseUntil: String?,
        val holder: String?,
    )
}
