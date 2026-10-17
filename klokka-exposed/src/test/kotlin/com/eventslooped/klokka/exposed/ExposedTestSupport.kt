@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.exposed

import com.eventslooped.klokka.JobEvent
import com.eventslooped.klokka.JobId
import com.eventslooped.klokka.JobRegistry
import com.eventslooped.klokka.jobType
import com.eventslooped.klokka.postgres.PostgresJobStore
import com.eventslooped.klokka.runtime.KlokkaRuntime
import com.eventslooped.klokka.runtime.KlokkaSettings
import com.eventslooped.klokka.spi.JobStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.filterIsInstance
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.jetbrains.exposed.v1.core.DatabaseConfig
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.JdbcTransaction
import org.postgresql.ds.PGSimpleDataSource
import org.testcontainers.containers.PostgreSQLContainer
import java.sql.Connection
import java.sql.ResultSet
import java.time.OffsetDateTime
import java.util.concurrent.CopyOnWriteArrayList
import javax.sql.DataSource
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

/** The job type every test in this module enqueues; the payload is a plain string. */
internal val Ping = jobType<String>("exposed.ping")

/**
 * One shared Postgres container for the module's tests (Testcontainers keeps it alive until
 * the JVM exits), Klokka's tables migrated once, plus a `scratch` table standing in for the
 * application's own data, so tests can tell a rolled-back transaction from a committed one.
 */
internal object ExposedTestDatabase {
    private val container: PostgreSQLContainer<*> by lazy {
        PostgreSQLContainer("postgres:16-alpine").also { it.start() }
    }

    val dataSource: DataSource by lazy {
        val ds =
            PGSimpleDataSource().apply {
                setURL(container.jdbcUrl)
                user = container.username
                password = container.password
            }
        runBlocking { PostgresJobStore(ds).migrate() }
        ds.connection.use { connection ->
            connection.createStatement().use {
                it.execute("CREATE TABLE IF NOT EXISTS scratch (id BIGSERIAL PRIMARY KEY, note TEXT NOT NULL)")
            }
        }
        ds
    }

    /** Savepoints are only used when `useNestedTransactions` is on, so tests pick the flavor. */
    val plain: Database by lazy { Database.connect(dataSource) }

    val nested: Database by lazy {
        Database.connect(dataSource, databaseConfig = DatabaseConfig { useNestedTransactions = true })
    }

    fun store(): PostgresJobStore = PostgresJobStore(dataSource)

    /** Empties Klokka's tables and the scratch table between tests. */
    fun reset() {
        dataSource.connection.use { connection ->
            connection.createStatement().use {
                it.execute("TRUNCATE scratch, klokka_jobs, klokka_schedules RESTART IDENTITY")
            }
        }
    }

    fun count(sql: String): Int =
        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery(sql).use { rs ->
                    rs.next()
                    rs.getInt(1)
                }
            }
        }

    fun jobCount(id: JobId): Int = count("SELECT count(*) FROM klokka_jobs WHERE id = ${id.value.toLong()}")

    fun jobState(id: JobId): String? =
        query("SELECT state FROM klokka_jobs WHERE id = ${id.value.toLong()}") { it.getString(1) }

    fun jobRunAt(id: JobId): Instant? =
        query("SELECT run_at FROM klokka_jobs WHERE id = ${id.value.toLong()}") { rs ->
            val at = rs.getObject(1, OffsetDateTime::class.java).toInstant()
            Instant.fromEpochSeconds(at.epochSecond, at.nano)
        }

    private fun <T> query(sql: String, read: (ResultSet) -> T): T? =
        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery(sql).use { rs -> if (rs.next()) read(rs) else null }
            }
        }
}

/** Inserts a scratch row through the Exposed transaction's own connection. */
internal fun JdbcTransaction.insertScratch(note: String) {
    (connection.connection as Connection).prepareStatement("INSERT INTO scratch (note) VALUES (?)").use {
        it.setString(1, note)
        it.executeUpdate()
    }
}

/**
 * A runtime over [store] with its Enqueued events collected into a thread-safe list. The
 * collector is subscribed before this returns, so nothing enqueued afterwards is missed.
 * Workers are never started: these tests look at rows and events, not execution.
 */
internal class EventProbe(
    store: JobStore = ExposedTestDatabase.store(),
) {
    val runtime = KlokkaRuntime(store, JobRegistry(), KlokkaSettings())
    val enqueued = CopyOnWriteArrayList<JobEvent.Enqueued>()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    init {
        scope.launch(start = CoroutineStart.UNDISPATCHED) {
            runtime.events.filterIsInstance<JobEvent.Enqueued>().collect { enqueued += it }
        }
    }

    fun close() = scope.cancel()

    /** Ids of the Enqueued events seen so far, in delivery order. */
    fun ids(): List<JobId> = enqueued.map { it.jobId }

    /** Lets the collector run, for assertions that something has NOT happened yet. */
    suspend fun settle() = delay(100.milliseconds)

    /**
     * Commits a control job in its own transaction and waits for its event. SharedFlow
     * delivery is ordered, so once the control event is in the list, any event an earlier
     * job was going to emit is in the list too. Returns the control job's id.
     */
    suspend fun awaitControl(): JobId {
        val id = runtime.enqueue(Ping, "control")
        withTimeout(10.seconds) {
            while (id !in ids()) delay(10.milliseconds)
        }
        return id
    }
}
