@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.postgres.multinode

import com.eventslooped.klokka.JobRegistry
import com.eventslooped.klokka.QueueName
import com.eventslooped.klokka.WorkerId
import com.eventslooped.klokka.every
import com.eventslooped.klokka.jobType
import com.eventslooped.klokka.postgres.PostgresJobStore
import com.eventslooped.klokka.runtime.KlokkaRuntime
import com.eventslooped.klokka.runtime.KlokkaSettings
import com.eventslooped.klokka.runtime.QueueConfig
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking
import org.postgresql.ds.PGSimpleDataSource
import javax.sql.DataSource
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime

private val SLOW_JOB = jobType<String>("multinode.slow")
private val TICK_JOB = jobType<String>("multinode.tick")

/**
 * Standalone worker node for the multi-node kill-test suite (see [MultiNodeHarness]).
 * Launched as a child process; it never exits on its own, only by being killed, SIGSTOPped,
 * or SIGCONTed from outside. Every job lifecycle moment it touches (start, mid-job hold,
 * completion) is written to `multinode_executions` via plain JDBC, so a test that kills this
 * process mid-job can still read what it did before it died.
 */
public fun main() {
    val jdbcUrl = requireEnv("KLOKKA_TEST_JDBC_URL")
    val dbUser = requireEnv("KLOKKA_TEST_DB_USER")
    val dbPassword = requireEnv("KLOKKA_TEST_DB_PASSWORD")
    val nodeId = requireEnv("KLOKKA_TEST_NODE_ID")
    val holdMillis = (System.getenv("KLOKKA_TEST_HOLD_MILLIS") ?: "5000").toLong()
    val recurring = (System.getenv("KLOKKA_TEST_RECURRING") ?: "false").toBoolean()

    val dataSource: DataSource =
        PGSimpleDataSource().apply {
            setURL(jdbcUrl)
            user = dbUser
            password = dbPassword
        }

    // Default config: autoMigrate handles racing boots via the store's own advisory lock,
    // so every worker process can start cold without a separate migration step.
    val store = PostgresJobStore(dataSource)

    val registry = JobRegistry()
    registry.handle(SLOW_JOB) { _ ->
        recordEvidence(dataSource, jobId.value, nodeId, attempt, "started")
        // Blocking sleep on purpose: with no suspension point between start and the
        // completion write, the handler's side effects always run to the end, even when
        // the runtime's local lease timeout expired while the process sat in SIGSTOP.
        // That makes the zombie scenario deterministic: the stale completion is always
        // WRITTEN, and only the fenced store transition is rejected.
        Thread.sleep(holdMillis)
        recordEvidence(dataSource, jobId.value, nodeId, attempt, "completed")
    }
    registry.handle(TICK_JOB) { _ ->
        recordEvidence(dataSource, jobId.value, nodeId, attempt, "started")
        recordEvidence(dataSource, jobId.value, nodeId, attempt, "completed")
    }

    val settings =
        KlokkaSettings(
            workerId = WorkerId(nodeId),
            // The runtime's per-job timeout equals the initial lease window, so every
            // hold time used by the suite must fit inside this.
            lease = 8.seconds,
            heartbeatInterval = 1.seconds,
            pollInterval = 500.milliseconds,
            claimBatch = 4,
            // IO dispatcher: the hold is a blocking sleep and must not starve Default.
            queues = listOf(QueueConfig(QueueName.DEFAULT, concurrency = 4, dispatcher = Dispatchers.IO)),
        )

    val runtime = KlokkaRuntime(store, registry, settings)
    if (recurring) {
        runtime.recurring(
            id = "multinode.tick",
            type = TICK_JOB,
            payload = "tick",
            schedule = every(2.seconds),
        )
    }

    runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        runtime.start(scope)
        println("READY")
        System.out.flush()
        awaitCancellation()
    }
}

private fun requireEnv(name: String): String =
    checkNotNull(System.getenv(name)) { "Missing required environment variable $name" }

private fun recordEvidence(dataSource: DataSource, jobId: String, worker: String, attempt: Int, phase: String) {
    dataSource.connection.use { connection ->
        connection.prepareStatement(
            "INSERT INTO multinode_executions (job_id, worker, attempt, phase) VALUES (?, ?, ?, ?)",
        ).use { statement ->
            statement.setString(1, jobId)
            statement.setString(2, worker)
            statement.setInt(3, attempt)
            statement.setString(4, phase)
            statement.executeUpdate()
        }
    }
}
