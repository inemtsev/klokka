@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.postgres.multinode

import com.eventslooped.klokka.JobRegistry
import com.eventslooped.klokka.postgres.PostgresJobStore
import com.eventslooped.klokka.postgres.PostgresTestSupport
import com.eventslooped.klokka.runtime.KlokkaRole
import com.eventslooped.klokka.runtime.KlokkaRuntime
import com.eventslooped.klokka.runtime.KlokkaSettings
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.test.fail
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime

/** One evidence row written by a worker process as it starts or finishes a job run. */
internal data class Execution(val jobId: String, val worker: String, val attempt: Int, val phase: String)

/** A live handle on a worker child process, letting a test kill, freeze, or resume it. */
internal class WorkerHandle(val nodeId: String, private val process: Process) {
    val pid: Long = process.pid()

    /** SIGKILL, then waits for the OS to reap it: the "no drain, no finally blocks" kill. */
    fun kill() {
        process.destroyForcibly()
        process.waitFor()
    }

    /** Freezes the process (SIGSTOP) to play the zombie: alive but not scheduled. */
    fun sigstop() = signal("STOP")

    /** Resumes a SIGSTOPped process (SIGCONT). */
    fun sigcont() = signal("CONT")

    /** Best-effort cleanup for a `finally` block: never throws, never blocks on a wait. */
    fun stop() {
        try {
            process.destroyForcibly()
            process.waitFor(5, TimeUnit.SECONDS)
        } catch (interrupted: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }

    fun isAlive(): Boolean = process.isAlive

    private fun signal(name: String) {
        val exit = ProcessBuilder("/bin/kill", "-$name", pid.toString()).start().waitFor()
        check(exit == 0) { "kill -$name $pid (worker $nodeId) exited with $exit" }
    }
}

/**
 * Drives the multi-node kill-test suite: starts real worker JVMs against the shared
 * [PostgresTestSupport] container, reads back their evidence trail, and gives tests a
 * delay-based wait primitive instead of sleeps. There is no injectable clock here, same as
 * [PostgresTestSupport]: every assertion reads the database, the only witness a killed
 * process leaves behind.
 */
internal object MultiNodeHarness {
    private const val EVIDENCE_TABLE_DDL =
        "CREATE TABLE IF NOT EXISTS multinode_executions (" +
            "id BIGSERIAL PRIMARY KEY, job_id TEXT NOT NULL, worker TEXT NOT NULL, " +
            "attempt INT NOT NULL, phase TEXT NOT NULL, at TIMESTAMPTZ NOT NULL DEFAULT now())"

    /** Truncates klokka's own tables via [PostgresTestSupport.freshStore] and the evidence table. */
    fun reset() {
        PostgresTestSupport.freshStore()
        PostgresTestSupport.dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(EVIDENCE_TABLE_DDL)
                statement.execute("TRUNCATE multinode_executions")
            }
        }
    }

    /** A producer-only runtime for enqueueing work from the test process itself. */
    fun producer(): KlokkaRuntime {
        val store = PostgresJobStore(PostgresTestSupport.dataSource)
        val settings = KlokkaSettings(role = KlokkaRole.Producer)
        return KlokkaRuntime(store, JobRegistry(), settings)
    }

    /** Launches a [WorkerProcess] child JVM and blocks until it reports readiness. */
    fun startWorker(nodeId: String, holdMillis: Long = 5000, recurring: Boolean = false): WorkerHandle {
        val javaBinary = System.getProperty("java.home") + "/bin/java"
        val builder =
            ProcessBuilder(
                javaBinary,
                "-cp",
                System.getProperty("java.class.path"),
                "com.eventslooped.klokka.postgres.multinode.WorkerProcessKt",
            )
        builder.environment().apply {
            put("KLOKKA_TEST_JDBC_URL", PostgresTestSupport.jdbcUrl)
            put("KLOKKA_TEST_DB_USER", PostgresTestSupport.username)
            put("KLOKKA_TEST_DB_PASSWORD", PostgresTestSupport.password)
            put("KLOKKA_TEST_NODE_ID", nodeId)
            put("KLOKKA_TEST_HOLD_MILLIS", holdMillis.toString())
            put("KLOKKA_TEST_RECURRING", recurring.toString())
        }
        builder.redirectErrorStream(true)
        val process = builder.start()

        val output = StringBuilder()
        val ready = ArrayBlockingQueue<Boolean>(1)
        val pump =
            Thread {
                process.inputStream.bufferedReader().forEachLine { line ->
                    synchronized(output) { output.appendLine(line) }
                    println("[$nodeId] $line")
                    if (line == "READY") ready.offer(true)
                }
            }
        pump.isDaemon = true
        pump.name = "klokka-worker-$nodeId-stdout"
        pump.start()

        val started = ready.poll(30, TimeUnit.SECONDS) ?: false
        if (!started) {
            val captured = synchronized(output) { output.toString() }
            fail("worker $nodeId did not print READY within 30s; captured output:\n$captured")
        }
        return WorkerHandle(nodeId, process)
    }

    /** Evidence rows, optionally filtered by [phase] and/or [worker], oldest first. */
    fun executions(phase: String? = null, worker: String? = null): List<Execution> {
        val conditions = mutableListOf<String>()
        val params = mutableListOf<String>()
        if (phase != null) {
            conditions += "phase = ?"
            params += phase
        }
        if (worker != null) {
            conditions += "worker = ?"
            params += worker
        }
        val where = if (conditions.isEmpty()) "" else " WHERE " + conditions.joinToString(" AND ")
        val sql = "SELECT job_id, worker, attempt, phase FROM multinode_executions$where ORDER BY id"
        return PostgresTestSupport.dataSource.connection.use { connection ->
            connection.prepareStatement(sql).use { statement ->
                params.forEachIndexed { index, value -> statement.setString(index + 1, value) }
                statement.executeQuery().use { rs ->
                    val result = mutableListOf<Execution>()
                    while (rs.next()) {
                        result +=
                            Execution(
                                jobId = rs.getString(1),
                                worker = rs.getString(2),
                                attempt = rs.getInt(3),
                                phase = rs.getString(4),
                            )
                    }
                    result
                }
            }
        }
    }

    /** job_id -> count of phase='completed' rows: at-least-once completions per job. */
    fun completionsPerJob(): Map<String, Int> =
        PostgresTestSupport.dataSource.connection.use { connection ->
            connection.prepareStatement(
                "SELECT job_id, count(*) FROM multinode_executions WHERE phase = 'completed' GROUP BY job_id",
            ).use { statement ->
                statement.executeQuery().use { rs ->
                    val result = mutableMapOf<String, Int>()
                    while (rs.next()) result[rs.getString(1)] = rs.getInt(2)
                    result
                }
            }
        }

    /** The `state` column of `klokka_jobs` for [jobId], or null when the id is unknown. */
    fun jobState(jobId: String): String? =
        PostgresTestSupport.dataSource.connection.use { connection ->
            connection.prepareStatement("SELECT state FROM klokka_jobs WHERE id = ?").use { statement ->
                statement.setLong(1, jobId.toLong())
                statement.executeQuery().use { rs -> if (rs.next()) rs.getString(1) else null }
            }
        }

    /** (attempt, fence) for [jobId], or null when the id is unknown. */
    fun jobAttemptAndFence(jobId: String): Pair<Int, Long>? =
        PostgresTestSupport.dataSource.connection.use { connection ->
            connection.prepareStatement("SELECT attempt, fence FROM klokka_jobs WHERE id = ?").use { statement ->
                statement.setLong(1, jobId.toLong())
                statement.executeQuery().use { rs ->
                    if (rs.next()) rs.getInt(1) to rs.getLong(2) else null
                }
            }
        }

    /** Count of `klokka_jobs` rows of [kind] in state Succeeded. */
    fun succeededCount(kind: String): Int =
        PostgresTestSupport.dataSource.connection.use { connection ->
            connection.prepareStatement(
                "SELECT count(*) FROM klokka_jobs WHERE kind = ? AND state = 'Succeeded'",
            ).use { statement ->
                statement.setString(1, kind)
                statement.executeQuery().use { rs ->
                    rs.next()
                    rs.getInt(1)
                }
            }
        }

    /** Count of `klokka_jobs` rows carrying [scheduleId]: total runs a schedule has emitted. */
    fun scheduleRunCount(scheduleId: String): Int =
        PostgresTestSupport.dataSource.connection.use { connection ->
            connection.prepareStatement("SELECT count(*) FROM klokka_jobs WHERE schedule_id = ?").use { statement ->
                statement.setString(1, scheduleId)
                statement.executeQuery().use { rs ->
                    rs.next()
                    rs.getInt(1)
                }
            }
        }

    /** (run_at, count) for any fire time of [scheduleId] emitted more than once: a contract violation. */
    fun duplicateFires(scheduleId: String): List<Pair<String, Int>> =
        PostgresTestSupport.dataSource.connection.use { connection ->
            connection.prepareStatement(
                "SELECT run_at::text, count(*) FROM klokka_jobs WHERE schedule_id = ? GROUP BY run_at HAVING count(*) > 1",
            ).use { statement ->
                statement.setString(1, scheduleId)
                statement.executeQuery().use { rs ->
                    val result = mutableListOf<Pair<String, Int>>()
                    while (rs.next()) result += rs.getString(1) to rs.getInt(2)
                    result
                }
            }
        }

    /**
     * Polls [predicate] every [poll] until it is true or [timeout] elapses. On timeout, fails
     * with [message] plus a dump of [executions] and per-state job counts, so a flake in CI
     * shows what the cluster was actually doing instead of just "timed out".
     */
    suspend fun awaitUntil(
        message: String,
        timeout: Duration = 60.seconds,
        poll: Duration = 250.milliseconds,
        predicate: () -> Boolean,
    ) {
        try {
            withTimeout(timeout) {
                while (!predicate()) {
                    delay(poll)
                }
            }
        } catch (timedOut: TimeoutCancellationException) {
            fail(
                "$message (timed out after $timeout)\n" +
                    "executions: ${executions()}\n" +
                    "job state counts: ${jobStateCounts()}",
            )
        }
    }

    private fun jobStateCounts(): Map<String, Int> =
        PostgresTestSupport.dataSource.connection.use { connection ->
            connection.prepareStatement("SELECT state, count(*) FROM klokka_jobs GROUP BY state").use { statement ->
                statement.executeQuery().use { rs ->
                    val result = mutableMapOf<String, Int>()
                    while (rs.next()) result[rs.getString(1)] = rs.getInt(2)
                    result
                }
            }
        }
}
