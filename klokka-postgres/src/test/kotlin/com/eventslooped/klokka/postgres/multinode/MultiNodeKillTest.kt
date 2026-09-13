@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.postgres.multinode

import com.eventslooped.klokka.jobType
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime

private val SLOW = jobType<String>("multinode.slow")

/**
 * The M1 exit criterion, executed literally: real worker JVMs against one Postgres,
 * killed with SIGKILL mid-job (no drain, no finally blocks, no requeue courtesy), and
 * one stopped with SIGSTOP to play the zombie. Every assertion reads the database,
 * because the database is the only witness a dead process leaves.
 */
internal class MultiNodeKillTest {
    @Test
    fun killedMidJobEverythingIsRevivedAndNothingIsLost() =
        runBlocking {
            MultiNodeHarness.reset()
            val producer = MultiNodeHarness.producer()
            val a = MultiNodeHarness.startWorker("node-a", holdMillis = 6000)
            var b: WorkerHandle? = null
            try {
                val ids = (1..20).map { producer.enqueue(SLOW, "job-$it").value }

                // Kill only once node-a is provably holding jobs mid-execution.
                MultiNodeHarness.awaitUntil("node-a has at least 3 executions in flight") {
                    MultiNodeHarness.executions(phase = "started", worker = "node-a").size >= 3
                }
                val interruptedCandidates =
                    MultiNodeHarness.executions(phase = "started", worker = "node-a").map { it.jobId } -
                        MultiNodeHarness.executions(phase = "completed", worker = "node-a").map { it.jobId }.toSet()
                a.kill()
                assertTrue(interruptedCandidates.isNotEmpty(), "the kill must interrupt at least one job")
                assertTrue(ids.containsAll(interruptedCandidates))

                b = MultiNodeHarness.startWorker("node-b", holdMillis = 100)
                MultiNodeHarness.awaitUntil("all 20 jobs Succeeded after the kill", timeout = 90.seconds) {
                    MultiNodeHarness.succeededCount(SLOW.kind) == 20
                }

                // Zero lost jobs, and exactly one completion each: at-least-once execution,
                // exactly-once COMPLETION when the first runner died before finishing.
                val completions = MultiNodeHarness.completionsPerJob()
                assertEquals(20, completions.size)
                assertTrue(completions.values.all { it == 1 }, "double completions: $completions")

                // Revival went through the lease machinery: attempt and fence both bumped.
                for (jobId in interruptedCandidates) {
                    val (attempt, fence) = assertNotNull(MultiNodeHarness.jobAttemptAndFence(jobId))
                    assertTrue(attempt >= 2, "job $jobId was interrupted but shows attempt $attempt")
                    assertTrue(fence >= 2, "job $jobId was interrupted but shows fence $fence")
                }
            } finally {
                a.stop()
                b?.stop()
            }
        }

    @Test
    fun twoNodesDrainConcurrentlyAndSurviveOneDying() =
        runBlocking {
            MultiNodeHarness.reset()
            val producer = MultiNodeHarness.producer()
            val a = MultiNodeHarness.startWorker("node-a", holdMillis = 300)
            val b = MultiNodeHarness.startWorker("node-b", holdMillis = 300)
            try {
                (1..50).forEach { producer.enqueue(SLOW, "job-$it") }

                // Both nodes must be actively sharing the queue before the kill.
                MultiNodeHarness.awaitUntil("both nodes have executed work") {
                    MultiNodeHarness.executions(worker = "node-a").isNotEmpty() &&
                        MultiNodeHarness.executions(worker = "node-b").isNotEmpty()
                }
                a.kill()

                MultiNodeHarness.awaitUntil("all 50 jobs Succeeded with one node down", timeout = 90.seconds) {
                    MultiNodeHarness.succeededCount(SLOW.kind) == 50
                }
                val completions = MultiNodeHarness.completionsPerJob()
                assertEquals(50, completions.size)
                assertTrue(completions.values.all { it == 1 }, "double completions: $completions")
                assertTrue(
                    MultiNodeHarness.executions(phase = "completed", worker = "node-b").isNotEmpty(),
                    "the survivor must have completed part of the work",
                )
            } finally {
                a.stop()
                b.stop()
            }
        }

    @Test
    fun schedulesFireExactlyOncePerDueTimeAcrossNodesDespiteAKill() =
        runBlocking {
            MultiNodeHarness.reset()
            val a = MultiNodeHarness.startWorker("node-a", holdMillis = 100, recurring = true)
            val b = MultiNodeHarness.startWorker("node-b", holdMillis = 100, recurring = true)
            try {
                MultiNodeHarness.awaitUntil("the schedule has fired at least 3 times") {
                    MultiNodeHarness.scheduleRunCount("multinode.tick") >= 3
                }
                a.kill()
                val atKill = MultiNodeHarness.scheduleRunCount("multinode.tick")

                MultiNodeHarness.awaitUntil("the surviving node keeps the cadence going") {
                    MultiNodeHarness.scheduleRunCount("multinode.tick") >= atKill + 3
                }

                // The contract under test: one emitted run per fire time, ever, regardless
                // of which node claimed the fire or died holding its lease.
                assertEquals(
                    emptyList(),
                    MultiNodeHarness.duplicateFires("multinode.tick"),
                    "a fire time was emitted twice",
                )
            } finally {
                a.stop()
                b.stop()
            }
        }

    @Test
    fun aStoppedZombieCannotOverwriteTheRevivedRun() =
        runBlocking {
            MultiNodeHarness.reset()
            val producer = MultiNodeHarness.producer()
            // node-a holds briefly; the SIGSTOP freezes it inside that window.
            val a = MultiNodeHarness.startWorker("node-a", holdMillis = 3000)
            var b: WorkerHandle? = null
            try {
                val id = producer.enqueue(SLOW, "the-one").value
                MultiNodeHarness.awaitUntil("node-a is mid-job") {
                    MultiNodeHarness.executions(phase = "started", worker = "node-a").any { it.jobId == id }
                }
                a.sigstop()

                // node-b holds long (but under the worker lease), so there is a window
                // where the zombie has finished but the live runner has not: exactly when
                // an unfenced write would corrupt.
                b = MultiNodeHarness.startWorker("node-b", holdMillis = 6000)
                MultiNodeHarness.awaitUntil("node-b revived the job after lease expiry") {
                    MultiNodeHarness.executions(phase = "started", worker = "node-b").any { it.jobId == id }
                }
                val (attempt, fence) = assertNotNull(MultiNodeHarness.jobAttemptAndFence(id))
                assertEquals(2, attempt, "revival must be attempt 2")
                assertEquals(2L, fence, "revival must bump the fence")

                // Wake the zombie. Its handler finishes (that side effect is exactly the
                // documented at-least-once case), but its Succeeded write carries fence 1.
                a.sigcont()
                MultiNodeHarness.awaitUntil("the zombie finished its stale execution") {
                    MultiNodeHarness.executions(phase = "completed", worker = "node-a").any { it.jobId == id }
                }
                assertEquals(
                    "Running",
                    MultiNodeHarness.jobState(id),
                    "the zombie's completion must NOT have transitioned the row while node-b still runs it",
                )

                MultiNodeHarness.awaitUntil("node-b finishes the live run", timeout = 90.seconds) {
                    MultiNodeHarness.jobState(id) == "Succeeded"
                }
                // Two completions in the evidence: at-least-once, visible and honest.
                assertEquals(2, MultiNodeHarness.completionsPerJob()[id])
                val finalRow = assertNotNull(MultiNodeHarness.jobAttemptAndFence(id))
                assertEquals(2L, finalRow.second, "the surviving fence must be node-b's")
            } finally {
                runCatching { a.sigcont() }
                a.stop()
                b?.stop()
            }
        }
}
