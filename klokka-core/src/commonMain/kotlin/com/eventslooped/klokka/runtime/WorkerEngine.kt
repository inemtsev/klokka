@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.runtime

import com.eventslooped.klokka.JobContext
import com.eventslooped.klokka.JobEvent
import com.eventslooped.klokka.JobId
import com.eventslooped.klokka.JobRegistry
import com.eventslooped.klokka.JobState
import com.eventslooped.klokka.NonRetryable
import com.eventslooped.klokka.QueueName
import com.eventslooped.klokka.WorkerId
import com.eventslooped.klokka.spi.ClaimedJob
import com.eventslooped.klokka.spi.JobStore
import com.eventslooped.klokka.spi.PushCapableStore
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime
import kotlin.time.Instant
import kotlin.time.TimeSource

/** Initial delay before the first restart of a crashed supervisor loop. */
private val RESTART_INITIAL_BACKOFF = 100.milliseconds

/** Ceiling for the restart backoff of a supervisor loop. */
private val RESTART_MAX_BACKOFF = 10.seconds

/** A supervisor loop that ran cleanly for at least this long resets its backoff to the initial value. */
private val RESTART_CLEAN_RUN_THRESHOLD = 60.seconds

/** Poll interval while [WorkerEngine.drain] waits for the in-flight set to empty. */
private val DRAIN_POLL_INTERVAL = 10.milliseconds

/** Cap on how long [WorkerEngine.drain] waits for cancelled in-flight jobs to finish requeueing. */
private val REQUEUE_GRACE_PERIOD = 5.seconds

/**
 * The claim/execute/heartbeat/sweep machinery behind [KlokkaRuntime.start]. Constructed once per
 * [KlokkaRuntime.start] call and discarded on [drain]; never restarted.
 *
 * Everything here is private/internal machinery, not part of the binding-portability surface:
 * [store] is used strictly through the public [JobStore] SPI.
 */
internal class WorkerEngine(
    private val store: JobStore,
    private val registry: JobRegistry,
    private val settings: KlokkaSettings,
    private val scope: CoroutineScope,
    private val emit: (JobEvent) -> Unit,
) {
    private val queueExecutors: List<QueueExecutor> = settings.queues.map { QueueExecutor(it, scope) }

    /** Coalesced "a claim round may be worth running" signal. Multiple producers, one consumer. */
    private val claimSignal = Channel<Unit>(Channel.CONFLATED)

    private val drainingFlow = MutableStateFlow(false)

    private val inFlightMutex = Mutex()
    private val inFlightIds = mutableSetOf<JobId>()

    /** Launches the claim loop, heartbeater and sweeper, each under its own restart supervisor. */
    fun start() {
        scope.launchSupervised { runClaimLoop() }
        scope.launchSupervised { runHeartbeater() }
        scope.launchSupervised { runSweeper() }
    }

    /** Graceful shutdown; see [KlokkaRuntime.drain] for the contract. */
    suspend fun drain(drainTimeout: Duration) {
        drainingFlow.value = true
        withTimeoutOrNull(drainTimeout) {
            while (inFlightMutex.withLock { inFlightIds.isNotEmpty() }) {
                delay(DRAIN_POLL_INTERVAL)
            }
        }
        scope.cancel()
        // In-flight handlers hit the CancellationException branch of executeJob and requeue
        // themselves under NonCancellable; give that a short grace period to actually finish.
        withTimeoutOrNull(REQUEUE_GRACE_PERIOD) {
            scope.coroutineContext[Job]?.join()
        }
    }

    // ---- claim loop -----------------------------------------------------------------------

    private suspend fun runClaimLoop(): Unit =
        coroutineScope {
            launch {
                while (true) {
                    delay(settings.pollInterval)
                    claimSignal.trySend(Unit)
                }
            }
            val pushCapable = store as? PushCapableStore
            if (pushCapable != null) {
                launch {
                    pushCapable.wakeups().collect { claimSignal.trySend(Unit) }
                }
            }
            // Kick an immediate round so work already due when the loop starts (or the loop's
            // own restart after a crash) does not wait for the first tick or a wakeup that fired
            // before this subscriber existed.
            claimSignal.trySend(Unit)

            while (true) {
                claimSignal.receive()
                if (!drainingFlow.value) {
                    claimRound()
                }
            }
        }

    private suspend fun claimRound() {
        for (executor in queueExecutors) {
            if (drainingFlow.value) return
            val free = executor.freePermits()
            if (free <= 0) continue
            val limit = minOf(free, settings.claimBatch)
            // Only kinds bound here: a job this worker cannot run is left for one that can
            // (a newer version mid-rollout, or a fleet draining a shared queue for a subset).
            val claimed = store.claim(listOf(executor.queue), registry.kinds(), limit, settings.lease, settings.workerId)
            for (job in claimed) {
                inFlightMutex.withLock { inFlightIds.add(job.id) }
                executor.dispatch(job, ::executeJob)
            }
        }
    }

    private fun nudgeClaimLoop() {
        claimSignal.trySend(Unit)
    }

    // ---- heartbeater ------------------------------------------------------------------------

    private suspend fun runHeartbeater() {
        while (true) {
            delay(settings.heartbeatInterval)
            val ids = inFlightMutex.withLock { inFlightIds.toList() }
            if (ids.isNotEmpty()) {
                store.heartbeat(ids, settings.workerId, settings.lease)
            }
        }
    }

    // ---- sweeper ------------------------------------------------------------------------------

    private suspend fun runSweeper() {
        store.sweep(settings.retention)
        while (true) {
            delay(settings.sweepInterval)
            store.sweep(settings.retention)
        }
    }

    // ---- execution sequence -------------------------------------------------------------------

    private suspend fun executeJob(job: ClaimedJob) {
        try {
            val registration = registry.lookup(job.kind)
            if (registration == null) {
                // Unreachable with a conforming store: claim() is filtered by registry.kinds().
                // Dead-letter loudly rather than requeue, which could loop forever.
                deadLetter(
                    job,
                    IllegalStateException(
                        "store returned kind '${job.kind}', which this worker does not bind (JobStore.claim contract violation)",
                    ),
                )
                return
            }

            val context = RuntimeJobContext(job, store, settings.workerId)
            val startedAt = settings.clock.now()
            val queueWait = (startedAt - job.scheduledFor).coerceAtLeast(Duration.ZERO)
            val late = queueWait > settings.lateThreshold
            emit(JobEvent.Started(jobId = job.id, kind = job.kind, at = startedAt, attempt = job.attempt, queueWait = queueWait))

            try {
                withTimeout(job.leaseUntil - settings.clock.now()) {
                    registration.execute(context, settings.codec, job.payload)
                }
                onSuccess(job, startedAt, late)
            } catch (e: CancellationException) {
                if (e is TimeoutCancellationException) {
                    fail(job, registration, e)
                } else {
                    // Not our own timeout: the scope is being cancelled (drain/shutdown).
                    // Requeue must go through even though this coroutine is cancelled.
                    withContext(NonCancellable) {
                        store.transition(job.id, JobState.Running, JobState.Enqueued, job.fence)
                    }
                    throw e
                }
            } catch (e: Throwable) {
                fail(job, registration, e)
            }
        } finally {
            inFlightMutex.withLock { inFlightIds.remove(job.id) }
            nudgeClaimLoop()
        }
    }

    private suspend fun onSuccess(job: ClaimedJob, startedAt: Instant, late: Boolean) {
        val runDuration = (settings.clock.now() - startedAt).coerceAtLeast(Duration.ZERO)
        if (store.transition(job.id, JobState.Running, JobState.Succeeded, job.fence)) {
            emit(
                JobEvent.Succeeded(
                    jobId = job.id,
                    kind = job.kind,
                    at = settings.clock.now(),
                    attempt = job.attempt,
                    runDuration = runDuration,
                    late = late,
                ),
            )
        }
    }

    private suspend fun fail(job: ClaimedJob, registration: JobRegistry.Registration<*>, error: Throwable) {
        if (error is NonRetryable) {
            deadLetter(job, error)
            return
        }
        val policy = registration.retry ?: settings.defaultRetry
        val delay = policy.nextDelay(job.attempt, error)
        if (delay == null) {
            deadLetter(job, error)
            return
        }
        val retryAt = settings.clock.now() + delay
        if (store.transition(job.id, JobState.Running, JobState.Failed(retryAt), job.fence)) {
            emit(
                JobEvent.FailedAttempt(
                    jobId = job.id,
                    kind = job.kind,
                    at = settings.clock.now(),
                    attempt = job.attempt,
                    error = error,
                    retryAt = retryAt,
                ),
            )
        }
    }

    private suspend fun deadLetter(job: ClaimedJob, error: Throwable) {
        if (store.transition(job.id, JobState.Running, JobState.DeadLettered, job.fence)) {
            emit(
                JobEvent.DeadLettered(
                    jobId = job.id,
                    kind = job.kind,
                    at = settings.clock.now(),
                    attempts = job.attempt,
                    error = error,
                ),
            )
        }
    }

    /**
     * Runs [block] forever, restarting it after a non-[CancellationException] failure with
     * exponential backoff (100ms doubling to a 10s cap). Backoff resets to the initial value once
     * a run has stayed up for [RESTART_CLEAN_RUN_THRESHOLD].
     */
    private fun CoroutineScope.launchSupervised(block: suspend () -> Unit): Job =
        launch {
            var backoff = RESTART_INITIAL_BACKOFF
            while (true) {
                val startedAt = TimeSource.Monotonic.markNow()
                try {
                    block()
                    // Supervised loops are infinite; a normal return is unexpected.
                    // Pace the relaunch so a future non-infinite block cannot hot-spin.
                    delay(backoff)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Throwable) {
                    if (startedAt.elapsedNow() >= RESTART_CLEAN_RUN_THRESHOLD) {
                        backoff = RESTART_INITIAL_BACKOFF
                    }
                    delay(backoff)
                    backoff = (backoff * 2).coerceAtMost(RESTART_MAX_BACKOFF)
                }
            }
        }
}

/** One queue's concurrency gate. Claims are handed here and run as launched children of [scope]. */
private class QueueExecutor(private val config: QueueConfig, private val scope: CoroutineScope) {
    val queue: QueueName get() = config.name

    private val semaphore = Semaphore(config.concurrency)

    fun freePermits(): Int = semaphore.availablePermits

    /**
     * Hands [job] off to a launched child. The caller must only invoke this after observing a
     * free permit via [freePermits]; claiming is single-consumer and sequential, so the
     * synchronous [Semaphore.tryAcquire] below cannot lose a race against another claimer.
     */
    fun dispatch(job: ClaimedJob, body: suspend (ClaimedJob) -> Unit) {
        check(semaphore.tryAcquire()) { "queue '${config.name.value}' had no free permit at dispatch time" }
        scope.launch {
            try {
                val dispatcher = config.dispatcher
                if (dispatcher != null) {
                    withContext(dispatcher) { body(job) }
                } else {
                    body(job)
                }
            } finally {
                semaphore.release()
            }
        }
    }
}

/** [JobContext] backed by a single [ClaimedJob]. One instance per execution attempt. */
private class RuntimeJobContext(
    private val job: ClaimedJob,
    private val store: JobStore,
    private val workerId: WorkerId,
) : JobContext {
    override val jobId: JobId = job.id
    override val kind: String = job.kind
    override val queue: QueueName = job.queue
    override val attempt: Int = job.attempt
    override val enqueuedAt: Instant = job.enqueuedAt
    override val scheduledFor: Instant = job.scheduledFor

    override suspend fun progress(fraction: Double) {
        // No sink yet: dashboard wiring lands in issue #15.
    }

    override suspend fun extendLease(by: Duration) {
        store.heartbeat(listOf(job.id), workerId, by)
    }
}
