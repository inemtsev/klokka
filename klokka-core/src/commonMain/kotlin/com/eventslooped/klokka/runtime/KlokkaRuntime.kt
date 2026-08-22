@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.runtime

import com.eventslooped.klokka.JobEvent
import com.eventslooped.klokka.JobId
import com.eventslooped.klokka.JobOptions
import com.eventslooped.klokka.JobRegistry
import com.eventslooped.klokka.JobType
import com.eventslooped.klokka.JsonPayloadCodec
import com.eventslooped.klokka.PayloadCodec
import com.eventslooped.klokka.QueueName
import com.eventslooped.klokka.RetentionPolicy
import com.eventslooped.klokka.RetryPolicy
import com.eventslooped.klokka.WorkerId
import com.eventslooped.klokka.spi.JobStore
import com.eventslooped.klokka.spi.NewJob
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

/** Which parts of the runtime this process runs. The same code serves all three shapes. */
public enum class KlokkaRole { Producer, Worker, Both }

/**
 * Per-queue execution settings. Concurrency is a coroutine count, not a thread count:
 * an I/O-bound queue can comfortably run hundreds. [dispatcher] overrides where handlers
 * run; set it (for example to a bounded IO dispatcher) when handlers block, such as JDBC.
 */
public class QueueConfig(
    public val name: QueueName,
    public val concurrency: Int = 8,
    public val dispatcher: CoroutineDispatcher? = null,
) {
    init {
        require(concurrency >= 1) { "concurrency must be at least 1" }
    }
}

/**
 * Runtime tuning. Defaults are production-sane; tests inject a [clock] aligned with the
 * store's clock and shrink the intervals.
 *
 * [queues] order is drain priority (earlier queues are claimed first).
 */
public class KlokkaSettings(
    public val queues: List<QueueConfig> = listOf(QueueConfig(QueueName.DEFAULT)),
    public val role: KlokkaRole = KlokkaRole.Both,
    public val defaultRetry: RetryPolicy = RetryPolicy.exponential(),
    public val codec: PayloadCodec = JsonPayloadCodec(),
    public val clock: Clock = Clock.System,
    public val lease: Duration = 5.minutes,
    public val heartbeatInterval: Duration = 1.minutes,
    public val pollInterval: Duration = 10.seconds,
    public val claimBatch: Int = 16,
    public val retention: RetentionPolicy = RetentionPolicy(),
    public val sweepInterval: Duration = 1.hours,
    public val drainTimeout: Duration = 25.seconds,
    /** A start later than scheduledFor by more than this marks the run late in events. */
    public val lateThreshold: Duration = 1.minutes,
    public val workerId: WorkerId = WorkerId.random(),
) {
    init {
        require(queues.isNotEmpty()) { "configure at least one queue" }
        require(queues.map { it.name }.toSet().size == queues.size) { "duplicate queue names" }
    }
}

/**
 * The engine: claims work from the [store], executes registered handlers, orchestrates
 * retries through CAS transitions, and emits [JobEvent]s. Framework-free: klokka-ktor
 * wires this to the Ktor application lifecycle, but any JVM main function can run it.
 *
 * Lifecycle: construct, [start] inside a scope, [drain] on shutdown. Producer-role
 * instances skip [start] entirely or start with role = Producer (no workers launched).
 */
public class KlokkaRuntime(
    private val store: JobStore,
    private val registry: JobRegistry,
    private val settings: KlokkaSettings = KlokkaSettings(),
) {
    private val eventsFlow =
        MutableSharedFlow<JobEvent>(
            replay = 0,
            extraBufferCapacity = 256,
            onBufferOverflow = BufferOverflow.DROP_OLDEST,
        )

    /** Hot stream of lifecycle events. Dropped under extreme backpressure, never blocking. */
    public val events: SharedFlow<JobEvent> = eventsFlow.asSharedFlow()

    private var started = false
    private var engine: WorkerEngine? = null

    /**
     * Persists a job due immediately, on [JobOptions.queue] if set, otherwise on [JobType.queue].
     * Returns the job id (existing id on uniqueKey dedup).
     */
    public suspend fun <T> enqueue(type: JobType<T>, payload: T, options: JobOptions = JobOptions()): JobId =
        persist(type, payload, settings.clock.now(), options)

    /** Persists a job due at [at]; queue resolution as in [enqueue]. */
    public suspend fun <T> schedule(type: JobType<T>, payload: T, at: Instant, options: JobOptions = JobOptions()): JobId =
        persist(type, payload, at, options)

    private suspend fun <T> persist(type: JobType<T>, payload: T, at: Instant, options: JobOptions): JobId {
        val encoded = settings.codec.encode(type.serializer, payload)
        val queue = options.queue ?: type.queue
        val id =
            store.enqueue(
                listOf(
                    NewJob(
                        kind = type.kind,
                        payload = encoded,
                        queue = queue,
                        runAt = at,
                        uniqueKey = options.uniqueKey,
                    ),
                ),
            ).first()
        eventsFlow.tryEmit(
            JobEvent.Enqueued(jobId = id, kind = type.kind, at = settings.clock.now(), queue = queue, scheduledFor = at),
        )
        return id
    }

    /**
     * Launches the worker machinery as supervised children of [scope]: the claim loop,
     * per-queue executors, the heartbeater, and the retention sweeper. No-op for
     * [KlokkaRole.Producer]. Idempotent: calling twice is an error.
     */
    public fun start(scope: CoroutineScope) {
        check(!started) { "KlokkaRuntime.start() was already called" }
        started = true
        if (settings.role == KlokkaRole.Producer) return
        val internalScope = CoroutineScope(scope.coroutineContext + SupervisorJob(scope.coroutineContext[Job]))
        engine = WorkerEngine(store, registry, settings, internalScope, ::emit).also { it.start() }
    }

    /**
     * Graceful shutdown: stop claiming, wait up to [KlokkaSettings.drainTimeout] for
     * in-flight jobs, then cancel stragglers and requeue them (transition Running to
     * Enqueued with the fence). Safe to call without [start] (no-op) and at most once.
     */
    public suspend fun drain() {
        val current = engine ?: return
        engine = null
        current.drain(settings.drainTimeout)
    }

    internal fun emit(event: JobEvent) {
        eventsFlow.tryEmit(event)
    }
}
