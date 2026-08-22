@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.ktor

import com.eventslooped.klokka.JobContext
import com.eventslooped.klokka.JobHandler
import com.eventslooped.klokka.JobRegistry
import com.eventslooped.klokka.JobType
import com.eventslooped.klokka.JsonPayloadCodec
import com.eventslooped.klokka.PayloadCodec
import com.eventslooped.klokka.QueueName
import com.eventslooped.klokka.RetentionPolicy
import com.eventslooped.klokka.RetryPolicy
import com.eventslooped.klokka.WorkerId
import com.eventslooped.klokka.runtime.KlokkaRole
import com.eventslooped.klokka.runtime.KlokkaRuntime
import com.eventslooped.klokka.runtime.KlokkaSettings
import com.eventslooped.klokka.runtime.QueueConfig
import com.eventslooped.klokka.spi.JobStore
import com.eventslooped.klokka.store.InMemoryJobStore
import kotlinx.coroutines.CoroutineDispatcher
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime

/**
 * Configuration receiver for `install(Klokka) { ... }`. A fresh instance is created per
 * installation. Every property mirrors a [KlokkaSettings] field and carries the same
 * default; [queue] declarations and handler bindings ([handle]) are
 * collected here and applied when the plugin builds the [KlokkaRuntime].
 */
public class KlokkaConfig {
    /** Backing job storage. Defaults to a non-persistent in-memory store, see the plugin's startup warning. */
    public var store: JobStore = InMemoryJobStore()

    /** Which parts of the runtime this process runs. */
    public var role: KlokkaRole = KlokkaRole.Both

    /** Retry policy used when a handler's registration does not specify its own. */
    public var defaultRetry: RetryPolicy = RetryPolicy.exponential()

    /** Payload serialization strategy. */
    public var codec: PayloadCodec = JsonPayloadCodec()

    /** Clock used for scheduling and event timestamps. */
    public var clock: Clock = Clock.System

    /** How long a claimed job's lease lasts before it is considered abandoned. */
    public var lease: Duration = 5.minutes

    /** How often in-flight jobs' leases are renewed. */
    public var heartbeatInterval: Duration = 1.minutes

    /** Fallback interval between claim rounds when no push wakeup arrives. */
    public var pollInterval: Duration = 10.seconds

    /** Maximum jobs claimed per queue per claim round. */
    public var claimBatch: Int = 16

    /** Retention for terminal job runs. */
    public var retention: RetentionPolicy = RetentionPolicy()

    /** How often the retention sweep runs. */
    public var sweepInterval: Duration = 1.hours

    /** How long [KlokkaRuntime.drain] waits for in-flight jobs before cancelling stragglers. */
    public var drainTimeout: Duration = 25.seconds

    /** A start later than scheduledFor by more than this marks the run late in events. */
    public var lateThreshold: Duration = 1.minutes

    /**
     * Identity this process claims and heartbeats leases under. Defaults to a random
     * `worker-<hex>` id drawn once per installation. Set a stable value (hostname, pod name)
     * when lease ownership should be attributable to the same node across restarts.
     */
    public var workerId: WorkerId = WorkerId.random()

    internal val registry: JobRegistry = JobRegistry()
    private val queueConfigs = mutableListOf<QueueConfig>()

    /**
     * Declares a queue this process drains. Declaration order is drain priority: queues
     * declared earlier are claimed first, mirroring [KlokkaSettings.queues]. If no queue is
     * declared, the runtime falls back to a single queue named [QueueName.DEFAULT].
     */
    public fun queue(name: QueueName, concurrency: Int = 8, dispatcher: CoroutineDispatcher? = null) {
        queueConfigs.add(QueueConfig(name, concurrency, dispatcher))
    }

    /** [queue] with the name as a plain string. */
    public fun queue(name: String, concurrency: Int = 8, dispatcher: CoroutineDispatcher? = null) {
        queue(QueueName(name), concurrency, dispatcher)
    }

    /**
     * Kinds bound with [handle] whose [JobType.queue] this process does not drain, grouped by
     * that queue. Non-empty means those jobs are never claimed here unless an enqueue call
     * overrides the queue. The plugin warns about it at startup for [KlokkaRole.Both], where
     * the same process enqueues and drains and the mismatch is almost always a mistake; a
     * [KlokkaRole.Worker] fleet may bind every handler and drain a deliberate subset.
     */
    internal fun boundKindsOnUndrainedQueues(): Map<QueueName, List<String>> {
        val drained = queues().map { it.name }.toSet()
        return registry.types()
            .filter { it.queue !in drained }
            .groupBy({ it.queue }, { it.kind })
    }

    /**
     * Binds a [JobHandler] object to [type]: the form for handlers with dependencies or their
     * own unit tests. [retry] overrides [defaultRetry] for this kind.
     */
    public fun <T> handle(type: JobType<T>, handler: JobHandler<T>, retry: RetryPolicy? = null) {
        registry.handle(type, handler, retry)
    }

    /** Binds a suspend lambda to [type]. Same semantics as the object form; [retry] overrides [defaultRetry] for this kind. */
    public fun <T> handle(type: JobType<T>, retry: RetryPolicy? = null, block: suspend JobContext.(T) -> Unit) {
        registry.handle(type, retry, block)
    }

    /** The declared queues in declaration order, or a single default queue if none were declared. */
    internal fun queues(): List<QueueConfig> = queueConfigs.ifEmpty { listOf(QueueConfig(QueueName.DEFAULT)) }
}
