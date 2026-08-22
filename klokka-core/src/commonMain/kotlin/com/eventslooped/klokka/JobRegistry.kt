package com.eventslooped.klokka

/**
 * Maps a job's stored kind string back to its [JobType] and [JobHandler]. Registration
 * happens once at startup; lookups happen on every execution.
 *
 * [kinds] is what the runtime hands to [com.eventslooped.klokka.spi.JobStore.claim], so a
 * worker only ever claims jobs it can run. A kind bound nowhere waits in Enqueued until a
 * worker that binds it appears (a newer version mid-rollout, for example) rather than being
 * dead-lettered by a worker that cannot run it. A claimed job whose kind is nevertheless
 * unbound here means the store broke that contract; the runtime dead-letters it rather
 * than requeue it, which could loop forever.
 */
public class JobRegistry {
    private val registrations = mutableMapOf<String, Registration<*>>()

    /**
     * Binds [handler] to [type]. [retry] overrides the runtime default for this kind; it lives
     * here, per kind and code-defined, so every node resolves the same policy (retry policies
     * are functions and cannot be persisted per enqueue). Binding the same kind twice is an
     * error.
     */
    public fun <T> handle(type: JobType<T>, handler: JobHandler<T>, retry: RetryPolicy? = null) {
        require(registrations.put(type.kind, Registration(type, handler, retry)) == null) {
            "Duplicate registration for job kind '${type.kind}'"
        }
    }

    /**
     * Binds a suspend lambda to [type]. The lambda is wrapped in a [JobHandler], so it has the
     * same semantics as the object form and can be swapped for a class later without touching
     * any enqueue site. [retry] as in the object form.
     */
    public fun <T> handle(type: JobType<T>, retry: RetryPolicy? = null, block: suspend JobContext.(T) -> Unit) {
        handle(
            type,
            object : JobHandler<T> {
                override suspend fun JobContext.execute(payload: T) = block(payload)
            },
            retry,
        )
    }

    /** The bound kind strings. */
    public fun kinds(): Set<String> = registrations.keys.toSet()

    /** Every bound [JobType], in binding order. For startup checks and tooling; kinds are unique. */
    public fun types(): List<JobType<*>> = registrations.values.map { it.type }

    internal fun lookup(kind: String): Registration<*>? = registrations[kind]

    internal class Registration<T>(
        val type: JobType<T>,
        val handler: JobHandler<T>,
        val retry: RetryPolicy?,
    ) {
        /** Decodes and executes in one place so the generic T never escapes. */
        suspend fun execute(context: JobContext, codec: PayloadCodec, payload: String) {
            val decoded = codec.decode(type.serializer, payload)
            with(handler) { context.execute(decoded) }
        }
    }
}
