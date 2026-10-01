@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka

import kotlin.time.ExperimentalTime
import kotlin.time.Instant

/**
 * The most recent failure recorded against a job: what a dead-letter is triaged from and
 * what a Failed job is waiting on. Written by the runtime on every transition to
 * [JobState.Failed] and [JobState.DeadLettered] through
 * [com.eventslooped.klokka.spi.JobStore.transition], and read back through
 * [com.eventslooped.klokka.spi.JobDetails.lastError].
 *
 * This is the last error only. Each failed attempt replaces the previous record; a full
 * attempt history and correlation or trace ids are additive later work, not part of it.
 */
public data class JobError(
    /** The exception's qualified class name, or its simple name where the platform has none. */
    val type: String,
    val message: String?,
    /**
     * The platform's rendering of the exception and its cause chain, cut at
     * [MAX_STACK_TRACE_CHARS] so one failure cannot bloat a row without bound.
     */
    val stackTrace: String,
    /** The 1-based attempt that produced this error. */
    val attempt: Int,
    /**
     * When the runtime recorded the error, by the runtime's clock. Informational only:
     * stores never compare it, so the SPI rule against caller-supplied time is untouched.
     */
    val at: Instant,
) {
    public companion object {
        /** Upper bound on [stackTrace] length, in characters. */
        public const val MAX_STACK_TRACE_CHARS: Int = 16_384

        /** Renders [error] as the record for [attempt], recorded [at]. */
        public fun of(error: Throwable, attempt: Int, at: Instant): JobError =
            JobError(
                type = error::class.qualifiedName ?: error::class.simpleName ?: "Throwable",
                message = error.message,
                stackTrace = error.stackTraceToString().take(MAX_STACK_TRACE_CHARS),
                attempt = attempt,
                at = at,
            )
    }
}
