package com.eventslooped.klokka

import kotlin.math.pow
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.seconds

/**
 * Marker for exceptions that must never be retried. The runtime short-circuits to
 * dead-letter BEFORE consulting any [RetryPolicy], so custom policies do not need
 * to check for this type themselves.
 */
public interface NonRetryable

/**
 * Decides the delay before the next attempt, or null to give up (dead-letter).
 *
 * [attempt] is the 1-based number of the attempt that just failed: after the first
 * failure the policy is called with attempt = 1.
 *
 * Deserialization failures never reach a retry policy: a payload that cannot be
 * decoded is dead-lettered immediately, because retrying it is pure waste.
 */
public fun interface RetryPolicy {
    public fun nextDelay(attempt: Int, error: Throwable): Duration?

    public companion object {
        /** No retries: every failure dead-letters immediately. */
        public val None: RetryPolicy = RetryPolicy { _, _ -> null }

        /**
         * Exponential backoff with multiplicative jitter.
         * Delay for attempt n is `base * factor^(n-1)`, capped at [max], then scaled by a
         * random factor in `[1 - jitter, 1 + jitter]`. Gives up after [maxAttempts] attempts.
         */
        public fun exponential(
            base: Duration = 10.seconds,
            factor: Double = 2.0,
            maxAttempts: Int = 8,
            jitter: Double = 0.2,
            max: Duration = 1.hours,
            random: Random = Random.Default,
        ): RetryPolicy {
            require(maxAttempts >= 1) { "maxAttempts must be at least 1" }
            require(jitter in 0.0..1.0) { "jitter must be within 0.0..1.0" }
            require(factor >= 1.0) { "factor must be at least 1.0" }
            return RetryPolicy { attempt, _ ->
                if (attempt >= maxAttempts) {
                    null
                } else {
                    val raw = base * factor.pow(attempt - 1)
                    val capped = if (raw > max) max else raw
                    val scale = 1.0 + jitter * (random.nextDouble() * 2.0 - 1.0)
                    capped * scale
                }
            }
        }

        /**
         * Explicit interval list: attempt n waits `delays[n - 1]`, and the job dead-letters
         * once the list is exhausted. `intervals(listOf(30.seconds, 2.minutes, 10.minutes))`
         * means up to 4 attempts total. Takes a List because Kotlin prohibits vararg
         * parameters of value-class types such as Duration.
         */
        public fun intervals(delays: List<Duration>): RetryPolicy {
            require(delays.isNotEmpty()) { "provide at least one delay, or use RetryPolicy.None" }
            val list = delays.toList()
            return RetryPolicy { attempt, _ -> list.getOrNull(attempt - 1) }
        }
    }
}

/**
 * What to do when a recurring schedule missed one or more fire times (downtime, saturation),
 * beyond the configured misfire threshold.
 */
public sealed interface MisfirePolicy {
    /** Skip missed fires entirely. Correct for jobs that must never double-fire (billing). */
    public data object Skip : MisfirePolicy

    /** Run once immediately to catch up, then resume the normal cadence (metric rollups). */
    public data object FireOnce : MisfirePolicy

    /** Replay up to [atMost] missed fires, oldest first, then resume. Bounded backfill. */
    public data class CatchUp(val atMost: Int) : MisfirePolicy {
        init {
            require(atMost >= 1) { "atMost must be at least 1" }
        }
    }
}

/**
 * Retention for terminal job runs. Cleanup executes as a built-in recurring job on
 * Klokka's own machinery. Dead-lettered runs default to being kept forever (null):
 * they are deleted only when an operator or an explicit policy says so.
 */
public data class RetentionPolicy(
    val succeededFor: Duration = 24.hours,
    val cancelledFor: Duration = 24.hours,
    val deadLetteredFor: Duration? = null,
)
