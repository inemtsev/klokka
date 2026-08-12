package com.eventslooped.klokka

import kotlin.math.pow
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

public class RetryPolicyTest {
    private val error = RuntimeException("boom")

    @Test
    public fun exponentialReturnsNullAtAndAfterMaxAttempts() {
        val policy = RetryPolicy.exponential(maxAttempts = 3)

        assertNotNull(policy.nextDelay(1, error))
        assertNotNull(policy.nextDelay(2, error))
        assertNull(policy.nextDelay(3, error))
        assertNull(policy.nextDelay(4, error))
    }

    @Test
    public fun exponentialGrowsWithAttemptAndStaysWithinJitterBounds() {
        val base = 1.seconds
        val factor = 2.0
        val jitter = 0.2
        val policy =
            RetryPolicy.exponential(
                base = base,
                factor = factor,
                maxAttempts = 6,
                jitter = jitter,
                max = 1.hours,
                random = Random(42),
            )

        var previous = Duration.ZERO
        for (attempt in 1..5) {
            val delay = policy.nextDelay(attempt, error)
            assertNotNull(delay, "expected a delay for attempt $attempt")

            val nominal = base * factor.pow(attempt - 1)
            val lowerBound = nominal * (1.0 - jitter)
            val upperBound = nominal * (1.0 + jitter)
            assertTrue(
                delay in lowerBound..upperBound,
                "attempt=$attempt delay=$delay expected within [$lowerBound, $upperBound]",
            )
            assertTrue(delay > previous, "attempt=$attempt delay=$delay expected greater than previous=$previous")
            previous = delay
        }
    }

    @Test
    public fun exponentialRespectsMaxCap() {
        val max = 1.minutes
        val jitter = 0.1
        val policy =
            RetryPolicy.exponential(
                base = 10.seconds,
                factor = 2.0,
                maxAttempts = 20,
                jitter = jitter,
                max = max,
                random = Random(7),
            )

        val delay = policy.nextDelay(10, error)
        assertNotNull(delay)
        assertTrue(delay <= max * (1.0 + jitter))
        assertTrue(delay >= max * (1.0 - jitter))
    }

    @Test
    public fun intervalsReturnsEachListedDelayInOrderThenNull() {
        val delays = listOf(30.seconds, 2.minutes, 10.minutes)
        val policy = RetryPolicy.intervals(delays)

        assertEquals(30.seconds, policy.nextDelay(1, error))
        assertEquals(2.minutes, policy.nextDelay(2, error))
        assertEquals(10.minutes, policy.nextDelay(3, error))
        assertNull(policy.nextDelay(4, error))
    }

    @Test
    public fun noneAlwaysReturnsNull() {
        assertNull(RetryPolicy.None.nextDelay(1, error))
        assertNull(RetryPolicy.None.nextDelay(100, error))
    }
}
