@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka

import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

/**
 * A [Clock] test double whose [now] only advances when told to. Lets tests drive
 * due-time, lease-expiry, and retention checks deterministically, without any
 * coupling to a coroutine test dispatcher's virtual time.
 */
public class TestClock(start: Instant) : Clock {
    private var current: Instant = start

    override fun now(): Instant = current

    /** Moves [now] forward by [duration]. */
    public fun advanceBy(duration: Duration) {
        current += duration
    }
}
