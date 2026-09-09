@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka

import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.TimeZone
import kotlinx.datetime.toInstant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

private val UTC = TimeZone.UTC
private val NEW_YORK = TimeZone.of("America/New_York")

private fun at(year: Int, month: Int, day: Int, hour: Int, minute: Int, zone: TimeZone = UTC): Instant =
    LocalDateTime(year, month, day, hour, minute).toInstant(zone)

public class ScheduleTest {
    @Test
    public fun everyFiresOneIntervalAfterThePreviousFire() {
        val schedule = every(5.minutes)
        val t0 = at(2026, 9, 4, 12, 0)
        assertEquals(t0 + 5.minutes, schedule.nextAfter(t0))
        assertEquals(t0 + 10.minutes, schedule.nextAfter(schedule.nextAfter(t0)!!))
    }

    @Test
    public fun everyRejectsNonPositiveIntervals() {
        assertFailsWith<IllegalArgumentException> { every(0.minutes) }
        assertFailsWith<IllegalArgumentException> { every((-1).minutes) }
    }

    @Test
    public fun dailyAtFiresTodayWhenTheTimeIsStillAhead() {
        val schedule = dailyAt(9, 30, UTC)
        assertEquals(at(2026, 9, 4, 9, 30), schedule.nextAfter(at(2026, 9, 4, 6, 0)))
    }

    @Test
    public fun dailyAtFiresTomorrowWhenTheTimeHasPassed() {
        val schedule = dailyAt(9, 30, UTC)
        assertEquals(at(2026, 9, 5, 9, 30), schedule.nextAfter(at(2026, 9, 4, 9, 30)))
        assertEquals(at(2026, 9, 5, 9, 30), schedule.nextAfter(at(2026, 9, 4, 23, 59)))
    }

    @Test
    public fun dailyAtShiftsForwardThroughTheSpringDstGap() {
        // 2026-03-08 02:30 does not exist in America/New_York: clocks jump 02:00 EST -> 03:00 EDT.
        val schedule = dailyAt(2, 30, NEW_YORK)
        val fromMidnight = at(2026, 3, 8, 0, 0, NEW_YORK)
        val fire = schedule.nextAfter(fromMidnight)!!
        // kotlinx-datetime resolves the gap by shifting forward its length: 03:30 EDT.
        assertEquals(at(2026, 3, 8, 3, 30, NEW_YORK), fire)
        // The next day is back to a plain 02:30.
        assertEquals(at(2026, 3, 9, 2, 30, NEW_YORK), schedule.nextAfter(fire))
    }

    @Test
    public fun dailyAtFiresOnceAtTheEarlierOffsetInTheFallDstOverlap() {
        // 2026-11-01 01:30 exists twice in America/New_York (EDT then EST).
        val schedule = dailyAt(1, 30, NEW_YORK)
        val fire = schedule.nextAfter(at(2026, 11, 1, 0, 0, NEW_YORK))!!
        assertEquals(Instant.parse("2026-11-01T05:30:00Z"), fire, "expected the earlier (EDT) offset")
        // The second pass through 01:30 (06:30Z) is skipped: the next fire is the next day.
        val next = schedule.nextAfter(fire)!!
        assertEquals(Instant.parse("2026-11-02T06:30:00Z"), next)
        assertTrue(next > fire)
    }

    @Test
    public fun dailyAtValidatesItsArguments() {
        assertFailsWith<IllegalArgumentException> { dailyAt(24, 0, UTC) }
        assertFailsWith<IllegalArgumentException> { dailyAt(9, 60, UTC) }
    }

    @Test
    public fun descriptionsAreStableAndDistinguishDefinitions() {
        assertEquals(every(5.minutes).description, every(5.minutes).description)
        assertEquals("daily at 09:05 UTC", dailyAt(9, 5, UTC).description)
        assertTrue(every(5.minutes).description != every(6.minutes).description)
        assertTrue(dailyAt(9, 0, UTC).description != dailyAt(9, 0, NEW_YORK).description)
    }
}
