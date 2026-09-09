@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka

import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.TimeZone
import kotlinx.datetime.toInstant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

private val UTC = TimeZone.UTC
private val NEW_YORK = TimeZone.of("America/New_York")

private fun at(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int = 0, zone: TimeZone = UTC): Instant =
    LocalDateTime(year, month, day, hour, minute, second).toInstant(zone)

public class CronExpressionTest {
    // ---- parse errors -------------------------------------------------------------------

    @Test
    public fun rejectsTooFewFieldsWithoutSeconds() {
        val error = assertFailsWith<IllegalArgumentException> { CronExpression("* * * *") }
        assertTrue(error.message!!.contains("5 fields"), error.message.toString())
    }

    @Test
    public fun rejectsTooManyFieldsWithoutSeconds() {
        val error = assertFailsWith<IllegalArgumentException> { CronExpression("* * * * * *") }
        assertTrue(error.message!!.contains("5 fields"), error.message.toString())
    }

    @Test
    public fun rejectsFiveFieldsWhenSecondsAreRequested() {
        val error = assertFailsWith<IllegalArgumentException> { CronExpression("* * * * *", withSeconds = true) }
        assertTrue(error.message!!.contains("6 fields"), error.message.toString())
        assertTrue(error.message!!.contains("withSeconds=true"), error.message.toString())
    }

    @Test
    public fun rejectsOutOfRangeMinute() {
        val error = assertFailsWith<IllegalArgumentException> { CronExpression("60 * * * *") }
        assertTrue(error.message!!.contains("minute"), error.message.toString())
    }

    @Test
    public fun rejectsOutOfRangeHour() {
        val error = assertFailsWith<IllegalArgumentException> { CronExpression("* 24 * * *") }
        assertTrue(error.message!!.contains("hour"), error.message.toString())
    }

    @Test
    public fun rejectsOutOfRangeDayOfMonth() {
        val error = assertFailsWith<IllegalArgumentException> { CronExpression("* * 32 * *") }
        assertTrue(error.message!!.contains("day-of-month"), error.message.toString())
    }

    @Test
    public fun rejectsOutOfRangeMonth() {
        val error = assertFailsWith<IllegalArgumentException> { CronExpression("* * * 13 *") }
        assertTrue(error.message!!.contains("month"), error.message.toString())
    }

    @Test
    public fun rejectsOutOfRangeDayOfWeek() {
        val error = assertFailsWith<IllegalArgumentException> { CronExpression("* * * * 8") }
        assertTrue(error.message!!.contains("day-of-week"), error.message.toString())
    }

    @Test
    public fun rejectsBackwardsRange() {
        val error = assertFailsWith<IllegalArgumentException> { CronExpression("0 5-1 * * *") }
        assertTrue(error.message!!.contains("hour"), error.message.toString())
    }

    @Test
    public fun rejectsZeroStep() {
        assertFailsWith<IllegalArgumentException> { CronExpression("*/0 * * * *") }
    }

    @Test
    public fun rejectsQuartzLastDayOperator() {
        val error = assertFailsWith<IllegalArgumentException> { CronExpression("0 0 L * *") }
        assertTrue(error.message!!.contains("day-of-month"), error.message.toString())
        assertTrue(error.message!!.contains("DSL"), error.message.toString())
    }

    @Test
    public fun rejectsQuartzNthWeekdayOperator() {
        val error = assertFailsWith<IllegalArgumentException> { CronExpression("0 0 * * 5#3") }
        assertTrue(error.message!!.contains("day-of-week"), error.message.toString())
        assertTrue(error.message!!.contains("DSL"), error.message.toString())
    }

    @Test
    public fun rejectsQuartzNoSpecificValueOperator() {
        val error = assertFailsWith<IllegalArgumentException> { CronExpression("0 0 ? * *") }
        assertTrue(error.message!!.contains("DSL"), error.message.toString())
    }

    @Test
    public fun rejectsQuartzNearestWeekdayOperator() {
        val error = assertFailsWith<IllegalArgumentException> { CronExpression("0 0 6W * *") }
        assertTrue(error.message!!.contains("DSL"), error.message.toString())
    }

    @Test
    public fun rejectsDateCombinationThatCanNeverOccur() {
        val error = assertFailsWith<IllegalArgumentException> { CronExpression("0 0 30 2 *") }
        assertTrue(error.message!!.contains("never occur"), error.message.toString())
    }

    @Test
    public fun rejectsDayOfMonthThatFitsNoMonthInTheSet() {
        // April has 30 days: the 31st never occurs in April, and no other month is scheduled.
        assertFailsWith<IllegalArgumentException> { CronExpression("0 0 31 4 *") }
    }

    // ---- basic field syntax ---------------------------------------------------------------

    @Test
    public fun everyFifteenMinutesFiresFromMidHour() {
        val cron = CronExpression("*/15 * * * *")
        assertEquals(at(2026, 1, 1, 10, 15), cron.nextAfter(at(2026, 1, 1, 10, 7), UTC))
    }

    @Test
    public fun numericWeekdayRangeSkipsTheWeekend() {
        val cron = CronExpression("0 9 * * 1-5")
        // Friday 2026-01-02 09:00 -> next weekday fire is Monday 2026-01-05, not the weekend.
        assertEquals(at(2026, 1, 5, 9, 0), cron.nextAfter(at(2026, 1, 2, 9, 0), UTC))
    }

    @Test
    public fun namedWeekdayRangeMatchesNumericForm() {
        val numeric = CronExpression("0 9 * * 1-5")
        val named = CronExpression("0 9 * * MON-FRI")
        val after = at(2026, 1, 2, 9, 0)
        assertEquals(numeric.nextAfter(after, UTC), named.nextAfter(after, UTC))
    }

    @Test
    public fun dayOfWeekZeroAndSevenBothMeanSunday() {
        val zero = CronExpression("0 0 * * 0")
        val seven = CronExpression("0 0 * * 7")
        val after = at(2026, 1, 1, 0, 0)
        assertEquals(zero.nextAfter(after, UTC), seven.nextAfter(after, UTC))
        // 2026-01-04 is a Sunday.
        assertEquals(at(2026, 1, 4, 0, 0), zero.nextAfter(after, UTC))
    }

    @Test
    public fun monthRolloverFromTheLastDayOfJanuary() {
        val cron = CronExpression("0 0 1 * *")
        assertEquals(at(2026, 2, 1, 0, 0), cron.nextAfter(at(2026, 1, 31, 12, 0), UTC))
    }

    @Test
    public fun yearRolloverFromNewYearsEve() {
        val cron = CronExpression("0 0 1 * *")
        assertEquals(at(2027, 1, 1, 0, 0), cron.nextAfter(at(2026, 12, 31, 23, 0), UTC))
    }

    @Test
    public fun listOfSteppedHourRangeFiresOnTheNextStep() {
        val cron = CronExpression("0 8-18/2 * * *")
        // Hours 8,10,12,14,16,18: just after 9:00 the next one is 10:00.
        assertEquals(at(2026, 1, 1, 10, 0), cron.nextAfter(at(2026, 1, 1, 9, 0), UTC))
    }

    @Test
    public fun minuteListWithSteppedHoursCrossesIntoTheNextStepHour() {
        val cron = CronExpression("15,45 */3 * * *")
        // Hours 0,3,6,...; minutes 15 and 45.
        assertEquals(at(2026, 1, 1, 0, 45), cron.nextAfter(at(2026, 1, 1, 0, 20), UTC))
        assertEquals(at(2026, 1, 1, 3, 15), cron.nextAfter(at(2026, 1, 1, 0, 45), UTC))
    }

    // ---- day-of-month / day-of-week OR semantics -------------------------------------------

    @Test
    public fun firesOnTheThirteenthEvenWhenItIsNotAFriday() {
        val cron = CronExpression("0 0 13 * 5")
        // 2026-01-13 is a Tuesday.
        assertEquals(at(2026, 1, 13, 0, 0), cron.nextAfter(at(2026, 1, 12, 23, 59, 59), UTC))
    }

    @Test
    public fun firesOnAFridayEvenWhenItIsNotTheThirteenth() {
        val cron = CronExpression("0 0 13 * 5")
        // 2026-01-02 is a Friday.
        assertEquals(at(2026, 1, 2, 0, 0), cron.nextAfter(at(2026, 1, 1, 23, 59, 59), UTC))
    }

    // ---- leap years -------------------------------------------------------------------------

    @Test
    public fun findsFeb29OfTheNextLeapYearAcrossAMultiYearGap() {
        val cron = CronExpression("0 0 29 2 *")
        // Just after Feb 29, 2024 (a leap year); the next one is 2028.
        assertEquals(at(2028, 2, 29, 0, 0), cron.nextAfter(at(2024, 3, 1, 0, 0), UTC))
    }

    // ---- withSeconds --------------------------------------------------------------------------

    @Test
    public fun withSecondsFiresAtTheGivenSecondEveryMinute() {
        val cron = CronExpression("30 * * * * *", withSeconds = true)
        assertEquals(at(2026, 1, 1, 10, 0, 30), cron.nextAfter(at(2026, 1, 1, 10, 0, 0), UTC))
    }

    @Test
    public fun nextAfterAnExactMatchReturnsTheFollowingOccurrence() {
        val cron = CronExpression("30 * * * * *", withSeconds = true)
        val fire = at(2026, 1, 1, 10, 0, 30)
        assertEquals(at(2026, 1, 1, 10, 1, 30), cron.nextAfter(fire, UTC))
    }

    // ---- DST ------------------------------------------------------------------------------

    @Test
    public fun springForwardShiftsIntoTheGapThenResumesNormally() {
        // 2026-03-08 02:30 does not exist in America/New_York: clocks jump 02:00 EST -> 03:00 EDT.
        val cron = CronExpression("30 2 * * *")
        val fire = cron.nextAfter(at(2026, 3, 8, 0, 0, zone = NEW_YORK), NEW_YORK)
        assertEquals(Instant.parse("2026-03-08T07:30:00Z"), fire, "expected the shifted, post-gap instant")
        assertEquals(at(2026, 3, 8, 3, 30, zone = NEW_YORK), fire)
        // The next day is back to a plain, unambiguous 02:30.
        assertEquals(at(2026, 3, 9, 2, 30, zone = NEW_YORK), cron.nextAfter(fire!!, NEW_YORK))
    }

    @Test
    public fun fallBackFiresOnceAtTheEarlierOffset() {
        // 2026-11-01 01:30 exists twice in America/New_York (EDT then EST).
        val cron = CronExpression("30 1 * * *")
        val fire = cron.nextAfter(at(2026, 11, 1, 0, 0, zone = NEW_YORK), NEW_YORK)
        assertEquals(Instant.parse("2026-11-01T05:30:00Z"), fire, "expected the earlier (EDT) offset")
        // The second pass through 01:30 (06:30Z) is skipped: the next fire is the next day.
        val next = cron.nextAfter(fire!!, NEW_YORK)
        assertEquals(Instant.parse("2026-11-02T06:30:00Z"), next)
        assertTrue(next!! > fire)
    }

    // ---- monotonicity -----------------------------------------------------------------------

    @Test
    public fun nextAfterIsStrictlyMonotoneWhenChained() {
        val expressions =
            listOf(
                CronExpression("*/7 * * * *"),
                CronExpression("15,45 */3 * * *"),
                CronExpression("0 9 * * 1-5"),
                CronExpression("30 * * * * *", withSeconds = true),
            )
        for (cron in expressions) {
            var current = at(2026, 1, 1, 0, 0)
            repeat(20) {
                val next = cron.nextAfter(current, UTC)
                assertTrue(next != null, "expected a next fire time")
                assertTrue(next!! > current, "expected $next to be strictly after $current")
                current = next
            }
        }
    }
}
