@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka

import kotlinx.datetime.DateTimeUnit
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlinx.datetime.TimeZone
import kotlinx.datetime.plus
import kotlinx.datetime.toInstant
import kotlinx.datetime.toLocalDateTime
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

/**
 * When a recurring job fires. Schedule evaluation happens entirely in the runtime; a
 * [com.eventslooped.klokka.spi.JobStore] only persists the next fire time and never sees
 * or evaluates an expression, so custom implementations of this interface (a "last
 * business day of the month" builder, for example) work with every store unchanged.
 */
public interface Schedule {
    /**
     * Canonical, human-readable form of this schedule, such as `every 5m` or
     * `cron '0 9 * * 1-5' Europe/Oslo`.
     *
     * Contract: the description must be STABLE for an unchanged schedule and MUST change
     * when the fire times change. It is part of the fingerprint the runtime uses to detect
     * definition changes; a description that varies between processes or restarts makes the
     * runtime reset the schedule's next fire time on every start.
     */
    public val description: String

    /**
     * The first fire time strictly after [after], or null if the schedule never fires again.
     *
     * Must be deterministic and monotone: the same [after] always yields the same result,
     * and the result is strictly greater than [after]. The runtime iterates this function
     * to enumerate missed fires, so it must be cheap.
     */
    public fun nextAfter(after: Instant): Instant?
}

/**
 * Fires every [interval], anchored at the previous fire: each fire time is exactly
 * [interval] after the one before it, so the cadence never drifts with execution timing.
 * The first fire is one [interval] after the schedule is first registered.
 */
public fun every(interval: Duration): Schedule {
    require(interval.isPositive()) { "every() needs a positive interval, was $interval" }
    return EverySchedule(interval)
}

/**
 * Fires once a day at the wall-clock time [hour]:[minute] in [zone], tracking live zone
 * rules. On a day when that local time does not exist (spring-forward DST gap) the fire
 * shifts to the first valid instant after the gap; when it exists twice (fall-back
 * overlap) it fires once, at the earlier offset.
 */
public fun dailyAt(hour: Int, minute: Int = 0, zone: TimeZone): Schedule {
    require(hour in 0..23) { "hour must be in 0..23, was $hour" }
    require(minute in 0..59) { "minute must be in 0..59, was $minute" }
    return DailyAtSchedule(hour, minute, zone)
}

/**
 * Standard 5-field cron (`minute hour day-of-month month day-of-week`), evaluated in
 * [zone] and validated here, at construction. With [withSeconds] a seconds field is
 * prepended (6 fields). Names (`MON`, `JAN`), lists, ranges and steps are supported;
 * Quartz `L`/`W`/`#` operators are not and never will be: complex business calendars
 * are served by the typed DSL. DST behavior matches [dailyAt].
 *
 * @throws IllegalArgumentException when [expression] does not parse, naming the offending field.
 */
public fun cron(expression: String, zone: TimeZone, withSeconds: Boolean = false): Schedule =
    CronSchedule(expression, zone, withSeconds)

private class EverySchedule(private val interval: Duration) : Schedule {
    override val description: String = "every $interval"

    override fun nextAfter(after: Instant): Instant = after + interval
}

private class DailyAtSchedule(
    private val hour: Int,
    private val minute: Int,
    private val zone: TimeZone,
) : Schedule {
    private val localTime = LocalTime(hour, minute)

    override val description: String =
        "daily at ${hour.toString().padStart(2, '0')}:${minute.toString().padStart(2, '0')} ${zone.id}"

    override fun nextAfter(after: Instant): Instant {
        val local = after.toLocalDateTime(zone)
        var date = local.date
        if (local.time >= localTime) {
            date = date.plus(1, DateTimeUnit.DAY)
        }
        // toInstant resolves DST: a gap shifts forward, an overlap takes the earlier offset.
        // The earlier offset can land at or before `after` inside an overlap; step days until
        // strictly after.
        var candidate = LocalDateTime(date, localTime).toInstant(zone)
        while (candidate <= after) {
            date = date.plus(1, DateTimeUnit.DAY)
            candidate = LocalDateTime(date, localTime).toInstant(zone)
        }
        return candidate
    }
}

private class CronSchedule(
    expression: String,
    private val zone: TimeZone,
    withSeconds: Boolean,
) : Schedule {
    private val parsed = CronExpression(expression, withSeconds)

    override val description: String =
        "cron '$expression' ${zone.id}" + if (withSeconds) " with seconds" else ""

    override fun nextAfter(after: Instant): Instant? = parsed.nextAfter(after, zone)
}
