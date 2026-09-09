@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka

import kotlinx.datetime.DateTimeUnit
import kotlinx.datetime.LocalDate
import kotlinx.datetime.TimeZone
import kotlinx.datetime.atTime
import kotlinx.datetime.isoDayNumber
import kotlinx.datetime.plus
import kotlinx.datetime.toInstant
import kotlinx.datetime.toLocalDateTime
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

/** How far ahead [CronExpression.nextAfter] scans before giving up: covers the 8-year gap between Feb 29 occurrences around a skipped century leap year (e.g. 1896 to 1904). */
private const val SEARCH_YEARS = 9

private val MONTH_NAMES = listOf("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC")
private val DAY_NAMES = listOf("SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT")

/** Max day-of-month per calendar month, January first; February uses the leap-year max of 29. */
private val DAYS_IN_MONTH = intArrayOf(31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)

private val QUARTZ_OPERATORS = setOf('L', 'W', '#', '?')
private val ALLOWED_FIELD_CHARS = ('0'..'9').toSet() + setOf(',', '-', '/', '*')

private const val QUARTZ_REFERRAL = "Klokka uses standard 5-field cron; complex calendars are served by the typed schedule DSL."

/**
 * A parsed standard cron expression: 5 fields (minute hour day-of-month month day-of-week), or
 * 6 with [withSeconds] set (a leading second field prepended). Field syntax is Vixie/POSIX
 * cron: `*`, single values, `a-b` ranges, comma-separated lists, and step syntax: `/n`
 * after `*`, a range, or a single value `a` (meaning `a-max/n`). Month names `JAN`..`DEC` and day names `SUN`..`SAT` are accepted
 * case-insensitively; day-of-week `0` and `7` both mean Sunday.
 *
 * Day-of-month and day-of-week combine with POSIX OR semantics: when both are restricted
 * (neither is a bare `*`), a date matches if either field matches; when only one is
 * restricted, that one alone decides; when neither is restricted, every day matches.
 *
 * Quartz-only operators (`L`, `W`, `#`, `?`) are rejected: Klokka's typed schedule DSL is the
 * intended way to express complex calendars, not an extended cron dialect.
 *
 * The constructor parses and validates eagerly, so a malformed expression fails at
 * registration time rather than at first fire.
 */
internal class CronExpression(expression: String, withSeconds: Boolean = false) {
    private val seconds: List<Int>
    private val minutes: List<Int>
    private val hours: List<Int>
    private val domSet: Set<Int>
    private val domRestricted: Boolean
    private val monthSet: Set<Int>
    private val dowSet: Set<Int>
    private val dowRestricted: Boolean

    init {
        val normalized = expression.trim().uppercase()
        val parts = normalized.split(Regex("\\s+")).filter { it.isNotEmpty() }
        val fieldNames =
            if (withSeconds) {
                listOf("second", "minute", "hour", "day-of-month", "month", "day-of-week")
            } else {
                listOf("minute", "hour", "day-of-month", "month", "day-of-week")
            }
        require(parts.size == fieldNames.size) {
            "Cron expression '$expression' needs ${fieldNames.size} fields (${fieldNames.joinToString(" ")}) " +
                "when withSeconds=$withSeconds, but found ${parts.size} field(s)"
        }

        var i = 0
        val secondRaw = if (withSeconds) parts[i++] else "0"
        val minuteRaw = parts[i++]
        val hourRaw = parts[i++]
        val domRaw = parts[i++]
        val monthRawOriginal = parts[i++]
        val dowRawOriginal = parts[i]

        val monthRaw = substituteNames(monthRawOriginal, MONTH_NAMES, offset = 1)
        val dowRaw = substituteNames(dowRawOriginal, DAY_NAMES, offset = 0)

        checkForbiddenCharacters("second", secondRaw, secondRaw)
        checkForbiddenCharacters("minute", minuteRaw, minuteRaw)
        checkForbiddenCharacters("hour", hourRaw, hourRaw)
        checkForbiddenCharacters("day-of-month", domRaw, domRaw)
        checkForbiddenCharacters("month", monthRawOriginal, monthRaw)
        checkForbiddenCharacters("day-of-week", dowRawOriginal, dowRaw)

        seconds = parseField("second", secondRaw, 0, 59).first.sorted()
        minutes = parseField("minute", minuteRaw, 0, 59).first.sorted()
        hours = parseField("hour", hourRaw, 0, 23).first.sorted()

        val (domValues, domRestrictedValue) = parseField("day-of-month", domRaw, 1, 31)
        domSet = domValues
        domRestricted = domRestrictedValue

        monthSet = parseField("month", monthRaw, 1, 12).first

        val (dowValues, dowRestrictedValue) = parseField("day-of-week", dowRaw, 0, 7)
        dowSet = dowValues.map { if (it == 7) 0 else it }.toSet()
        dowRestricted = dowRestrictedValue

        if (!dowRestricted) {
            val canOccur = monthSet.any { month -> domSet.any { day -> day <= DAYS_IN_MONTH[month - 1] } }
            require(canOccur) {
                "Cron expression '$expression' can never occur: no day-of-month value fits within any scheduled month"
            }
        }
    }

    /** The first instant strictly after [after] matched by this expression in [zone], or null if none exists within the search bound. */
    fun nextAfter(after: Instant, zone: TimeZone): Instant? {
        val startDate = after.toLocalDateTime(zone).date
        val bound = startDate.plus(SEARCH_YEARS, DateTimeUnit.YEAR)

        var date = startDate
        while (date < bound) {
            if (dateMatches(date)) {
                for (hour in hours) {
                    for (minute in minutes) {
                        for (second in seconds) {
                            val candidate = date.atTime(hour, minute, second).toInstant(zone)
                            if (candidate > after) return candidate
                        }
                    }
                }
            }
            date = date.plus(1, DateTimeUnit.DAY)
        }
        return null
    }

    private fun dateMatches(date: LocalDate): Boolean {
        if (date.monthNumber !in monthSet) return false
        val domMatch = date.dayOfMonth in domSet
        val dowMatch = (date.dayOfWeek.isoDayNumber % 7) in dowSet
        return when {
            domRestricted && dowRestricted -> domMatch || dowMatch
            domRestricted -> domMatch
            dowRestricted -> dowMatch
            else -> true
        }
    }
}

/** Replaces every occurrence of each name in [names] with its 1-based-by-[offset] index, e.g. `JAN` -> `1`. */
private fun substituteNames(raw: String, names: List<String>, offset: Int): String {
    var result = raw
    for ((index, name) in names.withIndex()) {
        result = result.replace(name, (index + offset).toString())
    }
    return result
}

/**
 * Scans [scanText] (the field after name substitution) for characters outside the plain
 * cron grammar, reporting against [display] (the field as the caller wrote it) so Quartz
 * name-derived digits never appear in the message in place of the user's own text.
 */
private fun checkForbiddenCharacters(fieldName: String, display: String, scanText: String) {
    for (c in scanText) {
        if (c in QUARTZ_OPERATORS) {
            throw IllegalArgumentException(
                "Invalid $fieldName field '$display': '$c' is a Quartz operator, not standard cron. $QUARTZ_REFERRAL",
            )
        }
        if (c !in ALLOWED_FIELD_CHARS) {
            throw IllegalArgumentException("Invalid $fieldName field '$display': unexpected character '$c'")
        }
    }
}

/** Parses a comma-separated field into its matched values and whether the field is restricted (not a bare `*`). */
private fun parseField(fieldName: String, raw: String, min: Int, max: Int): Pair<Set<Int>, Boolean> {
    val restricted = raw != "*"
    val values = sortedSetOf<Int>()
    for (part in raw.split(',')) {
        parsePart(fieldName, part, min, max, values)
    }
    require(values.isNotEmpty()) { "Invalid $fieldName field '$raw': matches no value" }
    return values to restricted
}

private fun parsePart(fieldName: String, part: String, min: Int, max: Int, out: MutableSet<Int>) {
    val stepParts = part.split('/')
    require(stepParts.size <= 2) { "Invalid $fieldName field value '$part': malformed step expression" }
    val base = stepParts[0]
    val step = if (stepParts.size == 2) parseStep(fieldName, part, stepParts[1]) else null

    if (step == null && base != "*" && '-' !in base) {
        out.add(parseValue(fieldName, base, min, max))
        return
    }

    val (lo, hi) =
        when {
            base == "*" -> min to max
            '-' in base -> {
                val bounds = base.split('-')
                require(bounds.size == 2) { "Invalid $fieldName field range '$part': malformed range" }
                val a = parseValue(fieldName, bounds[0], min, max)
                val b = parseValue(fieldName, bounds[1], min, max)
                require(a <= b) { "Invalid $fieldName field range '$part': start $a is greater than end $b" }
                a to b
            }
            // "a/n" means a..max stepped by n.
            else -> parseValue(fieldName, base, min, max) to max
        }

    var value = lo
    val step0 = step ?: 1
    while (value <= hi) {
        out.add(value)
        value += step0
    }
}

private fun parseStep(fieldName: String, part: String, token: String): Int {
    val step = token.toIntOrNull()
    require(step != null && step >= 1) { "Invalid $fieldName field step '$part': step must be a positive integer" }
    return step
}

private fun parseValue(fieldName: String, token: String, min: Int, max: Int): Int {
    val value = token.toIntOrNull() ?: throw IllegalArgumentException("Invalid $fieldName field value '$token': not a number")
    require(value in min..max) { "Invalid $fieldName field value '$token': must be within $min..$max" }
    return value
}
