@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.runtime

import com.eventslooped.klokka.MisfirePolicy
import com.eventslooped.klokka.OverlapPolicy
import com.eventslooped.klokka.QueueName
import com.eventslooped.klokka.Schedule
import com.eventslooped.klokka.every
import com.eventslooped.klokka.spi.ScheduleFire
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

private val EPOCH: Instant = Instant.fromEpochMilliseconds(0)

private fun definition(
    schedule: Schedule = every(5.minutes),
    misfire: MisfirePolicy = MisfirePolicy.FireOnce,
    threshold: Duration = 1.minutes,
    overlap: OverlapPolicy = OverlapPolicy.Allow,
): RecurringDefinition =
    RecurringDefinition(
        id = "digest",
        kind = "reports.digest",
        queue = QueueName.DEFAULT,
        payload = "{}",
        payloadVersion = 1,
        schedule = schedule,
        misfire = misfire,
        misfireThreshold = threshold,
        overlap = overlap,
    )

private fun fire(scheduledFor: Instant, now: Instant, fence: Long = 1): ScheduleFire =
    ScheduleFire(scheduleId = "digest", scheduledFor = scheduledFor, now = now, fence = fence)

public class RecurringComputationTest {
    @Test
    public fun onTimeFireEmitsOneRunAtTheIntendedTimeAndKeepsTheCadence() {
        val def = definition()
        val scheduledFor = EPOCH + 5.minutes
        val result = computeFire(def, fire(scheduledFor, now = scheduledFor + 2.seconds))
        assertEquals(listOf(scheduledFor), result.runs.map { it.runAt })
        // Next fire anchors on the intended time, not on "now": the cadence never drifts.
        assertEquals(scheduledFor + 5.minutes, result.nextFireAt)
        assertEquals(0, result.missedFires)
        val run = result.runs.single()
        assertEquals("reports.digest", run.kind)
        assertEquals("digest", run.scheduleId)
        assertNull(run.uniqueKey, "OverlapPolicy.Allow must not reserve a unique key")
    }

    @Test
    public fun latenessWithinTheThresholdIsNotAMisfire() {
        val def = definition(threshold = 1.minutes)
        val scheduledFor = EPOCH + 5.minutes
        val result = computeFire(def, fire(scheduledFor, now = scheduledFor + 59.seconds))
        assertEquals(0, result.missedFires)
        assertEquals(listOf(scheduledFor), result.runs.map { it.runAt })
    }

    @Test
    public fun skipEmitsNothingAndResumesFromNow() {
        val def = definition(misfire = MisfirePolicy.Skip)
        val scheduledFor = EPOCH + 5.minutes
        val now = EPOCH + 27.minutes
        val result = computeFire(def, fire(scheduledFor, now))
        assertTrue(result.runs.isEmpty())
        assertEquals(now + 5.minutes, result.nextFireAt)
        // Missed: 5, 10, 15, 20, 25 minutes.
        assertEquals(5, result.missedFires)
    }

    @Test
    public fun fireOnceEmitsTheMostRecentMissedFire() {
        val def = definition(misfire = MisfirePolicy.FireOnce)
        val result = computeFire(def, fire(EPOCH + 5.minutes, now = EPOCH + 27.minutes))
        assertEquals(listOf(EPOCH + 25.minutes), result.runs.map { it.runAt })
        assertEquals(EPOCH + 32.minutes, result.nextFireAt)
        assertEquals(5, result.missedFires)
    }

    @Test
    public fun catchUpEmitsTheMostRecentNOldestFirst() {
        val def = definition(misfire = MisfirePolicy.CatchUp(atMost = 3))
        val result = computeFire(def, fire(EPOCH + 5.minutes, now = EPOCH + 27.minutes))
        assertEquals(
            listOf(EPOCH + 15.minutes, EPOCH + 20.minutes, EPOCH + 25.minutes),
            result.runs.map { it.runAt },
        )
        assertEquals(5, result.missedFires)
    }

    @Test
    public fun catchUpEmitsEverythingWhenFewerFiresWereMissedThanAtMost() {
        val def = definition(misfire = MisfirePolicy.CatchUp(atMost = 10))
        val result = computeFire(def, fire(EPOCH + 5.minutes, now = EPOCH + 16.minutes))
        assertEquals(
            listOf(EPOCH + 5.minutes, EPOCH + 10.minutes, EPOCH + 15.minutes),
            result.runs.map { it.runAt },
        )
        assertEquals(3, result.missedFires)
    }

    @Test
    public fun skipIfRunningReservesTheScheduleUniqueKey() {
        val def = definition(overlap = OverlapPolicy.SkipIfRunning)
        val result = computeFire(def, fire(EPOCH + 5.minutes, now = EPOCH + 5.minutes))
        assertEquals("klokka:schedule:digest", result.runs.single().uniqueKey)
    }

    @Test
    public fun denseWindowSaturatesTheCountButStillFindsTheLatestFires() {
        // 30 minutes of every-100ms fires: 18k occurrences, past the enumeration cap.
        val def = definition(schedule = every(100.milliseconds), misfire = MisfirePolicy.FireOnce, threshold = 1.seconds)
        val scheduledFor = EPOCH + 5.minutes
        val now = EPOCH + 35.minutes
        val result = computeFire(def, fire(scheduledFor, now))
        assertEquals(10_000, result.missedFires, "count saturates at the enumeration cap")
        val emitted = result.runs.single().runAt
        assertTrue(emitted <= now && emitted > now - 1.seconds, "expected a fire adjacent to now, got $emitted")
    }

    @Test
    public fun fingerprintTracksTheDefinitionAndOnlyTheDefinition() {
        assertEquals(definition().fingerprint, definition().fingerprint)
        assertTrue(definition().fingerprint != definition(schedule = every(6.minutes)).fingerprint)
        assertTrue(definition().fingerprint != definition(misfire = MisfirePolicy.Skip).fingerprint)
        assertTrue(definition().fingerprint != definition(threshold = 2.minutes).fingerprint)
        assertTrue(definition().fingerprint != definition(overlap = OverlapPolicy.SkipIfRunning).fingerprint)
    }

    @Test
    public fun scheduleIdsFollowTheKindCharacterRules() {
        assertFailsWith<IllegalArgumentException> {
            RecurringDefinition(
                id = "Bad Id",
                kind = "reports.digest",
                queue = QueueName.DEFAULT,
                payload = "{}",
                payloadVersion = 1,
                schedule = every(5.minutes),
                misfire = MisfirePolicy.FireOnce,
                misfireThreshold = 1.minutes,
                overlap = OverlapPolicy.Allow,
            )
        }
    }
}
