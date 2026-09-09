@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.ktor.samples

import com.eventslooped.klokka.JobEvent
import com.eventslooped.klokka.MisfirePolicy
import com.eventslooped.klokka.OverlapPolicy
import com.eventslooped.klokka.dailyAt
import com.eventslooped.klokka.jobType
import com.eventslooped.klokka.ktor.Klokka
import com.eventslooped.klokka.ktor.klokka
import com.eventslooped.klokka.store.InMemoryJobStore
import io.ktor.client.request.post
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.response.respondText
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.filterIsInstance
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.TimeZone
import kotlinx.datetime.toInstant
import kotlinx.serialization.Serializable
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

// =========================================================================================
// Sample: "roll up yesterday's usage every night at 02:30, and let an admin run it now".
//
// Everything above the test class is what an application developer would write against the
// current Klokka API for a recurring job. The test class runs it end to end through a real
// Ktor test application with the in-memory store and a controllable clock, so this file
// doubles as an executable check that the recurring API holds up.
// =========================================================================================

// --- 1. The job ---------------------------------------------------------------------------
//
// A recurring job is an ordinary job: same descriptor, same handler shape. What makes it
// recurring is the `recurring(...)` declaration in section 3; nothing about the kind or the
// payload knows it is scheduled.

@Serializable
data class UsageRollup(val source: String)

val RollUpUsage = jobType<UsageRollup>("analytics.usage-rollup")

// --- 2. The handler -----------------------------------------------------------------------
//
// scheduledFor is the INTENDED fire time, not the wall clock at execution: for the 02:30
// schedule it is 02:30 even when the run starts late, and for a misfire catch-up run it is
// the fire time that was missed. That makes it the correct anchor for "which window of data
// does this run cover" and for idempotency keys, exactly as at-least-once execution needs.

interface RollupStore {
    suspend fun rollUp(source: String, window: ClosedRange<Instant>, idempotencyKey: String)
}

// --- 3. The module ------------------------------------------------------------------------

fun Application.analyticsModule(rollups: RollupStore, clock: Clock = Clock.System) {
    install(Klokka) {
        this.clock = clock
        store = InMemoryJobStore(clock)

        handle(RollUpUsage) { payload ->
            // 24 hours ending at the intended fire time. A redelivered or caught-up run
            // recomputes the same window and the same key, so the store stays idempotent.
            val window = (scheduledFor - 24.hours)..scheduledFor
            rollups.rollUp(payload.source, window, idempotencyKey = "rollup-${payload.source}-$scheduledFor")
        }

        // The schedule: code-defined, one line. `id` is its durable identity; changing the
        // definition (time, payload, policies) redeploys it, no migration involved.
        //
        // FireOnce: if the app was down over 02:30, run the most recent missed window once
        // on recovery instead of silently skipping a day (Skip) or replaying every missed
        // day (CatchUp). SkipIfRunning: never start tonight's rollup while yesterday's is
        // still executing.
        recurring(
            id = "nightly-usage-rollup",
            type = RollUpUsage,
            payload = UsageRollup(source = "api-gateway"),
            schedule = dailyAt(2, 30, TimeZone.of("UTC")),
            misfire = MisfirePolicy.FireOnce,
            overlap = OverlapPolicy.SkipIfRunning,
        )

        pollInterval = 25.milliseconds // test-speed polling; production default is 10s
    }

    routing {
        // On-demand run for operators: enqueues immediately, does not shift the 02:30 cadence.
        post("/admin/rollup/run") {
            val jobId = klokka.triggerNow("nightly-usage-rollup")
            call.respondText(jobId.value, status = HttpStatusCode.Accepted)
        }
    }
}

// =========================================================================================
// Executable check: run the module above inside a Ktor test application.
// =========================================================================================

/**
 * The clock an application test controls. Klokka takes any kotlin.time.Clock, so schedule
 * tests advance time explicitly instead of sleeping; the same clock drives the in-memory
 * store, which plays the database-time role.
 */
class MutableClock(start: Instant) : Clock {
    // Volatile: the test thread advances the clock while worker coroutines read it.
    @Volatile
    private var current: Instant = start

    override fun now(): Instant = current

    fun advanceBy(duration: Duration) {
        current += duration
    }
}

data class RecordedRollup(val source: String, val window: ClosedRange<Instant>, val idempotencyKey: String)

class FakeRollupStore : RollupStore {
    val recorded = mutableListOf<RecordedRollup>()
    val firstRollup = CompletableDeferred<RecordedRollup>()

    override suspend fun rollUp(source: String, window: ClosedRange<Instant>, idempotencyKey: String) {
        val rollup = RecordedRollup(source, window, idempotencyKey)
        recorded += rollup
        firstRollup.complete(rollup)
    }
}

private val UTC = TimeZone.of("UTC")
private val MIDNIGHT: Instant = LocalDateTime(2026, 9, 4, 0, 0).toInstant(UTC)
private val HALF_PAST_TWO: Instant = LocalDateTime(2026, 9, 4, 2, 30).toInstant(UTC)

class NightlyRollupSampleTest {
    @Test
    fun firesAtHalfPastTwoWithTheIntendedTimeAsWindowAnchor() =
        testApplication {
            val clock = MutableClock(MIDNIGHT)
            val rollups = FakeRollupStore()
            application { analyticsModule(rollups, clock) }
            startApplication()

            // Advancing the clock to 02:30 is all it takes; no real waiting, no sleeps.
            clock.advanceBy(2.hours + 30.minutes)

            val rollup = withTimeout(5.seconds) { rollups.firstRollup.await() }
            assertEquals("api-gateway", rollup.source)
            assertEquals((HALF_PAST_TWO - 24.hours)..HALF_PAST_TWO, rollup.window)
            assertEquals("rollup-api-gateway-$HALF_PAST_TWO", rollup.idempotencyKey)
            assertEquals(1, rollups.recorded.size)
        }

    @Test
    fun downtimeOverTheFireTimeRunsTheMostRecentMissedWindowOnce() =
        testApplication {
            val clock = MutableClock(MIDNIGHT)
            val rollups = FakeRollupStore()
            val misfired = CompletableDeferred<JobEvent.ScheduleMisfired>()
            application {
                analyticsModule(rollups, clock)
                klokka.events
                    .filterIsInstance<JobEvent.ScheduleMisfired>()
                    .onEach { misfired.complete(it) }
                    .launchIn(this)
            }
            startApplication()

            // "Two days of downtime": the clock jumps straight past two 02:30 fires,
            // landing at 01:00, before the third one.
            clock.advanceBy(2.days + 1.hours)

            val event = withTimeout(5.seconds) { misfired.await() }
            assertEquals("nightly-usage-rollup", event.scheduleId)
            assertEquals(2, event.missedFires)
            assertEquals(1, event.emitted)

            // FireOnce ran exactly one rollup, for the LATEST missed 02:30 window.
            val rollup = withTimeout(5.seconds) { rollups.firstRollup.await() }
            assertEquals((HALF_PAST_TWO + 1.days - 24.hours)..(HALF_PAST_TWO + 1.days), rollup.window)
            assertEquals(1, rollups.recorded.size)
        }

    @Test
    fun adminEndpointRunsTheRollupNowWithoutShiftingTheSchedule() =
        testApplication {
            val clock = MutableClock(MIDNIGHT)
            val rollups = FakeRollupStore()
            application { analyticsModule(rollups, clock) }

            val response = client.post("/admin/rollup/run")
            assertEquals(HttpStatusCode.Accepted, response.status)
            assertTrue(response.bodyAsText().isNotEmpty())

            val rollup = withTimeout(5.seconds) { rollups.firstRollup.await() }
            // Triggered at 00:00, so the window anchors on the trigger time, not on 02:30.
            assertEquals((MIDNIGHT - 24.hours)..MIDNIGHT, rollup.window)

            // The nightly cadence is untouched: 02:30 still fires.
            clock.advanceBy(2.hours + 30.minutes)
            withTimeout(5.seconds) {
                while (rollups.recorded.size < 2) yield()
            }
            assertEquals(2, rollups.recorded.size)
            assertEquals((HALF_PAST_TWO - 24.hours)..HALF_PAST_TWO, rollups.recorded[1].window)
        }
}
