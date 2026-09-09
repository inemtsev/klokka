@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.runtime

import com.eventslooped.klokka.MisfirePolicy
import com.eventslooped.klokka.OverlapPolicy
import com.eventslooped.klokka.QueueName
import com.eventslooped.klokka.Schedule
import com.eventslooped.klokka.spi.NewJob
import com.eventslooped.klokka.spi.ScheduleFire
import com.eventslooped.klokka.spi.ScheduleSpec
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

/**
 * Ceiling on how many fire times the runtime enumerates when measuring a missed window.
 * Beyond it the count saturates and the latest occurrences are located by probing
 * backwards from the end of the window instead of walking the whole window.
 */
private const val MISSED_ENUMERATION_CAP = 10_000

/** Probe rounds before [latestMissedFires] gives up refining and returns what it has. */
private const val PROBE_ROUND_CAP = 64

/**
 * One code-defined recurring schedule, as registered via `KlokkaRuntime.recurring`.
 * Everything the store must never own lives here: the schedule expression, the payload,
 * the misfire and overlap policies. The store sees only [spec].
 */
internal class RecurringDefinition(
    val id: String,
    val kind: String,
    val queue: QueueName,
    val payload: String,
    val payloadVersion: Int,
    val schedule: Schedule,
    val misfire: MisfirePolicy,
    val misfireThreshold: Duration,
    val overlap: OverlapPolicy,
) {
    init {
        require(id.isNotEmpty() && id.length <= 128 && id.all { it.isLowerCase() || it.isDigit() || it == '-' || it == '.' }) {
            "Schedule id must be 1..128 chars of lowercase letters, digits, '-' or '.': was '$id'"
        }
        require(misfireThreshold.isPositive()) { "misfireThreshold must be positive, was $misfireThreshold" }
    }

    /**
     * Opaque change-detection token for the store. Any change to the fields below resets
     * the schedule's next fire time on the next runtime start.
     */
    val fingerprint: String =
        listOf(
            kind,
            queue.value,
            payload,
            "v$payloadVersion",
            schedule.description,
            "misfire=$misfire",
            "threshold=$misfireThreshold",
            "overlap=$overlap",
        ).joinToString("|")

    fun spec(fireAt: Instant?): ScheduleSpec =
        ScheduleSpec(id = id, kind = kind, fingerprint = fingerprint, description = schedule.description, fireAt = fireAt)

    fun newRun(runAt: Instant): NewJob =
        NewJob(
            kind = kind,
            payload = payload,
            payloadVersion = payloadVersion,
            queue = queue,
            runAt = runAt,
            uniqueKey = if (overlap == OverlapPolicy.SkipIfRunning) "klokka:schedule:$id" else null,
            scheduleId = id,
        )
}

/** The recurring schedules registered on one runtime, keyed by id. Populated before start. */
internal class RecurringRegistry {
    private val definitions = LinkedHashMap<String, RecurringDefinition>()

    fun register(definition: RecurringDefinition) {
        require(definitions.put(definition.id, definition) == null) {
            "Duplicate recurring schedule id '${definition.id}'"
        }
    }

    fun ids(): Set<String> = definitions.keys.toSet()

    fun get(id: String): RecurringDefinition? = definitions[id]

    fun isEmpty(): Boolean = definitions.isEmpty()

    fun specs(fireAt: (Schedule) -> Instant?): List<ScheduleSpec> =
        definitions.values.map { it.spec(fireAt(it.schedule)) }
}

/** What one claimed [ScheduleFire] amounts to: the runs to insert and the new next-fire time. */
internal class FireComputation(
    val runs: List<NewJob>,
    val nextFireAt: Instant?,
    /** Fire times inside the missed window; 0 when the fire was on time. Saturates at [MISSED_ENUMERATION_CAP]. */
    val missedFires: Int,
)

/**
 * Turns a claimed fire into runs, applying the misfire policy. Pure: all times come from
 * [fire] (store clock) and [definition]; deterministic, so a node that redoes an expired
 * claim computes the same result.
 *
 * On time (within the threshold): one run at the intended fire time, next fire computed
 * from the intended time so the cadence never drifts. Missed: Skip emits nothing,
 * FireOnce emits the most recent missed fire time, CatchUp emits the most recent
 * `atMost` of them (oldest first); in all three cases the next fire is computed from the
 * store's now, never from the missed window.
 */
internal fun computeFire(definition: RecurringDefinition, fire: ScheduleFire): FireComputation {
    val schedule = definition.schedule
    if (fire.now - fire.scheduledFor <= definition.misfireThreshold) {
        return FireComputation(
            runs = listOf(definition.newRun(fire.scheduledFor)),
            nextFireAt = schedule.nextAfter(fire.scheduledFor),
            missedFires = 0,
        )
    }
    val keep =
        when (val policy = definition.misfire) {
            is MisfirePolicy.Skip -> 1
            is MisfirePolicy.FireOnce -> 1
            is MisfirePolicy.CatchUp -> policy.atMost
        }
    val missed = latestMissedFires(schedule, fire.scheduledFor, fire.now, keep)
    val runs =
        when (definition.misfire) {
            is MisfirePolicy.Skip -> emptyList()
            is MisfirePolicy.FireOnce -> missed.latest.takeLast(1).map(definition::newRun)
            is MisfirePolicy.CatchUp -> missed.latest.map(definition::newRun)
        }
    return FireComputation(runs = runs, nextFireAt = schedule.nextAfter(fire.now), missedFires = missed.count)
}

internal class MissedFires(
    /** Saturates at [MISSED_ENUMERATION_CAP] for pathologically dense windows. */
    val count: Int,
    /** The most recent occurrences in the window, oldest first, at most `keep` of them. */
    val latest: List<Instant>,
)

/**
 * The fire times in `[from, until]` ([from] itself is one: it is the stored next-fire
 * time that came due). Walks [Schedule.nextAfter] forward; if the window holds more than
 * [MISSED_ENUMERATION_CAP] occurrences, falls back to probing a shrinking-then-growing
 * suffix of the window so the latest [keep] occurrences are found without walking
 * millions of steps. The probe is best-effort under adversarially non-uniform schedules;
 * for real cadences it is exact.
 */
internal fun latestMissedFires(schedule: Schedule, from: Instant, until: Instant, keep: Int): MissedFires {
    val direct = enumerate(schedule, start = from, startInclusive = true, until = until, keep = keep)
    if (direct != null) return direct

    // Dense window: find a suffix (until - g, until] that holds at least `keep`
    // occurrences but fewer than the cap, doubling or halving g as needed.
    var g = 1.minutes
    var rounds = 0
    var best: MissedFires? = null
    while (rounds < PROBE_ROUND_CAP) {
        rounds += 1
        val start = if (until - g > from) until - g else from
        val suffix = enumerate(schedule, start = start, startInclusive = start == from, until = until, keep = keep)
        if (suffix == null) {
            g = g / 2
            continue
        }
        best = suffix
        if (suffix.latest.size >= keep || start == from) break
        g = g * 2
    }
    val found = best ?: MissedFires(MISSED_ENUMERATION_CAP, emptyList())
    // The forward walk overflowed the cap, so the true count is at least the cap.
    return MissedFires(MISSED_ENUMERATION_CAP, found.latest)
}

/** Walks the window forward; null when it holds more than [MISSED_ENUMERATION_CAP] occurrences. */
private fun enumerate(schedule: Schedule, start: Instant, startInclusive: Boolean, until: Instant, keep: Int): MissedFires? {
    val ring = ArrayDeque<Instant>(keep)
    var count = 0
    var t: Instant? = if (startInclusive) start else schedule.nextAfter(start)
    while (t != null && t <= until) {
        count += 1
        if (count > MISSED_ENUMERATION_CAP) return null
        if (ring.size == keep) ring.removeFirst()
        ring.addLast(t)
        t = schedule.nextAfter(t)
    }
    return MissedFires(count, ring.toList())
}
