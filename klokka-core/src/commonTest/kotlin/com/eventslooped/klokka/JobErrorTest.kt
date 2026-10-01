@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

private val AT: Instant = Instant.fromEpochMilliseconds(1_000)

private class CustomFailure(message: String?) : RuntimeException(message)

public class JobErrorTest {
    @Test
    public fun ofRecordsTypeMessageStackTraceAttemptAndTime() {
        val error = JobError.of(CustomFailure("kaput"), attempt = 3, at = AT)

        assertEquals(CustomFailure::class.qualifiedName, error.type)
        assertEquals("kaput", error.message)
        assertTrue(error.stackTrace.isNotEmpty())
        assertTrue(error.stackTrace.contains("kaput"))
        assertEquals(3, error.attempt)
        assertEquals(AT, error.at)
    }

    @Test
    public fun ofKeepsNullMessageAsNull() {
        val error = JobError.of(CustomFailure(null), attempt = 1, at = AT)

        assertNull(error.message)
    }

    @Test
    public fun ofCutsOversizedStackTraceToTheLimit() {
        val error = JobError.of(CustomFailure("x".repeat(JobError.MAX_STACK_TRACE_CHARS * 2)), attempt = 1, at = AT)

        assertEquals(JobError.MAX_STACK_TRACE_CHARS, error.stackTrace.length)
    }
}
