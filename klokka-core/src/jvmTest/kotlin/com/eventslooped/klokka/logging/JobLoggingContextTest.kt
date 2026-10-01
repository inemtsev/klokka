package com.eventslooped.klokka.logging

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import org.slf4j.MDC
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

public class JobLoggingContextTest {
    @Test
    public fun fieldsVisibleAcrossSuspensionAndThreadHopsThenGone() =
        runTest {
            val fields = mapOf("klokka.jobId" to "j1", "klokka.kind" to "k")
            withJobLoggingContext(fields) {
                assertEquals("j1", MDC.get("klokka.jobId"))
                delay(1)
                assertEquals("j1", MDC.get("klokka.jobId"))
                withContext(Dispatchers.Default) {
                    assertEquals("j1", MDC.get("klokka.jobId"))
                    assertEquals("k", MDC.get("klokka.kind"))
                }
                assertEquals("k", MDC.get("klokka.kind"))
            }
            assertNull(MDC.get("klokka.jobId"))
            assertNull(MDC.get("klokka.kind"))
        }

    @Test
    public fun preExistingEntriesRemainVisibleAndIntact() =
        runTest {
            MDC.put("caller.key", "caller")
            try {
                withJobLoggingContext(mapOf("klokka.jobId" to "j2")) {
                    assertEquals("caller", MDC.get("caller.key"))
                    assertEquals("j2", MDC.get("klokka.jobId"))
                }
                assertEquals("caller", MDC.get("caller.key"))
                assertNull(MDC.get("klokka.jobId"))
            } finally {
                MDC.remove("caller.key")
            }
        }

    @Test
    public fun exceptionPropagatesAndMdcIsRestored() =
        runTest {
            val failure =
                assertFailsWith<IllegalStateException> {
                    withJobLoggingContext(mapOf("klokka.jobId" to "j3")) {
                        assertEquals("j3", MDC.get("klokka.jobId"))
                        error("boom")
                    }
                }
            assertEquals("boom", failure.message)
            assertNull(MDC.get("klokka.jobId"))
        }
}
