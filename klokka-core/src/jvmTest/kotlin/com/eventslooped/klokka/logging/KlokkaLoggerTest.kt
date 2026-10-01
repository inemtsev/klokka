package com.eventslooped.klokka.logging

import kotlin.test.Test
import kotlin.test.fail

public class KlokkaLoggerTest {
    // With no slf4j binding on the test classpath, slf4j 2 falls back to a NOP logger on
    // which every level is disabled, so message lambdas must never be evaluated.
    @Test
    public fun disabledLevelsDoNotEvaluateMessageLambda() {
        val log = klokkaLogger("x")
        log.debug { fail("must not be evaluated") }
        log.info { fail("must not be evaluated") }
        log.warn { fail("must not be evaluated") }
        log.error { fail("must not be evaluated") }
        log.warn(RuntimeException("cause")) { fail("must not be evaluated") }
        log.error(RuntimeException("cause")) { fail("must not be evaluated") }
    }
}
