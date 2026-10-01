package com.eventslooped.klokka.logging

import kotlinx.coroutines.slf4j.MDCContext
import kotlinx.coroutines.withContext
import org.slf4j.MDC

internal actual suspend fun <T> withJobLoggingContext(fields: Map<String, String>, block: suspend () -> T): T {
    val merged = (MDC.getCopyOfContextMap() ?: emptyMap()) + fields
    return withContext(MDCContext(merged)) { block() }
}
