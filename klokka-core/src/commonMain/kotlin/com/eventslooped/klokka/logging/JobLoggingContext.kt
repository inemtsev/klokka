package com.eventslooped.klokka.logging

/**
 * Runs [block] with [fields] attached to the platform's diagnostic logging context, so every
 * log line written inside it, by the handler or by any library beneath it, carries the job's
 * identity. On the JVM this is slf4j MDC, propagated across suspension points. Platforms
 * without such a facility run [block] unchanged.
 */
internal expect suspend fun <T> withJobLoggingContext(fields: Map<String, String>, block: suspend () -> T): T
