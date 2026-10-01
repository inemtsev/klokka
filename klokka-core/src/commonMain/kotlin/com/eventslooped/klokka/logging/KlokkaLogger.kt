package com.eventslooped.klokka.logging

/** Minimal internal logging facade so common code can log without an slf4j dependency. Messages are lambdas so disabled levels cost nothing. */
internal interface KlokkaLogger {
    fun debug(message: () -> String)

    fun info(message: () -> String)

    fun warn(cause: Throwable? = null, message: () -> String)

    fun error(cause: Throwable? = null, message: () -> String)
}

/** Returns the logger for [name]. On the JVM this is an slf4j logger of that name. */
internal expect fun klokkaLogger(name: String): KlokkaLogger
