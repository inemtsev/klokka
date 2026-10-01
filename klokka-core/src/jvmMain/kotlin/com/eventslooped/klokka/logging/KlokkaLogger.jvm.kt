package com.eventslooped.klokka.logging

import org.slf4j.Logger
import org.slf4j.LoggerFactory

internal actual fun klokkaLogger(name: String): KlokkaLogger = Slf4jKlokkaLogger(LoggerFactory.getLogger(name))

private class Slf4jKlokkaLogger(private val delegate: Logger) : KlokkaLogger {
    override fun debug(message: () -> String) {
        if (delegate.isDebugEnabled) delegate.debug(message())
    }

    override fun info(message: () -> String) {
        if (delegate.isInfoEnabled) delegate.info(message())
    }

    override fun warn(cause: Throwable?, message: () -> String) {
        if (delegate.isWarnEnabled) delegate.warn(message(), cause)
    }

    override fun error(cause: Throwable?, message: () -> String) {
        if (delegate.isErrorEnabled) delegate.error(message(), cause)
    }
}
