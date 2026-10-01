package com.eventslooped.klokka.logging

import org.slf4j.ILoggerFactory
import org.slf4j.IMarkerFactory
import org.slf4j.helpers.BasicMDCAdapter
import org.slf4j.helpers.BasicMarkerFactory
import org.slf4j.helpers.NOPLoggerFactory
import org.slf4j.spi.MDCAdapter
import org.slf4j.spi.SLF4JServiceProvider

/**
 * Test-only slf4j binding: every logger is disabled (NOP), but MDC is a real thread-local
 * [BasicMDCAdapter]. Without any binding slf4j's MDC is a no-op and could not be asserted on.
 */
public class TestSlf4jServiceProvider : SLF4JServiceProvider {
    private val loggerFactory = NOPLoggerFactory()
    private val markerFactory = BasicMarkerFactory()
    private val mdcAdapter = BasicMDCAdapter()

    override fun getLoggerFactory(): ILoggerFactory = loggerFactory

    override fun getMarkerFactory(): IMarkerFactory = markerFactory

    override fun getMDCAdapter(): MDCAdapter = mdcAdapter

    override fun getRequestedApiVersion(): String = "2.0.99"

    override fun initialize() {
        // Nothing to set up.
    }
}
