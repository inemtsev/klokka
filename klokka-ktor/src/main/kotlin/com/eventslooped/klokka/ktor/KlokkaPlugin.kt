@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.ktor

import com.eventslooped.klokka.runtime.KlokkaRole
import com.eventslooped.klokka.runtime.KlokkaRuntime
import com.eventslooped.klokka.runtime.KlokkaSettings
import com.eventslooped.klokka.store.InMemoryJobStore
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationPlugin
import io.ktor.server.application.ApplicationStarted
import io.ktor.server.application.ApplicationStopPreparing
import io.ktor.server.application.createApplicationPlugin
import io.ktor.server.application.hooks.MonitoringEvent
import io.ktor.server.application.log
import io.ktor.util.AttributeKey
import kotlinx.coroutines.runBlocking
import kotlin.time.ExperimentalTime

/** Attribute key the plugin stores its built [KlokkaRuntime] under, on the installing [Application]. */
private val KlokkaRuntimeKey: AttributeKey<KlokkaRuntime> = AttributeKey("KlokkaRuntime")

/**
 * Ktor glue for the framework-free [KlokkaRuntime]: builds a [KlokkaSettings] and
 * [KlokkaRuntime] from the [KlokkaConfig] DSL, starts the runtime's worker machinery once
 * the application has finished starting, and drains it as the application begins shutting
 * down.
 *
 * ```
 * install(Klokka) {
 *     queue("critical", concurrency = 4)
 *     queue("default")
 *     handle(myJobType) { payload -> ... }
 * }
 * ```
 *
 * Access the built runtime from application code via [Application.klokka].
 */
public val Klokka: ApplicationPlugin<KlokkaConfig> =
    createApplicationPlugin(name = "Klokka", createConfiguration = ::KlokkaConfig) {
        val config = pluginConfig
        val settings =
            KlokkaSettings(
                queues = config.queues(),
                role = config.role,
                defaultRetry = config.defaultRetry,
                codec = config.codec,
                clock = config.clock,
                lease = config.lease,
                heartbeatInterval = config.heartbeatInterval,
                pollInterval = config.pollInterval,
                claimBatch = config.claimBatch,
                retention = config.retention,
                sweepInterval = config.sweepInterval,
                drainTimeout = config.drainTimeout,
                lateThreshold = config.lateThreshold,
                workerId = config.workerId,
            )
        val runtime = KlokkaRuntime(config.store, config.registry, settings)
        application.attributes.put(KlokkaRuntimeKey, runtime)

        // The in-memory store loses all state on restart (issue #29). Warn once outside
        // development mode, where that tradeoff is expected rather than a footgun.
        if (config.store is InMemoryJobStore && !application.developmentMode) {
            application.log.warn(
                "Klokka is configured with the in-memory job store: enqueued and in-flight " +
                    "jobs will not survive an application restart. Configure a persistent " +
                    "JobStore before deploying to production.",
            )
        }

        // A handler bound for a kind whose default queue this process never drains can only run
        // if every enqueue overrides the queue. In Role.Both the same process enqueues and drains,
        // so that is almost always the "declared critical, enqueued onto default" mistake; a Worker
        // fleet may bind every handler and drain a deliberate subset, so it is not warned.
        if (config.role == KlokkaRole.Both) {
            config.boundKindsOnUndrainedQueues().forEach { (queue, kinds) ->
                application.log.warn(
                    "Klokka: handlers are bound for kinds $kinds whose default queue '${queue.value}' is not " +
                        "declared in this process, so those jobs will not be claimed here unless an enqueue " +
                        "overrides the queue. Declare queue(\"${queue.value}\") or change the JobType's queue.",
                )
            }
        }

        on(MonitoringEvent(ApplicationStarted)) { app ->
            runtime.start(app)
        }

        // Shutdown path: ApplicationStopPreparing fires while application services (routing,
        // plugins) are still usable, before ApplicationStopping tears them down. The hook
        // handler itself is not a suspend function, so draining requires runBlocking here.
        on(MonitoringEvent(ApplicationStopPreparing)) {
            runBlocking { runtime.drain() }
        }
    }

/** The [KlokkaRuntime] built by [Klokka]. */
public val Application.klokka: KlokkaRuntime
    get() =
        attributes.getOrNull(KlokkaRuntimeKey)
            ?: error("Klokka plugin is not installed: call install(Klokka) before accessing Application.klokka.")
