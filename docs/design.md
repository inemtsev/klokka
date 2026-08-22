# Klokka: Design Document

| | |
|---|---|
| **Status** | Draft, open for community feedback |
| **Author** | [@inemtsev](https://github.com/inemtsev) |
| **Date** | 2026-08-12 |
| **Tracking** | [#1](https://github.com/inemtsev/klokka/issues/1) |
| **License** | Apache-2.0 |

**Klokka** (Norwegian for "the clock") is a persistent background-jobs plugin for [Ktor](https://ktor.io): a coroutines-native job queue and scheduler with typed suspend handlers, at-least-once execution on your existing database, transactional enqueue, and a live dashboard mounted in your own application. In Norwegian, something reliable "går som ei klokke": it runs like a clock. That is the promise.

This document is the public version of the project's design proposal. It exists to collect objections before code hardens. If you see a flaw in the core model, the storage SPI, or the semantics contract, please open an issue.

---

## 1. Motivation

Ktor has no background-jobs story, and the gap has been on record for a long time:

- In December 2019, a JetBrains engineer answered [ktorio/ktor#1530](https://github.com/ktorio/ktor/issues/1530) with: *"Currently, there is no scheduling feature in ktor. So you may simply launch a coroutine in the application scope with a loop inside."* That is still the official answer.
- [KTOR-6633](https://youtrack.jetbrains.com/issue/KTOR-6633), the YouTrack feature request for job scheduling, has sat untriaged since January 2024, and background jobs do not appear on the Ktor roadmap.
- The official plugin registry contains one scheduling plugin, which coordinates startup-defined cron tasks with a distributed lock. It has no runtime enqueue, no payloads, no retries, and no dashboard.
- Everything else is a workaround: `launch { while(true) { delay(...) } }` loops that break at two replicas or one restart, hand-wired Quartz (a blocking Java API bolted onto a coroutine-first framework), or JobRunr (blocking job model, LGPL, and correctness features such as job timeouts, missed-job catch-up, and transactional enqueue gated behind a paid tier).

The gap is wider than Ktor. No durable job runner anywhere on the JVM executes jobs as suspend functions. Meanwhile the demand for "the Sidekiq/Hangfire of the JVM" resurfaces every year in community threads. Klokka's design is informed by a study of the systems that won and lost in neighboring ecosystems: Hangfire, Quartz and Quartz.NET, JobRunr, db-scheduler, ShedLock, Coravel, TickerQ, Wolverine, NServiceBus, Sidekiq, Oban, and River. Section 13 records what was borrowed and what was deliberately avoided.

## 2. Goals and non-goals

**Goals**

- Fire-and-forget, delayed, and recurring (cron and interval) jobs as **one durable model**: a recurring job is a job that reschedules itself. Same retry, claim, and observability semantics for all three.
- **Typed suspend handlers** with `@Serializable` payloads.
- **At-least-once execution** on the developer's existing database, with retries, dead-lettering, and safe multi-instance coordination.
- **Transactional enqueue**: the job exists if and only if the surrounding transaction commits.
- Continuations ("run B after A succeeds") and atomic batch enqueue.
- A **dashboard** mounted as routes in the host application, free forever.
- First-class observability: lifecycle events as a `Flow`, optional Micrometer and OpenTelemetry modules.

**Non-goals, permanently**

- Workflow orchestration and step-level durable execution (Temporal's problem).
- Message brokering (Kafka's problem).
- Exactly-once execution. Nobody honestly has it; we will not pretend to.

Every successful library in this space owns one clear slice and states plainly what it does not do. Klokka stops at continuations and batches.

## 3. Design principles

1. **Rails, not cages.** In Ktor's spirit: well-thought-through defaults everywhere, constraints nowhere. Every default is replaceable. Klokka never decides your database, DI framework, deployment shape, or alerting strategy.
2. **Progressive disclosure.** The simple case is a lambda. The robust case is an interface. Both ride the same seams, so growing from one to the other is refactoring, never rewriting.
3. **Idiomatic Kotlin.** Suspend-first, sealed types for states and policies, `kotlin.time` durations, `Flow` for events, value classes for identifiers, and a DSL where a DSL genuinely reads better than builders.
4. **Honesty as API.** The semantics contract (at-least-once, misfire behavior, what survives a crash) is stated in types and documentation, never discovered in production.

Two opinions are kept deliberately, because they are correctness constraints rather than taste:

- **Typed job descriptors, never serialized lambdas.** Kotlin lambdas do not compile to anything durably serializable; JobRunr's Kotlin users hit this wall directly. A job is a stable string kind plus a `@Serializable` payload. This survives renames, package moves, and rolling deploys.
- **At-least-once, said out loud**, together with the tools that make it safe: idempotency keys, attempt counters, and the scheduled fire time in the handler context.

## 4. Programming model

Three levels. Each is a strict superset of the previous one.

### Level 0: one file, zero infrastructure

In-memory store, loud startup warning outside dev config.

```kotlin
val SendWelcome = JobType("send-welcome-email", WelcomeEmail.serializer())

@Serializable
data class WelcomeEmail(val userId: Long, val locale: String)

fun Application.module() {
    install(Klokka) {
        handle(SendWelcome) { payload ->          // this: JobContext, suspend
            emailService.sendWelcome(payload.userId, payload.locale)
        }
        recurring("session-cleanup", every = 1.hours) {
            sessions.purgeExpired()
        }
    }
    routing {
        post("/signup") {
            val user = userService.create(call.receive())
            klokka.enqueue(SendWelcome, WelcomeEmail(user.id, user.locale))
            call.respond(HttpStatusCode.Created)
        }
    }
}
```

### Level 1: production

Swap the store, keep everything else. The dashboard is a module mounted on your routes, behind your existing authentication. The auth parameter is required, not defaultable (see section 8).

```kotlin
install(Klokka) {
    store = PostgresJobStore(dataSource)          // artifact: klokka-postgres
    defaultRetry = RetryPolicy.exponential(
        base = 10.seconds, factor = 2.0, maxAttempts = 8, jitter = 0.2
    )
    role = Role.Both                               // or Producer / Worker
    queue("critical") { concurrency = 16 }         // declared order = drain priority
    queue("email")    { concurrency = 32 }
    queue("reports")  { concurrency = 4; dispatcher = Dispatchers.IO }
}

routing {
    authenticate("admin") {
        klokkaDashboard("/admin/jobs")   // inherits this auth block; fails closed if mounted bare
    }
}

klokka.schedule(SendReceipt, receipt, at = Clock.System.now() + 10.minutes)

klokka.recurring("daily-digest", DigestJob, payload = DigestConfig(...),
    cron = "0 9 * * 1-5", zone = TimeZone.of("America/Toronto"),
    misfire = MisfirePolicy.FireOnce)              // or Skip, or CatchUp(atMost = 3)
```

### Level 2: robust

The lambda from level 0 was always a `JobHandler` underneath. When a handler needs its own dependencies, lifecycle, or tests, it graduates to a class with no change to the enqueue sites.

```kotlin
class ReportHandler(private val reports: ReportService) : JobHandler<ReportRequest> {
    override suspend fun JobContext.execute(payload: ReportRequest) {
        // scheduledFor answers "which window does this run cover"
        val window = scheduledFor.minus(1.days)..scheduledFor
        reports.generate(payload, window) { pct -> progress(pct) }
    }
}

// Transactional enqueue: the job exists iff the transaction commits.
// The transaction is passed explicitly, because thread-local transaction
// state does not survive coroutine thread hops.
newSuspendedTransaction(db) {
    orders.insert(order)
    klokka.enqueue(SendReceipt, ReceiptFor(order.id), tx = this,
        options { uniqueKey = "receipt-${order.id}" })   // dedup, store-enforced
}

// Continuations and atomic batches
val export = klokka.enqueue(ExportData, request)
klokka.continueWith(export, NotifyUser, NotifyPayload(user.id), on = Outcome.Succeeded)

// Lifecycle events as a Flow: build your own alerting
klokka.events
    .filterIsInstance<JobEvent.DeadLettered>()
    .onEach { pagerDuty.alert(it.kind, it.error, it.jobId) }
    .launchIn(scope)
```

## 5. Semantics contract

### One durable model

A **job run** is the durable unit: a row with kind, payload, queue, state, attempt count, timestamps, and lease metadata. A **schedule** is a separate small entity whose only responsibility is emitting job runs. This is the lesson of Hangfire versus Quartz: Quartz persists recurrence rules and therefore cannot show job history; Hangfire persists invocations and layers recurrence on top. Klokka builds both, layered that way, so job history is a query rather than a missing feature.

### States

```
Scheduled -> Enqueued -> Running -> Succeeded
                                  | Failed (retrying)
                                  | DeadLettered
                                  | Cancelled
```

Modeled as a sealed hierarchy. Completion records whether the run was on time or late, so lateness is visible without log archaeology.

### Claiming and liveness

- Workers claim due rows with `SELECT ... FOR UPDATE SKIP LOCKED` and hold a **lease**, extended by heartbeat while the job runs.
- Workers claim only kinds they bind. A job whose kind no live worker binds waits in Enqueued; it is never dead-lettered by a worker that cannot run it, which is what keeps a rolling deploy from dead-lettering a kind only the newer version knows.
- All timestamp comparisons use **database time**. Node clocks are never trusted; both ShedLock and Quartz document clock skew as a production landmine.
- Every claim carries a monotonically increasing **fencing version**, so a zombie worker resurrected after a pause cannot overwrite newer state.
- A worker that misses its heartbeat budget has its in-flight jobs revived according to policy.
- The Postgres store pushes wake-ups via `LISTEN`/`NOTIFY`, so enqueue-to-start latency is not a polling knob. A slow poll remains as the safety net. Distributed claiming correctness is enforced by single atomic SQL statements and covered by multi-node integration tests; an in-process check is never trusted for a cross-node guarantee.

### Execution guarantee

**At-least-once.** A job that was claimed and whose worker died will run again. Handlers must be written to tolerate re-execution. Klokka provides the tools: an optional `uniqueKey` with a store-enforced partial unique index, the attempt number and scheduled fire time in `JobContext`, and a `singleton(key)` overlap guard.

### Retries and failure

`RetryPolicy` is one function:

```kotlin
fun interface RetryPolicy {
    fun nextDelay(attempt: Int, error: Throwable): Duration?   // null = give up
}
```

Built-ins cover exponential backoff with jitter and explicit interval lists. A `NonRetryable` marker interface short-circuits the policy. Exhaustion produces a **dead-letter record** rich enough to triage: payload, exception chain, attempt history, correlation and trace IDs. Two rules borrowed from systems that learned them expensively:

- Deserialization failures skip retries entirely and dead-letter immediately. Retrying a payload you cannot parse is pure waste.
- Retries are bounded and the dead-letter queue is an escalation channel to a human, not an infinite loop.

### Scheduling correctness

- **Misfire policy is explicit and per-schedule**: `Skip` (a missed billing run must never double-fire), `FireOnce` (catch up a metrics rollup once), or `CatchUp(atMost = n)` (bounded backfill), gated by a misfire threshold so a GC pause is not a misfire.
- **Timezone is stored per schedule** and next-fire times are recomputed against live zone rules, with documented behavior for nonexistent and ambiguous local times around DST transitions.
- The typed DSL (`every(5.minutes)`, `dailyAt(9, 0, zone)`) is the primary API. Five-field standard cron is the escape hatch, seconds opt-in, validated at install time with a helpful error. Quartz-style `L`/`W`/`#` operators are intentionally out of scope: complex business calendars ("last business day of the month") are better served by type-safe DSL builders than by opaque cron string extensions.
- **Priority is queue ordering**, Hangfire's model: workers drain queues in the order they are declared (`critical` before `default`). There are no per-job integer priorities; they complicate `SKIP LOCKED` claim queries and ruin partial-index strategies at scale. Unlike Hangfire, where queue-order semantics silently differ by storage backend, the ordering contract is part of the SPI and enforced by the conformance kit.
- The handler context exposes **`scheduledFor`**, the intended fire time, so "which window of data does this run cover" is answerable and idempotency keys are derivable.
- `triggerNow()` runs a schedule immediately without shifting it. An anti-overlap mode schedules the next run only after the current one completes.

### Lifecycle

The runtime owns a `SupervisorJob` scoped to the Ktor application. Worker loops restart with backoff on crash. Shutdown wires to `ApplicationStopPreparing`: stop claiming, drain within a configurable grace period aligned to the container's SIGTERM window, requeue whatever remains. Retention runs as a built-in recurring job on Klokka's own machinery: succeeded runs expire after roughly 24 hours, dead-lettered runs are kept long and never silently deleted, both configurable.

## 6. Architecture

A deliberately small core with capability-typed extension seams. The core is completely store-agnostic: the `JobStore` SPI plus the conformance kit is the contract, and you can bring your own backing from day one. Postgres ships as the reference implementation and golden path.

```kotlin
interface JobStore {
    suspend fun enqueue(jobs: List<NewJob>): List<JobId>
    suspend fun claim(queues: List<QueueName>,     // declared order = priority
                      kinds: Set<String>,           // only kinds this worker binds
                      limit: Int, lease: Duration, worker: WorkerId): List<ClaimedJob>
    suspend fun heartbeat(ids: List<JobId>, worker: WorkerId, extend: Duration)
    suspend fun transition(id: JobId, from: JobState, to: JobState): Boolean
    suspend fun dueSchedules(now: Instant, limit: Int): List<ScheduleFire>
    suspend fun sweep(retention: RetentionPolicy)
}

// Optional capabilities: detected with `is`, type-safe, no string flags
interface PushCapableStore : JobStore { fun wakeups(): Flow<Unit> }
interface TransactionalStore<TX> : JobStore {
    suspend fun enqueue(tx: TX, jobs: List<NewJob>): List<JobId>
}
```

Execution wraps in an interceptor pipeline with stable, documented ordering: metrics, tracing, MDC, user interceptors, then the handler, inside `withTimeout(lease)`.

### Modules

| Artifact | Contents | Dependencies |
|---|---|---|
| `klokka-core` | Framework-free engine: runtime, SPI, in-memory store, retry and misfire policies, events Flow. KMP-friendly common code; JVM the only shipped target initially. Usable from any JVM main function | kotlinx-coroutines, kotlinx-serialization, kotlinx-datetime (common); slf4j (jvm) |
| `klokka-ktor` | Ktor glue: `install(Klokka)` config DSL, `Application.klokka` accessor, lifecycle wiring (start on ApplicationStarted, drain on ApplicationStopPreparing), in-memory-store production warning | klokka-core, ktor-server-core |
| `klokka-postgres` | Postgres store: SKIP LOCKED, LISTEN/NOTIFY, partial indexes, bundled versioned SQL migrations | JDBC only |
| `klokka-exposed` | `TransactionalStore` bridge for Exposed transactions | Exposed |
| `klokka-dashboard` | Routes, SSE live updates, per-job logs, read-only mode, page-extension hook | ktor-server-sse |
| `klokka-micrometer`, `klokka-opentelemetry` | Metrics binder; trace-context capture at enqueue, restore at execute | optional |
| `klokka-tck` | Conformance suite any third-party store must pass, including multi-node claim atomicity tests | test scope |
| `klokka-ksp` (later) | Optional compile-time checks: duplicate kinds, unregistered handlers | KSP |

Maven coordinates: `com.eventslooped:klokka-*`.

Klokka works beautifully with [Exposed](https://github.com/JetBrains/Exposed) and does not require it: the docs and quickstart lead with Exposed examples, while the store internals stay on plain JDBC. The six store queries are raw SQL either way, and Exposed's thread-local transaction model is precisely the coroutine hazard the runtime is designed to avoid.

Blocking JDBC runs on a bounded IO dispatcher inside the store. A fully non-blocking R2DBC store is deliberately not part of v1: the R2DBC ecosystem is fragmented, and JDBC on a properly sized dispatcher covers the overwhelming majority of Kotlin backend deployments. The SPI is storage-agnostic, so a community R2DBC store can pass the conformance kit later if demand materializes.

`klokka-core` is structured as a Kotlin Multiplatform module even though the JVM is the only shipped target initially: the common source set holds the model, DSL, SPI, and policies in pure Kotlin (kotlinx-coroutines, kotlinx-serialization, kotlinx-datetime), while JVM-specific IO and slf4j logging live in the `jvm` source set behind small expect/actual seams. This costs almost nothing today and leaves the door open for Native or JS/Wasm targets later.

### Deployment shapes

`role = Role.Producer / Role.Worker / Role.Both`. The same code runs as a web app that also processes jobs, a web app that only enqueues, or a dedicated worker fleet. Klokka does not opinionate your topology.

## 7. Time

Timestamps use `kotlin.time.Instant` and `Clock` from the standard library; zone-aware schedules use kotlinx-datetime `TimeZone`. An internal clock abstraction keeps the SPI decoupled so the choice can be revisited cheaply if JDBC interop friction appears. Handlers can and should treat the injected `scheduledFor` and store-derived times as authoritative rather than calling wall-clock functions.

## 8. Dashboard

The dashboard ships in the first registry release, free forever. The evidence from every neighboring ecosystem is unambiguous: the bundled dashboard is the single most decisive adoption factor for job systems, and first impressions of "has a dashboard / does not" never get revised.

- Mounted through a `Route` extension, `klokkaDashboard()`, so it natively inherits whatever `authenticate { }` block wraps it: your host, your TLS, your providers, zero parallel auth configuration.
- **It fails closed.** If no authentication is in effect on the mounting route, the dashboard refuses to serve, logs exactly why at startup, and requires an explicit `allowAnonymous = true` to override (for localhost-only dev setups). Hangfire once shipped a release with no default dashboard authorization at all (CVE-2021-41238, CVSS 8.6); Klokka keeps that mistake unrepresentable while staying idiomatic. Read-only mode is one flag.
- Server-rendered HTML with SSE live updates. No Node build chain, no embedded SPA.
- Views: jobs by state, job detail with payload, attempt timeline and stack traces, per-job log capture via job-scoped MDC, retries, dead letters grouped by exception type, schedules with next-N fire times and trigger-now, workers with heartbeats, queue depths.
- Actions: requeue, delete, trigger now, pause queue.
- A page-extension hook so third parties can add views without forking.

## 9. Observability

Layered so the core stays unopinionated:

- The core emits a `SharedFlow<JobEvent>` (sealed: Enqueued, Started, Succeeded, Failed, Retried, DeadLettered, ScheduleMisfired) and nothing else. Build any alerting you want from the Flow.
- `klokka-micrometer` turns it into counters, gauges (queue depth, oldest-pending age, enqueue-to-start latency), and duration histograms.
- `klokka-opentelemetry` captures the trace context at enqueue and restores it at execution, so a job's span links to the request that created it.

The failure mode with no error gets first-class treatment: schedules expose a liveness record an external monitor can alert on, and queue wait time is measured separately from execution time, because a job that runs in two seconds after waiting thirty minutes is a thirty-minute outage.

## 10. Delivery plan

| Milestone | Contents | Exit criterion |
|---|---|---|
| **M0: Design validation** | This document published, community feedback collected, namespaces settled | No unresolved objection to the core model or SPI |
| **M1: v0.1 on Maven Central** | Core runtime, in-memory and Postgres stores, enqueue/delay/recurring, retries and dead-letter, graceful drain, events Flow, minimal dashboard, multi-node integration tests | A two-instance demo survives kill -9 mid-job with zero lost jobs |
| **M2: Registry release** | Transactional enqueue (JDBC and Exposed), unique keys, misfire policies, continuations and batches, SSE dashboard with per-job logs, Micrometer and OTel modules, docs site, then the ktor-plugin-registry PR | Listed on start.ktor.io with the dashboard in the screenshots |
| **M3: Ecosystem** | TCK published, KSP module, community store outreach, honest benchmarks | At least one third-party store passing the TCK |

The full backlog lives in [the issue tracker](https://github.com/inemtsev/klokka/issues).

## 11. Licensing promise

Klokka is Apache-2.0, and the following will never move behind a paid tier: the queue, delayed and recurring jobs, retries with backoff, dead-lettering, unique and idempotency keys, per-job timeouts, transactional enqueue, misfire policies including catch-up of missed recurring jobs, continuations, the Postgres and in-memory stores, and the dashboard.

This list is deliberate. In neighboring ecosystems, several of these exact features are sold as premium add-ons, and mid-life relicensing has repeatedly burned communities. If Klokka ever has a commercial layer, it will be operational surface around the library (hosted control planes, SLA alerting, support), never correctness features, and never retroactive.

## 12. Decisions log

| Decision | Choice | Date |
|---|---|---|
| Name | Klokka (Norwegian: "the clock") | 2026-08-12 |
| Maven group | `com.eventslooped` (existing verified umbrella namespace) | 2026-08-12 |
| Storage stance | Store-agnostic core; Postgres reference implementation on plain JDBC | 2026-08-12 |
| Exposed | Recommended in docs and quickstart, never required | 2026-08-12 |
| Dashboard rendering | Server-rendered HTML + SSE | 2026-08-12 |
| Recurring definitions | Code-defined in v0.1; runtime-mutable later if demanded | 2026-08-12 |
| Time API | kotlin.time Instant/Clock + kotlinx-datetime TimeZone | 2026-08-12 |
| License | Apache-2.0 with the section 11 promise | 2026-08-12 |
| Cron dialect | Standard 5-field, seconds opt-in; no Quartz `L`/`W`/`#`; DSL builders for complex calendars | 2026-08-12 |
| R2DBC | Not in v1; JDBC on a bounded IO dispatcher; SPI leaves the door open | 2026-08-12 |
| Dashboard auth | `Route`-receiver mounting inheriting `authenticate { }`, fail-closed without auth | 2026-08-12 |
| Multiplatform | KMP-friendly common core, JVM the only shipped target initially | 2026-08-12 |
| Priorities | Queue-ordering model (Hangfire style); no per-job integer priorities | 2026-08-12 |
| Module layering | klokka-core is framework-free; all Ktor glue lives in the separate klokka-ktor artifact | 2026-08-12 |
| Retry overrides | Per-kind at handler registration, never per enqueue: retry policies are functions and cannot be persisted across nodes | 2026-08-12 |
| Handler binding | One verb, `handle`, overloaded for a lambda and a `JobHandler` object; no separate `register` | 2026-08-22 |
| Default queue | Per-kind default on the `JobType` descriptor (`jobType(kind, queue = ...)`), because the producer writes the queue onto the row and the descriptor is what producer and worker code share; `JobOptions.queue` overrides per enqueue; handler registration never names a queue | 2026-08-22 |
| Claim by bound kinds | `claim` takes the worker's bound kinds and returns only those rows; a kind bound nowhere waits in Enqueued instead of being dead-lettered by a worker that cannot run it (rolling deploys, shared queues across heterogeneous fleets); #33 | 2026-08-22 |

## 13. Alternatives considered

**Lambda-capture enqueue (Hangfire-style `enqueue { svc.send(email) }`).** Rejected. Hangfire achieves it with .NET expression trees, JobRunr with ASM bytecode analysis, and both carry documented restrictions and refactoring hazards; a renamed method silently orphans queued jobs. Kotlin lambdas are not durably serializable at all, which JobRunr's own Kotlin support demonstrates. Typed descriptors with stable string kinds are the consensus landing zone of both ecosystems (JobRunr's recommended JobRequest pattern, River's kind strings, TickerQ's compile-time names).

**Building the store on an ORM.** Rejected. The store needs `SELECT FOR UPDATE SKIP LOCKED`, `LISTEN`/`NOTIFY`, and partial indexes: raw SQL either way. An ORM dependency in the mandatory path would also violate principle 1.

**Lock-only design (ShedLock-style).** Rejected as the core model. A lock deduplicates; it does not retry, recover, or record. ShedLock's own README says it best: it is not and will never be a scheduler.

**Polling-only storage access.** Rejected. Polling is the single most-complained-about operational property of Hangfire's default storage. Push with poll fallback costs little and removes the whole complaint class.

**A workflow engine.** Out of scope permanently. Durable execution (Temporal, Restate, DBOS) is a different product with a different failure model. Klokka stops at continuations and batches, and says so.

**String-flag storage capabilities (Hangfire's `HasFeature("Job.Queue")`).** Replaced with marker interfaces the compiler can check.

## 14. Resolved design questions

These were open questions in the first draft; all five are now decided.

1. **Cron dialect.** Standard 5-field cron, seconds opt-in. Quartz-style `L`/`W`/`#` operators will not be implemented: complex business schedules are better served by the type-safe DSL builders than by opaque cron string extensions.
2. **R2DBC.** Not prioritized for v1. JDBC on a bounded `Dispatchers.IO` pool is proven and covers the vast majority of Kotlin backend deployments. The SPI stays storage-agnostic so a community R2DBC store can arrive later.
3. **Dashboard authorization.** Mounted via a `Route` extension receiver so it natively inherits the surrounding `authenticate { }` block, with a fail-closed guard: no auth in effect means the dashboard refuses to serve unless explicitly overridden.
4. **Multiplatform.** `klokka-core` stays KMP-friendly (pure Kotlin plus kotlinx-coroutines, kotlinx-datetime, kotlinx-serialization in common code, JVM IO isolated behind expect/actual). JVM remains the only shipped target until real demand appears.
5. **Priorities.** Queue-ordering (Hangfire's model): workers drain queues in declared order. No per-job integer priorities, which complicate `SKIP LOCKED` claims and indexing at scale. The ordering contract is uniform across stores and enforced by the TCK.

## 15. Prior art

Klokka stands on a detailed study of: [Hangfire](https://www.hangfire.io) (the dashboard lesson, the storage SPI moat, the sliding invisibility timeout), [Quartz](https://www.quartz-scheduler.org)/[Quartz.NET](https://www.quartz-scheduler.net) (misfire vocabulary, and the cautionary tales: cross-node guarantees must be atomic SQL, wire formats must be versioned), [JobRunr](https://www.jobrunr.io) (the JVM port of Hangfire, and the strategic cost of paywalling correctness), [db-scheduler](https://github.com/kagkarlsson/db-scheduler) (single-table minimalism, SKIP LOCKED throughput, typed task descriptors), [ShedLock](https://github.com/lukas-krecan/ShedLock) (honest documentation of lease limits and clock skew), [Coravel](https://docs.coravel.net) (the fluent DSL bar), [TickerQ](https://tickerq.net) (compile-time registration, retry intervals as data, lateness as a first-class state), [Wolverine](https://wolverinefx.net) (transactional outbox as headline feature), [NServiceBus](https://particular.net) (bounded retries, the error queue as human escalation), [Sidekiq](https://sidekiq.org) (at-least-once honesty), [Oban](https://oban.pro) (telemetry events, bounded retention as a feature), and [River](https://riverqueue.com) (transactional enqueue guarantees, stable kind strings).
