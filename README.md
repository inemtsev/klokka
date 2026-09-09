# Klokka

Persistent background jobs for Ktor. A coroutines-native queue and scheduler with typed suspend handlers, retries, transactional enqueue, and a live dashboard, on the database you already run. In Norwegian, something reliable "går som ei klokke": it runs like a clock.

> **Status: pre-release.** The core runtime, recurring jobs, the in-memory store, and the Ktor plugin work and are tested; nothing is on Maven Central yet. The Postgres store and the dashboard are the current milestone. See [Roadmap](#roadmap) and the [design document](docs/design.md).

## Why

- **Suspend handlers, typed payloads.** A job is a stable string kind plus a `@Serializable` payload; a handler is a `suspend` function with a `JobContext` receiver. No serialized lambdas, no identity derived from class names, so renames and rolling deploys are safe.
- **Your database, your process.** Jobs live in a store you own (Postgres is the reference implementation); workers are coroutines inside your Ktor application or a dedicated worker fleet. No broker, no sidecar.
- **At-least-once, said out loud.** A job whose worker dies runs again, and the API gives you what you need to make that safe: the attempt number, the scheduled fire time, unique keys, and a `NonRetryable` marker for errors that must not retry.

## Quick start

One file, in-memory store, one job. The snippets below are adapted from the executable samples under `klokka-ktor/src/test/kotlin/com/eventslooped/klokka/ktor/samples/`.

```kotlin
@Serializable
data class WelcomeEmail(val userId: Long, val locale: String)

val SendWelcome = jobType<WelcomeEmail>("users.send-welcome")

fun Application.module() {
    install(Klokka) {
        handle(SendWelcome) { payload ->              // this: JobContext, suspend
            emailService.sendWelcome(payload.userId, payload.locale)
        }
    }
    routing {
        post("/signup") {
            val user = userService.create(call.receive<SignupRequest>())
            klokka.enqueue(SendWelcome, WelcomeEmail(user.id, user.locale))
            call.respond(HttpStatusCode.Created)
        }
    }
}
```

With no store configured, `install(Klokka)` uses the in-memory store and logs a warning outside development mode: jobs do not survive a restart.

## Growing up without rewriting

Queues, a default queue per job kind, a handler class with dependencies, a per-kind retry policy, and an error that must never retry. The enqueue sites do not change.

```kotlin
val SendOrderConfirmation =
    jobType<OrderConfirmation>("orders.send-confirmation", queue = QueueName("email"))

/** A malformed address never succeeds; NonRetryable dead-letters it at once. */
class InvalidRecipientException(address: String) :
    RuntimeException("Invalid recipient: $address"), NonRetryable

class OrderConfirmationHandler(private val email: EmailService) : JobHandler<OrderConfirmation> {
    override suspend fun JobContext.execute(payload: OrderConfirmation) {
        if ('@' !in payload.recipient) throw InvalidRecipientException(payload.recipient)
        // At-least-once: a business-level idempotency key turns a redelivery into a no-op at the provider.
        email.send(payload.recipient, "Order ${payload.orderId} confirmed",
            idempotencyKey = "order-confirmation-${payload.orderId}")
    }
}

install(Klokka) {
    queue("critical", concurrency = 8)        // declared order is drain priority
    queue("email", concurrency = 32)
    queue("default", concurrency = 16)

    handle(
        SendOrderConfirmation,
        OrderConfirmationHandler(emailService),
        retry = RetryPolicy.intervals(listOf(30.seconds, 2.minutes, 10.minutes)),
    )
}

// Lands on "email" because the JobType says so; options carry only per-call data.
klokka.enqueue(SendOrderConfirmation, confirmation, options { uniqueKey = "order-confirmation-$orderId" })

// Override the queue for one job, or run one later.
klokka.enqueue(SendOrderConfirmation, vipConfirmation, options { queue = QueueName("critical") })
klokka.schedule(SendReminder, reminder, at = Clock.System.now() + 3.days)
```

The lambda form and the class form are the same `handle`: a lambda is a `JobHandler` underneath, so graduating to a class changes no enqueue site.

## Many jobs

Group each domain's jobs in an extension function on `KlokkaConfig`, the way Ktor routes are grouped on `Route`. The install block stays a table of contents: infrastructure, queues, domains.

```kotlin
fun KlokkaConfig.orderJobs(email: EmailService, inventory: InventoryClient) {
    handle(SendOrderConfirmation, OrderConfirmationHandler(email), retry = RetryPolicy.intervals(listOf(30.seconds, 2.minutes, 10.minutes)))
    handle(ReserveInventory, ReserveInventoryHandler(inventory))
}

install(Klokka) {
    store = InMemoryJobStore()                 // PostgresJobStore(dataSource) once klokka-postgres ships
    role = KlokkaRole.Both                     // Producer on the web tier, Worker on the fleet, same module
    queue("critical", concurrency = 8)
    queue("email", concurrency = 32)
    queue("default", concurrency = 16)
    orderJobs(email, inventory)
    billingJobs(invoices, webhooks)
}
```

Dependencies arrive as parameters, so the pattern works the same with Koin, Ktor's built-in DI, or none.

## Watching it run

Lifecycle events are a `SharedFlow<JobEvent>`: `Enqueued`, `Started`, `Succeeded`, `FailedAttempt`, `DeadLettered`, `Cancelled`, `ScheduleMisfired`. Dead letters are an escalation to a human, so wire them to whatever alerting you already have.

```kotlin
klokka.events
    .filterIsInstance<JobEvent.DeadLettered>()
    .onEach { pagerDuty.alert(it.kind, it.error, it.jobId) }
    .launchIn(this)   // Application scope
```

## Semantics, briefly

- **At-least-once.** Never exactly-once; nobody honestly has it. Handlers must tolerate re-execution.
- **Queue order is priority.** Workers drain queues in the order they are declared. There are no per-job priority numbers.
- **Workers claim only kinds they bind.** A job whose kind no live worker binds waits in the store instead of being dead-lettered by a worker that cannot run it, so a rolling deploy that adds a kind is safe.
- **Claims are leases.** Database time decides due-ness, heartbeats extend leases, and every claim carries a fencing token so a zombie worker cannot overwrite newer state.
- **Retries are per kind, at binding time.** `RetryPolicy.exponential(...)`, `RetryPolicy.intervals(...)`, or `RetryPolicy.None`; a `NonRetryable` exception or an undecodable payload dead-letters immediately.
- **The producer chooses the queue.** `JobOptions.queue` if set, otherwise `JobType.queue`. Handler binding never names a queue; a worker only declares which queues it drains.
- **Recurring jobs are code-defined schedules.** `recurring("nightly-rollup", type, payload, schedule = dailyAt(2, 30, zone))`, with `every(...)` and standard 5-field `cron(...)` as the other schedule forms and a zero-payload lambda form for the simplest cases. Missed fires follow an explicit per-schedule `MisfirePolicy` (`FireOnce`, `Skip`, `CatchUp(atMost)`); `OverlapPolicy.SkipIfRunning` keeps a slow run from stacking; `triggerNow(id)` runs a schedule on demand without shifting its cadence. The handler's `scheduledFor` is the intended fire time, so data windows and idempotency keys derive from the schedule, not the wall clock.

## Modules

| Artifact | Contents | Status |
|---|---|---|
| `klokka-core` | Framework-free engine: runtime, `JobStore` SPI, in-memory store, retry policies, events | working |
| `klokka-ktor` | `install(Klokka)`, `Application.klokka`, lifecycle wiring | working |
| `klokka-postgres` | SKIP LOCKED claiming, LISTEN/NOTIFY wake-ups, bundled migrations | next (M1) |
| `klokka-dashboard` | Server-rendered views with SSE, fails closed without authentication | next (M1 minimal, M2 full) |
| `klokka-exposed`, `klokka-micrometer`, `klokka-opentelemetry`, `klokka-tck`, `klokka-ksp` | Bridges and tooling | later |

Maven coordinates will be `com.eventslooped:klokka-*`. Until the first release, build from source with `./gradlew build`.

## Roadmap

Tracked as GitHub milestones: [M1, v0.1 on Maven Central](https://github.com/inemtsev/klokka/milestone/2) (Postgres store, recurring jobs, minimal dashboard, multi-node tests), [M2, registry release](https://github.com/inemtsev/klokka/milestone/3) (transactional enqueue, unique keys, continuations, SSE dashboard, Micrometer and OpenTelemetry, docs site), [M3, ecosystem](https://github.com/inemtsev/klokka/milestone/4) (TCK, KSP checks, benchmarks). The [design document](docs/design.md) explains the choices and records decisions; objections to the core model or the storage SPI are welcome as issues.

## License

Apache-2.0. The queue, delayed and recurring jobs, retries, dead-lettering, unique keys, timeouts, transactional enqueue, misfire policies, continuations, the Postgres and in-memory stores, and the dashboard will never move behind a paid tier.
