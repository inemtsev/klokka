@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.ktor.samples

import com.eventslooped.klokka.JobContext
import com.eventslooped.klokka.JobEvent
import com.eventslooped.klokka.JobHandler
import com.eventslooped.klokka.NonRetryable
import com.eventslooped.klokka.QueueName
import com.eventslooped.klokka.RetryPolicy
import com.eventslooped.klokka.jobType
import com.eventslooped.klokka.ktor.Klokka
import com.eventslooped.klokka.ktor.klokka
import com.eventslooped.klokka.options
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.request.receiveText
import io.ktor.server.response.respondText
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.filterIsInstance
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime

// =========================================================================================
// Sample: "create an order, then send a confirmation email in the background".
//
// Everything above the test class is what an application developer would write against the
// current Klokka API. The test class at the bottom runs it end to end through a real Ktor
// test application with the in-memory store, so this file doubles as an executable check
// that the API design holds up for the ordinary case.
// =========================================================================================

// --- 1. The job ---------------------------------------------------------------------------
//
// A job is a stable kind string plus a @Serializable payload. The kind is what the store
// persists and what every node uses to find the handler, so it never derives from a class
// name: renaming OrderConfirmation or moving it to another package changes nothing.
// The queue is the routing default for this kind; enqueue sites only say where a job goes
// when it differs from that.

@Serializable
data class OrderLine(val sku: String, val quantity: Int)

@Serializable
data class OrderConfirmation(
    val orderId: String,
    val recipient: String,
    val locale: String,
    val lines: List<OrderLine>,
)

val SendOrderConfirmation = jobType<OrderConfirmation>("orders.send-confirmation", queue = QueueName("email"))

// --- 2. The imaginary email service and its failure modes --------------------------------

interface EmailService {
    /** [idempotencyKey]: the provider sends one email per key, so a redelivered call is a no-op. */
    suspend fun send(to: String, subject: String, body: String, idempotencyKey: String)
}

/** A malformed address will never succeed. NonRetryable makes Klokka dead-letter it at once. */
class InvalidRecipientException(address: String) :
    RuntimeException("Invalid recipient: $address"),
    NonRetryable

/** Provider down or rate-limiting: an ordinary exception, so the kind's retry policy applies. */
class EmailProviderUnavailable(message: String) : RuntimeException(message)

// --- 3. The handler -----------------------------------------------------------------------
//
// A class rather than a lambda because it has a dependency and deserves its own unit test.
// JobContext is the receiver, so attempt, scheduledFor, jobId are in scope directly.

class OrderConfirmationHandler(private val email: EmailService) : JobHandler<OrderConfirmation> {
    override suspend fun JobContext.execute(payload: OrderConfirmation) {
        if ('@' !in payload.recipient) throw InvalidRecipientException(payload.recipient)

        val subject = "Order ${payload.orderId} confirmed"
        val body =
            buildString {
                appendLine("Thanks for your order (attempt $attempt).")
                payload.lines.forEach { appendLine("${it.quantity} x ${it.sku}") }
            }

        // Klokka is at-least-once: if this worker dies after the provider accepted the email
        // but before the run is marked Succeeded, the job runs again on another worker. A
        // business-level idempotency key (one confirmation per order, ever) turns that second
        // delivery into a no-op at the provider. When no natural key exists, jobId.value is
        // stable across attempts of the same run and works too.
        email.send(payload.recipient, subject, body, idempotencyKey = "order-confirmation-${payload.orderId}")
    }
}

// --- 4. Business logic and Ktor wiring ----------------------------------------------------

@Serializable
data class CreateOrderRequest(val email: String, val locale: String, val lines: List<OrderLine>)

/** Stands in for the real order repository; `create` would be a DB insert. */
class OrderService {
    private var counter = 0

    fun create(request: CreateOrderRequest): String = "ord-${++counter}"
}

/**
 * The application module. Producer and worker in one process (the default role), which is
 * the Level 0 / small-deployment shape; split them with `role = KlokkaRole.Producer` /
 * `KlokkaRole.Worker` later without touching the handler or the route.
 */
fun Application.ordersModule(
    email: EmailService,
    orders: OrderService,
    onDeadLetter: (JobEvent.DeadLettered) -> Unit = {},
) {
    install(Klokka) {
        // Declared order is drain priority. One queue here; add queue("critical") above it
        // later and it is claimed first.
        queue("email", concurrency = 32)

        // Per-kind retry override: three tries spaced out, then dead-letter. Lives at
        // registration (identical on every node), never per enqueue.
        handle(
            SendOrderConfirmation,
            OrderConfirmationHandler(email),
            retry = RetryPolicy.intervals(listOf(30.seconds, 2.minutes, 10.minutes)),
        )
    }

    // Lifecycle events are a Flow. Dead letters are an escalation to a human, so wire them
    // to whatever alerting you already have.
    klokka.events
        .filterIsInstance<JobEvent.DeadLettered>()
        .onEach { onDeadLetter(it) }
        .launchIn(this)

    routing {
        post("/orders") {
            val request = Json.decodeFromString<CreateOrderRequest>(call.receiveText())

            // Business logic first...
            val orderId = orders.create(request)

            // ...then hand the slow, failure-prone part to Klokka. The request returns as
            // soon as the row is persisted; the email goes out from a worker coroutine.
            klokka.enqueue(
                SendOrderConfirmation,
                OrderConfirmation(orderId, request.email, request.locale, request.lines),
                options {
                    // Store-enforced dedup while the first job is still pending or running:
                    // a client retrying this POST does not queue a second email.
                    uniqueKey = "order-confirmation-$orderId"
                },
            )

            call.respondText(orderId, status = HttpStatusCode.Created)
        }
    }
}

// =========================================================================================
// Executable check: run the module above inside a Ktor test application.
// =========================================================================================

data class SentEmail(val to: String, val subject: String, val body: String, val idempotencyKey: String)

class FakeEmailService : EmailService {
    val sent = mutableListOf<SentEmail>()
    val firstSent = CompletableDeferred<SentEmail>()

    override suspend fun send(to: String, subject: String, body: String, idempotencyKey: String) {
        val email = SentEmail(to, subject, body, idempotencyKey)
        sent += email
        firstSent.complete(email)
    }
}

private const val VALID_ORDER_JSON =
    """{"email":"ada@example.com","locale":"en-GB","lines":[{"sku":"KLK-1","quantity":2},{"sku":"KLK-9","quantity":1}]}"""

private const val INVALID_RECIPIENT_ORDER_JSON =
    """{"email":"not-an-address","locale":"en-GB","lines":[{"sku":"KLK-1","quantity":1}]}"""

class OrderConfirmationSampleTest {
    @Test
    fun creatingAnOrderSendsOneConfirmationEmail() =
        testApplication {
            val email = FakeEmailService()
            application { ordersModule(email, OrderService()) }

            val response = client.post("/orders") { setBody(VALID_ORDER_JSON) }
            assertEquals(HttpStatusCode.Created, response.status)
            val orderId = response.bodyAsText()

            // Production-default intervals are untouched: the in-memory store pushes a
            // wakeup on enqueue, so the worker claims the job without waiting for a poll.
            val sent = withTimeout(5.seconds) { email.firstSent.await() }

            assertEquals("ada@example.com", sent.to)
            assertEquals("Order $orderId confirmed", sent.subject)
            assertEquals("order-confirmation-$orderId", sent.idempotencyKey)
            assertTrue(sent.body.contains("2 x KLK-1"), sent.body)
            assertEquals(1, email.sent.size)
        }

    @Test
    fun invalidRecipientDeadLettersWithoutRetrying() =
        testApplication {
            val email = FakeEmailService()
            val deadLettered = CompletableDeferred<JobEvent.DeadLettered>()
            application { ordersModule(email, OrderService(), onDeadLetter = { deadLettered.complete(it) }) }

            val response = client.post("/orders") { setBody(INVALID_RECIPIENT_ORDER_JSON) }
            assertEquals(HttpStatusCode.Created, response.status)

            // NonRetryable short-circuits the 30s/2m/10m retry policy: the dead-letter event
            // arrives immediately, on attempt 1, and no email was sent.
            val event = withTimeout(5.seconds) { deadLettered.await() }
            assertEquals(SendOrderConfirmation.kind, event.kind)
            assertEquals(1, event.attempts)
            assertIs<InvalidRecipientException>(event.error)
            assertTrue(email.sent.isEmpty())
        }
}
