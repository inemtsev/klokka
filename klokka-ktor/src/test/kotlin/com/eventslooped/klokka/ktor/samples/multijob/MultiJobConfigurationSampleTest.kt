@file:OptIn(ExperimentalTime::class)

package com.eventslooped.klokka.ktor.samples.multijob

import com.eventslooped.klokka.JobContext
import com.eventslooped.klokka.JobEvent
import com.eventslooped.klokka.JobHandler
import com.eventslooped.klokka.JobId
import com.eventslooped.klokka.JobState
import com.eventslooped.klokka.QueueName
import com.eventslooped.klokka.RetryPolicy
import com.eventslooped.klokka.jobType
import com.eventslooped.klokka.ktor.Klokka
import com.eventslooped.klokka.ktor.KlokkaConfig
import com.eventslooped.klokka.ktor.klokka
import com.eventslooped.klokka.ktor.samples.EmailService
import com.eventslooped.klokka.ktor.samples.FakeEmailService
import com.eventslooped.klokka.ktor.samples.OrderConfirmation
import com.eventslooped.klokka.ktor.samples.OrderConfirmationHandler
import com.eventslooped.klokka.ktor.samples.OrderLine
import com.eventslooped.klokka.ktor.samples.SendOrderConfirmation
import com.eventslooped.klokka.options
import com.eventslooped.klokka.runtime.KlokkaRole
import com.eventslooped.klokka.store.InMemoryJobStore
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
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime

// =========================================================================================
// Sample: several jobs across several domains, configured in one application.
//
// Reuses the orders domain from OrderConfirmationSampleTest (EmailService, the confirmation
// job and its handler) and adds an inventory job to it, plus a billing domain. Each domain
// contributes its jobs through an extension function on KlokkaConfig, the same way Ktor
// routes are grouped through extension functions on Route. The install block then reads
// as a table of contents: infrastructure, queues, domains.
// =========================================================================================

// --- Queue names, shared by the declaration site and every enqueue site -------------------

object Queues {
    val CRITICAL = QueueName("critical")
    val EMAIL = QueueName("email")
}

// --- Orders domain: a second job next to the confirmation email --------------------------

@Serializable
data class InventoryReservation(val orderId: String, val lines: List<OrderLine>)

val ReserveInventory = jobType<InventoryReservation>("orders.reserve-inventory", queue = Queues.CRITICAL)

interface InventoryClient {
    suspend fun reserve(orderId: String, lines: List<OrderLine>)
}

class ReserveInventoryHandler(private val inventory: InventoryClient) : JobHandler<InventoryReservation> {
    override suspend fun JobContext.execute(payload: InventoryReservation) {
        inventory.reserve(payload.orderId, payload.lines)
    }
}

/** Everything the orders domain runs in the background. */
fun KlokkaConfig.orderJobs(email: EmailService, inventory: InventoryClient) {
    handle(
        SendOrderConfirmation,
        OrderConfirmationHandler(email),
        retry = RetryPolicy.intervals(listOf(30.seconds, 2.minutes, 10.minutes)),
    )
    handle(ReserveInventory, ReserveInventoryHandler(inventory)) // runtime default retry
}

// --- Billing domain -----------------------------------------------------------------------

@Serializable
data class InvoiceRequest(val orderId: String, val amountCents: Long, val currency: String)

@Serializable
data class WebhookDelivery(val url: String, val event: String, val orderId: String)

val GenerateInvoice = jobType<InvoiceRequest>("billing.generate-invoice")
val DeliverWebhook = jobType<WebhookDelivery>("billing.deliver-webhook")

interface InvoiceStore {
    suspend fun save(orderId: String, amountCents: Long, currency: String): String
}

interface WebhookSender {
    suspend fun post(url: String, event: String, orderId: String)
}

class WebhookDeliveryHandler(private val sender: WebhookSender) : JobHandler<WebhookDelivery> {
    override suspend fun JobContext.execute(payload: WebhookDelivery) {
        sender.post(payload.url, payload.event, payload.orderId)
    }
}

/** Everything the billing domain runs in the background. */
fun KlokkaConfig.billingJobs(invoices: InvoiceStore, webhooks: WebhookSender) {
    // Lambda form: the handler is one call and has no lifecycle of its own.
    handle(GenerateInvoice, retry = RetryPolicy.exponential(base = 5.seconds, maxAttempts = 5)) { request ->
        invoices.save(request.orderId, request.amountCents, request.currency)
    }
    // Object form with a long, patient retry: third-party endpoints flap.
    handle(
        DeliverWebhook,
        WebhookDeliveryHandler(webhooks),
        retry = RetryPolicy.exponential(base = 10.seconds, factor = 2.0, maxAttempts = 10, max = 1.hours),
    )
}

// --- Application wiring -------------------------------------------------------------------

/** Stands in for the real order table. */
class OrderRepository {
    private var counter = 0

    fun insert(email: String, lines: List<OrderLine>): String = "ord-${++counter}"
}

class ShopDependencies(
    val orders: OrderRepository,
    val email: EmailService,
    val inventory: InventoryClient,
    val invoices: InvoiceStore,
    val webhooks: WebhookSender,
)

@Serializable
data class CheckoutRequest(val email: String, val lines: List<OrderLine>, val amountCents: Long, val webhookUrl: String)

fun Application.shopModule(deps: ShopDependencies) {
    install(Klokka) {
        // Infrastructure decisions in one place. Production: store = PostgresJobStore(dataSource).
        store = InMemoryJobStore()
        // Same module, three deployment shapes: the web tier sets Producer, the worker fleet
        // sets Worker, a single-process app keeps Both. Handlers bound in a Producer are inert.
        role = KlokkaRole.Both

        // Declared order is drain priority: reservations before email before everything else.
        queue(Queues.CRITICAL, concurrency = 8)
        queue(Queues.EMAIL, concurrency = 32)
        queue(QueueName.DEFAULT, concurrency = 16)

        // One extension function per domain.
        orderJobs(deps.email, deps.inventory)
        billingJobs(deps.invoices, deps.webhooks)
    }

    routing {
        post("/checkout") {
            val request = Json.decodeFromString<CheckoutRequest>(call.receiveText())
            val orderId = deps.orders.insert(request.email, request.lines)

            // One business event, four background jobs. Each lands on its JobType's default queue;
            // options carry only per-call data such as uniqueKey.
            klokka.enqueue(ReserveInventory, InventoryReservation(orderId, request.lines))
            klokka.enqueue(
                SendOrderConfirmation,
                OrderConfirmation(orderId, request.email, "en-GB", request.lines),
                options {
                    uniqueKey = "order-confirmation-$orderId"
                },
            )
            klokka.enqueue(GenerateInvoice, InvoiceRequest(orderId, request.amountCents, "EUR"))
            klokka.enqueue(DeliverWebhook, WebhookDelivery(request.webhookUrl, "order.created", orderId))

            call.respondText(orderId, status = HttpStatusCode.Created)
        }
    }
}

// =========================================================================================
// Executable checks
// =========================================================================================

class FakeInventory : InventoryClient {
    val reserved = CompletableDeferred<String>()

    override suspend fun reserve(orderId: String, lines: List<OrderLine>) {
        reserved.complete(orderId)
    }
}

class FakeInvoices : InvoiceStore {
    val saved = CompletableDeferred<String>()

    override suspend fun save(orderId: String, amountCents: Long, currency: String): String {
        saved.complete(orderId)
        return "inv-$orderId"
    }
}

class FakeWebhooks : WebhookSender {
    val delivered = CompletableDeferred<String>()

    override suspend fun post(url: String, event: String, orderId: String) {
        delivered.complete("$event:$orderId")
    }
}

private const val CHECKOUT_JSON =
    """{"email":"ada@example.com","lines":[{"sku":"KLK-1","quantity":2}],"amountCents":4990,"webhookUrl":"https://erp.example.com/hooks"}"""

class MultiJobConfigurationSampleTest {
    @Test
    fun checkoutFansOutToAllFourJobs() =
        testApplication {
            val email = FakeEmailService()
            val inventory = FakeInventory()
            val invoices = FakeInvoices()
            val webhooks = FakeWebhooks()
            application { shopModule(ShopDependencies(OrderRepository(), email, inventory, invoices, webhooks)) }

            val response = client.post("/checkout") { setBody(CHECKOUT_JSON) }
            assertEquals(HttpStatusCode.Created, response.status)
            val orderId = response.bodyAsText()

            withTimeout(5.seconds) {
                assertEquals(orderId, inventory.reserved.await())
                assertEquals("ada@example.com", email.firstSent.await().to)
                assertEquals(orderId, invoices.saved.await())
                assertEquals("order.created:$orderId", webhooks.delivered.await())
            }
        }

    /**
     * The multi-domain rule after #33: a worker only claims kinds it binds. A worker deployment
     * that binds only the orders domain shares the default queue with billing jobs without
     * harm: it never claims them, and they wait for a worker that binds billing. The same
     * mechanism is what keeps a rolling deploy from dead-lettering a kind that only the newer
     * version knows.
     */
    @Test
    fun workerWithoutAKindBoundLeavesTheJobForAnotherWorker() =
        testApplication {
            val sharedStore = InMemoryJobStore()
            val events = mutableListOf<JobEvent>()

            install(Klokka) {
                store = sharedStore
                queue(QueueName.DEFAULT.value)
                orderJobs(FakeEmailService(), FakeInventory()) // billingJobs deliberately absent
            }
            application {
                klokka.events.onEach { events.add(it) }.launchIn(this)
                routing {
                    post("/invoice") {
                        val id = klokka.enqueue(GenerateInvoice, InvoiceRequest("ord-7", 1000, "EUR"))
                        call.respondText(id.value)
                    }
                }
            }

            val response = client.post("/invoice")
            val idString = response.bodyAsText()

            // Let the enqueue's push wakeup and at least one claim round happen.
            delay(300.milliseconds)

            assertEquals(JobState.Enqueued, sharedStore.snapshot(JobId(idString))?.state)
            assertTrue(events.none { it is JobEvent.Started && it.kind == GenerateInvoice.kind })
            assertTrue(events.none { it is JobEvent.DeadLettered && it.kind == GenerateInvoice.kind })
        }
}
