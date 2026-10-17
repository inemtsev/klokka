package com.eventslooped.klokka.ktor.samples.transactional

import com.eventslooped.klokka.JobEvent
import com.eventslooped.klokka.exposed.enqueue
import com.eventslooped.klokka.ktor.Klokka
import com.eventslooped.klokka.ktor.klokka
import com.eventslooped.klokka.postgres.PostgresJobStore
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
import org.jetbrains.exposed.v1.core.dao.id.LongIdTable
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.insertAndGetId
import org.jetbrains.exposed.v1.jdbc.transactions.suspendTransaction
import java.util.concurrent.CopyOnWriteArrayList
import javax.sql.DataSource
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

// =========================================================================================
// Sample: the same "order plus receipt in one transaction", written with Exposed. Pass the
// transaction receiver as `tx = this`; Exposed's thread-local current transaction is never
// consulted, because it does not survive coroutine thread hops.
//
// With Exposed, Klokka can see the commit, so JobEvent.Enqueued is emitted after the commit
// and never for a rolled-back job. (The plain-Connection path cannot see the commit and
// emits no Enqueued event at all.)
// =========================================================================================

object Orders : LongIdTable("orders") {
    val customer = text("customer")
}

fun Application.exposedOrdersModule(
    dataSource: DataSource,
    receipts: ReceiptSender,
    onEnqueued: (JobEvent.Enqueued) -> Unit = {},
) {
    val database = Database.connect(dataSource)

    install(Klokka) {
        store = PostgresJobStore(dataSource)
        handle(SendReceipt) { receipt -> receipts.send(receipt) }
    }

    klokka.events
        .filterIsInstance<JobEvent.Enqueued>()
        .onEach { onEnqueued(it) }
        .launchIn(this)

    routing {
        post("/orders") {
            val customer = call.receiveText()
            val outcome =
                runCatching {
                    suspendTransaction(db = database) {
                        val orderId = Orders.insertAndGetId { it[Orders.customer] = customer }.value
                        klokka.enqueue(SendReceipt, ReceiptFor(orderId, customer), tx = this)
                        // Throwing rolls back the order AND the job.
                        if (customer == "declined") throw PaymentDeclined()
                        orderId
                    }
                }
            outcome.fold(
                onSuccess = { call.respondText(it.toString(), status = HttpStatusCode.Created) },
                onFailure = { call.respondText(it.message.orEmpty(), status = HttpStatusCode.PaymentRequired) },
            )
        }
    }
}

// =========================================================================================
// Executable check against a real Postgres (Testcontainers).
// =========================================================================================

class ExposedTransactionalEnqueueSampleTest {
    @BeforeTest
    fun reset() = SampleDatabase.reset()

    @Test
    fun committedOrderGetsItsReceiptAndOneEnqueuedEvent() =
        testApplication {
            val sent = CompletableDeferred<ReceiptFor>()
            val enqueued = CompletableDeferred<JobEvent.Enqueued>()
            application {
                exposedOrdersModule(SampleDatabase.dataSource, { sent.complete(it) }, { enqueued.complete(it) })
            }

            val response = client.post("/orders") { setBody("ada") }

            assertEquals(HttpStatusCode.Created, response.status)
            val receipt = withTimeout(10.seconds) { sent.await() }
            assertEquals(ReceiptFor(response.bodyAsText().toLong(), "ada"), receipt)
            assertEquals("orders.send-receipt", withTimeout(10.seconds) { enqueued.await() }.kind)
        }

    @Test
    fun rolledBackOrderLeavesNoJobAndNoEvent() =
        testApplication {
            val events = CopyOnWriteArrayList<JobEvent.Enqueued>()
            val committed = CompletableDeferred<Unit>()
            application {
                exposedOrdersModule(SampleDatabase.dataSource, {}, { event ->
                    events += event
                    committed.complete(Unit)
                })
            }

            val declined = client.post("/orders") { setBody("declined") }
            assertEquals(HttpStatusCode.PaymentRequired, declined.status)
            assertEquals(0, SampleDatabase.count("SELECT count(*) FROM orders"))
            assertEquals(0, SampleDatabase.count("SELECT count(*) FROM klokka_jobs"))

            // Events are delivered in order, so once the committed order's event arrives,
            // anything the rolled-back one emitted would already be in the list.
            val accepted = client.post("/orders") { setBody("grace") }
            assertEquals(HttpStatusCode.Created, accepted.status)
            withTimeout(10.seconds) { committed.await() }
            assertEquals(1, events.size, "only the committed order may emit Enqueued: $events")
        }
}
