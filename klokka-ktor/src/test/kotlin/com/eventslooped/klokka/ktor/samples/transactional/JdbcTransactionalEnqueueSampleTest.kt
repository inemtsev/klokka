package com.eventslooped.klokka.ktor.samples.transactional

import com.eventslooped.klokka.jdbc.enqueue
import com.eventslooped.klokka.jobType
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
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.Serializable
import java.sql.Connection
import java.sql.Statement
import javax.sql.DataSource
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

// =========================================================================================
// Sample: "insert an order and enqueue its receipt in ONE database transaction", with a
// plain JDBC Connection. Every JDBC library (jOOQ, Spring, Ktorm) can hand you the
// Connection its transaction runs on; this is the baseline they all reduce to.
//
// The promise: the receipt job exists if and only if the order commits. A declined payment
// after the enqueue rolls back both, so no receipt is ever sent for an order that does not
// exist, and no order exists without its receipt job.
// =========================================================================================

@Serializable
data class ReceiptFor(val orderId: Long, val customer: String)

val SendReceipt = jobType<ReceiptFor>("orders.send-receipt")

fun interface ReceiptSender {
    suspend fun send(receipt: ReceiptFor)
}

class PaymentDeclined : RuntimeException("payment declined")

/** The application's own data access, on the caller's connection and transaction. */
fun Connection.insertOrder(customer: String): Long =
    prepareStatement("INSERT INTO orders (customer) VALUES (?)", Statement.RETURN_GENERATED_KEYS).use { ps ->
        ps.setString(1, customer)
        ps.executeUpdate()
        ps.generatedKeys.use { keys ->
            keys.next()
            keys.getLong(1)
        }
    }

fun Application.jdbcOrdersModule(dataSource: DataSource, receipts: ReceiptSender) {
    install(Klokka) {
        // Klokka's store and the application share the database; that is what lets one
        // transaction cover both the order row and the job row.
        store = PostgresJobStore(dataSource)
        handle(SendReceipt) { receipt -> receipts.send(receipt) }
    }

    routing {
        post("/orders") {
            val customer = call.receiveText()
            val outcome =
                runCatching {
                    // Blocking JDBC belongs on the IO dispatcher, as with any JDBC call.
                    withContext(Dispatchers.IO) {
                        dataSource.connection.use { connection ->
                            connection.autoCommit = false
                            try {
                                val orderId = connection.insertOrder(customer)
                                // Same connection, same transaction: invisible to workers
                                // until commit, gone on rollback.
                                klokka.enqueue(SendReceipt, ReceiptFor(orderId, customer), tx = connection)
                                if (customer == "declined") throw PaymentDeclined()
                                connection.commit()
                                orderId
                            } catch (e: Throwable) {
                                connection.rollback()
                                throw e
                            }
                        }
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

class JdbcTransactionalEnqueueSampleTest {
    @BeforeTest
    fun reset() = SampleDatabase.reset()

    @Test
    fun committedOrderGetsItsReceipt() =
        testApplication {
            val sent = CompletableDeferred<ReceiptFor>()
            application { jdbcOrdersModule(SampleDatabase.dataSource) { sent.complete(it) } }

            val response = client.post("/orders") { setBody("ada") }

            assertEquals(HttpStatusCode.Created, response.status)
            val receipt = withTimeout(10.seconds) { sent.await() }
            assertEquals(ReceiptFor(response.bodyAsText().toLong(), "ada"), receipt)
        }

    @Test
    fun rolledBackOrderLeavesNoJob() =
        testApplication {
            application { jdbcOrdersModule(SampleDatabase.dataSource) { error("no receipt may be sent: $it") } }

            val response = client.post("/orders") { setBody("declined") }

            assertEquals(HttpStatusCode.PaymentRequired, response.status)
            assertEquals(0, SampleDatabase.count("SELECT count(*) FROM orders"))
            assertEquals(0, SampleDatabase.count("SELECT count(*) FROM klokka_jobs"))
        }
}
