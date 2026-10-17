package com.eventslooped.klokka.ktor.samples.transactional

import com.eventslooped.klokka.postgres.PostgresJobStore
import kotlinx.coroutines.runBlocking
import org.postgresql.ds.PGSimpleDataSource
import org.testcontainers.containers.PostgreSQLContainer
import javax.sql.DataSource

/**
 * Test plumbing, not part of the samples: one Postgres container for the transactional
 * samples, with Klokka's tables migrated and an application `orders` table beside them,
 * which is the point of transactional enqueue (both live in the same database).
 */
internal object SampleDatabase {
    private val container: PostgreSQLContainer<*> by lazy {
        PostgreSQLContainer("postgres:16-alpine").also { it.start() }
    }

    val dataSource: DataSource by lazy {
        PGSimpleDataSource().apply {
            setURL(container.jdbcUrl)
            user = container.username
            password = container.password
        }.also { ds ->
            runBlocking { PostgresJobStore(ds).migrate() }
            ds.connection.use { connection ->
                connection.createStatement().use {
                    it.execute("CREATE TABLE IF NOT EXISTS orders (id BIGSERIAL PRIMARY KEY, customer TEXT NOT NULL)")
                }
            }
        }
    }

    /** Empties the application table and Klokka's tables between tests. */
    fun reset() {
        dataSource.connection.use { connection ->
            connection.createStatement().use {
                it.execute("TRUNCATE orders, klokka_jobs, klokka_schedules RESTART IDENTITY")
            }
        }
    }

    fun count(sql: String): Int =
        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery(sql).use { rs ->
                    rs.next()
                    rs.getInt(1)
                }
            }
        }
}
