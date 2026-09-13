package com.eventslooped.klokka.postgres

import java.sql.Connection

/**
 * The built-in, forward-only migration runner. Deliberately tiny: bundled SQL resources,
 * a version table, and a session-level advisory lock so concurrent booting nodes apply
 * migrations exactly once. Teams that already run Flyway can point it at the same
 * resource directory (the files are named Flyway-style) and construct the store with
 * `autoMigrate = false`.
 */
internal object Migrations {
    /** Ordered migration names; version N is index N-1. Append only, never edit a shipped file. */
    private val ALL = listOf("V001__initial", "V002__wakeup_notify")

    /** Arbitrary but stable key ("klokka" digits) for pg_advisory_lock. */
    private const val ADVISORY_LOCK_KEY = 6_566_763_566L

    private const val RESOURCE_DIR = "klokka/postgres/migrations"

    /** Blocking; the store calls this on its IO dispatcher. */
    fun apply(connection: Connection, schema: String?) {
        connection.createStatement().use { statement ->
            if (schema != null) {
                statement.execute("CREATE SCHEMA IF NOT EXISTS $schema")
                statement.execute("SET search_path TO $schema")
            }
            statement.execute("SELECT pg_advisory_lock($ADVISORY_LOCK_KEY)")
            try {
                statement.execute(
                    "CREATE TABLE IF NOT EXISTS klokka_schema_version (" +
                        "version INT PRIMARY KEY, name TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())",
                )
                val applied =
                    statement.executeQuery("SELECT coalesce(max(version), 0) FROM klokka_schema_version").use { rs ->
                        rs.next()
                        rs.getInt(1)
                    }
                for ((index, name) in ALL.withIndex()) {
                    val version = index + 1
                    if (version <= applied) continue
                    // Each migration plus its version row applies in one transaction:
                    // Postgres DDL is transactional, so a failed migration leaves nothing behind.
                    connection.autoCommit = false
                    try {
                        statement.execute(load(name))
                        connection.prepareStatement("INSERT INTO klokka_schema_version (version, name) VALUES (?, ?)").use {
                            it.setInt(1, version)
                            it.setString(2, name)
                            it.executeUpdate()
                        }
                        connection.commit()
                    } catch (e: Throwable) {
                        connection.rollback()
                        throw e
                    } finally {
                        connection.autoCommit = true
                    }
                }
            } finally {
                statement.execute("SELECT pg_advisory_unlock($ADVISORY_LOCK_KEY)")
            }
        }
    }

    private fun load(name: String): String {
        val path = "$RESOURCE_DIR/$name.sql"
        val stream =
            checkNotNull(Migrations::class.java.classLoader.getResourceAsStream(path)) {
                "Missing bundled migration resource '$path'; the klokka-postgres jar is corrupt"
            }
        return stream.use { it.readBytes().decodeToString() }
    }
}
