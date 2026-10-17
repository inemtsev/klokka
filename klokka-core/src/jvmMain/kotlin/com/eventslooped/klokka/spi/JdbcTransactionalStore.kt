package com.eventslooped.klokka.spi

import java.sql.Connection

/**
 * A [TransactionalStore] that joins a caller's JDBC transaction, given as the [Connection]
 * the transaction runs on. Every rule of [TransactionalStore] applies. In JDBC terms:
 *
 * - The store executes its statements on the caller's connection and never calls `commit`,
 *   `rollback`, `setAutoCommit`, `setTransactionIsolation` or `close` on it, and never
 *   issues session-level `SET` statements (such as `SET search_path`).
 * - The runtime only hands over connections with auto-commit off; the store may assume it.
 * - Migrations, if the store runs them, run on the store's own connections, never inside
 *   the caller's transaction.
 *
 * This is the capability JDBC bridges (klokka-exposed, or application code holding a
 * plain `Connection`) build on, so a store that implements it works with all of them.
 */
public interface JdbcTransactionalStore : TransactionalStore<Connection>
