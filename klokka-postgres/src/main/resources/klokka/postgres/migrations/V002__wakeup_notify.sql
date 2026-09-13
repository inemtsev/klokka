-- Push wake-ups: the SCHEMA notifies, not the application, so every write path that
-- makes a job claimable produces a wake-up: Klokka's own enqueue, a future transactional
-- enqueue on the application's connection, a dashboard requeue, or an operator in psql.
-- NOTIFY is delivered at COMMIT (never on rollback), and duplicate notifications within
-- one transaction collapse, so a batch insert commits as a single wake-up.

CREATE FUNCTION klokka_notify_wakeup() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('klokka_wakeup', TG_TABLE_SCHEMA::text);
    RETURN NULL;
END;
$$;

-- A job inserted already due. Rows inserted as Scheduled (future run_at) stay silent:
-- their due moment is a time arriving, which is what the poll fallback covers.
CREATE TRIGGER klokka_jobs_wakeup_on_insert
    AFTER INSERT ON klokka_jobs
    FOR EACH ROW
    WHEN (NEW.state = 'Enqueued')
    EXECUTE FUNCTION klokka_notify_wakeup();

-- A job re-entering the claimable pool (drain requeues, operator requeues). Two separate
-- triggers because Postgres forbids OLD in an INSERT trigger's WHEN clause.
CREATE TRIGGER klokka_jobs_wakeup_on_requeue
    AFTER UPDATE OF state ON klokka_jobs
    FOR EACH ROW
    WHEN (OLD.state IS DISTINCT FROM NEW.state AND NEW.state = 'Enqueued')
    EXECUTE FUNCTION klokka_notify_wakeup();
