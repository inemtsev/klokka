-- The last recorded failure of a job. All five columns are written together, in the same
-- atomic UPDATE as the state change, on each transition to Failed or DeadLettered, and
-- replace any previous record. They are never cleared: a requeue or a later success keeps
-- the last error for triage. error_type NULL means the job has never failed.
ALTER TABLE klokka_jobs
    ADD COLUMN error_type    TEXT,
    ADD COLUMN error_message TEXT,
    ADD COLUMN error_stack   TEXT,
    ADD COLUMN error_attempt INT,
    ADD COLUMN error_at      TIMESTAMPTZ;
