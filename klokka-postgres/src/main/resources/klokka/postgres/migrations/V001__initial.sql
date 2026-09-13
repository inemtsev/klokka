-- Klokka initial schema. Applied by the built-in migrator (or Flyway pointed at this
-- directory) inside a pg_advisory_lock, so concurrent booting nodes do not race.

CREATE TABLE klokka_jobs (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind            TEXT        NOT NULL,
    payload         TEXT        NOT NULL,
    payload_version INT         NOT NULL,
    queue           TEXT        NOT NULL,
    state           TEXT        NOT NULL,
    run_at          TIMESTAMPTZ NOT NULL,
    retry_at        TIMESTAMPTZ,
    attempt         INT         NOT NULL DEFAULT 0,
    fence           BIGINT      NOT NULL DEFAULT 0,
    lease_until     TIMESTAMPTZ,
    holder          TEXT,
    unique_key      TEXT,
    schedule_id     TEXT,
    enqueued_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    terminal_at     TIMESTAMPTZ,
    CONSTRAINT klokka_jobs_state_check CHECK (
        state IN ('Scheduled', 'Enqueued', 'Running', 'Succeeded', 'Failed', 'DeadLettered', 'Cancelled')
    )
);

-- The claim path: pending rows per queue, ordered by due time.
CREATE INDEX klokka_jobs_pending_idx
    ON klokka_jobs (queue, run_at)
    WHERE state IN ('Scheduled', 'Enqueued');

-- Booked retries, ordered by their retry time.
CREATE INDEX klokka_jobs_retry_idx
    ON klokka_jobs (queue, retry_at)
    WHERE state = 'Failed';

-- Zombie revival: running rows by lease expiry.
CREATE INDEX klokka_jobs_lease_idx
    ON klokka_jobs (lease_until)
    WHERE state = 'Running';

-- Store-enforced dedup: at most one NON-TERMINAL row per unique_key, as a constraint.
CREATE UNIQUE INDEX klokka_jobs_unique_key_idx
    ON klokka_jobs (unique_key)
    WHERE unique_key IS NOT NULL AND state NOT IN ('Succeeded', 'DeadLettered', 'Cancelled');

-- The retention sweep.
CREATE INDEX klokka_jobs_terminal_idx
    ON klokka_jobs (terminal_at)
    WHERE terminal_at IS NOT NULL;

CREATE TABLE klokka_schedules (
    id            TEXT        PRIMARY KEY,
    kind          TEXT        NOT NULL,
    fingerprint   TEXT        NOT NULL,
    description   TEXT        NOT NULL,
    next_fire_at  TIMESTAMPTZ,
    last_fired_at TIMESTAMPTZ,
    fence         BIGINT      NOT NULL DEFAULT 0,
    lease_until   TIMESTAMPTZ,
    holder        TEXT
);

CREATE INDEX klokka_schedules_due_idx
    ON klokka_schedules (next_fire_at)
    WHERE next_fire_at IS NOT NULL;
