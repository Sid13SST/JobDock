-- Jobs table: all queue state lives here. See docs/DESIGN.md section 2.
CREATE TABLE jobs (
    id              uuid        PRIMARY KEY,
    type            text        NOT NULL CHECK (length(type) BETWEEN 1 AND 100),
    payload         jsonb       NOT NULL,
    status          text        NOT NULL
        CHECK (status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED_RETRY', 'DEAD')),
    attempts        integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    idempotency_key text        UNIQUE CHECK (length(idempotency_key) BETWEEN 1 AND 255),
    run_at          timestamptz NOT NULL DEFAULT now(),
    locked_by       text,
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    started_at      timestamptz,
    finished_at     timestamptz,
    -- A RUNNING job always has an owner; no other state has one.
    CONSTRAINT jobs_owner_iff_running CHECK ((status = 'RUNNING') = (locked_by IS NOT NULL))
);

-- The claim query only scans claimable rows.
CREATE INDEX jobs_claimable_idx ON jobs (run_at, created_at)
    WHERE status IN ('QUEUED', 'FAILED_RETRY');
