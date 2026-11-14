-- Retry limit per job. The DEFAULT only backfills rows that existed before this migration;
-- the application always sets the value explicitly (NewJob.DEFAULT_MAX_ATTEMPTS).
ALTER TABLE jobs
    ADD COLUMN max_attempts integer NOT NULL DEFAULT 5
        CONSTRAINT jobs_max_attempts_range CHECK (max_attempts BETWEEN 1 AND 100);
