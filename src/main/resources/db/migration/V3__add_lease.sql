-- Lease (visibility timeout) for RUNNING jobs. See docs/DESIGN.md section 5.
ALTER TABLE jobs ADD COLUMN locked_until timestamptz;

-- RUNNING rows from before leases existed get an already-expired lease, so the reaper recovers them.
UPDATE jobs SET locked_until = now() WHERE status = 'RUNNING';

-- A RUNNING job always has a lease expiry; no other state has one.
ALTER TABLE jobs ADD CONSTRAINT jobs_lease_iff_running
    CHECK ((status = 'RUNNING') = (locked_until IS NOT NULL));

-- The reaper only scans RUNNING rows, ordered by lease expiry.
CREATE INDEX jobs_running_lease_idx ON jobs (locked_until) WHERE status = 'RUNNING';
