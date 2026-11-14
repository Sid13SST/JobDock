-- How many times the reaper took this job back from a worker whose lease expired.
-- Kept per job so the total is durable and correct across any number of instances.
ALTER TABLE jobs ADD COLUMN lease_expirations integer NOT NULL DEFAULT 0 CHECK (lease_expirations >= 0);
