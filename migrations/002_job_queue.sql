-- migrations/002_job_queue.sql  (v2 — safe to re-run, fixes original idempotency gap)
--
-- Durable enrichment job queue.
--
-- HOW TO APPLY:
--   psql $DATABASE_URL -f migrations/002_job_queue.sql
--
-- This script is fully idempotent:
--   - CREATE TABLE IF NOT EXISTS
--   - CREATE UNIQUE INDEX IF NOT EXISTS
--   - CREATE INDEX IF NOT EXISTS
-- Safe to re-run without side effects.
--
-- WHAT THIS FIXES:
--   The worker fails immediately on startup with:
--     [Worker] startup stale recovery error
--     [Worker] claimBatch error
--   Both fail because enrich_jobs does not exist (PG error 42P01: relation not found).
--   This migration creates the table and indexes.

BEGIN;

CREATE TABLE IF NOT EXISTS enrich_jobs (
    id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    game_id      UUID         NOT NULL REFERENCES games(id) ON DELETE CASCADE,
    platform     TEXT         NOT NULL,
    external_id  TEXT         NOT NULL,
    status       TEXT         NOT NULL DEFAULT 'queued'
                                CHECK (status IN ('queued','running','succeeded','failed')),
    attempts     INTEGER      NOT NULL DEFAULT 0,
    max_attempts INTEGER      NOT NULL DEFAULT 3,
    error        TEXT,
    queued_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    payload      JSONB        NOT NULL DEFAULT '{}'
);

-- Prevent duplicate queued/running jobs for the same game
CREATE UNIQUE INDEX IF NOT EXISTS uq_enrich_jobs_active
    ON enrich_jobs (game_id)
    WHERE status IN ('queued', 'running');

-- Worker picks oldest queued jobs, skipping locked rows (multi-instance safe)
CREATE INDEX IF NOT EXISTS idx_enrich_jobs_queue
    ON enrich_jobs (queued_at ASC)
    WHERE status = 'queued';

-- Verify the table was created / already exists
DO $$
DECLARE
    col_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO col_count
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name   = 'enrich_jobs';

    IF col_count < 12 THEN
        RAISE EXCEPTION 'enrich_jobs created but column count unexpected: %', col_count;
    END IF;

    RAISE NOTICE 'enrich_jobs OK — % columns present', col_count;
END $$;

COMMIT;