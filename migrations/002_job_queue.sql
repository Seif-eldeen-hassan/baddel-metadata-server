-- migrations/002_job_queue.sql
--
-- Durable enrichment job queue.
-- Run once against production DB before deploying the new server.
-- Idempotent — safe to re-run.
--
-- Jobs survive restarts and multiple instances won't double-process
-- because of the FOR UPDATE SKIP LOCKED pattern.

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

COMMIT;


