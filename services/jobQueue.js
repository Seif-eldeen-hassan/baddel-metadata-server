'use strict';

/**
 * services/jobQueue.js
 *
 * Durable PostgreSQL-backed enrichment job queue.
 * Replaces the old in-memory _enrichQueue / _enqueuedIds / _enrichRunning.
 *
 * Design:
 *   - enrich_jobs table is the source of truth
 *   - enqueue()       — INSERT with conflict-safe idempotency (one active job per game)
 *   - claimBatch()    — SELECT … FOR UPDATE SKIP LOCKED → mark running atomically
 *   - complete()      — mark succeeded
 *   - fail()          — mark failed, increment attempts; re-queue if under max_attempts
 *   - getJob()        — fetch one job by id
 *   - getJobsByGame() — fetch recent jobs for a game
 */

const db  = require('../db');
const log = require('../config/logger');

const MAX_ATTEMPTS   = 3;
const CLAIM_BATCH    = 5; // how many jobs to claim per worker tick

// ─── Enqueue ──────────────────────────────────────────────────────────────────

/**
 * Add a job to the queue.
 * Safe to call multiple times for the same game — only one queued/running job
 * per game is allowed (enforced by the unique partial index).
 *
 * @param {{ gameId: string, platform: string, externalId: string, clientData?: object }} opts
 * @returns {Promise<{ id: string, created: boolean }>}
 */
async function enqueue({ gameId, platform, externalId, clientData = {} }) {
    const payload = { platform, externalId, clientData };

    // ON CONFLICT DO NOTHING preserves idempotency.
    // The unique partial index on (game_id) WHERE status IN ('queued','running')
    // prevents double-queuing an active game.
    const { rows } = await db.query(
        `INSERT INTO enrich_jobs (game_id, platform, external_id, payload)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT DO NOTHING
         RETURNING id`,
        [gameId, platform, externalId, JSON.stringify(payload)]
    );

    if (rows.length > 0) {
        log.info({ gameId, jobId: rows[0].id }, '[Queue] job enqueued');
        return { id: rows[0].id, created: true };
    }

    // Already has an active job — return existing
    const existing = await db.query(
        `SELECT id FROM enrich_jobs
         WHERE game_id = $1 AND status IN ('queued','running')
         ORDER BY queued_at DESC LIMIT 1`,
        [gameId]
    );
    const id = existing.rows[0]?.id || null;
    log.info({ gameId, jobId: id }, '[Queue] job already active, skipped');
    return { id, created: false };
}

// ─── Claim ────────────────────────────────────────────────────────────────────

/**
 * Atomically claim up to `limit` queued jobs and mark them running.
 * Uses FOR UPDATE SKIP LOCKED — safe for multiple worker instances.
 *
 * @param {number} limit
 * @returns {Promise<Array>} claimed job rows
 */
async function claimBatch(limit = CLAIM_BATCH) {
    const client = await db.connect();
    try {
        await client.query('BEGIN');

        const { rows } = await client.query(
            `SELECT id, game_id, platform, external_id, payload, attempts
             FROM enrich_jobs
             WHERE status = 'queued' AND attempts < $1
             ORDER BY queued_at ASC
             LIMIT $2
             FOR UPDATE SKIP LOCKED`,
            [MAX_ATTEMPTS, limit]
        );

        if (rows.length === 0) {
            await client.query('COMMIT');
            return [];
        }

        const ids = rows.map(r => r.id);
        await client.query(
            `UPDATE enrich_jobs
             SET status = 'running', started_at = now(), attempts = attempts + 1
             WHERE id = ANY($1::uuid[])`,
            [ids]
        );

        await client.query('COMMIT');
        log.info({ count: rows.length }, '[Queue] claimed jobs');
        return rows;
    } catch (err) {
        await client.query('ROLLBACK');
        throw err;
    } finally {
        client.release();
    }
}

// ─── Complete ─────────────────────────────────────────────────────────────────

async function complete(jobId) {
    await db.query(
        `UPDATE enrich_jobs
         SET status = 'succeeded', finished_at = now(), error = NULL
         WHERE id = $1`,
        [jobId]
    );
    log.info({ jobId }, '[Queue] job succeeded');
}

// ─── Fail ─────────────────────────────────────────────────────────────────────

/**
 * Mark a job failed. If attempts < max_attempts, re-queue it automatically.
 */
async function fail(jobId, errorMessage) {
    const { rows } = await db.query(
        `UPDATE enrich_jobs
         SET status = CASE WHEN attempts < $2 THEN 'queued' ELSE 'failed' END,
             finished_at = CASE WHEN attempts < $2 THEN NULL ELSE now() END,
             error = $3
         WHERE id = $1
         RETURNING status, attempts`,
        [jobId, MAX_ATTEMPTS, String(errorMessage).slice(0, 1000)]
    );
    const row = rows[0];
    if (row?.status === 'queued') {
        log.warn({ jobId, attempts: row.attempts }, '[Queue] job re-queued after failure');
    } else {
        log.error({ jobId, attempts: row?.attempts }, '[Queue] job permanently failed');
    }
}

// ─── Status reads ─────────────────────────────────────────────────────────────

async function getJob(jobId) {
    const { rows } = await db.query(
        `SELECT id, game_id, platform, external_id, status,
                attempts, max_attempts, error,
                queued_at, started_at, finished_at
         FROM enrich_jobs WHERE id = $1`,
        [jobId]
    );
    return rows[0] || null;
}

async function getJobsByGame(gameId, limit = 10) {
    const { rows } = await db.query(
        `SELECT id, game_id, platform, external_id, status,
                attempts, max_attempts, error,
                queued_at, started_at, finished_at
         FROM enrich_jobs
         WHERE game_id = $1
         ORDER BY queued_at DESC
         LIMIT $2`,
        [gameId, limit]
    );
    return rows;
}

// ─── Stale job recovery ───────────────────────────────────────────────────────

/**
 * Reset jobs that got stuck in 'running' due to a crashed process.
 * Called on worker startup and periodically during polling.
 *
 * A job is considered stale if it has been 'running' for longer than
 * STALE_THRESHOLD_MINUTES without completing.
 *
 * Stale jobs are moved back to 'queued' so they will be retried.
 * If attempts >= max_attempts, they are moved to 'failed' instead.
 *
 * @returns {Promise<number>} count of jobs recovered
 */
const STALE_THRESHOLD_MINUTES = 10;

async function recoverStaleJobs() {
    const { rows } = await db.query(
        `UPDATE enrich_jobs
         SET
           status     = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'queued' END,
           started_at = NULL,
           error      = CASE WHEN attempts >= max_attempts
                             THEN 'Timed out (process crash recovery)'
                             ELSE error
                        END
         WHERE status = 'running'
           AND started_at < now() - ($1 || ' minutes')::interval
         RETURNING id, game_id, attempts, max_attempts`,
        [STALE_THRESHOLD_MINUTES]
    );

    if (rows.length > 0) {
        const requeued = rows.filter(r => r.attempts < r.max_attempts).length;
        const failed   = rows.length - requeued;
        log.warn({ recovered: rows.length, requeued, failed }, '[Queue] stale running jobs recovered');
    }

    return rows.length;
}

module.exports = {
    enqueue,
    claimBatch,
    complete,
    fail,
    getJob,
    getJobsByGame,
    recoverStaleJobs,
    MAX_ATTEMPTS,
    CLAIM_BATCH,
    STALE_THRESHOLD_MINUTES,
};