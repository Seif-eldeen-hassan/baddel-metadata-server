'use strict';

/**
 * services/jobQueue.js
 *
 * Durable PostgreSQL-backed enrichment job queue.
 *
 * CHANGES vs original:
 *  1. recoverStaleJobs: fixed interval SQL.
 *     Original: ($1 || ' minutes')::interval  — broken when $1 is a JS number
 *     (node-postgres sends JS numbers as int4; the || operator does not exist
 *     for integer || unknown, giving PG error 42883).
 *     Fixed:    $1 * interval '1 minute'  — standard SQL, works on all PG ≥ 9.
 *
 *  2. All throw-sites now attach pgErrFlat (flat string) so Railway shows the
 *     real error in the log line, not just a nested object that Railway collapses.
 *
 *  3. Added checkSchema() — called once at startup to verify enrich_jobs exists
 *     and has the expected columns. Prints a clear PASS/FAIL line.
 */

const db  = require('../db');
const log = require('../config/logger');

const MAX_ATTEMPTS          = 3;
const CLAIM_BATCH           = 5;
const STALE_THRESHOLD_MINUTES = 10;

// ─── Flat error serialiser ────────────────────────────────────────────────────
//
// Railway's log viewer renders the TOP-LEVEL string fields of a pino/bunyan
// log object. Nested objects like { err: { message, code } } appear collapsed
// and are invisible in the Railway UI.  By promoting key fields to a flat
// string we guarantee they appear in every log line.
//
function pgErrFlat(err) {
    // Returns a single string: "message [code=42P01] detail: ..."
    const parts = [err?.message || String(err)];
    if (err?.code)   parts.push(`[pg_code=${err.code}]`);
    if (err?.detail) parts.push(`detail: ${err.detail}`);
    if (err?.hint)   parts.push(`hint: ${err.hint}`);
    return parts.join(' ');
}

// ─── Startup schema self-check ────────────────────────────────────────────────

const EXPECTED_COLUMNS = [
    'id', 'game_id', 'platform', 'external_id', 'status',
    'attempts', 'max_attempts', 'error',
    'queued_at', 'started_at', 'finished_at', 'payload',
];

/**
 * Verify enrich_jobs table + columns exist.
 * Called once at worker startup.  Logs a clear PASS or FAIL line.
 * Does NOT throw — failure is logged so Railway shows it immediately,
 * and the worker loop will then also log real PG errors on every tick.
 */
async function checkSchema() {
    // 1. Does the table exist?
    let tableExists;
    try {
        const { rows } = await db.query(
            `SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name   = 'enrich_jobs'
             ) AS exists`
        );
        tableExists = rows[0]?.exists === true;
    } catch (err) {
        log.error(
            { errFlat: pgErrFlat(err), stack: err?.stack },
            '[Queue:checkSchema] DB query failed — cannot verify schema'
        );
        return;
    }

    if (!tableExists) {
        log.error(
            { errFlat: 'enrich_jobs table does NOT exist in public schema' },
            '[Queue:checkSchema] FAIL — migration 002_job_queue.sql has NOT been applied. ' +
            'Run: psql $DATABASE_URL -f migrations/002_job_queue.sql'
        );
        return;
    }

    // 2. Check columns
    const { rows: colRows } = await db.query(
        `SELECT column_name FROM information_schema.columns
         WHERE table_schema = 'public' AND table_name = 'enrich_jobs'`
    );
    const existing = new Set(colRows.map(r => r.column_name));
    const missing  = EXPECTED_COLUMNS.filter(c => !existing.has(c));

    if (missing.length > 0) {
        log.error(
            { errFlat: `Missing columns: ${missing.join(', ')}`, existing: [...existing] },
            '[Queue:checkSchema] FAIL — enrich_jobs schema mismatch'
        );
        return;
    }

    log.info(
        { columns: EXPECTED_COLUMNS.length, table: 'enrich_jobs' },
        '[Queue:checkSchema] PASS — enrich_jobs exists with all expected columns'
    );
}

// ─── Enqueue ──────────────────────────────────────────────────────────────────

async function enqueue({ gameId, platform, externalId, clientData = {} }) {
    const payload = { platform, externalId, clientData };
    try {
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

        const existing = await db.query(
            `SELECT id FROM enrich_jobs
             WHERE game_id = $1 AND status IN ('queued','running')
             ORDER BY queued_at DESC LIMIT 1`,
            [gameId]
        );
        const id = existing.rows[0]?.id || null;
        log.info({ gameId, jobId: id }, '[Queue] job already active, skipped');
        return { id, created: false };
    } catch (err) {
        log.error(
            { gameId, errFlat: pgErrFlat(err), stack: err?.stack },
            '[Queue] enqueue() failed'
        );
        throw err;
    }
}

// ─── Claim ────────────────────────────────────────────────────────────────────

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
        await client.query('ROLLBACK').catch(() => {});
        // Re-throw with flat error info attached so worker can log it visibly
        err._pgFlat = pgErrFlat(err);
        throw err;
    } finally {
        client.release();
    }
}

// ─── Complete ─────────────────────────────────────────────────────────────────

async function complete(jobId) {
    try {
        await db.query(
            `UPDATE enrich_jobs
             SET status = 'succeeded', finished_at = now(), error = NULL
             WHERE id = $1`,
            [jobId]
        );
        log.info({ jobId }, '[Queue] job succeeded');
    } catch (err) {
        log.error({ jobId, errFlat: pgErrFlat(err), stack: err?.stack }, '[Queue] complete() failed');
        throw err;
    }
}

// ─── Fail ─────────────────────────────────────────────────────────────────────

async function fail(jobId, errorMessage) {
    try {
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
    } catch (err) {
        log.error({ jobId, errFlat: pgErrFlat(err), stack: err?.stack }, '[Queue] fail() failed');
        throw err;
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
//
// FIX: original used ($1 || ' minutes')::interval
//
//   node-postgres sends JS numbers as int4.
//   The || operator does not exist for: integer || unknown
//   PG throws: ERROR 42883 operator does not exist: integer || unknown
//
//   Correct form: $1 * interval '1 minute'
//   Works on all PostgreSQL versions >= 9.  $1 is cast to numeric implicitly.
//

async function recoverStaleJobs() {
    try {
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
               AND started_at < now() - ($1 * interval '1 minute')
             RETURNING id, game_id, attempts, max_attempts`,
            [STALE_THRESHOLD_MINUTES]
        );

        if (rows.length > 0) {
            const requeued = rows.filter(r => r.attempts < r.max_attempts).length;
            const failed   = rows.length - requeued;
            log.warn({ recovered: rows.length, requeued, failed }, '[Queue] stale running jobs recovered');
        }

        return rows.length;
    } catch (err) {
        log.error(
            { errFlat: pgErrFlat(err), stack: err?.stack },
            '[Queue] recoverStaleJobs() failed'
        );
        throw err;
    }
}

module.exports = {
    enqueue,
    claimBatch,
    complete,
    fail,
    getJob,
    getJobsByGame,
    recoverStaleJobs,
    checkSchema,
    pgErrFlat,
    MAX_ATTEMPTS,
    CLAIM_BATCH,
    STALE_THRESHOLD_MINUTES,
};