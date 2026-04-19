'use strict';

/**
 * services/jobWorker.js
 *
 * CHANGES vs original:
 *  1. All log.error calls now emit errFlat (flat string) as a top-level field
 *     so Railway's log viewer shows the actual error message in the log line,
 *     not a collapsed nested object.
 *  2. checkSchema() is called at startup before the first tick — prints a clear
 *     PASS/FAIL line so you know immediately if the migration is missing.
 *  3. On claimBatch error, logs err._pgFlat (set by jobQueue) + err.stack.
 */

const { claimBatch, complete, fail, recoverStaleJobs, checkSchema, pgErrFlat } = require('./jobQueue');
const { enrichGame } = require('./enrichGame');
const log            = require('../config/logger');

const POLL_INTERVAL_MS  = 2000;
const ACTIVE_INTERVAL   = 100;
const STALE_RECOVERY_MS = 5 * 60 * 1000;

let _running    = false;
let _stopping   = false;
let _timer      = null;
let _staleTimer = null;
let _inFlight   = 0;

// ─── Main poll loop ───────────────────────────────────────────────────────────

async function _tick() {
    if (_stopping) return;

    let jobs;
    try {
        jobs = await claimBatch();
    } catch (err) {
        // errFlat is a top-level string field — Railway renders it in the log line
        log.error(
            { errFlat: err._pgFlat || pgErrFlat(err), stack: err?.stack },
            '[Worker] claimBatch error'
        );
        _scheduleNext(POLL_INTERVAL_MS);
        return;
    }

    if (jobs.length === 0) {
        _scheduleNext(POLL_INTERVAL_MS);
        return;
    }

    _inFlight += jobs.length;
    await Promise.allSettled(jobs.map(_processJob));
    _inFlight -= jobs.length;

    _scheduleNext(jobs.length > 0 ? ACTIVE_INTERVAL : POLL_INTERVAL_MS);
}

async function _processJob(job) {
    const { id: jobId, game_id: gameId, platform, external_id: externalId } = job;
    const clientData = job.payload?.clientData || {};

    log.info({ jobId, gameId, platform, externalId }, '[Worker] processing job');

    try {
        await enrichGame(gameId, platform, externalId, clientData);
        await complete(jobId);
        log.info({ jobId, gameId }, '[Worker] job complete');
    } catch (err) {
        log.error(
            { jobId, gameId, errFlat: pgErrFlat(err), stack: err?.stack },
            '[Worker] job failed'
        );
        await fail(jobId, err.message).catch(e =>
            log.error({ jobId, errFlat: pgErrFlat(e), stack: e?.stack }, '[Worker] fail() error')
        );
    }
}

function _scheduleNext(ms) {
    if (_stopping) return;
    _timer = setTimeout(_tick, ms);
}

// ─── Public API ───────────────────────────────────────────────────────────────

function start() {
    if (_running) return;
    _running  = true;
    _stopping = false;
    log.info('[Worker] started');

    // Schema self-check — runs once, logs PASS or FAIL immediately
    // This is the first thing that hits the DB on startup.
    // If enrich_jobs is missing you will see the exact error within ~300ms.
    checkSchema().catch(err =>
        log.error({ errFlat: pgErrFlat(err), stack: err?.stack }, '[Worker] checkSchema error')
    );

    // Recover any jobs stuck in 'running' from a previous crashed process
    recoverStaleJobs().catch(err =>
        log.error({ errFlat: pgErrFlat(err), stack: err?.stack }, '[Worker] startup stale recovery error')
    );

    // Periodic stale recovery every 5 minutes
    _staleTimer = setInterval(() => {
        recoverStaleJobs().catch(err =>
            log.error({ errFlat: pgErrFlat(err), stack: err?.stack }, '[Worker] periodic stale recovery error')
        );
    }, STALE_RECOVERY_MS);

    _scheduleNext(500);
}

async function stop(timeoutMs = 30_000) {
    if (!_running) return;
    _stopping = true;
    if (_timer)      { clearTimeout(_timer);       _timer      = null; }
    if (_staleTimer) { clearInterval(_staleTimer); _staleTimer = null; }

    log.info({ inFlight: _inFlight }, '[Worker] stopping, draining in-flight');

    const deadline = Date.now() + timeoutMs;
    while (_inFlight > 0 && Date.now() < deadline) {
        await new Promise(r => setTimeout(r, 200));
    }

    if (_inFlight > 0) {
        log.warn({ inFlight: _inFlight }, '[Worker] shutdown timeout — in-flight jobs abandoned');
    }

    _running = false;
    log.info('[Worker] stopped');
}

function isRunning() { return _running && !_stopping; }
function inFlight()  { return _inFlight; }

module.exports = { start, stop, isRunning, inFlight };