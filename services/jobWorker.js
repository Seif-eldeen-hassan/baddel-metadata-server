'use strict';

/**
 * services/jobWorker.js
 *
 * Polls enrich_jobs for queued work and processes it.
 * Safe to run on multiple instances simultaneously — uses FOR UPDATE SKIP LOCKED.
 *
 * Usage:
 *   const worker = require('./services/jobWorker');
 *   worker.start();   // begins polling loop
 *   await worker.stop(); // drains in-flight, stops cleanly
 */

const { claimBatch, complete, fail, recoverStaleJobs } = require('./jobQueue');
const { enrichGame }                 = require('./enrichGame');
const log                            = require('../config/logger');

const POLL_INTERVAL_MS      = 2000;
const ACTIVE_INTERVAL       = 100;
const STALE_RECOVERY_MS     = 5 * 60 * 1000; // run stale recovery every 5 minutes

let _running      = false;
let _stopping     = false;
let _timer        = null;
let _staleTimer   = null;
let _inFlight     = 0;

// ─── Error serialiser — includes stack + PostgreSQL error code ────────────────

function serializeError(err) {
    return {
        message: err?.message,
        stack:   err?.stack,
        // PostgreSQL driver surfaces these on query errors
        code:    err?.code,       // e.g. '42P01' (undefined_table), '23505' (unique_violation)
        detail:  err?.detail,
        hint:    err?.hint,
        where:   err?.where,
    };
}

// ─── Main poll loop ───────────────────────────────────────────────────────────

async function _tick() {
    if (_stopping) return;

    let jobs;
    try {
        jobs = await claimBatch();
    } catch (err) {
        log.error({ err: serializeError(err) }, '[Worker] claimBatch error');
        _scheduleNext(POLL_INTERVAL_MS);
        return;
    }

    if (jobs.length === 0) {
        _scheduleNext(POLL_INTERVAL_MS);
        return;
    }

    // Process claimed jobs concurrently (they're already claimed/locked)
    _inFlight += jobs.length;
    await Promise.allSettled(jobs.map(_processJob));
    _inFlight -= jobs.length;

    // If we got a full batch, poll again immediately — more may be waiting
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
        log.error({ jobId, gameId, err: serializeError(err) }, '[Worker] job failed');
        await fail(jobId, err.message).catch(e =>
            log.error({ jobId, err: serializeError(e) }, '[Worker] fail() error')
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

    // Recover any jobs stuck in 'running' from a previous crashed process
    recoverStaleJobs().catch(err =>
        log.error({ err: serializeError(err) }, '[Worker] startup stale recovery error')
    );

    // Also recover stale jobs periodically (every 5 minutes)
    _staleTimer = setInterval(() => {
        recoverStaleJobs().catch(err =>
            log.error({ err: serializeError(err) }, '[Worker] periodic stale recovery error')
        );
    }, STALE_RECOVERY_MS);

    _scheduleNext(500);
}

/**
 * Stop the worker cleanly.
 * - Stops scheduling new ticks
 * - Waits for in-flight jobs to finish (up to timeoutMs)
 */
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