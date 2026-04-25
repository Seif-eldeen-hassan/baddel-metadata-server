'use strict';

const { execFile }  = require('child_process');
const path          = require('path');
const log           = require('../config/logger');

// ─── Python binary — resolved lazily (never blocks the event loop) ────────────
//
// The old code called execSync('which python3') at module load time, which
// blocks Node's event loop on every server startup.  We now resolve it once,
// on first use, using execFile (async) so startup is never stalled.

const PYTHON_SCRIPT_PATH = path.join(__dirname, '..', 'baddel_steam_fetcher.py');

const PYTHON_TIMEOUT_MS  = 60_000; // Steam scraping can be slow; give it 60 s
const PYTHON_MAX_BUF     = 10 * 1024 * 1024; // 10 MB

// Ordered fallback list — checked once at first call
const PYTHON_CANDIDATES = process.platform === 'win32'
    ? ['python', 'python3']
    : ['python3', '/usr/bin/python3', '/usr/local/bin/python3'];

let _pythonBin     = null;
let _pythonBinLock = null; // in-flight promise — prevents concurrent probes

async function _resolvePythonBin() {
    if (_pythonBin) return _pythonBin;

    // If another call is already probing, wait for it
    if (_pythonBinLock) return _pythonBinLock;

    _pythonBinLock = (async () => {
        for (const candidate of PYTHON_CANDIDATES) {
            const ok = await new Promise((res) => {
                execFile(candidate, ['--version'], { timeout: 3000 }, (err) => res(!err));
            });
            if (ok) {
                _pythonBin = candidate;
                log.info({ bin: candidate }, '[SteamBridge] python binary resolved');
                return candidate;
            }
        }
        // Fall back to first candidate and let individual calls fail naturally
        _pythonBin = PYTHON_CANDIDATES[0];
        log.warn({ bin: _pythonBin, tried: PYTHON_CANDIDATES }, '[SteamBridge] no python binary found — using fallback');
        return _pythonBin;
    })();

    const result = await _pythonBinLock;
    _pythonBinLock = null;
    return result;
}

// ─── Concurrency limiter ──────────────────────────────────────────────────────
// Steam rate-limits aggressively. We cap at 1 concurrent request and add a
// 2 s delay between calls to avoid getting blocked.

const MAX_CONCURRENT   = 1;
const DELAY_BETWEEN_MS = 2000;

let _activeCount = 0;
const _waitQueue = [];

function _releaseSlot() {
    _activeCount--;
    setTimeout(_next, DELAY_BETWEEN_MS);
}

function _next() {
    if (_waitQueue.length === 0 || _activeCount >= MAX_CONCURRENT) return;
    const { resolve } = _waitQueue.shift();
    resolve();
}

function _acquireSlot() {
    return new Promise((resolve) => {
        if (_activeCount < MAX_CONCURRENT) resolve();
        else _waitQueue.push({ resolve });
    });
}

// ─── Core fetch ───────────────────────────────────────────────────────────────

async function fetchSteamDataFromPython(appId) {
    if (!appId) return null;

    const bin = await _resolvePythonBin();

    await _acquireSlot();
    _activeCount++;

    return new Promise((resolve) => {
        execFile(
            bin,
            [PYTHON_SCRIPT_PATH, String(appId)],
            { maxBuffer: PYTHON_MAX_BUF, timeout: PYTHON_TIMEOUT_MS },
            (error, stdout, stderr) => {
                _releaseSlot();

                if (error) {
                    const isTimeout = error.killed || error.code === 'ETIMEDOUT';
                    log.error(
                        { appId, isTimeout, err: error.message, stderr: stderr?.slice(0, 500) },
                        '[SteamBridge] python error'
                    );
                    return resolve(null);
                }

                const cleanStdout = stdout.trim();

                if (!cleanStdout) {
                    log.warn({ appId, stderr: stderr?.slice(0, 300) }, '[SteamBridge] empty stdout');
                    return resolve(null);
                }

                try {
                    const data = JSON.parse(cleanStdout);
                    if (data.error) {
                        log.warn({ appId, scriptError: data.error }, '[SteamBridge] script returned error');
                        return resolve(null);
                    }
                    resolve(data);
                } catch (parseError) {
                    log.error(
                        { appId, err: parseError.message, preview: cleanStdout.slice(0, 200) },
                        '[SteamBridge] JSON parse error'
                    );
                    resolve(null);
                }
            }
        );
    });
}

module.exports = { fetchSteamDataFromPython };
