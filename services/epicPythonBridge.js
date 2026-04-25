'use strict';

const { execFile } = require('child_process');
const path         = require('path');
const log          = require('../config/logger');

const PYTHON_SCRIPT_PATH = path.join(__dirname, '..', 'baddel_epic_fetcher.py');

// Hard cap: if the Python script hangs, kill it after 45 s and release the
// job-worker slot. Without this the worker leaks a slot permanently.
const PYTHON_TIMEOUT_MS  = 45_000;
const PYTHON_MAX_BUF     = 10 * 1024 * 1024; // 10 MB

/**
 * Fetch structured game data from the Epic Games Store via the Python bridge.
 *
 * @param {string} target  Epic namespace or slug
 * @returns {Promise<object|null>}
 */
async function fetchEpicDataFromPython(target) {
    if (!target) return null;

    return new Promise((resolve) => {
        execFile(
            'python3',
            [PYTHON_SCRIPT_PATH, target],
            { maxBuffer: PYTHON_MAX_BUF, timeout: PYTHON_TIMEOUT_MS },
            (error, stdout, stderr) => {
                if (error) {
                    // ETIMEDOUT / SIGTERM means the script exceeded PYTHON_TIMEOUT_MS
                    const isTimeout = error.killed || error.code === 'ETIMEDOUT';
                    log.error(
                        { target, isTimeout, err: error.message, stderr: stderr?.slice(0, 500) },
                        '[EpicBridge] python error'
                    );
                    return resolve(null);
                }

                const cleanStdout = stdout.trim();

                if (!cleanStdout) {
                    log.warn({ target }, '[EpicBridge] no data returned by script');
                    return resolve(null);
                }

                try {
                    const data = JSON.parse(cleanStdout);
                    resolve(data);
                } catch (parseError) {
                    log.error(
                        { target, err: parseError.message, preview: cleanStdout.slice(0, 200) },
                        '[EpicBridge] JSON parse error'
                    );
                    resolve(null);
                }
            }
        );
    });
}

module.exports = { fetchEpicDataFromPython };
