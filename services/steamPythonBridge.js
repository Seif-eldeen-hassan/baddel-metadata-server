'use strict';

const { execFile, execSync } = require('child_process');
const path = require('path');

let PYTHON_BIN = 'python3';
try {
    PYTHON_BIN = execSync('which python3').toString().trim();
} catch {
    PYTHON_BIN = process.platform === 'win32' ? 'python' : '/usr/bin/python3';
}

const PYTHON_SCRIPT_PATH = path.join(__dirname, '..', 'baddel_steam_fetcher.py');

/**
 * @param {string} appId
 * @returns {Promise<Object|null>}
 */
async function fetchSteamDataFromPython(appId) {
    if (!appId) return null;

    return new Promise((resolve) => {
        execFile(PYTHON_BIN, [PYTHON_SCRIPT_PATH, appId], { maxBuffer: 1024 * 1024 * 10 }, (error, stdout, stderr) => {
            if (error) {
                console.error(`[SteamBridge] EXIT ERROR for "${appId}":`, error.message);
                console.error(`[SteamBridge] STDERR:`, stderr);
                return resolve(null);
            }

            const cleanStdout = stdout.trim();

            if (!cleanStdout) {
                console.error(`[SteamBridge] EMPTY STDOUT for "${appId}". STDERR:`, stderr);
                return resolve(null);
            }

            try {
                const data = JSON.parse(cleanStdout);
                if (data.error) {
                    console.warn(`[SteamBridge] Script returned error for "${appId}":`, data.error);
                    return resolve(null);
                }
                resolve(data);
            } catch (parseError) {
                console.error(`[SteamBridge] JSON Parse Error for "${appId}":`, parseError.message);
                resolve(null);
            }
        });
    });
}

module.exports = { fetchSteamDataFromPython };