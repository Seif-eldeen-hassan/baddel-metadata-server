'use strict';

const { execFile } = require('child_process');
const path = require('path');

const PYTHON_SCRIPT_PATH = path.join(__dirname, '..', 'baddel_steam_fetcher.py');

/**
 * @param {string} appId 
 * @returns {Promise<Object|null>} 
 */
async function fetchSteamDataFromPython(appId) {
    if (!appId) return null;

    return new Promise((resolve) => {
        execFile('python3', [PYTHON_SCRIPT_PATH, appId], { maxBuffer: 1024 * 1024 * 10 }, (error, stdout, stderr) => {
            if (error) {
                console.error(`[SteamBridge] Python Error for AppID "${appId}":`, stderr || error.message);
                return resolve(null);
            }
            
            const cleanStdout = stdout.trim();
            if (!cleanStdout) {
                console.warn(`[SteamBridge] No data found for AppID: "${appId}"`);
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