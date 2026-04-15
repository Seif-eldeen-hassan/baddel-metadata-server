'use strict';

const { execFile } = require('child_process');
const path = require('path');

const PYTHON_SCRIPT_PATH = path.join(__dirname, '..', 'baddel_epic_fetcher.py');

/**
 * @param {string} target 
 * @returns {Promise<Object|null>} 
 */
async function fetchEpicDataFromPython(target) {
    if (!target) return null;

    return new Promise((resolve) => {
        execFile('python3', [PYTHON_SCRIPT_PATH, target], { maxBuffer: 1024 * 1024 * 10 }, (error, stdout, stderr) => {
            if (error) {
                console.error(`[EpicBridge] Python Error for "${target}":`, stderr || error.message);
                return resolve(null);
            }
            
            // تنظيف النتيجة من أي مسافات فاضية
            const cleanStdout = stdout.trim();
            
            // لو السكربت مرجعش حاجة (اللعبة مش موجودة)
            if (!cleanStdout) {
                console.warn(`[EpicBridge] No data found on Epic for: "${target}"`);
                return resolve(null);
            }
            
            try {
                const data = JSON.parse(cleanStdout);
                resolve(data);
            } catch (parseError) {
                console.error(`[EpicBridge] JSON Parse Error for "${target}":`, parseError.message);
                console.log(`[EpicBridge] Raw output:`, cleanStdout.substring(0, 200)); 
                resolve(null);
            }
        });
    });
}

module.exports = { fetchEpicDataFromPython };