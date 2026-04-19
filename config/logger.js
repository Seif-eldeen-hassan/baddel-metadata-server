'use strict';

/**
 * config/logger.js
 *
 * Structured JSON logger with request-ID support.
 * Uses pino if installed, falls back to a lightweight built-in.
 * Outputs JSON lines to stdout — works with Railway, Render, Fly, etc.
 */

let _pino;
try {
    _pino = require('pino');
} catch {
    _pino = null;
}

function _makeBuiltin(level = 'info') {
    const LEVELS = { trace: 10, debug: 20, info: 30, warn: 40, error: 50, fatal: 60 };
    const minLevel = LEVELS[level] ?? 30;

    const write = (lvl, obj, msg) => {
        if (LEVELS[lvl] < minLevel) return;
        const line = JSON.stringify({
            time:  new Date().toISOString(),
            level: lvl,
            msg:   msg || (typeof obj === 'string' ? obj : undefined),
            ...(typeof obj === 'object' ? obj : {}),
        });
        (LEVELS[lvl] >= 40 ? process.stderr : process.stdout).write(line + '\n');
    };

    const logger = {};
    for (const lvl of ['trace', 'debug', 'info', 'warn', 'error', 'fatal']) {
        logger[lvl] = (objOrMsg, msg) => {
            if (typeof objOrMsg === 'string') write(lvl, {}, objOrMsg);
            else write(lvl, objOrMsg, msg);
        };
    }
    logger.child = (bindings) => {
        const child = {};
        for (const lvl of ['trace', 'debug', 'info', 'warn', 'error', 'fatal']) {
            child[lvl] = (objOrMsg, msg) => {
                if (typeof objOrMsg === 'string') write(lvl, bindings, objOrMsg);
                else write(lvl, { ...bindings, ...objOrMsg }, msg);
            };
        }
        child.child = (b2) => _makeBuiltin(level).child({ ...bindings, ...b2 });
        return child;
    };
    return logger;
}

const log = _pino
    ? _pino({ level: process.env.LOG_LEVEL || 'info' })
    : _makeBuiltin(process.env.LOG_LEVEL || 'info');

module.exports = log;
