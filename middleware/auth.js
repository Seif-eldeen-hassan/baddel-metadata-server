'use strict';

/**
 * middleware/auth.js
 *
 * Simple bearer-token authentication for admin/mutating routes.
 * Set API_SECRET in env (strong random, 32+ chars recommended).
 *
 * Usage:
 *   app.post('/games/import', requireAuth, handler);
 *
 * Clients must send:
 *   Authorization: Bearer <API_SECRET>
 */

const log = require('../config/logger');

function requireAuth(req, res, next) {
    const header = req.headers['authorization'] || '';
    const token  = header.startsWith('Bearer ') ? header.slice(7) : null;

    if (!token || token !== process.env.API_SECRET) {
        log.warn({ path: req.path, ip: req.ip }, 'Unauthorized request rejected');
        return res.status(401).json({ status: 'error', message: 'Unauthorized' });
    }
    next();
}

/**
 * Cron endpoint guard.
 * If CRON_SECRET is not set, the route is disabled entirely (fail closed).
 * If set, requires X-Cron-Secret header to match.
 */
function requireCronSecret(req, res, next) {
    const secret = process.env.CRON_SECRET;
    if (!secret) {
        // Fail closed: route is disabled when secret not configured
        log.warn({ path: req.path }, 'Cron endpoint hit but CRON_SECRET not set — disabled');
        return res.status(503).json({ status: 'error', message: 'Cron endpoint not configured' });
    }
    if (req.headers['x-cron-secret'] !== secret) {
        log.warn({ path: req.path, ip: req.ip }, 'Invalid cron secret');
        return res.status(401).json({ status: 'error', message: 'Unauthorized' });
    }
    next();
}

module.exports = { requireAuth, requireCronSecret };
