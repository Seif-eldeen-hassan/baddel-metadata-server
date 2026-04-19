'use strict';

/**
 * middleware/validate.js
 *
 * Lightweight request body/query validation.
 * No external dependency — uses plain JS.
 */

function validateImport(req, res, next) {
    const { platform, games } = req.body || {};

    if (!platform || typeof platform !== 'string') {
        return res.status(400).json({ status: 'error', message: 'platform must be a non-empty string' });
    }
    if (!['steam', 'epic'].includes(platform)) {
        return res.status(400).json({ status: 'error', message: 'platform must be "steam" or "epic"' });
    }
    if (!Array.isArray(games) || games.length === 0) {
        return res.status(400).json({ status: 'error', message: 'games must be a non-empty array' });
    }
    if (games.length > 500) {
        return res.status(400).json({ status: 'error', message: 'games array exceeds max size of 500' });
    }
    for (const g of games) {
        if (!g.id || typeof g.id !== 'string') {
            return res.status(400).json({ status: 'error', message: 'each game must have a string id' });
        }
    }
    next();
}

function validateEnrichBatch(req, res, next) {
    const { gameIds, enrichAll, limit } = req.body || {};

    if (!enrichAll && (!Array.isArray(gameIds) || gameIds.length === 0)) {
        return res.status(400).json({ status: 'error', message: 'Provide gameIds array OR enrichAll:true' });
    }
    if (limit !== undefined && (typeof limit !== 'number' || limit < 1 || limit > 1000)) {
        return res.status(400).json({ status: 'error', message: 'limit must be a number between 1 and 1000' });
    }
    if (Array.isArray(gameIds) && gameIds.length > 500) {
        return res.status(400).json({ status: 'error', message: 'gameIds array exceeds max size of 500' });
    }
    next();
}

function validateLookup(req, res, next) {
    const { uuid, platform, namespace, id, slug, title } = req.query;
    const hasOne = uuid || (platform && namespace && id) || slug || title;
    if (!hasOne) {
        return res.status(400).json({ status: 'error', message: 'Provide uuid, platform+namespace+id, slug, or title' });
    }
    // Basic length guard to prevent overly long inputs
    for (const [key, val] of Object.entries(req.query)) {
        if (val && val.length > 500) {
            return res.status(400).json({ status: 'error', message: `Query param "${key}" too long` });
        }
    }
    next();
}

function validatePromoteImages(req, res, next) {
    const { limit } = req.body || {};
    if (limit !== undefined && (typeof limit !== 'number' || limit < 1 || limit > 5000)) {
        return res.status(400).json({ status: 'error', message: 'limit must be between 1 and 5000' });
    }
    next();
}

module.exports = { validateImport, validateEnrichBatch, validateLookup, validatePromoteImages };
