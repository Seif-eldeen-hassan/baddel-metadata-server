'use strict';

/**
 * middleware/validateResolveMetadataRequest.js
 *
 * Strict, low-trust validation for POST /client/resolve-metadata.
 *
 * Allowed body fields: title, slug, platformHint, exeName, folderName,
 *                      command, pathHint, igdbId
 * Everything else is rejected.
 *
 * Design notes:
 *   - title is required (the primary resolution signal)
 *   - all other fields are optional hints; none are required
 *   - string fields have tight length caps
 *   - igdbId, if provided, must be a positive integer (allows direct-ID fast path)
 *   - unknown fields are rejected outright (abuse-surface reduction)
 *   - pathHint and command are accepted but never used in resolver logic that
 *     makes outbound calls — they are stored only for telemetry/logging
 */

const MAX_TITLE_LENGTH    = 200;
const MAX_SLUG_LENGTH     = 200;
const MAX_HINT_LENGTH     = 300;   // exeName, folderName, command, pathHint, platformHint

const ALLOWED_FIELDS = new Set([
    'title', 'slug', 'platformHint',
    'exeName', 'folderName', 'command', 'pathHint',
    'igdbId',
    'releaseYear',   // optional: positive integer ≤ current year + 2
    'developer',     // optional: string hint (same length cap as other hints)
    'publisher',     // optional: string hint
]);

const SAFE_STRING_RE = /^[^\x00-\x1f\x7f]*$/; // no ASCII control chars

function validateResolveMetadataRequest(req, res, next) {
    const body = req.body;

    // Must be a plain object
    if (!body || typeof body !== 'object' || Array.isArray(body)) {
        return res.status(400).json({ status: 'error', message: 'Request body must be a JSON object' });
    }

    // Reject unknown fields
    for (const key of Object.keys(body)) {
        if (!ALLOWED_FIELDS.has(key)) {
            return res.status(400).json({ status: 'error', message: `Unknown field: "${key}"` });
        }
    }

    const { title, slug, platformHint, exeName, folderName, command, pathHint, igdbId, releaseYear, developer, publisher } = body;

    // title — required, non-empty string
    if (!title || typeof title !== 'string') {
        return res.status(400).json({ status: 'error', message: 'title is required' });
    }
    if (title.trim().length === 0) {
        return res.status(400).json({ status: 'error', message: 'title must not be blank' });
    }
    if (title.length > MAX_TITLE_LENGTH) {
        return res.status(400).json({ status: 'error', message: `title exceeds maximum length of ${MAX_TITLE_LENGTH}` });
    }
    if (!SAFE_STRING_RE.test(title)) {
        return res.status(400).json({ status: 'error', message: 'title contains invalid control characters' });
    }

    // slug — optional string
    if (slug !== undefined) {
        if (typeof slug !== 'string') {
            return res.status(400).json({ status: 'error', message: 'slug must be a string' });
        }
        if (slug.length > MAX_SLUG_LENGTH) {
            return res.status(400).json({ status: 'error', message: `slug exceeds maximum length of ${MAX_SLUG_LENGTH}` });
        }
    }

    // string hint fields — all optional
    for (const field of ['platformHint', 'exeName', 'folderName', 'command', 'pathHint']) {
        const val = body[field];
        if (val !== undefined) {
            if (typeof val !== 'string') {
                return res.status(400).json({ status: 'error', message: `${field} must be a string` });
            }
            if (val.length > MAX_HINT_LENGTH) {
                return res.status(400).json({ status: 'error', message: `${field} exceeds maximum length of ${MAX_HINT_LENGTH}` });
            }
            if (!SAFE_STRING_RE.test(val)) {
                return res.status(400).json({ status: 'error', message: `${field} contains invalid control characters` });
            }
        }
    }

    // igdbId — optional positive integer
    if (igdbId !== undefined) {
        if (!Number.isInteger(igdbId) || igdbId <= 0) {
            return res.status(400).json({ status: 'error', message: 'igdbId must be a positive integer' });
        }
    }

    // releaseYear — optional positive integer in a plausible game-release range
    if (releaseYear !== undefined) {
        const currentYear = new Date().getFullYear();
        if (!Number.isInteger(releaseYear) || releaseYear < 1970 || releaseYear > currentYear + 2) {
            return res.status(400).json({ status: 'error', message: 'releaseYear must be a 4-digit integer between 1970 and ' + (currentYear + 2) });
        }
    }

    // developer / publisher — optional strings, same safety rules as other hint fields
    for (const field of ['developer', 'publisher']) {
        const val = body[field];
        if (val !== undefined) {
            if (typeof val !== 'string') {
                return res.status(400).json({ status: 'error', message: field + ' must be a string' });
            }
            if (val.length > MAX_HINT_LENGTH) {
                return res.status(400).json({ status: 'error', message: field + ' exceeds maximum length of ' + MAX_HINT_LENGTH });
            }
            if (!SAFE_STRING_RE.test(val)) {
                return res.status(400).json({ status: 'error', message: field + ' contains invalid control characters' });
            }
        }
    }

    next();
}

module.exports = { validateResolveMetadataRequest };
