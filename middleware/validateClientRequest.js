'use strict';

/**
 * middleware/validateClientRequest.js
 *
 * Strict, minimal validation for POST /client/request-enrich.
 *
 * Allowed body fields: platform, id, title (optional)
 * Everything else is rejected.
 *
 * Limits are deliberately tight because this is a public, low-trust endpoint.
 *
 * Request headers used (optional, never for auth):
 *   X-Baddel-Install-Id  — used as the primary rate-limit key when present and valid
 *   X-Baddel-App-Version — logged for telemetry/abuse analysis only
 *
 * Neither header is required. An invalid or missing install-id causes the
 * rate limiter to fall back to keying by IP; it does NOT reject the request.
 */

// Steam app IDs are up to 7 digits. Epic namespace slugs are typically 32 hex chars.
// We allow up to 64 chars to cover edge cases, but no more.
const MAX_ID_LENGTH    = 64;
const MAX_TITLE_LENGTH = 200;

// Only these exact top-level fields are allowed — reject anything extra
const ALLOWED_FIELDS = new Set(['platform', 'id', 'title']);

function validateClientEnrichRequest(req, res, next) {
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

    const { platform, id, title } = body;

    // platform — required, exact enum
    if (!platform || typeof platform !== 'string') {
        return res.status(400).json({ status: 'error', message: 'platform is required' });
    }
    if (!['steam', 'epic'].includes(platform)) {
        return res.status(400).json({ status: 'error', message: 'platform must be "steam" or "epic"' });
    }

    // id — required, string, sane length, safe characters only
    if (!id || typeof id !== 'string') {
        return res.status(400).json({ status: 'error', message: 'id is required' });
    }
    if (id.trim().length === 0) {
        return res.status(400).json({ status: 'error', message: 'id must not be blank' });
    }
    if (id.length > MAX_ID_LENGTH) {
        return res.status(400).json({ status: 'error', message: `id exceeds maximum length of ${MAX_ID_LENGTH}` });
    }
    // Allow alphanumeric, hyphens, underscores only — no URLs, no HTML, no special chars
    if (!/^[a-zA-Z0-9_\-]+$/.test(id)) {
        return res.status(400).json({ status: 'error', message: 'id contains invalid characters' });
    }

    // title — optional, string, sane length
    if (title !== undefined) {
        if (typeof title !== 'string') {
            return res.status(400).json({ status: 'error', message: 'title must be a string' });
        }
        if (title.length > MAX_TITLE_LENGTH) {
            return res.status(400).json({ status: 'error', message: `title exceeds maximum length of ${MAX_TITLE_LENGTH}` });
        }
    }

    next();
}

module.exports = { validateClientEnrichRequest };
