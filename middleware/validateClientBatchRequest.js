'use strict';

/**
 * middleware/validateClientBatchRequest.js
 *
 * Strict, low-trust validation for POST /client/request-enrich/batch.
 *
 * Allowed body fields: platform, games
 * Each game object allows: id, title (optional)
 * Everything else is rejected.
 *
 * Design:
 *   - Hard cap at MAX_BATCH_SIZE items per request (see rationale below)
 *   - Deduplication happens here so the service layer never sees duplicates
 *   - Invalid individual items are collected and returned as `invalid` results
 *     rather than aborting the whole batch — the launcher should not have to
 *     retry 400 games because one id had a typo
 *   - Unknown top-level fields are rejected outright (abuse surface reduction)
 *
 * MAX_BATCH_SIZE = 200
 *   A 300–400 game library needs at most 2 requests (200 + remainder).
 *   200 is large enough to cover most full-library cold syncs in one shot,
 *   while being small enough that a single DB transaction storm is avoidable
 *   (games are processed with Promise.allSettled, not in a single TX).
 *   At 200 games the JSON body is ~15–20 KB — well within the 2 MB express limit.
 *   Raising to 500+ risks memory pressure and very long-tail response times that
 *   cause launcher timeouts. 200 is the safe ceiling.
 */

const MAX_BATCH_SIZE   = 200;
const MAX_ID_LENGTH    = 64;
const MAX_TITLE_LENGTH = 200;

const ALLOWED_TOP_FIELDS  = new Set(['platform', 'games']);
const ALLOWED_GAME_FIELDS = new Set(['id', 'title']);

const VALID_ID_RE = /^[a-zA-Z0-9_\-]+$/;

/**
 * Validate and normalise a single game entry within the batch.
 * Returns { ok: true, id, title } or { ok: false, id, reason }.
 */
function validateGameEntry(entry, index) {
    // Must be a plain object
    if (!entry || typeof entry !== 'object' || Array.isArray(entry)) {
        return { ok: false, id: null, index, reason: `games[${index}]: must be an object` };
    }

    // Reject unknown fields on each game object
    for (const key of Object.keys(entry)) {
        if (!ALLOWED_GAME_FIELDS.has(key)) {
            return { ok: false, id: entry.id ?? null, index, reason: `games[${index}]: unknown field "${key}"` };
        }
    }

    const { id, title } = entry;

    // id — required, string, safe characters, sane length
    if (!id || typeof id !== 'string') {
        return { ok: false, id: null, index, reason: `games[${index}]: id is required` };
    }
    if (id.trim().length === 0) {
        return { ok: false, id: null, index, reason: `games[${index}]: id must not be blank` };
    }
    if (id.length > MAX_ID_LENGTH) {
        return { ok: false, id, index, reason: `games[${index}]: id exceeds maximum length of ${MAX_ID_LENGTH}` };
    }
    if (!VALID_ID_RE.test(id)) {
        return { ok: false, id, index, reason: `games[${index}]: id contains invalid characters` };
    }

    // title — optional, string, sane length
    if (title !== undefined) {
        if (typeof title !== 'string') {
            return { ok: false, id, index, reason: `games[${index}]: title must be a string` };
        }
        if (title.length > MAX_TITLE_LENGTH) {
            return { ok: false, id, index, reason: `games[${index}]: title exceeds maximum length of ${MAX_TITLE_LENGTH}` };
        }
    }

    return { ok: true, id: id.trim(), title: title ? title.trim() : undefined };
}

/**
 * Express middleware.
 *
 * On success, attaches to req:
 *   req.validatedBatch = {
 *     platform: 'steam' | 'epic',
 *     validGames: [{ id, title? }, ...],   // deduplicated, validated
 *     invalidResults: [{ id, reason, status: 'invalid' }, ...],
 *     dedupCount: number,  // items dropped as exact duplicates within the batch
 *   }
 *
 * Hard failures (wrong top-level shape, oversized batch, bad platform) still
 * return 400 and stop processing — these indicate a misconfigured client.
 * Per-item failures are collected into invalidResults so the launcher can
 * surface them without retrying the whole batch.
 */
function validateClientBatchRequest(req, res, next) {
    const body = req.body;

    // Must be a plain object
    if (!body || typeof body !== 'object' || Array.isArray(body)) {
        return res.status(400).json({ status: 'error', message: 'Request body must be a JSON object' });
    }

    // Reject unknown top-level fields
    for (const key of Object.keys(body)) {
        if (!ALLOWED_TOP_FIELDS.has(key)) {
            return res.status(400).json({ status: 'error', message: `Unknown field: "${key}"` });
        }
    }

    const { platform, games } = body;

    // platform — required, exact enum
    if (!platform || typeof platform !== 'string') {
        return res.status(400).json({ status: 'error', message: 'platform is required' });
    }
    if (!['steam', 'epic'].includes(platform)) {
        return res.status(400).json({ status: 'error', message: 'platform must be "steam" or "epic"' });
    }

    // games — required, array, non-empty, hard cap
    if (!Array.isArray(games)) {
        return res.status(400).json({ status: 'error', message: 'games must be an array' });
    }
    if (games.length === 0) {
        return res.status(400).json({ status: 'error', message: 'games must not be empty' });
    }
    if (games.length > MAX_BATCH_SIZE) {
        return res.status(400).json({
            status:  'error',
            message: `Batch exceeds maximum size of ${MAX_BATCH_SIZE}. Split into multiple requests.`,
            maxBatchSize: MAX_BATCH_SIZE,
        });
    }

    // Validate each game entry
    const validGames   = [];
    const invalidResults = [];
    const seenIds      = new Map(); // platform+id -> first index seen (for dedup tracking)
    let   dedupCount   = 0;

    for (let i = 0; i < games.length; i++) {
        const result = validateGameEntry(games[i], i);

        if (!result.ok) {
            invalidResults.push({ id: result.id, index: i, reason: result.reason, status: 'invalid' });
            continue;
        }

        const dedupKey = `${platform}:${result.id}`;
        if (seenIds.has(dedupKey)) {
            // Silently drop the duplicate — it is not an error, just redundant
            dedupCount++;
            continue;
        }

        seenIds.set(dedupKey, i);
        validGames.push({ id: result.id, title: result.title });
    }

    req.validatedBatch = { platform, validGames, invalidResults, dedupCount };
    next();
}

module.exports = { validateClientBatchRequest, MAX_BATCH_SIZE };
