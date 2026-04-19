'use strict';

/**
 * services/importOrQueueService.js
 *
 * Core logic shared by:
 *   - POST /games/import        (admin, trusted, batch)
 *   - POST /client/request-enrich (public, low-trust, single game)
 *
 * Single responsibility: given a platform + external ID, ensure the game
 * exists in the DB and is queued for enrichment. Returns a structured result
 * that callers can translate into their own response shapes.
 *
 * Does NOT:
 *   - accept arbitrary client payloads (cover_url, hero_url, epic_metadata
 *     are admin-path only and explicitly excluded from the client path)
 *   - make HTTP calls to its own server routes
 *   - trust anything from the public client beyond platform + id + title
 */

const db                               = require('../db');
const log                              = require('../config/logger');
const { resolveSteamGame,
        resolveEpicGame }              = require('./coverResolver');
const { enqueue, getJobsByGame }       = require('./jobQueue');

// ─── Enrichment completeness check ───────────────────────────────────────────
//
// A game is "sufficiently enriched" when it has at least one metadata row
// and at least one non-cover image.  This is a fast heuristic — good enough
// to avoid re-queuing games that are already fully processed.
//
async function isEnriched(gameId) {
    const [metaRes, imgRes] = await Promise.all([
        db.query(`SELECT 1 FROM game_metadata WHERE game_id=$1 LIMIT 1`, [gameId]),
        db.query(`SELECT 1 FROM game_images   WHERE game_id=$1 AND image_type != 'cover' LIMIT 1`, [gameId]),
    ]);
    return metaRes.rows.length > 0 && imgRes.rows.length > 0;
}

// ─── Core: import one game and enqueue enrichment ─────────────────────────────

/**
 * Looks up, creates if needed, and enqueues enrichment for one game.
 *
 * @param {{ platform: string, externalId: string, hintTitle?: string }} opts
 *   platform   — 'steam' | 'epic'
 *   externalId — the raw external id (app_id or catalog_namespace value)
 *   hintTitle  — optional title hint from the client (used only as slug seed,
 *                never stored directly without server-side resolution)
 *
 * @returns {Promise<{
 *   gameId:        string|null,
 *   jobId:         string|null,
 *   queued:        boolean,
 *   alreadyQueued: boolean,
 *   reason:        'created_and_queued'|'needs_enrich'|'already_queued'|'already_exists'
 * }>}
 */
async function importAndEnqueueOne({ platform, externalId, hintTitle = null }) {
    const ns = platform === 'steam' ? 'app_id' : 'catalog_namespace';

    // ── 1. Check if game already exists ──────────────────────────────────────
    const existing = await db.query(
        `SELECT g.id, g.title FROM games g
         JOIN platform_ids p ON p.game_id = g.id
         WHERE p.platform=$1 AND p.namespace=$2 AND p.external_id=$3`,
        [platform, ns, externalId]
    );

    let gameId = existing.rows[0]?.id || null;

    // ── 2. If game exists, check enrichment state ─────────────────────────────
    if (gameId) {
        const enriched = await isEnriched(gameId);

        if (enriched) {
            // Already fully enriched — nothing to do
            return { gameId, jobId: null, queued: false, alreadyQueued: false, reason: 'already_exists' };
        }

        // Exists but not enriched — check if already queued/running
        const activeJobs = await db.query(
            `SELECT id FROM enrich_jobs
             WHERE game_id=$1 AND status IN ('queued','running')
             LIMIT 1`,
            [gameId]
        );

        if (activeJobs.rows.length > 0) {
            const jobId = activeJobs.rows[0].id;
            return { gameId, jobId, queued: false, alreadyQueued: true, reason: 'already_queued' };
        }

        // Exists, not enriched, not currently queued — enqueue it
        const { id: jobId } = await enqueue({ gameId, platform, externalId, clientData: {} });
        log.info({ gameId, jobId, platform, externalId }, '[ImportOrQueue] needs_enrich — enqueued');
        return { gameId, jobId, queued: true, alreadyQueued: false, reason: 'needs_enrich' };
    }

    // ── 3. Game does not exist — resolve from platform and create ─────────────
    let title      = null;
    let entry_type = 'game';
    let sourceUrl  = null;

    if (platform === 'steam') {
        try {
            const r = await resolveSteamGame(externalId);
            title      = r.title      || null;
            entry_type = r.entry_type || 'game';
            sourceUrl  = r.sourceUrl  || null;
        } catch (err) {
            log.warn({ platform, externalId, err: err.message }, '[ImportOrQueue] Steam resolve failed, using hint');
        }
    } else {
        try {
            const r = await resolveEpicGame(externalId, null);
            sourceUrl = r.sourceUrl || null;
        } catch (err) {
            log.warn({ platform, externalId, err: err.message }, '[ImportOrQueue] Epic resolve failed');
        }
    }

    // Fall back to hint title only if platform resolution gave us nothing
    if (!title && hintTitle) title = hintTitle;

    const slugFromTitle = title
        ? title.toLowerCase().replace(/[^a-z0-9\s]/g, '').trim().replace(/\s+/g, '-')
        : null;
    const slug = slugFromTitle || `${platform}-${externalId}`;
    if (!title) title = slug;

    // ── 4. Insert game + platform_id + cover in a transaction ─────────────────
    const client = await db.connect();
    try {
        await client.query('BEGIN');

        const gameRes = await client.query(
            `INSERT INTO games (slug, title, entry_type)
             VALUES ($1, $2, $3)
             ON CONFLICT (slug) DO UPDATE
               SET title=EXCLUDED.title, entry_type=EXCLUDED.entry_type, updated_at=now()
             RETURNING id`,
            [slug, title, entry_type]
        );
        gameId = gameRes.rows[0].id;

        await client.query(
            `INSERT INTO platform_ids (game_id, platform, namespace, external_id)
             VALUES ($1,$2,$3,$4) ON CONFLICT (platform, namespace, external_id) DO NOTHING`,
            [gameId, platform, ns, externalId]
        );

        if (sourceUrl) {
            await client.query(
                `INSERT INTO game_images (game_id, source, image_type, url, cdn_url, sort_order)
                 VALUES ($1,$2,'cover',$3,NULL,0) ON CONFLICT DO NOTHING`,
                [gameId, platform, sourceUrl]
            );
        }

        await client.query('COMMIT');
        log.info({ gameId, platform, externalId, slug }, '[ImportOrQueue] game created');
    } catch (err) {
        await client.query('ROLLBACK');
        throw err;
    } finally {
        client.release();
    }

    // ── 5. Enqueue enrichment ─────────────────────────────────────────────────
    const { id: jobId } = await enqueue({ gameId, platform, externalId, clientData: {} });
    log.info({ gameId, jobId, platform, externalId }, '[ImportOrQueue] created_and_queued');

    return { gameId, jobId, queued: true, alreadyQueued: false, reason: 'created_and_queued' };
}

module.exports = { importAndEnqueueOne, isEnriched };
