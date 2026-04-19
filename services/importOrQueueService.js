'use strict';

/**
 * services/importOrQueueService.js
 *
 * CHANGES vs original:
 *   All catch blocks now log errFlat (flat string) as a top-level log field
 *   so Railway shows the real error in the log line rather than a collapsed
 *   nested object.  No logic changes.
 */

const db                               = require('../db');
const log                              = require('../config/logger');
const { resolveSteamGame,
        resolveEpicGame }              = require('./coverResolver');
const { enqueue, getJobsByGame,
        pgErrFlat }                    = require('./jobQueue');

// ─── Enrichment completeness check ───────────────────────────────────────────

async function isEnriched(gameId) {
    const [metaRes, imgRes] = await Promise.all([
        db.query(`SELECT 1 FROM game_metadata WHERE game_id=$1 LIMIT 1`, [gameId]),
        db.query(`SELECT 1 FROM game_images   WHERE game_id=$1 AND image_type != 'cover' LIMIT 1`, [gameId]),
    ]);
    return metaRes.rows.length > 0 && imgRes.rows.length > 0;
}

// ─── Core: import one game and enqueue enrichment ─────────────────────────────

async function importAndEnqueueOne({ platform, externalId, hintTitle = null }) {
    const ns = platform === 'steam' ? 'app_id' : 'catalog_namespace';

    // ── 1. Check if game already exists ──────────────────────────────────────
    let existing;
    try {
        existing = await db.query(
            `SELECT g.id, g.title FROM games g
             JOIN platform_ids p ON p.game_id = g.id
             WHERE p.platform=$1 AND p.namespace=$2 AND p.external_id=$3`,
            [platform, ns, externalId]
        );
    } catch (err) {
        log.error(
            { platform, externalId, errFlat: pgErrFlat(err), stack: err?.stack },
            '[ImportOrQueue] DB lookup failed'
        );
        throw err;
    }

    let gameId = existing.rows[0]?.id || null;

    // ── 2. If game exists, check enrichment state ─────────────────────────────
    if (gameId) {
        let enriched;
        try {
            enriched = await isEnriched(gameId);
        } catch (err) {
            log.error(
                { gameId, errFlat: pgErrFlat(err), stack: err?.stack },
                '[ImportOrQueue] isEnriched check failed'
            );
            throw err;
        }

        if (enriched) {
            log.info({ gameId, platform, externalId }, '[ImportOrQueue] already_exists — no work needed');
            return { gameId, jobId: null, queued: false, alreadyQueued: false, reason: 'already_exists' };
        }

        let activeJobs;
        try {
            activeJobs = await db.query(
                `SELECT id FROM enrich_jobs
                 WHERE game_id=$1 AND status IN ('queued','running')
                 LIMIT 1`,
                [gameId]
            );
        } catch (err) {
            log.error(
                { gameId, errFlat: pgErrFlat(err), stack: err?.stack },
                '[ImportOrQueue] active job check failed'
            );
            throw err;
        }

        if (activeJobs.rows.length > 0) {
            const jobId = activeJobs.rows[0].id;
            log.info({ gameId, jobId, platform, externalId }, '[ImportOrQueue] already_queued — active job exists');
            return { gameId, jobId, queued: false, alreadyQueued: true, reason: 'already_queued' };
        }

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
        await client.query('ROLLBACK').catch(() => {});
        log.error(
            { platform, externalId, slug, errFlat: pgErrFlat(err), stack: err?.stack },
            '[ImportOrQueue] game insert transaction failed'
        );
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