'use strict';

/**
 * services/enrichGame.js
 *
 * Orchestrates full game enrichment after initial import:
 *
 *  Images  (cover / hero / logo):  SteamGridDB first → IGDB fallback
 *  Metadata / Ratings / Videos:    IGDB first
 *  System Requirements:            Provided by client in import payload (server-side sources blocked on Railway)
 */

const db                    = require('../db');
const { resolveAndFetch }   = require('./igdbResolver');
const { resolveSgdbImages } = require('./steamGridDbResolver');
const { uploadImageToR2, buildCoverKey } = require('./r2');

// ─── Image upload helper ──────────────────────────────────────────────────────

async function uploadImage(platform, imageType, idStr, imageUrl) {
    if (!imageUrl) return { sourceUrl: null, cdnUrl: null };
    try {
        const key    = buildCoverKey(`${platform}_${imageType}`, idStr, imageUrl);
        const cdnUrl = await uploadImageToR2(imageUrl, key);
        return { sourceUrl: imageUrl, cdnUrl };
    } catch (err) {
        console.warn(`[Enrich] R2 upload failed (${imageType}):`, err.message);
        return { sourceUrl: null, cdnUrl: null };
    }
}

// ─── DB helpers ───────────────────────────────────────────────────────────────

async function upsertImage(gameId, source, imageType, sourceUrl, cdnUrl, width, height, sortOrder = 0) {
    if (!sourceUrl) return;
    await db.query(
        `INSERT INTO game_images
             (game_id, source, image_type, url, cdn_url, width, height, sort_order)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
         ON CONFLICT DO NOTHING`,
        [gameId, source, imageType, sourceUrl, cdnUrl || null, width || null, height || null, sortOrder]
    );
}

async function upsertMetadata(gameId, data) {
    await db.query(
        `INSERT INTO game_metadata
             (game_id, source, description, genres, tags, developer, publisher)
         VALUES ($1,'igdb',$2,$3,$4,$5,$6)
         ON CONFLICT (game_id, source) DO UPDATE SET
             description = EXCLUDED.description,
             genres      = EXCLUDED.genres,
             tags        = EXCLUDED.tags,
             developer   = EXCLUDED.developer,
             publisher   = EXCLUDED.publisher,
             fetched_at  = now()`,
        [
            gameId,
            data.description || null,
            data.genres      || [],
            data.tags        || [],
            data.developer   || null,
            data.publisher   || null,
        ]
    );
}

async function upsertRating(gameId, rating) {
    if (!rating?.score) return;
    await db.query(
        `INSERT INTO game_ratings
             (game_id, source, score, max_score, total_reviews)
         VALUES ($1,$2,$3,100,$4)
         ON CONFLICT (game_id, source) DO UPDATE SET
             score         = EXCLUDED.score,
             total_reviews = EXCLUDED.total_reviews,
             fetched_at    = now()`,
        [gameId, rating.source, Math.round(rating.score * 10) / 10, rating.count || null]
    );
}

async function upsertVideos(gameId, videos) {
    for (let i = 0; i < videos.length; i++) {
        const v = videos[i];
        await db.query(
            `INSERT INTO game_media
                 (game_id, source, media_type, url, thumbnail_url, title, sort_order)
             VALUES ($1,'igdb','trailer',$2,$3,$4,$5)
             ON CONFLICT (game_id, url) DO NOTHING`,
            [gameId, v.url, v.thumb || null, v.title || null, i]
        );
    }
}

async function upsertSysreq(gameId, rows, source = 'steam') {
    for (const r of rows) {
        await db.query(
            `INSERT INTO game_system_requirements
                 (game_id, source, platform, tier, os_versions, cpu, gpu, ram, storage, directx, notes)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
             ON CONFLICT (game_id, source, platform, tier) DO UPDATE SET
                 os_versions = EXCLUDED.os_versions,
                 cpu         = EXCLUDED.cpu,
                 gpu         = EXCLUDED.gpu,
                 ram         = EXCLUDED.ram,
                 storage     = EXCLUDED.storage,
                 directx     = EXCLUDED.directx,
                 notes       = EXCLUDED.notes,
                 fetched_at  = now()`,
            [gameId, source, r.platform, r.tier,
             r.os_versions || null, r.cpu || null, r.gpu || null,
             r.ram || null, r.storage || null, r.directx || null, r.notes || null]
        );
    }
}

async function upsertSteamRating(gameId, steamRating) {
    if (!steamRating?.score) return;
    await db.query(
        `INSERT INTO game_ratings
             (game_id, source, score, max_score, positive_count, negative_count, total_reviews)
         VALUES ($1,'steam',$2,100,$3,$4,$5)
         ON CONFLICT (game_id, source) DO UPDATE SET
             score         = EXCLUDED.score,
             positive_count= EXCLUDED.positive_count,
             negative_count= EXCLUDED.negative_count,
             total_reviews = EXCLUDED.total_reviews,
             fetched_at    = now()`,
        [
            gameId,
            steamRating.score,
            steamRating.positive || null,
            steamRating.negative || null,
            steamRating.total    || null,
        ]
    );
}

async function updateGameMeta(gameId, igdbData) {
    if (igdbData.releaseYear) {
        await db.query(
            `UPDATE games SET
                 release_year = COALESCE(release_year, $2),
                 updated_at   = now()
             WHERE id = $1`,
            [gameId, igdbData.releaseYear]
        );
    }
}

// ─── Main enrich function ─────────────────────────────────────────────────────

/**
 * Enrich a single game record already in the DB.
 *
 * @param {string} gameId
 * @param {'steam'|'epic'} platform
 * @param {string} externalId
 * @param {object} [clientData]             - optional data sent from client
 * @param {object} [clientData.steamRating] - { score, positive, negative, total }
 * @param {Array}  [clientData.sysreq]      - [{ platform, tier, os_versions, cpu, gpu, ram, storage, directx }]
 */
async function enrichGame(gameId, platform, externalId, clientData = {}) {
    console.log(`[Enrich] ▶ gameId=${gameId} platform=${platform} id=${externalId}`);

    // ── Step 1: Get game title from DB ────────────────────────────────────────
    let gameTitle = null;
    try {
        const r = await db.query('SELECT title FROM games WHERE id=$1', [gameId]);
        gameTitle = r.rows[0]?.title || null;
    } catch (err) {
        console.warn('[Enrich] Could not fetch game title:', err.message);
    }

    // ── Step 2: Resolve IGDB data ─────────────────────────────────────────────
    let igdbResult = null;
    try {
        igdbResult = await resolveAndFetch(platform, externalId, gameTitle);
    } catch (err) {
        console.warn(`[Enrich] IGDB lookup failed —`, err.message);
    }

    const igdbData = igdbResult?.data  ?? null;
    const igdbId   = igdbResult?.igdbId ?? null;

    // ── Step 3: Resolve SteamGridDB images ────────────────────────────────────
    const steamAppId    = platform === 'steam' ? externalId : undefined;
    const epicNamespace = platform === 'epic'  ? externalId : undefined;
    const titleForSgdb  = igdbData?.title || gameTitle || undefined;

    let sgdb = { cover: null, hero: null, logo: null };
    try {
        sgdb = await resolveSgdbImages({
            steamAppId,
            epicNamespace,
            igdbId:  igdbId   ?? undefined,
            title:   titleForSgdb,
        });
    } catch (err) {
        console.warn(`[Enrich] SGDB fetch failed —`, err.message);
    }

    // ── Step 4: Decide final image sources (SGDB → IGDB fallback) ────────────
    const coverSource = sgdb.cover?.url || igdbData?.images?.cover?.url        || null;
    const heroSource  = sgdb.hero?.url  || igdbData?.images?.artworks?.[0]?.url || null;
    const logoSource  = sgdb.logo?.url  || null;

    const coverMeta = sgdb.cover || igdbData?.images?.cover        || {};
    const heroMeta  = sgdb.hero  || igdbData?.images?.artworks?.[0] || {};

    // ── Step 5: Upload images to R2 ───────────────────────────────────────────
    const [coverR2, heroR2, logoR2] = await Promise.all([
        uploadImage(platform, 'cover', externalId, coverSource),
        uploadImage(platform, 'hero',  externalId, heroSource),
        uploadImage(platform, 'logo',  externalId, logoSource),
    ]);

    // ── Step 6: Persist images ────────────────────────────────────────────────
    const coverSrc = sgdb.cover?.url ? 'steamgriddb' : 'igdb';
    const heroSrc  = sgdb.hero?.url  ? 'steamgriddb' : 'igdb';

    await Promise.all([
        upsertImage(gameId, coverSrc,      'cover',      coverR2.sourceUrl, coverR2.cdnUrl, coverMeta.width, coverMeta.height, 0),
        upsertImage(gameId, heroSrc,       'hero',       heroR2.sourceUrl,  heroR2.cdnUrl,  heroMeta.width,  heroMeta.height,  0),
        upsertImage(gameId, 'steamgriddb', 'logo',       logoR2.sourceUrl,  logoR2.cdnUrl,  null, null, 0),
    ]);

    // ── Step 7: Persist screenshots from IGDB ────────────────────────────────
    if (igdbData?.images?.screenshots?.length) {
        for (let i = 0; i < igdbData.images.screenshots.length; i++) {
            const ss = igdbData.images.screenshots[i];
            const r2 = await uploadImage(platform, `screenshot_${i}`, externalId, ss.url);
            await upsertImage(gameId, 'igdb', 'screenshot', r2.sourceUrl, r2.cdnUrl, ss.width, ss.height, i);
        }
    }

    // ── Step 8: Persist IGDB metadata, ratings, videos ───────────────────────
    if (igdbData) {
        await Promise.all([
            upsertMetadata(gameId, igdbData),
            upsertRating(gameId, igdbData.rating),
            upsertVideos(gameId, igdbData.videos || []),
            updateGameMeta(gameId, igdbData),
        ]);
    }

    // ── Step 9: Persist client-provided Steam rating ──────────────────────────
    if (clientData.steamRating) {
        await upsertSteamRating(gameId, clientData.steamRating);
    }

    // ── Step 10: Persist client-provided system requirements ─────────────────
    if (Array.isArray(clientData.sysreq) && clientData.sysreq.length) {
        await upsertSysreq(gameId, clientData.sysreq, 'steam');
    }

    console.log(
        `[Enrich] ✅ gameId=${gameId}` +
        ` cover=${coverR2.cdnUrl ? '✅' : '❌'}` +
        ` hero=${heroR2.cdnUrl   ? '✅' : '❌'}` +
        ` logo=${logoR2.cdnUrl   ? '✅' : '❌'}` +
        ` igdb=${igdbData        ? '✅' : '❌'}`
    );

    return {
        gameId,
        igdbId,
        cover:       coverR2.cdnUrl,
        hero:        heroR2.cdnUrl,
        logo:        logoR2.cdnUrl,
        hasMetadata: !!igdbData,
    };
}

/**
 * Enrich multiple games sequentially (background job).
 */
async function enrichGames(items) {
    for (const item of items) {
        try {
            await enrichGame(item.gameId, item.platform, item.externalId, item.clientData || {});
        } catch (err) {
            console.error(`[Enrich] ❌ gameId=${item.gameId} —`, err.message);
        }
    }
}

module.exports = { enrichGame, enrichGames };
