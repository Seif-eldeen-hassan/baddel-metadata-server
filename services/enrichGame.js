'use strict';

/**
 * services/enrichGame.js
 *
 * Orchestrates full game enrichment after initial import:
 *
 *  Images  (cover / hero / logo):
 *      1st choice → SteamGridDB
 *      fallback   → IGDB
 *
 *  Everything else (metadata, ratings, videos):
 *      1st choice → IGDB
 *      (future)   → other sources
 *
 * Called either inline or as a background job from the /games/enrich endpoint.
 */

const db                    = require('../db');
const { resolveAndFetch }   = require('./igdbResolver');
const { resolveSgdbImages } = require('./steamGridDbResolver');
const { uploadImageToR2, buildCoverKey } = require('./r2');
const { resolveSysreq }             = require('./sysreqResolver');

// ─── Image upload helper ──────────────────────────────────────────────────────

/**
 * Upload an image to R2 and return { sourceUrl, cdnUrl }.
 * Returns { sourceUrl: null, cdnUrl: null } on failure.
 */
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
             score        = EXCLUDED.score,
             total_reviews= EXCLUDED.total_reviews,
             fetched_at   = now()`,
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
             ON CONFLICT DO NOTHING`,
            [gameId, v.url, v.thumb || null, v.title || null, i]
        );
    }
}

async function upsertSysreq(gameId, rows) {
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
            [gameId, r.source, r.platform, r.tier,
             r.os_versions, r.cpu, r.gpu, r.ram, r.storage, r.directx, r.notes]
        );
    }
}

async function updateGameMeta(gameId, igdbData) {
    // Only update release_year if not already set
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
 * Enrich a single game record that is already in the DB.
 *
 * @param {string} gameId        - UUID in games table
 * @param {'steam'|'epic'} platform
 * @param {string} externalId    - Steam appId or Epic catalog namespace
 */
async function enrichGame(gameId, platform, externalId) {
    console.log(`[Enrich] ▶ gameId=${gameId} platform=${platform} id=${externalId}`);

    // ── Step 1: Resolve IGDB data ──────────────────────────────────────────────
    let igdbResult = null;
    try {
        igdbResult = await resolveAndFetch(platform, externalId);
    } catch (err) {
        console.warn(`[Enrich] IGDB lookup failed for ${platform}:${externalId} —`, err.message);
    }

    const igdbData  = igdbResult?.data  ?? null;
    const igdbId    = igdbResult?.igdbId ?? null;

    // ── Step 2: Resolve SteamGridDB images ─────────────────────────────────────
    const steamAppId = platform === 'steam' ? externalId : undefined;
    let sgdb = { cover: null, hero: null, logo: null };
    try {
        sgdb = await resolveSgdbImages({ steamAppId, igdbId: igdbId ?? undefined });
    } catch (err) {
        console.warn(`[Enrich] SGDB fetch failed —`, err.message);
    }

    // ── Step 3: Decide final image sources (SGDB → IGDB fallback) ─────────────
    //
    //   cover: sgdb.cover  → igdbData.images.cover
    //   hero:  sgdb.hero   → igdbData.images.artworks[0]
    //   logo:  sgdb.logo   → (no IGDB equivalent — logos aren't in IGDB game data)

    const coverSource = sgdb.cover?.url   || igdbData?.images?.cover?.url   || null;
    const heroSource  = sgdb.hero?.url    || igdbData?.images?.artworks?.[0]?.url || null;
    const logoSource  = sgdb.logo?.url    || null;

    const coverMeta   = sgdb.cover || igdbData?.images?.cover || {};
    const heroMeta    = sgdb.hero  || igdbData?.images?.artworks?.[0] || {};

    // ── Step 4: Upload images to R2 in parallel ────────────────────────────────
    const idStr = externalId; // used in R2 key

    const [coverR2, heroR2, logoR2] = await Promise.all([
        uploadImage(platform, 'cover', idStr, coverSource),
        uploadImage(platform, 'hero',  idStr, heroSource),
        uploadImage(platform, 'logo',  idStr, logoSource),
    ]);

    // ── Step 5: Persist images ─────────────────────────────────────────────────
    // Determine source label (sgdb or igdb) for cover/hero
    const coverSrc = sgdb.cover?.url ? 'steamgriddb' : 'igdb';
    const heroSrc  = sgdb.hero?.url  ? 'steamgriddb' : 'igdb';
    const logoSrc  = 'steamgriddb';

    await Promise.all([
        upsertImage(gameId, coverSrc, 'cover',
            coverR2.sourceUrl, coverR2.cdnUrl,
            coverMeta.width, coverMeta.height, 0),
        upsertImage(gameId, heroSrc,  'hero',
            heroR2.sourceUrl,  heroR2.cdnUrl,
            heroMeta.width,  heroMeta.height,  0),
        upsertImage(gameId, logoSrc,  'logo',
            logoR2.sourceUrl,  logoR2.cdnUrl,
            null, null, 0),
    ]);

    // ── Step 6: Persist screenshots from IGDB ─────────────────────────────────
    if (igdbData?.images?.screenshots?.length) {
        for (let i = 0; i < igdbData.images.screenshots.length; i++) {
            const ss = igdbData.images.screenshots[i];
            const r2 = await uploadImage(platform, `screenshot_${i}`, idStr, ss.url);
            await upsertImage(gameId, 'igdb', 'screenshot',
                r2.sourceUrl, r2.cdnUrl, ss.width, ss.height, i);
        }
    }

    // ── Step 7: Persist metadata, ratings, videos ──────────────────────────────
    if (igdbData) {
        await Promise.all([
            upsertMetadata(gameId, igdbData),
            upsertRating(gameId, igdbData.rating),
            upsertVideos(gameId, igdbData.videos || []),
            updateGameMeta(gameId, igdbData),
        ]);
    }

    // ── Step 8: System Requirements ───────────────────────────────────────────
    const title = igdbData?.title || null;
    const sysreqRows = await resolveSysreq({ platform, externalId, title });
    if (sysreqRows.length) {
        await upsertSysreq(gameId, sysreqRows);
    }

    console.log(
        `[Enrich] ✅ gameId=${gameId}` +
        ` cover=${coverR2.cdnUrl ? '✅' : '❌'}` +
        ` hero=${heroR2.cdnUrl   ? '✅' : '❌'}` +
        ` logo=${logoR2.cdnUrl   ? '✅' : '❌'}` +
        ` igdb=${igdbData         ? '✅' : '❌'}`
    );

    return {
        gameId,
        igdbId,
        cover: coverR2.cdnUrl,
        hero:  heroR2.cdnUrl,
        logo:  logoR2.cdnUrl,
        hasMetadata: !!igdbData,
    };
}

/**
 * Enrich multiple games sequentially (background job).
 *
 * @param {{ gameId: string, platform: string, externalId: string }[]} items
 */
async function enrichGames(items) {
    for (const item of items) {
        try {
            await enrichGame(item.gameId, item.platform, item.externalId);
        } catch (err) {
            console.error(`[Enrich] ❌ gameId=${item.gameId} —`, err.message);
        }
    }
}

module.exports = { enrichGame, enrichGames };
