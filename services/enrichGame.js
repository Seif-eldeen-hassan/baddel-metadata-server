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
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

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

    // ── Step 1: Get game title + existing images from DB ─────────────────────
    let gameTitle    = null;
    let existingCover = null;
    let existingHero  = null;
    try {
        const [titleRes, imagesRes] = await Promise.all([
            db.query('SELECT title FROM games WHERE id=$1', [gameId]),
            db.query(
                `SELECT image_type, url, cdn_url FROM game_images
                 WHERE game_id=$1 AND image_type IN ('cover','hero')
                 ORDER BY sort_order ASC`,
                [gameId]
            ),
        ]);
        gameTitle     = titleRes.rows[0]?.title || null;
        existingCover = imagesRes.rows.find(r => r.image_type === 'cover') || null;
        existingHero  = imagesRes.rows.find(r => r.image_type === 'hero')  || null;
    } catch (err) {
        console.warn('[Enrich] Could not fetch game info from DB:', err.message);
    }

    // Epic metadata sent from client (Legendary disk cache) — used as fallback
    // for cover, hero, description, release_year, developer when IGDB/SGDB fail
    const epicMeta = clientData.epic_metadata || null;

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

    // ── Step 4: Decide final image sources ────────────────────────────────────
    //
    // Cover priority:
    //   existing DB cover (already uploaded during import) → SGDB → IGDB
    //   rationale: Epic/Steam cover was already saved at import time — trust it,
    //              don't waste R2 bandwidth re-uploading a worse image.
    //
    // Hero priority:
    //   SGDB → IGDB artworks → existing DB hero (Epic DieselGameBox from import)
    //
    // Logo priority:
    //   SGDB → IGDB
    //
    const coverSource = existingCover
        ? null  // already in DB — skip upload, reuse below
        : (sgdb.cover?.url || igdbData?.images?.cover?.url || null);

    const heroSource = sgdb.hero?.url
        || igdbData?.images?.artworks?.[0]?.url
        || null;
        // existing DB hero handled separately below if heroR2 fails

    const logoSource = sgdb.logo?.url || igdbData?.images?.logo?.url || null;

    const coverMeta = sgdb.cover || igdbData?.images?.cover        || {};
    const heroMeta  = sgdb.hero  || igdbData?.images?.artworks?.[0] || {};

    // ── Step 5: Upload images to R2 ───────────────────────────────────────────
    const [coverR2, heroR2, logoR2] = await Promise.all([
        coverSource
            ? uploadImage(platform, 'cover', externalId, coverSource)
            : Promise.resolve({ sourceUrl: null, cdnUrl: null }),
        heroSource
            ? uploadImage(platform, 'hero', externalId, heroSource)
            : Promise.resolve({ sourceUrl: null, cdnUrl: null }),
        logoSource
            ? uploadImage(platform, 'logo', externalId, logoSource)
            : Promise.resolve({ sourceUrl: null, cdnUrl: null }),
    ]);

    // ── Step 6: Persist images ────────────────────────────────────────────────

    // Cover — only write if we actually fetched a new one (existing already in DB)
    if (coverR2.sourceUrl) {
        const coverSrc = sgdb.cover?.url ? 'steamgriddb' : 'igdb';
        await upsertImage(gameId, coverSrc, 'cover', coverR2.sourceUrl, coverR2.cdnUrl, coverMeta.width, coverMeta.height, 0);
    } else if (existingCover) {
        console.log(`[Enrich] cover already in DB — skipping (${existingCover.cdn_url || existingCover.url})`);
    }

    // Hero — write SGDB/IGDB result; if nothing found, fall back to Epic hero as last resort
    if (heroR2.sourceUrl) {
        const heroSrc = sgdb.hero?.url ? 'steamgriddb' : 'igdb';
        await upsertImage(gameId, heroSrc, 'hero', heroR2.sourceUrl, heroR2.cdnUrl, heroMeta.width, heroMeta.height, 0);
    } else if (existingHero) {
        console.log(`[Enrich] hero: SGDB+IGDB both failed — keeping existing DB hero (${existingHero.cdn_url || existingHero.url})`);
    } else {
        // Last resort: Epic hero — try _heroUrl first (direct URL stored at import),
        // then fall back to picking from keyImages array.
        const epicHeroUrl = epicMeta?._heroUrl
            || _pickEpicImageType(epicMeta?.keyImages, ['DieselGameBox', 'OfferImageWide']);

        if (epicHeroUrl) {
            const epicHeroR2 = await uploadImage('epic', 'hero', externalId, epicHeroUrl);
            if (epicHeroR2.sourceUrl) {
                await upsertImage(gameId, 'epic', 'hero', epicHeroR2.sourceUrl, epicHeroR2.cdnUrl, null, null, 0);
                console.log(`[Enrich] hero: used Epic fallback (${epicMeta?._heroUrl ? '_heroUrl' : 'keyImages'}) → ${epicHeroR2.cdnUrl}`);
            }
        } else {
            console.log(`[Enrich] hero: no source found (SGDB, IGDB, Epic all failed)`);
        }
    }

    // Logo
    if (logoR2.sourceUrl) {
        const logoSrc = sgdb.logo?.url ? 'steamgriddb' : 'igdb';
        await upsertImage(gameId, logoSrc, 'logo', logoR2.sourceUrl, logoR2.cdnUrl, null, null, 0);
    }

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

    // ── Step 8b: Epic/Steam fallbacks for fields IGDB couldn't fill ──────────
    await _applyClientFallbacks(gameId, platform, igdbData, epicMeta, clientData);

    // ── Step 9: Persist client-provided Steam rating ──────────────────────────
    if (clientData.steamRating) {
        await upsertSteamRating(gameId, clientData.steamRating);
    }

    // ── Step 10: Persist client-provided system requirements ─────────────────
    if (Array.isArray(clientData.sysreq) && clientData.sysreq.length) {
        await upsertSysreq(gameId, clientData.sysreq, 'steam');
    }

    const finalCover = coverR2.cdnUrl || existingCover?.cdn_url || null;
    const finalHero  = heroR2.cdnUrl  || existingHero?.cdn_url  || null;

    console.log(
        `[Enrich] ✅ gameId=${gameId}` +
        ` cover=${finalCover ? '✅' : '❌'}` +
        ` hero=${finalHero   ? '✅' : '❌'}` +
        ` logo=${logoR2.cdnUrl   ? '✅' : '❌'}` +
        ` igdb=${igdbData        ? '✅' : '❌'}`
    );

    return {
        gameId,
        igdbId,
        cover:       finalCover,
        hero:        finalHero,
        logo:        logoR2.cdnUrl,
        hasMetadata: !!igdbData,
    };
}

// ─── Epic keyImage picker helper ──────────────────────────────────────────────

function _pickEpicImageType(keyImages, types) {
    if (!Array.isArray(keyImages)) return null;
    for (const type of types) {
        const img = keyImages.find(k => k.type === type);
        if (img?.url) return img.url;
    }
    return null;
}

// ─── Client fallbacks for fields IGDB couldn't fill ──────────────────────────
//
// release_year: IGDB → Epic creationDate year (Epic) / already in DB (Steam)
// developer:    IGDB → Epic metadata.developer
// description:  IGDB → Epic metadata.description (only if it differs from the title)
//
async function _applyClientFallbacks(gameId, platform, igdbData, epicMeta, clientData) {
    try {
        const current = await db.query(
            `SELECT title, release_year FROM games WHERE id=$1`,
            [gameId]
        );
        const row = current.rows[0];
        if (!row) return;

        // release_year fallback
        if (!igdbData?.releaseYear && !row.release_year) {
            let fallbackYear = null;

            if (platform === 'epic' && epicMeta?.creationDate) {
                fallbackYear = new Date(epicMeta.creationDate).getFullYear() || null;
            }

            if (fallbackYear) {
                await db.query(
                    `UPDATE games SET release_year=COALESCE(release_year,$2), updated_at=now() WHERE id=$1`,
                    [gameId, fallbackYear]
                );
                console.log(`[Enrich] release_year fallback → ${fallbackYear} (from Epic creationDate)`);
            }
        }

        // developer + description fallback — only when IGDB returned nothing
        if (!igdbData && platform === 'epic' && epicMeta) {
            const epicDeveloper   = epicMeta.developer   || null;
            // Epic description is often just the title — skip it if so
            const rawDesc         = epicMeta.description || null;
            const epicDescription = rawDesc && rawDesc.toLowerCase() !== (row.title || '').toLowerCase()
                ? rawDesc
                : null;

            if (epicDeveloper || epicDescription) {
                await db.query(
                    `INSERT INTO game_metadata (game_id, source, description, genres, tags, developer, publisher)
                     VALUES ($1, 'epic', $2, '{}', '{}', $3, NULL)
                     ON CONFLICT (game_id, source) DO UPDATE SET
                         description = COALESCE(EXCLUDED.description, game_metadata.description),
                         developer   = COALESCE(EXCLUDED.developer,   game_metadata.developer),
                         fetched_at  = now()`,
                    [gameId, epicDescription, epicDeveloper]
                );
                console.log(`[Enrich] Epic metadata fallback — developer="${epicDeveloper}" description=${!!epicDescription}`);
            }
        }
    } catch (err) {
        console.warn('[Enrich] _applyClientFallbacks error:', err.message);
    }
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
        await delay(500);
    }
}

module.exports = { enrichGame, enrichGames };
