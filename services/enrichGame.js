'use strict';

/**
 * services/enrichGame.js
 *
 * Two-phase bounded pipeline for game enrichment:
 *
 *  Phase 1 — Lookup pool (8 games concurrent, pure HTTP GET)
 *    IGDB resolve + fetch, SteamGridDB resolve
 *    Results go into a bounded buffer (max 15 items)
 *    Phase 1 pauses when buffer is full to avoid RAM spike
 *
 *  Phase 2 — Upload worker (1 game at a time)
 *    Sharp compress → R2 PUT (cover, hero, logo, screenshots in batches of 3)
 *    DB writes run in parallel within a single game
 *    Emits SSE event after each game finishes
 *
 * enrichGame()      — single-game path, used by direct API calls
 * enrichGames()     — sequential fallback, used by old queue
 * enrichGamesBatch()— new bounded pipeline, used by server._drainQueue
 */

const { EventEmitter }                   = require('events');
const db                                 = require('../db');
const { resolveAndFetch }                = require('./igdbResolver');
const { resolveSgdbImages }              = require('./steamGridDbResolver');
const { uploadImageToR2, buildCoverKey } = require('./r2');

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// ─── Shared event emitter (server.js SSE endpoint listens here) ───────────────
const enrichEmitter = new EventEmitter();
enrichEmitter.setMaxListeners(100);

// ─── Pipeline tunables ────────────────────────────────────────────────────────
const LOOKUP_CONCURRENCY = 8;   // IGDB allows ~4 req/s; each game ≈ 2 calls → 8 games safe
const BUFFER_MAX         = 15;  // max resolved items waiting for Phase 2

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
        [gameId, data.description || null, data.genres || [], data.tags || [],
         data.developer || null, data.publisher || null]
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
                 os_versions = EXCLUDED.os_versions, cpu = EXCLUDED.cpu,
                 gpu = EXCLUDED.gpu, ram = EXCLUDED.ram, storage = EXCLUDED.storage,
                 directx = EXCLUDED.directx, notes = EXCLUDED.notes, fetched_at = now()`,
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
             score          = EXCLUDED.score,
             positive_count = EXCLUDED.positive_count,
             negative_count = EXCLUDED.negative_count,
             total_reviews  = EXCLUDED.total_reviews,
             fetched_at     = now()`,
        [gameId, steamRating.score, steamRating.positive || null,
         steamRating.negative || null, steamRating.total || null]
    );
}

async function updateGameMeta(gameId, igdbData) {
    if (igdbData.releaseYear) {
        await db.query(
            `UPDATE games SET release_year = COALESCE(release_year, $2), updated_at = now()
             WHERE id = $1`,
            [gameId, igdbData.releaseYear]
        );
    }
}

async function _applyClientFallbacks(gameId, platform, igdbData, epicMeta, clientData) {
    try {
        const { rows } = await db.query(`SELECT title, release_year FROM games WHERE id=$1`, [gameId]);
        const row = rows[0];
        if (!row) return;

        if (!igdbData?.releaseYear && !row.release_year && platform === 'epic' && epicMeta?.creationDate) {
            const fallbackYear = new Date(epicMeta.creationDate).getFullYear() || null;
            if (fallbackYear) {
                await db.query(
                    `UPDATE games SET release_year=COALESCE(release_year,$2), updated_at=now() WHERE id=$1`,
                    [gameId, fallbackYear]
                );
            }
        }

        if (!igdbData && platform === 'epic' && epicMeta) {
            const epicDeveloper   = epicMeta.developer || null;
            const rawDesc         = epicMeta.description || null;
            const epicDescription = rawDesc && rawDesc.toLowerCase() !== (row.title || '').toLowerCase()
                ? rawDesc : null;

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
            }
        }
    } catch (err) {
        console.warn('[Enrich] _applyClientFallbacks error:', err.message);
    }
}

function _pickEpicImageType(keyImages, types) {
    if (!Array.isArray(keyImages)) return null;
    for (const type of types) {
        const img = keyImages.find(k => k.type === type);
        if (img?.url) return img.url;
    }
    return null;
}

// ─── Phase 1: pure lookup — no uploads, no heavy DB writes ───────────────────
//
// Runs IGDB + SteamGridDB in parallel for a single game.
// Returns a "resolved" object that Phase 2 consumes.
//
async function _lookupOnly(item) {
    const { gameId, platform, externalId, clientData = {} } = item;

    let gameTitle = null, existingCover = null, existingHero = null;
    try {
        const [titleRes, imagesRes] = await Promise.all([
            db.query('SELECT title FROM games WHERE id=$1', [gameId]),
            db.query(
                `SELECT image_type, url, cdn_url FROM game_images
                 WHERE game_id=$1 AND image_type IN ('cover','hero') ORDER BY sort_order ASC`,
                [gameId]
            ),
        ]);
        gameTitle     = titleRes.rows[0]?.title || null;
        existingCover = imagesRes.rows.find(r => r.image_type === 'cover') || null;
        existingHero  = imagesRes.rows.find(r => r.image_type === 'hero')  || null;
    } catch (err) {
        console.warn(`[Lookup] DB read failed for ${gameId}:`, err.message);
    }

    // IGDB and SteamGridDB run in parallel — both are pure HTTP GET
    const [igdbSettled, sgdbSettled] = await Promise.allSettled([
        resolveAndFetch(platform, externalId, gameTitle),
        resolveSgdbImages({
            steamAppId:    platform === 'steam' ? externalId : undefined,
            epicNamespace: platform === 'epic'  ? externalId : undefined,
            igdbId:        undefined, // not available yet — IGDB lookup runs in parallel
            title:         gameTitle  || undefined,
        }),
    ]);

    const igdbData = igdbSettled.status === 'fulfilled' ? igdbSettled.value?.data  ?? null : null;
    const igdbId   = igdbSettled.status === 'fulfilled' ? igdbSettled.value?.igdbId ?? null : null;
    const sgdbData = sgdbSettled.status === 'fulfilled'
        ? sgdbSettled.value
        : { cover: null, hero: null, logo: null };

    if (igdbSettled.status === 'rejected') console.warn(`[Lookup] IGDB failed ${externalId}:`, igdbSettled.reason?.message);
    if (sgdbSettled.status === 'rejected') console.warn(`[Lookup] SGDB failed ${externalId}:`, sgdbSettled.reason?.message);

    return {
        gameId, platform, externalId, clientData,
        igdbData, igdbId, sgdbData,
        coverSource:  existingCover ? null : (sgdbData.cover?.url || igdbData?.images?.cover?.url || null),
        heroSource:   sgdbData.hero?.url || igdbData?.images?.artworks?.[0]?.url || null,
        logoSource:   sgdbData.logo?.url || igdbData?.images?.logo?.url           || null,
        coverMeta:    sgdbData.cover || igdbData?.images?.cover        || {},
        heroMeta:     sgdbData.hero  || igdbData?.images?.artworks?.[0] || {},
        screenshots:  igdbData?.images?.screenshots || [],
        existingCover,
        existingHero,
    };
}

// ─── Phase 2: upload + persist (one game at a time) ───────────────────────────
//
// All the memory-heavy work: Sharp, R2 PUT, DB writes.
//
async function _uploadAndSave(resolved) {
    const {
        gameId, platform, externalId, clientData,
        igdbData, igdbId, sgdbData,
        coverSource, heroSource, logoSource,
        coverMeta, heroMeta, screenshots,
        existingCover, existingHero,
    } = resolved;

    const epicMeta = clientData?.epic_metadata || null;

    // Upload cover / hero / logo in parallel (3 concurrent uploads is fine)
    const [coverR2, heroR2, logoR2] = await Promise.all([
        coverSource ? uploadImage(platform, 'cover', externalId, coverSource)
                    : Promise.resolve({ sourceUrl: null, cdnUrl: null }),
        heroSource  ? uploadImage(platform, 'hero',  externalId, heroSource)
                    : Promise.resolve({ sourceUrl: null, cdnUrl: null }),
        logoSource  ? uploadImage(platform, 'logo',  externalId, logoSource)
                    : Promise.resolve({ sourceUrl: null, cdnUrl: null }),
    ]);

    // Persist cover
    if (coverR2.sourceUrl) {
        await upsertImage(gameId, sgdbData.cover?.url ? 'steamgriddb' : 'igdb',
                          'cover', coverR2.sourceUrl, coverR2.cdnUrl,
                          coverMeta.width, coverMeta.height, 0);
    }

    // Persist hero
    if (heroR2.sourceUrl) {
        await upsertImage(gameId, sgdbData.hero?.url ? 'steamgriddb' : 'igdb',
                          'hero', heroR2.sourceUrl, heroR2.cdnUrl,
                          heroMeta.width, heroMeta.height, 0);
    } else if (!existingHero) {
        const epicHeroUrl = epicMeta?._heroUrl
            || _pickEpicImageType(epicMeta?.keyImages, ['DieselGameBox', 'OfferImageWide']);
        if (epicHeroUrl) {
            const r2 = await uploadImage('epic', 'hero', externalId, epicHeroUrl);
            if (r2.sourceUrl) {
                await upsertImage(gameId, 'epic', 'hero', r2.sourceUrl, r2.cdnUrl, null, null, 0);
            }
        }
    }

    // Persist logo
    if (logoR2.sourceUrl) {
        await upsertImage(gameId, sgdbData.logo?.url ? 'steamgriddb' : 'igdb',
                          'logo', logoR2.sourceUrl, logoR2.cdnUrl, null, null, 0);
    }

    // Screenshots in batches of 3 — keeps Sharp memory bounded
    if (screenshots.length) {
        const BATCH = 3;
        for (let i = 0; i < screenshots.length; i += BATCH) {
            await Promise.all(
                screenshots.slice(i, i + BATCH).map(async (ss, j) => {
                    const r2 = await uploadImage(platform, `screenshot_${i + j}`, externalId, ss.url);
                    if (r2.sourceUrl) {
                        await upsertImage(gameId, 'igdb', 'screenshot',
                                          r2.sourceUrl, r2.cdnUrl, ss.width, ss.height, i + j);
                    }
                })
            );
        }
    }

    // IGDB metadata, ratings, videos — all parallel, no memory cost
    if (igdbData) {
        await Promise.all([
            upsertMetadata(gameId, igdbData),
            upsertRating(gameId, igdbData.rating),
            upsertVideos(gameId, igdbData.videos || []),
            updateGameMeta(gameId, igdbData),
        ]);
    }

    await _applyClientFallbacks(gameId, platform, igdbData, epicMeta, clientData);

    if (clientData?.steamRating) await upsertSteamRating(gameId, clientData.steamRating);
    if (Array.isArray(clientData?.sysreq) && clientData.sysreq.length) {
        await upsertSysreq(gameId, clientData.sysreq, 'steam');
    }

    const finalCover = coverR2.cdnUrl || existingCover?.cdn_url || null;
    const finalHero  = heroR2.cdnUrl  || existingHero?.cdn_url  || null;

    console.log(
        `[Upload] ✅ ${externalId}` +
        ` cover=${finalCover     ? '✅' : '❌'}` +
        ` hero=${finalHero       ? '✅' : '❌'}` +
        ` logo=${logoR2.cdnUrl   ? '✅' : '❌'}` +
        ` igdb=${igdbData        ? '✅' : '❌'}`
    );

    return { gameId, externalId, igdbId, cover: finalCover, hero: finalHero,
             logo: logoR2.cdnUrl || null, hasMetadata: !!igdbData };
}

// ─── Bounded pipeline (main export used by server._drainQueue) ────────────────
//
//  Producer (Phase 1): fetches IGDB + SGDB for 8 games at a time,
//                      pauses when buffer hits BUFFER_MAX.
//  Consumer (Phase 2): drains the buffer one game at a time,
//                      uploads to R2, writes to DB, emits SSE event.
//
//  Both run concurrently via Promise.all — producer fills while consumer drains.
//
async function enrichGamesBatch(items) {
    if (!items?.length) return;

    console.log(`[Pipeline] ${items.length} games | concurrency=${LOOKUP_CONCURRENCY} buffer_max=${BUFFER_MAX}`);

    const buffer   = [];
    let lookupDone = false;

    // Producer
    const producer = (async () => {
        for (let i = 0; i < items.length; i += LOOKUP_CONCURRENCY) {
            // Back-pressure: wait if consumer is falling behind
            while (buffer.length >= BUFFER_MAX) await delay(300);

            const chunk   = items.slice(i, i + LOOKUP_CONCURRENCY);
            const results = await Promise.allSettled(chunk.map(_lookupOnly));

            for (let k = 0; k < results.length; k++) {
                const r = results[k];
                if (r.status === 'fulfilled') {
                    buffer.push(r.value);
                } else {
                    const failed = chunk[k];
                    console.error(`[Pipeline] Lookup failed ${failed?.externalId}:`, r.reason?.message);
                    enrichEmitter.emit('error', {
                        gameId:     failed?.gameId,
                        externalId: failed?.externalId,
                        error:      r.reason?.message || 'lookup failed',
                    });
                }
            }

            console.log(`[Pipeline] Lookup ${Math.min(i + LOOKUP_CONCURRENCY, items.length)}/${items.length} | buffer=${buffer.length}`);
        }
        lookupDone = true;
    })();

    // Consumer
    const consumer = (async () => {
        let processed = 0;
        while (true) {
            if (buffer.length === 0) {
                if (lookupDone) break;
                await delay(200);
                continue;
            }

            const resolved = buffer.shift();
            try {
                const result = await _uploadAndSave(resolved);
                enrichEmitter.emit('done', result);
                processed++;
                console.log(`[Pipeline] Upload ${processed}/${items.length}`);
            } catch (err) {
                console.error(`[Pipeline] Upload failed ${resolved.externalId}:`, err.message);
                enrichEmitter.emit('error', {
                    gameId:     resolved.gameId,
                    externalId: resolved.externalId,
                    error:      err.message,
                });
            }

            // Small cooldown — lets R2 / Railway recover between uploads
            await delay(500);
        }
    })();

    await Promise.all([producer, consumer]);
    enrichEmitter.emit('batch_done', { count: items.length });
    console.log(`[Pipeline] Done`);
}

// ─── Single-game path (direct API calls) ─────────────────────────────────────

async function enrichGame(gameId, platform, externalId, clientData = {}) {
    console.log(`[Enrich] ▶ ${platform}:${externalId}`);
    const resolved = await _lookupOnly({ gameId, platform, externalId, clientData });
    return _uploadAndSave(resolved);
}

// ─── Sequential fallback ──────────────────────────────────────────────────────

async function enrichGames(items) {
    for (const item of items) {
        try {
            const result = await enrichGame(item.gameId, item.platform, item.externalId, item.clientData || {});
            enrichEmitter.emit('done', result);
        } catch (err) {
            console.error(`[Enrich] ❌ ${item.gameId}:`, err.message);
            enrichEmitter.emit('error', { gameId: item.gameId, externalId: item.externalId, error: err.message });
        }
        await delay(1500);
    }
    enrichEmitter.emit('batch_done', { count: items.length });
}

module.exports = { enrichGame, enrichGames, enrichGamesBatch, enrichEmitter };
