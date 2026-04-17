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
 *  Phase 2 — Save worker (one game at a time)
 *    Writes raw URLs directly to DB — NO Sharp, NO R2 upload.
 *    cdn_url stays NULL until the nightly cron job promotes them.
 *    DB writes run in parallel within a single game.
 *    Emits SSE event after each game finishes.
 *
 * enrichGame()       — single-game path, used by direct API calls
 * enrichGamesBatch() — bounded pipeline, used by server._drainQueue
 * promotePendingImages() — nightly cron: Sharp + R2 for all cdn_url IS NULL rows
 */

const { EventEmitter }                   = require('events');
const db                                 = require('../db');
const { resolveAndFetch }                = require('./igdbResolver');
const { resolveSgdbImages }              = require('./steamGridDbResolver');
const { uploadImageToR2, buildCoverKey } = require('./r2');
const { fetchEpicDataFromPython } = require('./epicPythonBridge');
const { fetchSteamDataFromPython } = require('./steamPythonBridge');

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// ─── Shared event emitter (server.js SSE endpoint listens here) ───────────────
const enrichEmitter = new EventEmitter();
enrichEmitter.setMaxListeners(100);

// ─── Pipeline tunables ────────────────────────────────────────────────────────
const LOOKUP_CONCURRENCY  = 3;   // igdbResolver throttles to 3 req/s; 3 games × ~2 calls = 6 slots/s max → safe
const BUFFER_MAX          = 15;  // max resolved items waiting for Phase 2
const PROMOTE_BATCH_SIZE  = 5;   // images promoted per batch in nightly cron (keeps Sharp memory low)
const PROMOTE_DELAY_MS    = 500; // pause between cron batches

// ─── DB helpers ───────────────────────────────────────────────────────────────

/**
 * Insert image row with raw url only — cdn_url intentionally left NULL.
 * The nightly cron will fill cdn_url once it uploads to R2.
 */
async function upsertImage(gameId, source, imageType, sourceUrl, width, height, sortOrder = 0) {
    if (!sourceUrl) return;
    await db.query(
        `INSERT INTO game_images
             (game_id, source, image_type, url, cdn_url, width, height, sort_order)
         VALUES ($1,$2,$3,$4, NULL,$5,$6,$7)
         ON CONFLICT DO NOTHING`,
        [gameId, source, imageType, sourceUrl, width || null, height || null, sortOrder]
    );
}

async function upsertMetadata(gameId, data, source = 'igdb') {
    await db.query(
        `INSERT INTO game_metadata
             (game_id, source, description, short_description, genres, tags, developer, publisher)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
         ON CONFLICT (game_id, source) DO UPDATE SET
             description       = EXCLUDED.description,
             short_description = EXCLUDED.short_description,
             genres            = EXCLUDED.genres,
             tags              = EXCLUDED.tags,
             developer         = EXCLUDED.developer,
             publisher         = EXCLUDED.publisher,
             fetched_at        = now()`,
        [
            gameId, source, 
            data.description || null, 
            data.short_description || null, 
            data.genres || [], 
            data.tags || [],
            data.developer ? (Array.isArray(data.developer) ? data.developer[0] : data.developer) : null, 
            data.publisher ? (Array.isArray(data.publisher) ? data.publisher[0] : data.publisher) : null
        ]
    );
}

async function upsertRating(gameId, rating) {
    if (!rating?.score) return;
    
    const maxScore = rating.max_score || 10;

    await db.query(
        `INSERT INTO game_ratings
             (game_id, source, score, max_score, total_reviews)
         VALUES ($1,$2,$3,$4,$5)
         ON CONFLICT (game_id, source) DO UPDATE SET
             score         = EXCLUDED.score,
             max_score     = EXCLUDED.max_score,
             total_reviews = EXCLUDED.total_reviews,
             fetched_at    = now()`,
        [gameId, rating.source, Math.round(rating.score * 10) / 10, maxScore, rating.count || null]
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

async function _applyClientFallbacks(gameId, platform, igdbData, epicMeta) {
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
// ─── Phase 1: pure lookup — no uploads, no heavy DB writes ───────────────────
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

    // Prepare Promises
    const taskSgdb = resolveSgdbImages({
        steamAppId:    platform === 'steam' ? externalId : undefined,
        epicNamespace: platform === 'epic'  ? externalId : undefined,
        title:         gameTitle || undefined,
    });
    
    // Run Python scripts based on platform
    const taskEpic  = platform === 'epic' ? fetchEpicDataFromPython(gameTitle || externalId) : Promise.resolve(null);
    const taskSteam = platform === 'steam' ? fetchSteamDataFromPython(externalId) : Promise.resolve(null);
    
    // IGDB always runs as a fallback for missing data
    const taskIgdb  = resolveAndFetch(platform, externalId, gameTitle);

    const [sgdbSettled, epicSettled, steamSettled, igdbSettled] = await Promise.allSettled([taskSgdb, taskEpic, taskSteam, taskIgdb]);

    const sgdbData       = sgdbSettled.status === 'fulfilled' ? sgdbSettled.value : { cover: null, hero: null, logo: null };
    const epicPythonData = epicSettled.status === 'fulfilled' ? epicSettled.value : null;
    const steamPythonData= steamSettled.status === 'fulfilled' ? steamSettled.value : null;
    const igdbData       = igdbSettled.status === 'fulfilled' ? igdbSettled.value?.data : null;

    let coverSource = null, heroSource = null, logoSource = null;
    let screenshots = [];

    // --- Determine Image Sources based on Priority ---
    // Priority for Logo: SGDB > Steam/Epic > IGDB
    logoSource = sgdbData.logo?.url || steamPythonData?.logo || epicPythonData?.logo || igdbData?.images?.logo?.url || null;

    if (platform === 'steam' && steamPythonData) {
        // 1. معالجة مشكلة الكوفر (إزالة _2x عشان نتجنب الـ 404)
        let steamCover = steamPythonData.poster;
        if (steamCover && steamCover.includes('_2x.jpg')) {
            steamCover = steamCover.replace('_2x.jpg', '.jpg');
        }

        // 2. معالجة الهيرو (استبعاد الصورة المزرقة)
        let steamHero = steamPythonData.background;
        if (steamHero && steamHero.includes('storepage')) {
            steamHero = null; // بنصفرها عشان الكود يروح يسحب من البدائل
        }

        // 3. تحديد المصادر النهائية
        coverSource = steamCover || sgdbData.cover?.url || igdbData?.images?.cover?.url || null;
        heroSource  = steamHero  || sgdbData.hero?.url  || igdbData?.images?.artworks?.[0]?.url || null;
        screenshots = steamPythonData.screenshots || igdbData?.images?.screenshots || [];
    } 
    else if (platform === 'epic' && epicPythonData) {
        coverSource = epicPythonData.cover || sgdbData.cover?.url || igdbData?.images?.cover?.url || null;
        heroSource  = epicPythonData.hero  || sgdbData.hero?.url  || igdbData?.images?.artworks?.[0]?.url || null;
        screenshots = epicPythonData.screenshots || igdbData?.images?.screenshots || [];
    } 
    else {
        // Fallback for IGDB
        coverSource = sgdbData.cover?.url || igdbData?.images?.cover?.url || null;
        heroSource  = sgdbData.hero?.url  || igdbData?.images?.artworks?.[0]?.url || null;
        screenshots = igdbData?.images?.screenshots || [];
    }

    return {
        gameId, platform, externalId, clientData,
        igdbData, epicPythonData, steamPythonData, sgdbData,
        coverSource: existingCover ? null : coverSource,
        heroSource, logoSource,
        coverMeta: {}, heroMeta: {},
        screenshots, existingCover, existingHero,
    };
}


// ─── Phase 2: save raw URLs to DB (no Sharp, no R2) ─────────────────────────
async function _saveRawUrls(resolved) {
    const {
        gameId, platform, externalId, clientData,
        igdbData, epicPythonData, steamPythonData, sgdbData,
        coverSource, heroSource, logoSource,
        coverMeta, heroMeta, screenshots,
        existingCover, existingHero,
    } = resolved;

    const epicMeta = clientData?.epic_metadata || null;

    // ── Cover ──
    if (coverSource) {
        const coverSource_ = sgdbData.cover?.url ? 'steamgriddb' : (epicPythonData ? 'epic' : (steamPythonData ? 'steam' : 'igdb'));
        await upsertImage(gameId, coverSource_, 'cover', coverSource, coverMeta.width, coverMeta.height, 0);
    }

    // ── Hero ──
    if (heroSource) {
        const heroSource_ = sgdbData.hero?.url ? 'steamgriddb' : (epicPythonData ? 'epic' : (steamPythonData ? 'steam' : 'igdb'));
        await upsertImage(gameId, heroSource_, 'hero', heroSource, heroMeta.width, heroMeta.height, 0);
    } else if (!existingHero) {
        // Epic fallback hero
        const epicHeroUrl = epicMeta?._heroUrl
            || _pickEpicImageType(epicMeta?.keyImages, ['DieselGameBox', 'OfferImageWide']);
        if (epicHeroUrl) {
            await upsertImage(gameId, 'epic', 'hero', epicHeroUrl, null, null, 0);
        }
    }

    // ── Logo ──
    if (logoSource) {
        const logoSource_ = sgdbData.logo?.url ? 'steamgriddb' : (epicPythonData ? 'epic' : (steamPythonData ? 'steam' : 'igdb'));
        await upsertImage(gameId, logoSource_, 'logo', logoSource, null, null, 0);
    }

    // ── Screenshots ──
    for (let i = 0; i < screenshots.length; i++) {
        const ss = screenshots[i];
        const url = typeof ss === 'string' ? ss : ss.url;
        const w = typeof ss === 'string' ? null : ss.width;
        const h = typeof ss === 'string' ? null : ss.height;
        const ssSource = platform === 'steam' ? 'steam' : (platform === 'epic' ? 'epic' : 'igdb');
        await upsertImage(gameId, ssSource, 'screenshot', url, w, h, i);
    }

    // ── IGDB Fallback Metadata ──
    // Save from IGDB only if the store Python script returned no data
    // ─── IGDB Fallback Metadata ──
    // هنختبر الداتا اللي جاية من ستيم أو إيبك، هل هي مفيدة؟
    const isRichData = (data) => {
        if (!data) return false;
        const hasDev = !!data.developer;
        const hasGoodDesc = data.description && data.description.length > 20;
        return hasDev || hasGoodDesc;
    };

    const hasGoodStoreData = (platform === 'epic' && isRichData(epicPythonData)) || 
                             (platform === 'steam' && isRichData(steamPythonData));

    // نحفظ بيانات IGDB لو الاستور مرجعش داتا محترمة
    if (igdbData && !hasGoodStoreData) { 
        await Promise.all([
            upsertMetadata(gameId, igdbData, 'igdb'),
            upsertVideos(gameId, igdbData.videos || []),
            updateGameMeta(gameId, igdbData),
        ]);
    }
    
    if (igdbData && Array.isArray(igdbData.ratings)) {
        for (const ratingObj of igdbData.ratings) {
            await upsertRating(gameId, ratingObj);
        }
    }
    
    // ─── Store Python Metadata (Epic OR Steam) ───
    const storeData = platform === 'steam' ? steamPythonData : epicPythonData;
    
    if (storeData) {
        storeData.tags = storeData.features || [];

        const mappedTrailers = (storeData.trailers || []).map(t => {
            const videoUrl = typeof t === 'string' ? t : t?.video;
            const thumbUrl = typeof t === 'string' ? null : t?.thumbnail;
            return { url: videoUrl, thumb: thumbUrl, title: `${platform.toUpperCase()} Trailer` };
        });

        const storeSysreqs = [];
        if (storeData.requirements?.systems) {
            for (const [osName, tiers] of Object.entries(storeData.requirements.systems)) {
                // macOS format handled for Steam, Mac for Epic
                const platformMap = { 'Windows': 'win', 'Mac': 'mac', 'Linux': 'linux', 'macOS': 'mac' };
                const dbPlatform = platformMap[osName] || osName.toLowerCase();

                for (const [tierName, reqs] of Object.entries(tiers)) {
                    if (!reqs) continue;
                    storeSysreqs.push({
                        platform: dbPlatform,
                        tier: tierName, 
                        os_versions: reqs.os_versions || null,
                        cpu: reqs.cpu || null,
                        gpu: reqs.gpu || null,
                        ram: reqs.ram || null,
                        storage: reqs.storage || null,
                        directx: reqs.directx || null, 
                        notes: reqs.notes || null
                    });
                }
            }
        }

        // توحيد التقييم بين ستيم (rating.score) وإيبك (avg_rating)
        const ratingScore = platform === 'steam' ? storeData.rating?.score : storeData.avg_rating;
        const ratingMax = platform === 'steam' ? (storeData.rating?.max_score || 100) : 5;
        const ratingSource = platform === 'steam' ? (storeData.rating?.source || 'Metacritic') : 'epic';

        const storePromises = [
            upsertMetadata(gameId, storeData, platform),
            
            // لو في تقييم أكبر من صفر، احفظه
            ratingScore > 0 ? upsertRating(gameId, { 
                score: ratingScore, 
                max_score: ratingMax, 
                source: ratingSource 
            }) : Promise.resolve(),
            
            upsertVideos(gameId, mappedTrailers),
            updateGameMeta(gameId, { releaseYear: storeData.release_year }),
        ];

        if (storeSysreqs.length > 0) {
            storePromises.push(upsertSysreq(gameId, storeSysreqs, platform));
        }
        
        // Steam specific reviews
        if (platform === 'steam' && storeData.steam_reviews?.total_reviews > 0) {
            const positive = storeData.steam_reviews.positive_reviews;
            const total = storeData.steam_reviews.total_reviews;
            const scorePercent = total > 0 ? (positive / total) * 100 : 0;
            
            storePromises.push(upsertSteamRating(gameId, {
                score: scorePercent,
                positive: positive,
                negative: storeData.steam_reviews.negative_reviews,
                total: total
            }));
        }

        await Promise.all(storePromises);
    }

    await _applyClientFallbacks(gameId, platform, igdbData, epicMeta);

    if (clientData?.steamRating) await upsertSteamRating(gameId, clientData.steamRating);
    if (Array.isArray(clientData?.sysreq) && clientData.sysreq.length) {
        await upsertSysreq(gameId, clientData.sysreq, 'steam');
    }

    // Report
    const cover = coverSource || existingCover?.url || null;
    const hero  = heroSource  || existingHero?.url  || null;
    const hasMeta = !!(igdbData || epicPythonData || steamPythonData);

    console.log(
        `[Save] ✅ ${externalId}` +
        ` cover=${cover ? '✅' : '❌'}` +
        ` hero=${hero ? '✅' : '❌'}` +
        ` logo=${logoSource ? '✅' : '❌'}` +
        ` Meta=${hasMeta ? '✅' : '❌'}`
    );

    return { gameId, externalId, cover, hero, logo: logoSource || null, hasMetadata: hasMeta };
}

// ─── Bounded pipeline (main export used by server._drainQueue) ────────────────
//
//  Producer (Phase 1): fetches IGDB + SGDB for 8 games at a time,
//                      pauses when buffer hits BUFFER_MAX.
//  Consumer (Phase 2): drains the buffer one game at a time,
//                      saves raw URLs to DB, emits SSE event.
//
//  Both run concurrently via Promise.all.
//
async function enrichGamesBatch(items) {
    if (!items?.length) return;

    console.log(`[Pipeline] ${items.length} games | concurrency=${LOOKUP_CONCURRENCY} buffer_max=${BUFFER_MAX}`);

    const buffer   = [];
    let lookupDone = false;

    // Producer
    const producer = (async () => {
        for (let i = 0; i < items.length; i += LOOKUP_CONCURRENCY) {
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
                const result = await _saveRawUrls(resolved);
                enrichEmitter.emit('done', result);
                processed++;
                console.log(`[Pipeline] Saved ${processed}/${items.length}`);
            } catch (err) {
                console.error(`[Pipeline] Save failed ${resolved.externalId}:`, err.message);
                enrichEmitter.emit('error', {
                    gameId:     resolved.gameId,
                    externalId: resolved.externalId,
                    error:      err.message,
                });
            }

            await delay(100); // much shorter — no R2 round-trip to wait for
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
    return _saveRawUrls(resolved);
}

// ─── Nightly cron: promote raw URLs → WebP → R2 ──────────────────────────────
//
// Finds all game_images rows where cdn_url IS NULL,
// converts to WebP via Sharp, uploads to R2, updates cdn_url.
//
// Called by POST /jobs/promote-images in server.js.
// Designed to run at 00:00 daily when traffic is low.
//
async function promotePendingImages(limit = 500) {
    const { rows } = await db.query(
        `SELECT id, game_id, source, image_type, url
         FROM game_images
         WHERE cdn_url IS NULL AND url IS NOT NULL
         ORDER BY created_at ASC
         LIMIT $1`,
        [limit]
    );

    if (!rows.length) {
        console.log('[Promote] Nothing to promote.');
        return { promoted: 0, failed: 0 };
    }

    console.log(`[Promote] ${rows.length} images to promote`);
    let promoted = 0, failed = 0;

    for (let i = 0; i < rows.length; i += PROMOTE_BATCH_SIZE) {
        const batch = rows.slice(i, i + PROMOTE_BATCH_SIZE);

        await Promise.allSettled(batch.map(async (row) => {
            try {
                const key    = buildCoverKey(`${row.source}_${row.image_type}`, row.game_id, row.url);
                const cdnUrl = await uploadImageToR2(row.url, key);

                if (cdnUrl) {
                    await db.query(
                        `UPDATE game_images SET cdn_url=$1 WHERE id=$2`,
                        [cdnUrl, row.id]
                    );
                    promoted++;
                    console.log(`[Promote] ✅ ${row.image_type} for game ${row.game_id}`);
                } else {
                    failed++;
                    console.warn(`[Promote] ⚠️  R2 returned null for image id=${row.id}`);
                }
            } catch (err) {
                failed++;
                console.error(`[Promote] ❌ image id=${row.id}:`, err.message);
            }
        }));

        // Pause between batches to keep Railway memory stable
        if (i + PROMOTE_BATCH_SIZE < rows.length) await delay(PROMOTE_DELAY_MS);
    }

    console.log(`[Promote] Done — promoted=${promoted} failed=${failed}`);
    return { promoted, failed, total: rows.length };
}

module.exports = { enrichGame, enrichGamesBatch, enrichEmitter, promotePendingImages };
