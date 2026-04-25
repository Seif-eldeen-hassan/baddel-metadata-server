'use strict';

/**
 * services/enrichGame.js
 *
 * Two-phase bounded pipeline for game enrichment.
 *
 * Key production fixes applied:
 *   - FIX #7: Epic image mapping now reads poster/background/logo directly
 *             from epicPythonData instead of epicPythonData.keyImages
 *   - FIX #8: upsertVideos() now receives and stores the real source tag
 *             ('steam', 'epic', 'igdb') instead of always 'igdb'
 *   - FIX #9: Multi-table saves wrapped in DB transactions
 *   - FIX #6: buildImageKey(row) used in promote path for unique keys
 */

const { EventEmitter }                     = require('events');
const db                                   = require('../db');
const { resolveAndFetch }                  = require('./igdbResolver');
const { resolveSgdbImages }                = require('./steamGridDbResolver');
const { uploadImageToR2, buildImageKey }   = require('./r2');
const { fetchEpicDataFromPython }          = require('./epicPythonBridge');
const { fetchSteamDataFromPython }         = require('./steamPythonBridge');
const log                                  = require('../config/logger');

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// ─── Shared event emitter ─────────────────────────────────────────────────────
const enrichEmitter = new EventEmitter();
enrichEmitter.setMaxListeners(100);

// ─── Pipeline tunables ────────────────────────────────────────────────────────
const LOOKUP_CONCURRENCY = 3;
const BUFFER_MAX         = 15;
const PROMOTE_BATCH_SIZE = 5;
const PROMOTE_DELAY_MS   = 500;

// ─── Steam cover validation ────────────────────────────────────────────────────
async function getValidSteamCover(url) {
    if (!url) return null;
    try {
        const check = async (testUrl) => {
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), 1500);
            const res = await fetch(testUrl, { method: 'HEAD', signal: controller.signal });
            clearTimeout(timeout);
            return res.ok ? testUrl : null;
        };
        let validUrl = await check(url);
        if (validUrl) return validUrl;
        if (url.includes('_2x.jpg')) {
            validUrl = await check(url.replace('_2x.jpg', '.jpg'));
            if (validUrl) return validUrl;
        }
        return null;
    } catch {
        return null;
    }
}

// ─── DB helpers ───────────────────────────────────────────────────────────────

async function upsertImage(client, gameId, source, imageType, sourceUrl, width, height, sortOrder = 0) {
    if (!sourceUrl) return;
    await client.query(
        `INSERT INTO game_images
             (game_id, source, image_type, url, cdn_url, width, height, sort_order)
         VALUES ($1,$2,$3,$4, NULL,$5,$6,$7)
         ON CONFLICT DO NOTHING`,
        [gameId, source, imageType, sourceUrl, width || null, height || null, sortOrder]
    );
}

/**
 * Cover-specific upsert: enforces exactly one authoritative cover row per game.
 *
 * Deletes ALL existing cover rows for the game first, then inserts a single
 * fresh row. This is necessary because the production unique index is on
 * (game_id, image_type, source) WHERE image_type IN ('cover','logo') — a
 * three-column partial index — so a game can accumulate multiple historical
 * cover rows from different sources (e.g. 'epic' + 'steamgriddb'). Updating
 * all of them to the same source/url would violate that unique index, and
 * leaving them in place creates ambiguous duplicate state.
 *
 * DELETE + INSERT inside the same transaction guarantees:
 *   - exactly one cover row survives
 *   - no unique-index conflicts
 *   - cdn_url is NULL so the promote cron re-uploads the image to R2
 *   - previously wrong Epic covers are corrected on re-enrich
 */
async function upsertCover(client, gameId, source, sourceUrl, width, height) {
    if (!sourceUrl) return;
    await client.query(
        `DELETE FROM game_images
         WHERE  game_id    = $1
           AND  image_type = 'cover'`,
        [gameId]
    );
    await client.query(
        `INSERT INTO game_images
             (game_id, source, image_type, url, cdn_url, width, height, sort_order)
         VALUES ($1,$2,'cover',$3,NULL,$4,$5,0)`,
        [gameId, source, sourceUrl, width || null, height || null]
    );
}

async function upsertMetadata(client, gameId, data, source = 'igdb') {
    await client.query(
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
            data.publisher ? (Array.isArray(data.publisher) ? data.publisher[0] : data.publisher) : null,
        ]
    );
}

async function upsertRating(client, gameId, rating) {
    if (!rating?.score) return;
    const maxScore = rating.max_score || 10;
    await client.query(
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

/**
 * FIX #8: source parameter is now required — callers pass the real source.
 */
async function upsertVideos(client, gameId, videos, source) {
    if (!source) throw new Error('upsertVideos: source is required');
    for (let i = 0; i < videos.length; i++) {
        const v = videos[i];
        await client.query(
            `INSERT INTO game_media
                 (game_id, source, media_type, url, thumbnail_url, title, sort_order)
             VALUES ($1,$2,'trailer',$3,$4,$5,$6)
             ON CONFLICT (game_id, url) DO NOTHING`,
            [gameId, source, v.url, v.thumb || null, v.title || null, i]
        );
    }
}

async function upsertSysreq(client, gameId, rows, source = 'steam') {
    for (const r of rows) {
        await client.query(
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

async function upsertSteamRating(client, gameId, steamRating) {
    if (!steamRating?.score) return;
    await client.query(
        `INSERT INTO game_ratings
             (game_id, source, score, max_score, positive_count, negative_count, total_reviews, rating_label)
         VALUES ($1,'steam',$2,100,$3,$4,$5,$6)
         ON CONFLICT (game_id, source) DO UPDATE SET
             score          = EXCLUDED.score,
             positive_count = EXCLUDED.positive_count,
             negative_count = EXCLUDED.negative_count,
             total_reviews  = EXCLUDED.total_reviews,
             rating_label   = EXCLUDED.rating_label,
             fetched_at     = now()`,
        [
            gameId,
            steamRating.score,
            steamRating.positive || null,
            steamRating.negative || null,
            steamRating.total    || null,
            steamRating.label    || null,
        ]
    );
}

async function updateGameMeta(client, gameId, igdbData) {
    if (igdbData.releaseYear) {
        await client.query(
            `UPDATE games SET release_year = COALESCE(release_year, $2), updated_at = now()
             WHERE id = $1`,
            [gameId, igdbData.releaseYear]
        );
    }
}

// ─── Transaction helper ───────────────────────────────────────────────────────

async function withTransaction(fn) {
    const client = await db.connect();
    try {
        await client.query('BEGIN');
        const result = await fn(client);
        await client.query('COMMIT');
        return result;
    } catch (err) {
        await client.query('ROLLBACK');
        throw err;
    } finally {
        client.release();
    }
}

// ─── Epic image helpers ───────────────────────────────────────────────────────

/**
 * FIX #7: Reads poster/background/logo from epicPythonData top-level fields.
 * The Python script outputs these directly — not nested under keyImages.
 * Falls back to keyImages scan only when the direct fields are absent.
 */
function _getEpicImages(epicPythonData) {
    if (!epicPythonData) return { poster: null, background: null, logo: null };

    // Primary: direct fields from Python output
    const poster     = epicPythonData.poster     || null;
    const background = epicPythonData.background || null;
    const logo       = epicPythonData.logo       || null;

    return { poster, background, logo };
}

/** Legacy helper — only used as last-resort fallback if direct fields are absent */
function _pickEpicImageType(keyImages, types) {
    if (!Array.isArray(keyImages)) return null;
    for (const type of types) {
        const img = keyImages.find(k => k.type === type);
        if (img?.url) return img.url;
    }
    return null;
}

// ─── Client fallbacks ─────────────────────────────────────────────────────────

async function _applyClientFallbacks(client, gameId, platform, igdbData, epicMeta) {
    try {
        const { rows } = await client.query(`SELECT title, release_year FROM games WHERE id=$1`, [gameId]);
        const row = rows[0];
        if (!row) return;

        if (!igdbData?.releaseYear && !row.release_year && platform === 'epic' && epicMeta?.creationDate) {
            const fallbackYear = new Date(epicMeta.creationDate).getFullYear() || null;
            if (fallbackYear) {
                await client.query(
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
                await client.query(
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
        log.warn({ gameId, err: err.message }, '[Enrich] _applyClientFallbacks error');
    }
}

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
        log.warn({ gameId, err: err.message }, '[Lookup] DB read failed');
    }

    const taskSgdb  = resolveSgdbImages({
        steamAppId:    platform === 'steam' ? externalId : undefined,
        epicNamespace: platform === 'epic'  ? externalId : undefined,
        title:         gameTitle || undefined,
    });
    const taskEpic  = platform === 'epic'  ? fetchEpicDataFromPython(gameTitle || externalId) : Promise.resolve(null);
    const taskSteam = platform === 'steam' ? fetchSteamDataFromPython(externalId)             : Promise.resolve(null);
    const taskIgdb  = resolveAndFetch(platform, externalId, gameTitle);

    const [sgdbSettled, epicSettled, steamSettled, igdbSettled] = await Promise.allSettled([
        taskSgdb, taskEpic, taskSteam, taskIgdb,
    ]);

    const sgdbData        = sgdbSettled.status  === 'fulfilled' ? sgdbSettled.value       : { cover: null, hero: null, logo: null };
    const epicPythonData  = epicSettled.status  === 'fulfilled' ? epicSettled.value       : null;
    const steamPythonData = steamSettled.status === 'fulfilled' ? steamSettled.value      : null;
    const igdbData        = igdbSettled.status  === 'fulfilled' ? igdbSettled.value?.data : null;

    let coverSource = null, heroSource = null, logoSource = null;
    let screenshots = [];
    const coverMeta = { width: null, height: null };
    const heroMeta  = { width: null, height: null };

    // Priority for Logo: SGDB > Steam/Epic direct fields > IGDB
    logoSource = sgdbData.logo?.url
        || steamPythonData?.logo
        || epicPythonData?.logo           // FIX #7: use direct field
        || igdbData?.images?.logo?.url
        || null;

    if (platform === 'steam' && steamPythonData) {
        const steamCover = await getValidSteamCover(steamPythonData.poster);

        let steamHero = steamPythonData.background;
        const badHeroPatterns = ['storepage', 'page_bg_raw', 'page_bg_generated'];
        const isSteamHeroBad = steamHero && badHeroPatterns.some(p => steamHero.includes(p));

        coverSource = steamCover || sgdbData.cover?.url || igdbData?.images?.cover?.url || null;
        heroSource  = isSteamHeroBad
            ? (sgdbData.hero?.url || igdbData?.images?.artworks?.[0]?.url || steamHero)
            : (steamHero || sgdbData.hero?.url || igdbData?.images?.artworks?.[0]?.url || null);
        screenshots = steamPythonData.screenshots || igdbData?.images?.screenshots || [];

    } else if (platform === 'epic' && epicPythonData) {
        // FIX #7: read direct poster/background fields first
        const { poster: epicPoster, background: epicBg } = _getEpicImages(epicPythonData);

        // Epic poster is the authoritative cover for Epic games.
        // SGDB is a fallback when Epic has no poster, not a preferred source.
        coverSource = epicPoster
            || sgdbData.cover?.url
            // last-resort keyImages scan when neither direct poster nor SGDB is available
            || _pickEpicImageType(epicPythonData?.keyImages, ['DieselGameBoxTall', 'OfferImageTall'])
            || igdbData?.images?.cover?.url
            || null;

        heroSource = sgdbData.hero?.url
            || epicBg
            || _pickEpicImageType(epicPythonData?.keyImages, ['DieselGameBox', 'OfferImageWide'])
            || igdbData?.images?.artworks?.[0]?.url
            || null;

        screenshots = epicPythonData.screenshots || igdbData?.images?.screenshots || [];

    } else {
        coverSource = sgdbData.cover?.url || igdbData?.images?.cover?.url || null;
        heroSource  = sgdbData.hero?.url  || igdbData?.images?.artworks?.[0]?.url || null;
        screenshots = igdbData?.images?.screenshots || [];
    }

    return {
        gameId, platform, externalId, clientData,
        igdbData, epicPythonData, steamPythonData, sgdbData,
        coverSource, heroSource, logoSource,
        coverMeta, heroMeta, screenshots,
        existingCover, existingHero,
    };
}

// ─── Phase 2: save raw URLs to DB inside a transaction ───────────────────────
async function _saveRawUrls(resolved) {
    const {
        gameId, platform, externalId, clientData,
        igdbData, epicPythonData, steamPythonData, sgdbData,
        coverSource, heroSource, logoSource,
        coverMeta, heroMeta, screenshots,
        existingCover, existingHero,
    } = resolved;

    const epicMeta = clientData?.epic_metadata || null;

    // Wrap all multi-table writes in a single transaction
    await withTransaction(async (client) => {

        // ── Cover ──
        if (coverSource) {
            // Derive source to match the actual precedence used in _lookupOnly.
            // Epic:  epicPoster -> sgdbCover -> keyImages fallback -> igdbCover
            // Steam: steamCover -> sgdbCover -> igdbCover
            // Other: sgdbCover -> igdbCover
            let src;
            if (platform === 'epic') {
                const epicPoster = epicPythonData?.poster || null;
                src = coverSource === epicPoster                      ? 'epic'
                    : coverSource === sgdbData.cover?.url            ? 'steamgriddb'
                    : epicPythonData                                  ? 'epic'   // keyImages fallback
                    :                                                   'igdb';
            } else if (platform === 'steam') {
                src = coverSource === sgdbData.cover?.url ? 'steamgriddb'
                    : steamPythonData                     ? 'steam'
                    :                                       'igdb';
            } else {
                src = sgdbData.cover?.url ? 'steamgriddb' : 'igdb';
            }
            await upsertCover(client, gameId, src, coverSource, coverMeta.width, coverMeta.height);
        }

        // ── Hero ──
        if (heroSource) {
            const src = sgdbData.hero?.url ? 'steamgriddb'
                : (epicPythonData ? 'epic' : (steamPythonData ? 'steam' : 'igdb'));
            await upsertImage(client, gameId, src, 'hero', heroSource, heroMeta.width, heroMeta.height, 0);
        } else if (!existingHero) {
            // FIX #7: use direct epic logo field first
            const epicHeroUrl = epicMeta?._heroUrl
                || epicPythonData?.background
                || _pickEpicImageType(epicMeta?.keyImages, ['DieselGameBox', 'OfferImageWide']);
            if (epicHeroUrl) {
                await upsertImage(client, gameId, 'epic', 'hero', epicHeroUrl, null, null, 0);
            }
        }

        // ── Logo ──
        if (logoSource) {
            const src = sgdbData.logo?.url ? 'steamgriddb'
                : (epicPythonData ? 'epic' : (steamPythonData ? 'steam' : 'igdb'));
            await upsertImage(client, gameId, src, 'logo', logoSource, null, null, 0);
        }

        // ── Screenshots ──
        for (let i = 0; i < screenshots.length; i++) {
            const ss  = screenshots[i];
            const url = typeof ss === 'string' ? ss : ss.url;
            const w   = typeof ss === 'string' ? null : ss.width;
            const h   = typeof ss === 'string' ? null : ss.height;
            const src = platform === 'steam' ? 'steam' : (platform === 'epic' ? 'epic' : 'igdb');
            await upsertImage(client, gameId, src, 'screenshot', url, w, h, i);
        }

        // ── IGDB Fallback Metadata ──
        const isRichData = (data) => !!(data?.developer || (data?.description && data.description.length > 20));
        const hasGoodStoreData = (platform === 'epic' && isRichData(epicPythonData))
                              || (platform === 'steam' && isRichData(steamPythonData));

        if (igdbData && !hasGoodStoreData) {
            await upsertMetadata(client, gameId, igdbData, 'igdb');
            // FIX #8: pass source 'igdb' explicitly
            await upsertVideos(client, gameId, igdbData.videos || [], 'igdb');
            await updateGameMeta(client, gameId, igdbData);
        }

        if (igdbData && Array.isArray(igdbData.ratings)) {
            for (const ratingObj of igdbData.ratings) {
                await upsertRating(client, gameId, ratingObj);
            }
        }

        // ── Store Metadata (Epic OR Steam) ──
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
                    const platformMap = { Windows: 'win', Mac: 'mac', Linux: 'linux', macOS: 'mac' };
                    const dbPlatform  = platformMap[osName] || osName.toLowerCase();
                    for (const [tierName, reqs] of Object.entries(tiers)) {
                        if (!reqs) continue;
                        storeSysreqs.push({
                            platform:    dbPlatform,
                            tier:        tierName,
                            os_versions: reqs.os_versions || null,
                            cpu:         reqs.cpu         || null,
                            gpu:         reqs.gpu         || null,
                            ram:         reqs.ram         || null,
                            storage:     reqs.storage     || null,
                            directx:     reqs.directx     || null,
                            notes:       reqs.notes       || null,
                        });
                    }
                }
            }

            const ratingScore  = platform === 'steam' ? storeData.rating?.score   : storeData.avg_rating;
            const ratingMax    = platform === 'steam' ? (storeData.rating?.max_score || 100) : 5;
            const ratingSource = platform === 'steam' ? (storeData.rating?.source || 'Metacritic') : 'epic';

            await upsertMetadata(client, gameId, storeData, platform);
            if (ratingScore > 0) {
                await upsertRating(client, gameId, { score: ratingScore, max_score: ratingMax, source: ratingSource });
            }
            // FIX #8: real source tag for trailers
            await upsertVideos(client, gameId, mappedTrailers, platform);
            await updateGameMeta(client, gameId, { releaseYear: storeData.release_year });
            if (storeSysreqs.length > 0) {
                await upsertSysreq(client, gameId, storeSysreqs, platform);
            }

            if (platform === 'steam' && storeData.steam_reviews?.total_reviews > 0) {
                const positive     = storeData.steam_reviews.positive_reviews;
                const total        = storeData.steam_reviews.total_reviews;
                const scorePercent = total > 0 ? (positive / total) * 100 : 0;
                await upsertSteamRating(client, gameId, {
                    score:    scorePercent,
                    positive: positive,
                    negative: storeData.steam_reviews.negative_reviews,
                    total:    total,
                    label:    storeData.steam_reviews.review_score_desc,
                });
            }
        }

        await _applyClientFallbacks(client, gameId, platform, igdbData, epicMeta);

        if (clientData?.steamRating) {
            await upsertSteamRating(client, gameId, clientData.steamRating);
        }
        if (Array.isArray(clientData?.sysreq) && clientData.sysreq.length) {
            await upsertSysreq(client, gameId, clientData.sysreq, 'steam');
        }

    }); // end transaction

    const cover   = coverSource || existingCover?.url || null;
    const hero    = heroSource  || existingHero?.url  || null;
    const hasMeta = !!(igdbData || epicPythonData || steamPythonData);

    log.info({
        externalId,
        cover:   !!cover,
        hero:    !!hero,
        logo:    !!logoSource,
        hasMeta,
    }, '[Save] enrichment complete');

    return { gameId, externalId, cover, hero, logo: logoSource || null, hasMetadata: hasMeta };
}

// ─── Bounded pipeline ────────────────────────────────────────────────────────
async function enrichGamesBatch(items) {
    if (!items?.length) return;

    log.info({ count: items.length, concurrency: LOOKUP_CONCURRENCY }, '[Pipeline] starting batch');

    const buffer   = [];
    let lookupDone = false;

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
                    log.error({ externalId: failed?.externalId, err: r.reason?.message }, '[Pipeline] Lookup failed');
                    enrichEmitter.emit('error', {
                        gameId: failed?.gameId, externalId: failed?.externalId, error: r.reason?.message || 'lookup failed',
                    });
                }
            }
            log.info({ done: Math.min(i + LOOKUP_CONCURRENCY, items.length), total: items.length, buffer: buffer.length }, '[Pipeline] lookup progress');
        }
        lookupDone = true;
    })();

    const consumer = (async () => {
        // ── Intentionally sequential (single-threaded) ──────────────────────────
        // _saveRawUrls() opens a DB transaction that touches 6+ tables plus an
        // optional image fetch.  Running multiple saves in parallel would multiply
        // DB connection usage and can cause lock contention on the game_images
        // partial unique index.  The producer already saturates external APIs at
        // LOOKUP_CONCURRENCY; the save phase is the intentional throttle point
        // that keeps peak DB connections predictable.
        // ────────────────────────────────────────────────────────────────────────
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
                log.info({ processed, total: items.length }, '[Pipeline] saved');
            } catch (err) {
                log.error({ externalId: resolved.externalId, err: err.message }, '[Pipeline] Save failed');
                enrichEmitter.emit('error', {
                    gameId: resolved.gameId, externalId: resolved.externalId, error: err.message,
                });
            }
            await delay(100);
        }
    })();

    await Promise.all([producer, consumer]);
    enrichEmitter.emit('batch_done', { count: items.length });
    log.info({ count: items.length }, '[Pipeline] batch done');
}

// ─── Single-game path ─────────────────────────────────────────────────────────
async function enrichGame(gameId, platform, externalId, clientData = {}) {
    log.info({ platform, externalId }, '[Enrich] starting single game');
    const resolved = await _lookupOnly({ gameId, platform, externalId, clientData });
    return _saveRawUrls(resolved);
}

// ─── Nightly cron: promote raw URLs → WebP → R2 ──────────────────────────────
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
        log.info('[Promote] Nothing to promote.');
        return { promoted: 0, failed: 0 };
    }

    log.info({ count: rows.length }, '[Promote] starting');
    let promoted = 0, failed = 0;

    for (let i = 0; i < rows.length; i += PROMOTE_BATCH_SIZE) {
        const batch = rows.slice(i, i + PROMOTE_BATCH_SIZE);

        await Promise.allSettled(batch.map(async (row) => {
            try {
                // FIX #6: use buildImageKey(row) for unique keys per image row
                const key    = buildImageKey(row);
                const cdnUrl = await uploadImageToR2(row.url, key);

                if (cdnUrl) {
                    await db.query(
                        `UPDATE game_images SET cdn_url=$1 WHERE id=$2`,
                        [cdnUrl, row.id]
                    );
                    promoted++;
                    log.info({ imageType: row.image_type, gameId: row.game_id }, '[Promote] ✅ promoted');
                } else {
                    failed++;
                    log.warn({ imageId: row.id }, '[Promote] R2 returned null');
                }
            } catch (err) {
                failed++;
                log.error({ imageId: row.id, err: err.message }, '[Promote] error');
            }
        }));

        if (i + PROMOTE_BATCH_SIZE < rows.length) await delay(PROMOTE_DELAY_MS);
    }

    log.info({ promoted, failed, total: rows.length }, '[Promote] done');
    return { promoted, failed, total: rows.length };
}

module.exports = { enrichGame, enrichGamesBatch, enrichEmitter, promotePendingImages };