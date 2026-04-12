'use strict';

/**
 * services/steamGridDbResolver.js
 *
 * Fetches hero / cover / logo images from SteamGridDB.
 * Priority:  SteamGridDB → IGDB (handled by caller)
 *
 * SteamGridDB endpoints used:
 *   GET /grids/game/{gameId}   → covers  (600×900 portrait, "tall" style)
 *   GET /heroes/game/{gameId}  → heroes  (wide banner)
 *   GET /logos/game/{gameId}   → logos
 *
 * gameId here is the *SteamGridDB* game ID, resolved from a Steam appId
 * via:  GET /games/steam/{steamAppId}
 *
 * If we only have an IGDB ID (Epic games), we use:
 *       GET /games/igdb/{igdbId}
 */

const SGDB_BASE = 'https://www.steamgriddb.com/api/v2';

// ─── HTTP helper ──────────────────────────────────────────────────────────────

async function sgdbFetch(path) {
    const res = await fetch(`${SGDB_BASE}${path}`, {
        headers: {
            Authorization: `Bearer ${process.env.STEAMGRIDDB_API_KEY}`,
            Accept:        'application/json',
        },
    });

    if (res.status === 404) return null;          // game / images not found
    if (!res.ok) throw new Error(`SteamGridDB ${path} → HTTP ${res.status}`);

    const json = await res.json();
    return json?.success ? json : null;
}

// ─── 1. Resolve SteamGridDB game ID ──────────────────────────────────────────

/**
 * Resolve the SteamGridDB internal game ID.
 * Tries Steam appId first; falls back to IGDB ID.
 *
 * @param {{ steamAppId?: string, igdbId?: number }} ids
 * @returns {Promise<number|null>}
 */
async function resolveSgdbGameId({ steamAppId, igdbId }) {
    if (steamAppId) {
        const data = await sgdbFetch(`/games/steam/${steamAppId}`);
        if (data?.data?.id) return data.data.id;
    }

    if (igdbId) {
        const data = await sgdbFetch(`/games/igdb/${igdbId}`);
        if (data?.data?.id) return data.data.id;
    }

    return null;
}

// ─── 2. Image fetchers ────────────────────────────────────────────────────────

/**
 * Pick the best image from a SteamGridDB image list.
 * Prefers: highest score → nsfw=false → animated=false
 *
 * @param {object[]} items  - raw SteamGridDB image objects
 * @returns {object|null}
 */
function pickBest(items) {
    if (!items?.length) return null;

    // Filter SFW & static first; if nothing left fall back to all items
    const sfw    = items.filter(i => !i.nsfw && !i.epilepsy);
    const static_ = sfw.filter(i => !i.url.endsWith('.webm'));
    const pool    = static_.length ? static_ : sfw.length ? sfw : items;

    // Sort by score desc (null scores treated as 0)
    pool.sort((a, b) => (b.score || 0) - (a.score || 0));
    return pool[0] ?? null;
}

/**
 * Fetch covers (portrait 600×900) from SteamGridDB for a given sgdbId.
 * Requests "tall" style (portrait) images only.
 *
 * @param {number} sgdbId
 * @returns {Promise<{url:string, width:number|null, height:number|null}|null>}
 */
async function fetchSgdbCover(sgdbId) {
    // dimensions=600x900 → portrait style
    const data = await sgdbFetch(
        `/grids/game/${sgdbId}?dimensions=600x900,342x482&types=static&limit=20`
    );
    const best = pickBest(data?.data);
    if (!best) return null;
    return { url: best.url, width: best.width || null, height: best.height || null };
}

/**
 * Fetch hero (wide banner) from SteamGridDB.
 *
 * @param {number} sgdbId
 * @returns {Promise<{url:string, width:number|null, height:number|null}|null>}
 */
async function fetchSgdbHero(sgdbId) {
    const data = await sgdbFetch(`/heroes/game/${sgdbId}?types=static&limit=20`);
    const best = pickBest(data?.data);
    if (!best) return null;
    return { url: best.url, width: best.width || null, height: best.height || null };
}

/**
 * Fetch logo from SteamGridDB.
 *
 * @param {number} sgdbId
 * @returns {Promise<{url:string, width:number|null, height:number|null}|null>}
 */
async function fetchSgdbLogo(sgdbId) {
    const data = await sgdbFetch(`/logos/game/${sgdbId}?types=static&limit=20`);
    const best = pickBest(data?.data);
    if (!best) return null;
    return { url: best.url, width: best.width || null, height: best.height || null };
}

// ─── 3. Main resolver ─────────────────────────────────────────────────────────

/**
 * Resolve all three image types (cover, hero, logo) from SteamGridDB.
 * Returns only the images it found; caller handles IGDB fallback.
 *
 * @param {{ steamAppId?: string, igdbId?: number }} ids
 * @returns {Promise<SgdbImages>}
 *
 * @typedef {{ cover: ImageInfo|null, hero: ImageInfo|null, logo: ImageInfo|null }} SgdbImages
 * @typedef {{ url: string, width: number|null, height: number|null }}              ImageInfo
 */
async function resolveSgdbImages({ steamAppId, igdbId }) {
    const sgdbId = await resolveSgdbGameId({ steamAppId, igdbId });

    if (!sgdbId) {
        console.warn(`[SGDB] No SGDB game found for steam:${steamAppId} igdb:${igdbId}`);
        return { cover: null, hero: null, logo: null };
    }

    // Fetch all three in parallel
    const [cover, hero, logo] = await Promise.all([
        fetchSgdbCover(sgdbId).catch(() => null),
        fetchSgdbHero(sgdbId).catch(() => null),
        fetchSgdbLogo(sgdbId).catch(() => null),
    ]);

    console.log(
        `[SGDB] sgdbId=${sgdbId}` +
        ` cover=${cover ? '✅' : '❌'}` +
        ` hero=${hero   ? '✅' : '❌'}` +
        ` logo=${logo   ? '✅' : '❌'}`
    );

    return { cover, hero, logo };
}

module.exports = { resolveSgdbImages, resolveSgdbGameId };
