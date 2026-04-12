'use strict';

/**
 * services/steamGridDbResolver.js
 *
 * Strategy for resolving SteamGridDB game ID:
 *   1. By Steam appId        → /games/steam/:id
 *   2. By Epic namespace     → /games/egs/:namespace
 *   3. By IGDB ID            → /games/igdb/:igdbId
 *   4. Fallback: name search → /search/autocomplete/:name
 *      - strips Demo / Chapter suffixes before searching
 *      - strips colon suffix for episodic-safe retry
 *      - uses scored candidate picking (exact > prefix > substring)
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

    if (res.status === 404) return null;
    if (!res.ok) throw new Error(`SteamGridDB ${path} → HTTP ${res.status}`);

    const json = await res.json();
    return json?.success ? json : null;
}

// ─── Name utils ───────────────────────────────────────────────────────────────

function normalizeTitle(value) {
    return String(value || '')
        .toLowerCase()
        .replace(/['"®™©]/g, '')
        .replace(/[^a-z0-9\s]/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

function isEpisodicTitle(name) {
    return /\bep\s*\d+\b|\bepisode\s*\d+\b|season\s*\d+/i.test(name);
}

/**
 * Build name candidates by stripping Demo and Chapter suffixes.
 * Returns an array ordered from most-specific to least-specific.
 */
function buildNameCandidates(rawTitle) {
    const candidates = [rawTitle];

    // Strip "(Demo)", "- Demo", " Demo" from the end
    const noDemo = rawTitle
        .replace(/\s*[\(-]\s*demo\s*\)?$/i, '')
        .replace(/\s+demo$/i, '')
        .trim();

    if (noDemo && noDemo !== rawTitle) candidates.push(noDemo);

    // Strip "Chapter:N ..." or "Chapter N ..." and everything after
    const noChapter = (noDemo || rawTitle)
        .replace(/\s*[–-]?\s*chapter[:\s]\S.*$/i, '')
        .trim();

    if (noChapter && noChapter !== rawTitle && !candidates.includes(noChapter)) {
        candidates.push(noChapter);
    }

    return [...new Set(candidates)];
}

/**
 * Pick the best matching game from SGDB search results.
 * Scoring: exact match > starts with > contains > partial
 * Penalizes large length differences.
 */
function pickBestSgdbCandidate(results = [], requestedName = '') {
    const target = normalizeTitle(requestedName);
    if (!target || !results.length) return results[0] ?? null;

    let best      = null;
    let bestScore = -Infinity;

    for (const item of results) {
        const title = normalizeTitle(item?.name || item?.types?.[0] || '');
        if (!title) continue;

        let score = 0;
        if (title === target)              score += 100;
        else if (title.startsWith(target)) score += 60;
        else if (target.startsWith(title)) score += 25;
        else if (title.includes(target))   score += 40;
        else if (target.includes(title))   score += 15;

        // Penalize length difference
        score -= Math.min(30, Math.abs(title.length - target.length));

        if (score > bestScore) {
            bestScore = score;
            best      = item;
        }
    }

    return best ?? results[0] ?? null;
}

// ─── 1. Resolve SteamGridDB game ID ──────────────────────────────────────────

/**
 * Try all available lookup strategies and return SGDB game ID.
 *
 * @param {{ steamAppId?: string, epicNamespace?: string, igdbId?: number, title?: string }} ids
 * @returns {Promise<number|null>}
 */
async function resolveSgdbGameId({ steamAppId, epicNamespace, igdbId, title }) {

    // ── Strategy 1: Steam appId ──
    if (steamAppId) {
        const data = await sgdbFetch(`/games/steam/${steamAppId}`).catch(() => null);
        if (data?.data?.id) {
            console.log(`[SGDB] matched by Steam ID: ${data.data.id}`);
            return data.data.id;
        }
    }

    // ── Strategy 2: Epic namespace ──
    if (epicNamespace) {
        const data = await sgdbFetch(`/games/egs/${epicNamespace}`).catch(() => null);
        if (data?.data?.id) {
            console.log(`[SGDB] matched by Epic namespace: ${data.data.id}`);
            return data.data.id;
        }
    }

    // ── Strategy 3: IGDB ID ──
    if (igdbId) {
        const data = await sgdbFetch(`/games/igdb/${igdbId}`).catch(() => null);
        if (data?.data?.id) {
            console.log(`[SGDB] matched by IGDB ID: ${data.data.id}`);
            return data.data.id;
        }
    }

    // ── Strategy 4: Name search (with Demo/Chapter stripping) ──
    if (title) {
        const sgdbId = await searchByName(title);
        if (sgdbId) return sgdbId;
    }

    return null;
}

/**
 * Search SGDB by name.
 * Tries multiple name candidates (original → demo-stripped → chapter-stripped)
 * and for each also retries with the pre-colon short name.
 */
async function searchByName(title) {
    const raw = title.replace(/['"®™©]/g, '').trim();
    const candidates = buildNameCandidates(raw);

    const trySearch = async (name) => {
        const encoded = encodeURIComponent(name.replace(/[^a-zA-Z0-9\s]/g, '').trim());
        if (!encoded) return null;
        const data = await sgdbFetch(`/search/autocomplete/${encoded}`).catch(() => null);
        if (!data?.data?.length) return null;
        const best = pickBestSgdbCandidate(data.data, name);
        return best?.id ?? null;
    };

    for (const candidate of candidates) {
        // Try the candidate as-is
        let sgdbId = await trySearch(candidate);
        if (sgdbId) {
            console.log(`[SGDB] matched by name search "${candidate}": ${sgdbId}`);
            return sgdbId;
        }

        // Retry with pre-colon short name (skip for episodic titles)
        if (candidate.includes(':') && !isEpisodicTitle(candidate)) {
            const short = candidate.split(':')[0].trim();
            if (short.length >= 4) {
                sgdbId = await trySearch(short);
                if (sgdbId) {
                    console.log(`[SGDB] matched by short name "${short}": ${sgdbId}`);
                    return sgdbId;
                }
            }
        }
    }

    console.warn(`[SGDB] no match found for "${raw}" (tried ${candidates.length} candidate(s))`);
    return null;
}

// ─── 2. Image fetchers ────────────────────────────────────────────────────────

function pickBestImage(items) {
    if (!items?.length) return null;

    const sfw     = items.filter(i => !i.nsfw && !i.epilepsy);
    const static_ = sfw.filter(i => !i.url?.endsWith('.webm'));
    const pool    = static_.length ? static_ : sfw.length ? sfw : items;

    pool.sort((a, b) => (b.score || 0) - (a.score || 0));
    return pool[0] ?? null;
}

async function fetchSgdbCover(sgdbId) {
    const data = await sgdbFetch(`/grids/game/${sgdbId}?dimensions=600x900,342x482&types=static&limit=20`).catch(() => null);
    const best = pickBestImage(data?.data);
    if (!best) return null;
    return { url: best.url, width: best.width || null, height: best.height || null };
}

async function fetchSgdbHero(sgdbId) {
    const data = await sgdbFetch(`/heroes/game/${sgdbId}?types=static&limit=20`).catch(() => null);
    const best = pickBestImage(data?.data);
    if (!best) return null;
    return { url: best.url, width: best.width || null, height: best.height || null };
}

async function fetchSgdbLogo(sgdbId) {
    const data = await sgdbFetch(`/logos/game/${sgdbId}?types=static&limit=20`).catch(() => null);
    const best = pickBestImage(data?.data);
    if (!best) return null;
    return { url: best.url, width: best.width || null, height: best.height || null };
}

// ─── 3. Main resolver ─────────────────────────────────────────────────────────

/**
 * Resolve cover, hero, logo from SteamGridDB.
 *
 * @param {{ steamAppId?: string, epicNamespace?: string, igdbId?: number, title?: string }} ids
 * @returns {Promise<{ cover, hero, logo }>}
 */
async function resolveSgdbImages({ steamAppId, epicNamespace, igdbId, title }) {
    const sgdbId = await resolveSgdbGameId({ steamAppId, epicNamespace, igdbId, title });

    if (!sgdbId) {
        console.warn(`[SGDB] no game ID resolved`);
        return { cover: null, hero: null, logo: null };
    }

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
