'use strict';

/**
 * services/igdbResolver.js
 *
 * Strategy:
 *   1. Lookup by external uid — Steam only (Epic namespaces are not in IGDB external_games)
 *   2. Fallback: exact name search on /games (with PC platform preference)
 *      - strips Demo / Chapter suffixes before searching
 *      - strips colon suffix for episodic-safe short-name retry
 *   3. Fallback: fuzzy search sorted by popularity (total_rating_count)
 *      - rejects low-confidence matches (similarity < 0.55)
 *
 * Rate limiting:
 *   IGDB (Twitch) free tier = 4 requests/second burst, ~200 req/min sustained.
 *   We throttle to MAX_RPS requests/second using a token-bucket queue so we
 *   never hit 429 regardless of how many games are enriched concurrently.
 */

// ─── Token Cache ──────────────────────────────────────────────────────────────

let _token     = null;
let _expiresAt = 0;

async function getAccessToken() {
    if (_token && Date.now() < _expiresAt - 60_000) return _token;

    const res = await fetch(
        `https://id.twitch.tv/oauth2/token` +
        `?client_id=${process.env.IGDB_CLIENT_ID}` +
        `&client_secret=${process.env.IGDB_CLIENT_SECRET}` +
        `&grant_type=client_credentials`,
        { method: 'POST' }
    );

    if (!res.ok) throw new Error(`Twitch token error: ${res.status}`);

    const data  = await res.json();
    _token      = data.access_token;
    _expiresAt  = Date.now() + data.expires_in * 1000;
    return _token;
}

// ─── Rate-limit throttler ─────────────────────────────────────────────────────
//
// IGDB free tier: 4 req/s burst.
// We allow MAX_RPS=3 to stay safely under the limit even with clock jitter.
// All igdbFetch() calls go through this queue — one slot released per interval.
//
const MAX_RPS          = 3;                        // requests per second
const SLOT_INTERVAL_MS = Math.ceil(1000 / MAX_RPS); // ~334ms per slot

let _queue        = [];
let _tickRunning  = false;

function _scheduleTick() {
    if (_tickRunning) return;
    _tickRunning = true;

    const tick = () => {
        if (_queue.length === 0) {
            _tickRunning = false;
            return;
        }
        const resolve = _queue.shift();
        resolve();
        setTimeout(tick, SLOT_INTERVAL_MS);
    };

    setTimeout(tick, 0);
}

/**
 * Acquire a rate-limit slot before making an IGDB request.
 * Returns a promise that resolves when it's safe to proceed.
 */
function _acquireSlot() {
    return new Promise((resolve) => {
        _queue.push(resolve);
        _scheduleTick();
    });
}

async function igdbFetch(endpoint, body) {
    await _acquireSlot(); // wait for rate-limit slot

    const token = await getAccessToken();
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 10000); // 10 ثواني وتفصل لو مفيش رد

    try {
        const res = await fetch(`https://api.igdb.com/v4/${endpoint}`, {
            method:  'POST',
            headers: {
                'Client-ID':     process.env.IGDB_CLIENT_ID,
                'Authorization': `Bearer ${token}`,
                'Accept':        'application/json',
                'Content-Type':  'text/plain',
            },
            body,
            signal: controller.signal // 👈 ربطنا الـ Timeout بالـ Fetch هنا
        });

        clearTimeout(timeoutId); // لو ردت بسرعة، نلغي التايمر

        if (!res.ok) throw new Error(`IGDB ${endpoint} error: ${res.status}`);
        return await res.json();
    } catch (err) {
        clearTimeout(timeoutId);
        
        if (err.name === 'AbortError') {
            console.warn(`[IGDB] Timeout error for ${endpoint}`);
            return null; // نرجع null عشان السيرفر يتجاهل اللعبة دي ويكمل شغل في الباقي
        }
        
        throw err;
    }
}

// ─── Constants ────────────────────────────────────────────────────────────────

const PC_PLATFORM_IDS = [6, 14]; // 6 = Windows, 14 = Mac

const GAME_FIELDS = `
    name, slug, summary, storyline,
    first_release_date,
    genres.name,
    themes.name,
    keywords.name,
    involved_companies.company.name, involved_companies.developer, involved_companies.publisher,
    aggregated_rating, aggregated_rating_count,
    rating, rating_count, total_rating_count,
    artworks.url, artworks.image_id, artworks.width, artworks.height,
    cover.url, cover.image_id, cover.width, cover.height,
    screenshots.url, screenshots.image_id, screenshots.width, screenshots.height,
    videos.video_id, videos.name,
    websites.url, websites.category,
    platforms.id, platforms.name,
    game_modes.name
`.trim();

// ─── Name similarity utils ────────────────────────────────────────────────────

function cleanName(s) {
    return String(s || '').toLowerCase().replace(/['"®™©]/g, '').replace(/[^a-z0-9\s]/g, ' ').replace(/\s+/g, ' ').trim();
}

function nameSimilarity(a, b) {
    const ca = cleanName(a);
    const cb = cleanName(b);
    if (ca === cb) return 1;

    const lengthRatio = Math.min(ca.length, cb.length) / Math.max(ca.length, cb.length);

    if (lengthRatio > 0.7 && (ca.includes(cb) || cb.includes(ca))) return 0.85;

    const setA = new Set(ca.split(/\s+/));
    const setB = new Set(cb.split(/\s+/));
    const intersection = [...setA].filter(w => setB.has(w)).length;
    const union        = new Set([...setA, ...setB]).size;
    const jaccard      = union === 0 ? 0 : intersection / union;
    if (jaccard >= 0.55) return jaccard;

    if (lengthRatio > 0.5) {
        const bigrams = s => {
            const set = new Set();
            for (let i = 0; i < s.length - 1; i++) set.add(s.slice(i, i + 2));
            return set;
        };
        const ba = bigrams(ca);
        const bb = bigrams(cb);
        const bigramIntersect = [...ba].filter(g => bb.has(g)).length;
        const bigramUnion     = new Set([...ba, ...bb]).size;
        const bigramScore     = bigramUnion === 0 ? 0 : bigramIntersect / bigramUnion;
        return Math.max(jaccard, bigramScore * 0.9);
    }

    return jaccard;
}

function isEpisodicTitle(name) {
    return /\bep\s*\d+\b|\bepisode\s*\d+\b|season\s*\d+/i.test(name);
}

// ─── Title normalisation ──────────────────────────────────────────────────────

function buildNameCandidates(rawTitle) {
    const push = (arr, val) => {
        const v = val?.trim();
        if (v && !arr.includes(v)) arr.push(v);
    };

    const candidates = [rawTitle];

    const noDemo = rawTitle
        .replace(/\s*[\(-]\s*demo\s*\)?$/i, '')
        .replace(/\s+demo$/i, '')
        .trim();
    push(candidates, noDemo);

    const base = noDemo || rawTitle;

    const noPrologue = base
        .replace(/\s*[–\-]?\s*\(?\bprologue\b\)?$/i, '')
        .trim();
    push(candidates, noPrologue);

    const noTrial = base
        .replace(/\s*[–\-]?\s*\(?\bfree\s+trial\b\)?$/i, '')
        .replace(/\s*[–\-]\s*\(?\btrial\b\)?$/i, '')
        .replace(/\s+\(trial\)$/i, '')
        .trim();
    push(candidates, noTrial);

    const noChapter = base
        .replace(/\s*[–-]?\s*chapter[:\s]\S.*$/i, '')
        .trim();
    push(candidates, noChapter);

    return [...new Set(candidates)];
}

// ─── 1. Resolve IGDB game ID from external platform ID ───────────────────────

async function resolveIgdbIdByUid(platform, externalId) {
    if (platform !== 'steam') {
        console.log(`[IGDB] skipping UID lookup for platform="${platform}" (not indexed in IGDB)`);
        return null;
    }

    const query = `fields game,uid,name; where uid="${externalId}"; limit 5;`;
    console.log(`[IGDB] external_games uid lookup:`, query);

    let rows;
    try {
        rows = await igdbFetch('external_games', query);
    } catch (err) {
        console.warn('[IGDB] external_games error:', err.message);
        return null;
    }

    console.log(`[IGDB] external_games response:`, JSON.stringify(rows));

    if (!rows?.length) return null;

    return rows.find(r => r.game)?.game ?? null;
}

// ─── 2. Search by name ────────────────────────────────────────────────────────

async function searchExact(name) {
    const body = `fields ${GAME_FIELDS}; search "${name}"; limit 50;`;
    const results = await igdbFetch('games', body);
    if (!results?.length) return null;

    const exactPC = results.find(g =>
        g.name?.toLowerCase() === name.toLowerCase() &&
        g.platforms?.some(p => PC_PLATFORM_IDS.includes(p.id))
    );
    if (exactPC) return exactPC;

    return results.find(g => g.name?.toLowerCase() === name.toLowerCase()) ?? null;
}

async function searchFuzzy(name) {
    const body = `fields ${GAME_FIELDS}; search "${name}"; limit 50;`;
    const results = await igdbFetch('games', body);
    if (!results?.length) return null;

    const valid = results.filter(g => g.name);
    if (!valid.length) return null;

    if (!isEpisodicTitle(name)) {
        valid.sort((a, b) => (b.total_rating_count || 0) - (a.total_rating_count || 0));
    }

    const pcMatch = valid.find(g =>
        g.platforms?.some(p => PC_PLATFORM_IDS.includes(p.id)) &&
        nameSimilarity(g.name, name) > 0.82
    );
    if (pcMatch) return pcMatch;

    const best = valid.reduce((prev, curr) =>
        nameSimilarity(curr.name, name) > nameSimilarity(prev.name, name) ? curr : prev
    , valid[0]);

    return nameSimilarity(best.name, name) > 0.55 ? best : null;
}

async function resolveIgdbIdByName(title) {
    if (!title) return null;

    const raw = title.replace(/['"®™©]/g, '').trim();
    const candidates = buildNameCandidates(raw);
    console.log(`[IGDB] name candidates:`, candidates);

    for (const name of candidates) {
        console.log(`[IGDB] name search: "${name}"`);

        let game = await searchExact(name);

        if (!game && name.includes(':') && !isEpisodicTitle(name)) {
            const short = name.split(':')[0].trim();
            if (short.length >= 4) {
                console.log(`[IGDB] retrying with short name: "${short}"`);
                game = await searchExact(short);
            }
        }

        if (!game) {
            console.log(`[IGDB] falling back to fuzzy search for "${name}"`);
            game = await searchFuzzy(name);
        }

        if (game) {
            console.log(`[IGDB] name search matched: ${game.name} (id=${game.id}) via candidate "${name}"`);
            return game.id;
        }
    }

    console.log(`[IGDB] no match found for "${raw}" (tried ${candidates.length} candidate(s))`);
    return null;
}

// ─── 3. Full game data ────────────────────────────────────────────────────────

async function _fetchIgdbLogo(igdbId) {
    try {
        const rows = await igdbFetch('game_logos', `fields image_id, width, height; where game=${igdbId}; limit 1;`);
        const l = rows?.[0];
        if (!l?.image_id) return null;
        return {
            url:    `https://images.igdb.com/igdb/image/upload/t_logo_med/${l.image_id}.png`,
            width:  l.width  || null,
            height: l.height || null,
        };
    } catch { return null; }
}

async function fetchIgdbGameData(igdbId) {
    const [rows, logo] = await Promise.all([
        igdbFetch('games', `fields ${GAME_FIELDS}; where id=${igdbId}; limit 1;`),
        _fetchIgdbLogo(igdbId),
    ]);
    const g = rows?.[0];
    if (!g) return null;

    const developers = (g.involved_companies || []).filter(ic => ic.developer).map(ic => ic.company?.name).filter(Boolean);
    const publishers = (g.involved_companies || []).filter(ic => ic.publisher).map(ic => ic.company?.name).filter(Boolean);

    const genres = (g.genres   || []).map(x => x.name);
    const tags   = [...(g.themes || []).map(x => x.name), ...(g.keywords || []).map(x => x.name)];

    const releaseYear = g.first_release_date
        ? new Date(g.first_release_date * 1000).getFullYear()
        : null;

    const imgUrl = (image_id, size = '720p') =>
        image_id ? `https://images.igdb.com/igdb/image/upload/t_${size}/${image_id}.jpg` : null;

    const cover = g.cover?.image_id
        ? { url: imgUrl(g.cover.image_id, '720p'), width: g.cover.width || null, height: g.cover.height || null }
        : null;

    const artworks = (g.artworks || []).map(a => ({
        url: imgUrl(a.image_id, '1080p'), width: a.width || null, height: a.height || null,
    }));

    const screenshots = (g.screenshots || []).map(s => ({
        url: imgUrl(s.image_id, '1080p'), width: s.width || null, height: s.height || null,
    }));

    const videos = (g.videos || []).map(v => ({
        url:   `https://www.youtube-nocookie.com/embed/${v.video_id}?autoplay=0&rel=0`,
        thumb: `https://img.youtube.com/vi/${v.video_id}/mqdefault.jpg`,
        title: v.name || 'Trailer',
    }));

    return {
        igdbId,
        title:       g.name      || null,
        slug:        g.slug      || null,
        releaseYear,
        description: g.summary   || null,
        storyline:   g.storyline || null,
        genres,
        tags,
        developer:   developers[0] || null,
        publisher:   publishers[0] || null,
        rating: g.aggregated_rating
            ? { score: g.aggregated_rating, count: g.aggregated_rating_count, source: 'igdb' }
            : g.rating
            ? { score: g.rating,            count: g.rating_count,            source: 'igdb_users' }
            : null,
        images: { cover, logo, artworks, screenshots },
        videos,
    };
}

// ─── Public API ───────────────────────────────────────────────────────────────

async function resolveIgdbGameId(platform, externalId, title) {
    let igdbId = await resolveIgdbIdByUid(platform, externalId);

    if (!igdbId && title) {
        console.log(`[IGDB] falling back to name search: "${title}"`);
        igdbId = await resolveIgdbIdByName(title);
    }

    return igdbId;
}

async function resolveAndFetch(platform, externalId, title) {
    const igdbId = await resolveIgdbGameId(platform, externalId, title);
    if (!igdbId) return null;

    const data = await fetchIgdbGameData(igdbId);
    if (!data)  return null;

    return { igdbId, data };
}

module.exports = { resolveIgdbGameId, fetchIgdbGameData, resolveAndFetch };
