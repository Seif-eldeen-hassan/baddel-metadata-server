'use strict';

/**
 * services/igdbResolver.js
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

async function igdbFetch(endpoint, body) {
    const token = await getAccessToken();

    const res = await fetch(`https://api.igdb.com/v4/${endpoint}`, {
        method:  'POST',
        headers: {
            'Client-ID':     process.env.IGDB_CLIENT_ID,
            'Authorization': `Bearer ${token}`,
            'Accept':        'application/json',
            'Content-Type':  'text/plain',
        },
        body,
    });

    if (!res.ok) throw new Error(`IGDB ${endpoint} error: ${res.status}`);
    return res.json();
}

// ─── 1. Resolve IGDB game ID ──────────────────────────────────────────────────

async function resolveIgdbIdForSteam(steamAppId) {
    // Filter /games directly by nested external_games fields
    const query = `fields id,name,slug; where external_games.uid="${steamAppId}" & external_games.category=1; limit 1;`;
    console.log(`[IGDB] games query (steam):`, query);

    let rows;
    try {
        rows = await igdbFetch('games', query);
    } catch (err) {
        console.error('[IGDB] games fetch error (steam):', err.message);
        return null;
    }
    console.log(`[IGDB] games response (steam):`, JSON.stringify(rows));
    return rows?.[0]?.id ?? null;
}

async function resolveIgdbIdForEpic(namespace) {
    const query = `fields game,uid,name; where category=26 & uid="${namespace}"; limit 1;`;
    console.log(`[IGDB] external_games query (epic):`, query);

    let rows;
    try {
        rows = await igdbFetch('external_games', query);
    } catch (err) {
        console.error('[IGDB] external_games fetch error (epic):', err.message);
        return null;
    }
    console.log(`[IGDB] external_games response (epic):`, JSON.stringify(rows));
    return rows?.[0]?.game ?? null;
}

async function resolveIgdbGameId(platform, externalId) {
    if (platform === 'steam') return resolveIgdbIdForSteam(externalId);
    if (platform === 'epic')  return resolveIgdbIdForEpic(externalId);
    return null;
}

// ─── 2. Full game data ────────────────────────────────────────────────────────

async function fetchIgdbGameData(igdbId) {
    const rows = await igdbFetch(
        'games',
        `fields
            name, slug, summary, storyline,
            first_release_date,
            genres.name,
            themes.name,
            keywords.name,
            involved_companies.company.name, involved_companies.developer, involved_companies.publisher,
            aggregated_rating, aggregated_rating_count,
            rating, rating_count,
            artworks.url, artworks.width, artworks.height,
            cover.url, cover.width, cover.height,
            screenshots.url, screenshots.width, screenshots.height,
            videos.video_id, videos.name,
            websites.url, websites.category,
            platforms.name,
            game_modes.name;
         where id=${igdbId}; limit 1;`
    );

    const g = rows?.[0];
    if (!g) return null;

    const developers = [];
    const publishers = [];
    for (const ic of g.involved_companies || []) {
        const name = ic.company?.name;
        if (!name) continue;
        if (ic.developer) developers.push(name);
        if (ic.publisher) publishers.push(name);
    }

    const genres = (g.genres || []).map(x => x.name);
    const tags   = [
        ...(g.themes   || []).map(x => x.name),
        ...(g.keywords || []).map(x => x.name),
    ];

    const releaseYear = g.first_release_date
        ? new Date(g.first_release_date * 1000).getFullYear()
        : null;

    const fixUrl = (u, size = 'cover_big') =>
        u ? 'https:' + u.replace(/\/t_[^/]+\//, `/t_${size}/`) : null;

    const cover = g.cover
        ? { url: fixUrl(g.cover.url, '720p'), width: g.cover.width || null, height: g.cover.height || null }
        : null;

    const artworks = (g.artworks || []).map(a => ({
        url: fixUrl(a.url, '1080p'), width: a.width || null, height: a.height || null,
    }));

    const screenshots = (g.screenshots || []).map(s => ({
        url: fixUrl(s.url, '1080p'), width: s.width || null, height: s.height || null,
    }));

    const videos = (g.videos || []).map(v => ({
        url:   `https://www.youtube.com/watch?v=${v.video_id}`,
        thumb: `https://img.youtube.com/vi/${v.video_id}/hqdefault.jpg`,
        title: v.name || null,
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
        images: { cover, artworks, screenshots },
        videos,
    };
}

// ─── Public API ───────────────────────────────────────────────────────────────

async function resolveAndFetch(platform, externalId) {
    const igdbId = await resolveIgdbGameId(platform, externalId);
    if (!igdbId) return null;

    const data = await fetchIgdbGameData(igdbId);
    if (!data)  return null;

    return { igdbId, data };
}

module.exports = { resolveIgdbGameId, fetchIgdbGameData, resolveAndFetch };
