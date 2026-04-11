'use strict';

/**
 * services/coverResolver.js
 *
 * Epic  → cover URL already in the import payload (DieselGameBoxTall from keyImages)
 * Steam → build library_600x900 URL from app_id, verify it exists, fallback to capsule
 */

const { uploadImageToR2, buildCoverKey } = require('./r2');

// ─── Steam CDN ────────────────────────────────────────────────────────────────

const STEAM_STORE = 'https://shared.akamai.steamstatic.com/store_item_assets/steam/apps';
const STEAM_CDN   = 'https://steamcdn-a.akamaihd.net/steam/apps';

async function resolveSteamCoverUrl(appId) {
    const candidates = [
        `${STEAM_STORE}/${appId}/library_600x900.jpg`,
        `${STEAM_CDN}/${appId}/library_600x900.jpg`,
        `${STEAM_CDN}/${appId}/capsule_616x353.jpg`,
    ];
    for (const url of candidates) {
        try {
            const res = await fetch(url, { method: 'HEAD', redirect: 'follow' });
            if (res.ok) return url;
        } catch { /* try next */ }
    }
    return null;
}

// ─── Steam App Details ────────────────────────────────────────────────────────

const STEAM_TYPE_MAP = {
    game:        'game',
    demo:        'demo',
    dlc:         'dlc',
    mod:         'mod',
    tool:        'tool',
    application: 'tool',
    video:       'other',
};

async function fetchSteamAppDetails(appId) {
    try {
        const res = await fetch(
            `https://store.steampowered.com/api/appdetails?appids=${appId}&filters=basic`,
            { headers: { 'User-Agent': 'BaddelServer/1.0' } }
        );
        if (!res.ok) return null;

        const data    = await res.json();
        const appData = data?.[String(appId)];
        if (!appData?.success) return null;

        const raw = appData.data;
        return {
            title:      raw.name || null,
            entry_type: STEAM_TYPE_MAP[raw.type] || 'game',
        };
    } catch {
        return null;
    }
}

// ─── Main Resolvers ───────────────────────────────────────────────────────────

/**
 * Steam: fetch app details + cover URL from CDN, upload to R2.
 * @returns {{ title, entry_type, sourceUrl, cdnUrl }}
 */
async function resolveSteamGame(appId) {
    const [details, sourceUrl] = await Promise.all([
        fetchSteamAppDetails(appId),
        resolveSteamCoverUrl(appId),
    ]);

    let cdnUrl = null;
    if (sourceUrl) {
        const key = buildCoverKey('steam', appId, sourceUrl);
        cdnUrl    = await uploadImageToR2(sourceUrl, key);
    }

    return {
        title:      details?.title      || null,
        entry_type: details?.entry_type || 'game',
        sourceUrl,
        cdnUrl,
    };
}

/**
 * Epic: cover URL already in payload — just upload to R2.
 * @returns {{ sourceUrl, cdnUrl }}
 */
async function resolveEpicGame(namespace, coverUrl) {
    if (!coverUrl) return { sourceUrl: null, cdnUrl: null };

    const key    = buildCoverKey('epic', namespace, coverUrl);
    const cdnUrl = await uploadImageToR2(coverUrl, key);

    return { sourceUrl: coverUrl, cdnUrl };
}

module.exports = { resolveSteamGame, resolveEpicGame };
