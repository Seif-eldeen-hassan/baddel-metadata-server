'use strict';

/**
 * services/coverResolver.js
 *
 * Hot-path version — NO Sharp, NO R2 upload.
 * Just resolve the best raw source URL and return it.
 * The nightly cron job in server.js handles WebP conversion + R2 upload.
 *
 * Epic  → cover URL already in the import payload (DieselGameBoxTall from keyImages)
 * Steam → probe CDN candidates, return the first 200 OK
 */

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
 * Steam: fetch title + entry_type from Steam API, resolve best cover URL.
 * Returns raw source URL only — NO R2 upload, NO Sharp.
 *
 * @returns {{ title, entry_type, sourceUrl }}
 */
async function resolveSteamGame(appId) {
    const [details, sourceUrl] = await Promise.all([
        fetchSteamAppDetails(appId),
        resolveSteamCoverUrl(appId),
    ]);

    return {
        title:      details?.title      || null,
        entry_type: details?.entry_type || 'game',
        sourceUrl,
    };
}

/**
 * Epic: cover URL already in payload — just pass it through.
 * Returns raw source URL only — NO R2 upload, NO Sharp.
 *
 * @returns {{ sourceUrl }}
 */
async function resolveEpicGame(namespace, coverUrl) {
    return { sourceUrl: coverUrl || null };
}

module.exports = { resolveSteamGame, resolveEpicGame };
