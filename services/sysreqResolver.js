'use strict';

/**
 * services/sysreqResolver.js
 *
 * Fetches system requirements:
 *   Steam games  → Steam Store API (appdetails)
 *   Epic games   → PCGamingWiki API (fallback, works by title or Steam ID)
 *
 * Both return the same normalized structure ready for game_system_requirements table.
 */

// ─── Steam ────────────────────────────────────────────────────────────────────

/**
 * Parse Steam's raw HTML-ish sysreq string into plain text.
 * Steam returns strings like:
 *   "<strong>OS:</strong> Windows 7<br><strong>CPU:</strong> ..."
 */
function parseHtml(str) {
    if (!str) return null;
    return str
        .replace(/<br\s*\/?>/gi, '\n')
        .replace(/<[^>]+>/g, '')
        .replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&')
        .trim();
}

/**
 * Extract a specific field from a Steam sysreq block.
 * e.g. extractField(text, 'OS') → "Windows 7 / 8 / 10"
 */
function extractField(text, label) {
    if (!text) return null;
    const regex = new RegExp(`${label}\\s*:?\\s*([^\\n]+)`, 'i');
    const match = text.match(regex);
    return match ? match[1].trim() : null;
}

/**
 * Fetch and parse system requirements from Steam Store API.
 *
 * @param {string} steamAppId
 * @returns {Promise<SysreqRow[]>}
 */
async function fetchSteamSysreq(steamAppId) {
    try {
        const res = await fetch(
            `https://store.steampowered.com/api/appdetails?appids=${steamAppId}&filters=pc_requirements,mac_requirements,linux_requirements`,
            { headers: { 'User-Agent': 'BaddelServer/1.0' } }
        );
        if (!res.ok) return [];

        const json    = await res.json();
        const appData = json?.[String(steamAppId)];
        if (!appData?.success) return [];

        const data = appData.data;
        const rows = [];

        const platformMap = {
            pc_requirements:    'windows',
            mac_requirements:   'mac',
            linux_requirements: 'linux',
        };

        for (const [key, platform] of Object.entries(platformMap)) {
            const block = data[key];
            if (!block || (!block.minimum && !block.recommended)) continue;

            for (const tier of ['minimum', 'recommended']) {
                const raw = block[tier];
                if (!raw) continue;

                const text = parseHtml(raw);

                rows.push({
                    source:      'steam',
                    platform,
                    tier,
                    os_versions: extractField(text, 'OS'),
                    cpu:         extractField(text, 'Processor'),
                    gpu:         extractField(text, 'Graphics'),
                    ram:         extractField(text, 'Memory'),
                    storage:     extractField(text, 'Storage') || extractField(text, 'Hard Drive'),
                    directx:     extractField(text, 'DirectX'),
                    notes:       null,
                });
            }
        }

        return rows;
    } catch (err) {
        console.warn(`[SysReq] Steam fetch failed for ${steamAppId}:`, err.message);
        return [];
    }
}

// ─── PCGamingWiki ─────────────────────────────────────────────────────────────

const PCGW_API = 'https://www.pcgamingwiki.com/w/api.php';

/**
 * Query PCGamingWiki using their MediaWiki API.
 * Searches by game title and extracts system requirements from the infobox.
 *
 * @param {string} title  - Game title
 * @returns {Promise<SysreqRow[]>}
 */
async function fetchPCGWSysreq(title) {
    try {
        // Step 1: search for the page
        const searchUrl = `${PCGW_API}?action=query&list=search&srsearch=${encodeURIComponent(title)}&format=json&srlimit=1`;
        const searchRes = await fetch(searchUrl, { headers: { 'User-Agent': 'BaddelServer/1.0' } });
        if (!searchRes.ok) return [];

        const searchJson = await searchRes.json();
        const pageTitle  = searchJson?.query?.search?.[0]?.title;
        if (!pageTitle) return [];

        // Step 2: fetch the page content (wikitext)
        const pageUrl = `${PCGW_API}?action=parse&page=${encodeURIComponent(pageTitle)}&prop=wikitext&format=json`;
        const pageRes = await fetch(pageUrl, { headers: { 'User-Agent': 'BaddelServer/1.0' } });
        if (!pageRes.ok) return [];

        const pageJson = await pageRes.json();
        const wikitext = pageJson?.parse?.wikitext?.['*'];
        if (!wikitext) return [];

        // Step 3: parse sysreq from wikitext
        return parsePCGWWikitext(wikitext);
    } catch (err) {
        console.warn(`[SysReq] PCGamingWiki fetch failed for "${title}":`, err.message);
        return [];
    }
}

/**
 * Parse PCGamingWiki wikitext and extract system requirements.
 * PCGW uses templates like {{System requirements|...}}
 */
function parsePCGWWikitext(wikitext) {
    const rows  = [];
    // Match {{System requirements/row|...}} blocks
    const sysreqRegex = /\{\{System requirements\s*\|([^}]+)\}\}/gi;
    const rowRegex    = /\{\{System requirements\/row\s*\|([^}]+)\}\}/gi;

    // Try to find the main sysreq block
    const mainMatch = wikitext.match(/==\s*System requirements\s*==[\s\S]*?(?===[^=]|$)/i);
    const section   = mainMatch ? mainMatch[0] : wikitext;

    // Extract OS/CPU/GPU/RAM/Storage per tier
    const tiers = [
        { label: 'minimum',     regex: /\|\s*minOS\s*=\s*([^\n|]+)/i,      osKey: 'minOS',  cpuKey: 'minCPU',  gpuKey: 'minGPU',  ramKey: 'minRAM',  storKey: 'minHD'  },
        { label: 'recommended', regex: /\|\s*recOS\s*=\s*([^\n|]+)/i,      osKey: 'recOS',  cpuKey: 'recCPU',  gpuKey: 'recGPU',  ramKey: 'recRAM',  storKey: 'recHD'  },
    ];

    const extract = (text, key) => {
        const r = new RegExp(`\\|\\s*${key}\\s*=\\s*([^\\n|]+)`, 'i');
        const m = text.match(r);
        return m ? m[1].replace(/\[\[([^\]|]+)(?:\|[^\]]+)?\]\]/g, '$1').trim() : null;
    };

    // Detect platform blocks (windows, osx, linux)
    const platformBlocks = [
        { platform: 'windows', marker: /windows/i },
        { platform: 'mac',     marker: /osx|mac/i  },
        { platform: 'linux',   marker: /linux/i    },
    ];

    // Split section into platform subsections if possible
    const subsections = section.split(/\n(?=;|\{\{)/);

    for (const pb of platformBlocks) {
        const sub = subsections.find(s => pb.marker.test(s)) || section;

        for (const tier of tiers) {
            const os      = extract(sub, tier.osKey);
            const cpu     = extract(sub, tier.cpuKey);
            const gpu     = extract(sub, tier.gpuKey);
            const ram     = extract(sub, tier.ramKey);
            const storage = extract(sub, tier.storKey);

            // Only add if we got at least one field
            if (os || cpu || gpu || ram || storage) {
                rows.push({
                    source:      'pcgamingwiki',
                    platform:    pb.platform,
                    tier:        tier.label,
                    os_versions: os,
                    cpu,
                    gpu,
                    ram,
                    storage,
                    directx:     null,
                    notes:       null,
                });
            }
        }
    }

    return rows;
}

// ─── Public API ───────────────────────────────────────────────────────────────

/**
 * Fetch system requirements for a game.
 * Steam games   → Steam API (primary) + PCGamingWiki (fallback if Steam returns nothing)
 * Epic games    → PCGamingWiki only
 *
 * @param {{ platform: 'steam'|'epic', externalId: string, title: string }} opts
 * @returns {Promise<SysreqRow[]>}
 *
 * @typedef {{
 *   source: string, platform: string, tier: string,
 *   os_versions: string|null, cpu: string|null, gpu: string|null,
 *   ram: string|null, storage: string|null, directx: string|null, notes: string|null
 * }} SysreqRow
 */
async function resolveSysreq({ platform, externalId, title }) {
    if (platform === 'steam') {
        const rows = await fetchSteamSysreq(externalId);
        if (rows.length) {
            console.log(`[SysReq] ✅ Steam: ${rows.length} rows for ${externalId}`);
            return rows;
        }
        // Steam returned nothing — fallback to PCGamingWiki
        console.warn(`[SysReq] Steam empty, trying PCGamingWiki for "${title}"`);
    }

    // Epic or Steam fallback
    if (title) {
        const rows = await fetchPCGWSysreq(title);
        if (rows.length) {
            console.log(`[SysReq] ✅ PCGamingWiki: ${rows.length} rows for "${title}"`);
            return rows;
        }
    }

    console.warn(`[SysReq] ⚠️  No sysreq found for ${platform}:${externalId}`);
    return [];
}

module.exports = { resolveSysreq };
