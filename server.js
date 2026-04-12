require('dotenv').config();
const express = require('express');
const cors    = require('cors');
const db      = require('./db');
const { resolveSteamGame, resolveEpicGame } = require('./services/coverResolver');
const { enrichGame, enrichGames }           = require('./services/enrichGame');
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const app = express();
app.use(cors());
app.use(express.json());

// ─── Health Check ─────────────────────────────────────────────
app.get('/health', async (req, res) => {
    try {
        const result = await db.query('SELECT NOW()');
        res.status(200).json({
            status:        'success',
            message:       'Baddel Metadata Server is running smoothly!',
            database_time: result.rows[0].now,
        });
    } catch (error) {
        res.status(500).json({
            status:  'error',
            message: 'Server is running, but Database connection failed.',
            error:   error.message,
        });
    }
});

// ─── Game Lookup ───────────────────────────────────────────────
app.get('/games/lookup', async (req, res) => {
    const { uuid, platform, namespace, id, slug, title } = req.query;

    try {
        let gameId = null;

        if (uuid) {
            const r = await db.query(`SELECT id FROM games WHERE id = $1`, [uuid]);
            if (r.rows.length) gameId = r.rows[0].id;
        } else if (platform && namespace && id) {
            const r = await db.query(
                `SELECT game_id FROM platform_ids WHERE platform=$1 AND namespace=$2 AND external_id=$3`,
                [platform, namespace, id]
            );
            if (r.rows.length) gameId = r.rows[0].game_id;
        } else if (slug) {
            const r = await db.query(`SELECT id FROM games WHERE slug=$1`, [slug]);
            if (r.rows.length) gameId = r.rows[0].id;
        } else if (title) {
            const r = await db.query(`SELECT id FROM games WHERE lower(title)=lower($1)`, [title]);
            if (r.rows.length) gameId = r.rows[0].id;
        } else {
            return res.status(400).json({
                status:  'error',
                message: 'Provide at least one of: uuid, platform+namespace+id, slug, title',
            });
        }

        if (!gameId) {
            return res.status(404).json({ status: 'not_found', message: 'Game not found' });
        }

        const [gameRes, platformRes, imagesRes, mediaRes, ratingsRes, metadataRes, sysreqRes] =
            await Promise.all([
                db.query(`SELECT * FROM games WHERE id=$1`, [gameId]),
                db.query(`SELECT * FROM platform_ids WHERE game_id=$1`, [gameId]),
                db.query(`SELECT * FROM game_images WHERE game_id=$1 ORDER BY image_type, sort_order`, [gameId]),
                db.query(`SELECT * FROM game_media WHERE game_id=$1 ORDER BY media_type, sort_order`, [gameId]),
                db.query(`SELECT * FROM game_ratings WHERE game_id=$1`, [gameId]),
                db.query(`SELECT * FROM game_metadata WHERE game_id=$1`, [gameId]),
                db.query(`SELECT * FROM game_system_requirements WHERE game_id=$1 ORDER BY platform, tier`, [gameId]),
            ]);

        return res.status(200).json({
            status: 'success',
            data: {
                ...gameRes.rows[0],
                platform_ids:        platformRes.rows,
                images:              imagesRes.rows,
                media:               mediaRes.rows,
                ratings:             ratingsRes.rows,
                metadata:            metadataRes.rows,
                system_requirements: sysreqRes.rows,
            },
        });
    } catch (error) {
        console.error('Lookup error:', error);
        res.status(500).json({ status: 'error', message: 'Internal server error', error: error.message });
    }
});

// ─── Games Import ──────────────────────────────────────────────
//
// Steam payload:
// {
//   "platform": "steam",
//   "games": [
//     { "id": "292030", "title": "The Witcher 3", "slug": "the-witcher-3" }
//   ]
// }
//
// Epic payload:
// {
//   "platform": "epic",
//   "games": [
//     {
//       "id": "27834480410d4000a987b32801e9abba",   ← catalog namespace
//       "title": "First Class Trouble",
//       "slug": "first-class-trouble",
//       "cover_url": "https://cdn1.epicgames.com/...",  ← DieselGameBoxTall (1200x1600)
//       "hero_url":  "https://cdn1.epicgames.com/...",  ← DieselGameBox     (2560x1440)
//       "entry_type": "game"                            ← from categories
//     }
//   ]
// }

app.post('/games/import', async (req, res) => {
    const { platform, games } = req.body;

    if (!platform || !Array.isArray(games) || games.length === 0) {
        return res.status(400).json({
            status:  'error',
            message: 'Provide platform and a non-empty games array',
        });
    }

    const found   = [];
    const pending = [];

    await Promise.all(games.map(async (game) => {
        const { id } = game;
        if (!id) return;

        // namespace differs per platform
        const ns = platform === 'steam' ? 'app_id' : 'catalog_namespace';

        const result = await db.query(
            `SELECT g.* FROM games g
             JOIN platform_ids p ON p.game_id = g.id
             WHERE p.platform=$1 AND p.namespace=$2 AND p.external_id=$3`,
            [platform, ns, id]
        );

        if (result.rows.length) {
            found.push(result.rows[0]);
        } else {
            pending.push({ ...game, platform });
        }
    }));

    // Respond immediately — cover fetch runs in background
    res.status(200).json({
        status:  'success',
        found,
        pending,
        summary: { total: games.length, found: found.length, pending: pending.length },
    });

    if (pending.length > 0) {
        console.log(`[Import] ${pending.length} pending — fetching covers in background...`);
        fetchAndSaveGames(pending).catch(err =>
            console.error('[Import] Background error:', err.message)
        );
    }
});

// ─── Background Job ────────────────────────────────────────────
async function fetchAndSaveGames(pending) {
    for (const item of pending) {
        try {
            const { platform, id, title: payloadTitle, slug: payloadSlug, cover_url, hero_url, entry_type: payloadType, epic_metadata } = item;
            const ns = platform === 'steam' ? 'app_id' : 'catalog_namespace';

            // ── 1. Resolve cover + metadata ──────────────────
            let title      = payloadTitle  || null;
            let entry_type = payloadType   || 'game';
            let sourceUrl  = null;
            let cdnUrl     = null;

            if (platform === 'steam') {
                const result = await resolveSteamGame(id);
                // Steam API fills in title + entry_type when not provided
                if (!title)      title      = result.title;
                if (!entry_type) entry_type = result.entry_type;
                sourceUrl = result.sourceUrl;
                cdnUrl    = result.cdnUrl;
            } else {
                // Epic: cover + hero already in payload
                const result = await resolveEpicGame(id, cover_url);
                sourceUrl = result.sourceUrl;
                cdnUrl    = result.cdnUrl;
            }

            // ── 2. Build slug ─────────────────────────────────
            // ── 2. Build slug ─────────────────────────────────
            const slugFromTitle = title
                ? title.toLowerCase().replace(/[^a-z0-9\s]/g, '').trim().replace(/\s+/g, '-')
                : null;
            const slug = payloadSlug || slugFromTitle || `${platform}-${id}`;

            // Fallback title if still null
            if (!title) title = slug;

            // ── 3. Upsert game ────────────────────────────────
            const gameRes = await db.query(
                `INSERT INTO games (slug, title, entry_type)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (slug) DO UPDATE
                   SET title=EXCLUDED.title, entry_type=EXCLUDED.entry_type, updated_at=now()
                 RETURNING id`,
                [slug, title, entry_type]
            );
            const gameId = gameRes.rows[0].id;

            // ── 4. Upsert platform_id ─────────────────────────
            await db.query(
                `INSERT INTO platform_ids (game_id, platform, namespace, external_id)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (platform, namespace, external_id) DO NOTHING`,
                [gameId, platform, ns, id]
            );

            // ── 5. Save cover in game_images ──────────────────
            if (cdnUrl) {
                await db.query(
                    `INSERT INTO game_images (game_id, source, image_type, url, cdn_url, sort_order)
                     VALUES ($1, $2, 'cover', $3, $4, 0)
                     ON CONFLICT DO NOTHING`,
                    [gameId, platform, sourceUrl, cdnUrl]
                );
                console.log(`[Cover] ✅ ${platform}:${id} → ${cdnUrl}`);
            } else {
                console.warn(`[Cover] ⚠️  No cover for ${platform}:${id}`);
            }

            // ── 6. Save Epic hero (DieselGameBox) separately ──
            // Stored as a standalone 'hero' entity with sort_order=0.
            // If a better hero arrives later (e.g. from IGDB), it can be added
            // with a higher sort_order and the client always picks sort_order ASC LIMIT 1.
            if (platform === 'epic' && hero_url) {
                try {
                    const { buildCoverKey, uploadImageToR2 } = require('./services/r2');
                    const heroKey    = buildCoverKey('epic_hero', id, hero_url);
                    const heroCdnUrl = await uploadImageToR2(hero_url, heroKey);
                    if (heroCdnUrl) {
                        await db.query(
                            `INSERT INTO game_images (game_id, source, image_type, url, cdn_url, sort_order)
                             VALUES ($1, 'epic', 'hero', $2, $3, 0)
                             ON CONFLICT DO NOTHING`,
                            [gameId, hero_url, heroCdnUrl]
                        );
                        console.log(`[Hero]  ✅ epic:${id} → ${heroCdnUrl}`);
                    }
                } catch (err) {
                    console.warn(`[Hero]  ⚠️  epic:${id} —`, err.message);
                }
            }

            // ── 7. Store Epic metadata from Legendary disk cache ──────────────
            // Saved as source='epic' in game_metadata so enrich pipeline can use
            // developer, description, creationDate as fallbacks when IGDB fails.
            if (platform === 'epic' && epic_metadata) {
                try {
                    const epicDev  = epic_metadata.developer   || null;
                    const rawDesc  = epic_metadata.description || null;
                    // Skip description if it's just the game title repeated
                    const epicDesc = rawDesc && rawDesc.toLowerCase() !== (title || '').toLowerCase()
                        ? rawDesc
                        : null;
                    const epicYear = epic_metadata.creationDate
                        ? new Date(epic_metadata.creationDate).getFullYear() || null
                        : null;

                    if (epicDev || epicDesc) {
                        await db.query(
                            `INSERT INTO game_metadata (game_id, source, description, genres, tags, developer, publisher)
                             VALUES ($1, 'epic', $2, '{}', '{}', $3, NULL)
                             ON CONFLICT (game_id, source) DO UPDATE SET
                                 description = COALESCE(EXCLUDED.description, game_metadata.description),
                                 developer   = COALESCE(EXCLUDED.developer,   game_metadata.developer),
                                 fetched_at  = now()`,
                            [gameId, epicDesc, epicDev]
                        );
                    }

                    // Store creationDate year as release_year fallback
                    if (epicYear) {
                        await db.query(
                            `UPDATE games SET release_year=COALESCE(release_year,$2), updated_at=now() WHERE id=$1`,
                            [gameId, epicYear]
                        );
                    }

                    console.log(`[Meta]  ✅ epic:${id} — developer="${epicDev}" year=${epicYear}`);
                } catch (err) {
                    console.warn(`[Meta]  ⚠️  epic_metadata save failed for ${id}:`, err.message);
                }
            }

        } catch (err) {
            console.error(`[Cover] ❌ ${item.platform}:${item.id} —`, err.message);
        }
        await delay(500);
    }
}

// ─── Enrich Single Game ────────────────────────────────────────
//
// POST /games/enrich/:gameId
// Enriches one game that already exists in the DB.
// Looks up its platform_ids, then pulls IGDB + SteamGridDB data.
//
// Optional body: { "background": true }  → respond immediately, run in background
//
app.post('/games/enrich/:gameId', async (req, res) => {
    const { gameId }    = req.params;
    const background    = req.body?.background === true;

    // Pull platform_ids for this game
    let pidRows;
    try {
        const r = await db.query(
            `SELECT platform, namespace, external_id FROM platform_ids WHERE game_id=$1`,
            [gameId]
        );
        pidRows = r.rows;
    } catch (err) {
        return res.status(500).json({ status: 'error', message: err.message });
    }

    if (!pidRows.length) {
        return res.status(404).json({
            status: 'not_found',
            message: 'Game not found or has no platform_ids',
        });
    }

    // Prefer steam, then epic, then whatever exists
    const pid =
        pidRows.find(p => p.platform === 'steam') ||
        pidRows.find(p => p.platform === 'epic')  ||
        pidRows[0];

    const { platform, external_id: externalId } = pid;

    if (background) {
        res.status(202).json({ status: 'accepted', message: 'Enrichment started in background', gameId });
        enrichGame(gameId, platform, externalId).catch(err =>
            console.error('[Enrich] Background error:', err.message)
        );
    } else {
        try {
            const result = await enrichGame(gameId, platform, externalId);
            res.status(200).json({ status: 'success', data: result });
        } catch (err) {
            res.status(500).json({ status: 'error', message: err.message });
        }
    }
});

// ─── Enrich Batch ──────────────────────────────────────────────
//
// POST /games/enrich
// Enrich multiple games at once (always runs in background).
//
// Body:
// {
//   "gameIds": ["uuid1", "uuid2", ...]          ← enrich specific games
// }
// OR:
// {
//   "enrichAll": true,                           ← enrich ALL games with no igdb metadata yet
//   "limit": 50                                  ← optional cap (default 100)
// }
//
app.post('/games/enrich', async (req, res) => {
    const { gameIds, enrichAll, limit = 100 } = req.body || {};

    let targets = [];

    try {
        if (enrichAll) {
            // Find games that haven't been enriched from IGDB yet
            const r = await db.query(
                `SELECT g.id AS game_id, p.platform, p.external_id
                 FROM games g
                 JOIN platform_ids p ON p.game_id = g.id
                 WHERE p.platform IN ('steam','epic')
                   AND NOT EXISTS (
                       SELECT 1 FROM game_metadata m
                       WHERE m.game_id = g.id AND m.source = 'igdb'
                   )
                 ORDER BY g.created_at DESC
                 LIMIT $1`,
                [limit]
            );
            targets = r.rows.map(row => ({
                gameId:     row.game_id,
                platform:   row.platform,
                externalId: row.external_id,
            }));
        } else if (Array.isArray(gameIds) && gameIds.length) {
            const r = await db.query(
                `SELECT g.id AS game_id, p.platform, p.external_id
                 FROM games g
                 JOIN platform_ids p ON p.game_id = g.id
                 WHERE g.id = ANY($1::uuid[])
                   AND p.platform IN ('steam','epic')`,
                [gameIds]
            );
            targets = r.rows.map(row => ({
                gameId:     row.game_id,
                platform:   row.platform,
                externalId: row.external_id,
            }));
        } else {
            return res.status(400).json({
                status:  'error',
                message: 'Provide gameIds array OR enrichAll:true',
            });
        }
    } catch (err) {
        return res.status(500).json({ status: 'error', message: err.message });
    }

    res.status(202).json({
        status:  'accepted',
        message: `Enriching ${targets.length} games in background`,
        count:   targets.length,
    });

    if (targets.length > 0) {
        console.log(`[Enrich] Batch: ${targets.length} games queued`);
        enrichGames(targets).catch(err =>
            console.error('[Enrich] Batch error:', err.message)
        );
    }
});

// ─── Start ─────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`=================================`);
    console.log(`Baddel Server is alive on Port: ${PORT}`);
    console.log(`=================================`);
});
