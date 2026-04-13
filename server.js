require('dotenv').config();
const express = require('express');
const cors    = require('cors');
const db      = require('./db');
const { resolveSteamGame, resolveEpicGame }                          = require('./services/coverResolver');
const { enrichGame, enrichGamesBatch, enrichEmitter,
        promotePendingImages }                                        = require('./services/enrichGame');

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const app = express();
app.use(cors());
app.use(express.json({ limit: '10mb' }));

// ─── Enrich Queue ──────────────────────────────────────────────────────────────
//
// Items accumulate here while fetchAndSaveGames() is still running.
// Once fetchAndSaveGames finishes, we drain the whole queue at once using
// enrichGamesBatch() which runs the bounded pipeline (8 parallel lookups,
// 1 sequential save — no R2 in hot path).
//
const _enrichQueue   = [];
let   _enrichRunning = false;

function _enqueue(item) {
    _enrichQueue.push(item);
}

async function _drainQueue() {
    if (_enrichRunning || _enrichQueue.length === 0) return;
    _enrichRunning = true;

    const items = _enrichQueue.splice(0);
    console.log(`[Queue] Draining ${items.length} games`);

    try {
        await enrichGamesBatch(items);
    } catch (err) {
        console.error('[Queue] Pipeline error:', err.message);
    }

    _enrichRunning = false;

    if (_enrichQueue.length > 0) _drainQueue();
}

// ─── Health ────────────────────────────────────────────────────────────────────
app.get('/health', async (req, res) => {
    try {
        const result = await db.query('SELECT NOW()');
        res.status(200).json({
            status:        'success',
            message:       'Baddel Metadata Server is running smoothly!',
            database_time: result.rows[0].now,
            queue_length:  _enrichQueue.length,
            queue_running: _enrichRunning,
        });
    } catch (error) {
        res.status(500).json({ status: 'error', message: 'Database connection failed.', error: error.message });
    }
});

// ─── SSE: Enrichment Stream ────────────────────────────────────────────────────
app.get('/games/enrich-stream', (req, res) => {
    res.setHeader('Content-Type',      'text/event-stream');
    res.setHeader('Cache-Control',     'no-cache');
    res.setHeader('Connection',        'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.flushHeaders();

    const send = (event, data) => {
        try { res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`); } catch {}
    };

    const onDone      = (data) => send('enriched',   data);
    const onError     = (data) => send('error',      data);
    const onBatchDone = (data) => send('batch_done', data);

    enrichEmitter.on('done',       onDone);
    enrichEmitter.on('error',      onError);
    enrichEmitter.on('batch_done', onBatchDone);

    const heartbeat = setInterval(() => {
        try { res.write(':ping\n\n'); } catch { clearInterval(heartbeat); }
    }, 25000);

    req.on('close', () => {
        enrichEmitter.off('done',       onDone);
        enrichEmitter.off('error',      onError);
        enrichEmitter.off('batch_done', onBatchDone);
        clearInterval(heartbeat);
    });
});

// ─── Game Lookup ───────────────────────────────────────────────────────────────
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
            return res.status(400).json({ status: 'error', message: 'Provide uuid, platform+namespace+id, slug, or title' });
        }

        if (!gameId) return res.status(404).json({ status: 'not_found', message: 'Game not found' });

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
        res.status(500).json({ status: 'error', message: error.message });
    }
});

// ─── Games Import ──────────────────────────────────────────────────────────────
app.post('/games/import', async (req, res) => {
    const { platform, games } = req.body;

    if (!platform || !Array.isArray(games) || games.length === 0) {
        return res.status(400).json({ status: 'error', message: 'Provide platform and a non-empty games array' });
    }

    const found = [], pending = [];

    await Promise.all(games.map(async (game) => {
        if (!game.id) return;
        const ns = platform === 'steam' ? 'app_id' : 'catalog_namespace';
        const result = await db.query(
            `SELECT g.* FROM games g
             JOIN platform_ids p ON p.game_id = g.id
             WHERE p.platform=$1 AND p.namespace=$2 AND p.external_id=$3`,
            [platform, ns, game.id]
        );
        result.rows.length ? found.push(result.rows[0]) : pending.push({ ...game, platform });
    }));

    res.status(200).json({
        status: 'success',
        found,
        pending,
        summary: { total: games.length, found: found.length, pending: pending.length },
    });

    if (pending.length > 0) {
        fetchAndSaveGames(pending).catch(err =>
            console.error('[Import] Background error:', err.message)
        );
    }
});

// ─── Save new games + raw cover URL, enqueue for enrichment ───────────────────
//
// No Sharp, no R2 here — we just probe the cover URL and store it raw.
// cdn_url will be NULL until the nightly cron promotes it.
//
async function fetchAndSaveGames(pending) {
    for (const item of pending) {
        try {
            const { platform, id, title: payloadTitle, slug: payloadSlug,
                    cover_url, hero_url, entry_type: payloadType, epic_metadata } = item;
            const ns = platform === 'steam' ? 'app_id' : 'catalog_namespace';

            let title = payloadTitle || null;
            let entry_type = payloadType || 'game';
            let sourceUrl = null;

            if (platform === 'steam') {
                const r = await resolveSteamGame(id);
                if (!title)      title      = r.title;
                if (!entry_type) entry_type = r.entry_type;
                sourceUrl = r.sourceUrl;  // raw URL — no upload
            } else {
                const r = await resolveEpicGame(id, cover_url);
                sourceUrl = r.sourceUrl;  // raw URL — no upload
            }

            const slugFromTitle = title
                ? title.toLowerCase().replace(/[^a-z0-9\s]/g, '').trim().replace(/\s+/g, '-')
                : null;
            const slug = payloadSlug || slugFromTitle || `${platform}-${id}`;
            if (!title) title = slug;

            const gameRes = await db.query(
                `INSERT INTO games (slug, title, entry_type)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (slug) DO UPDATE
                   SET title=EXCLUDED.title, entry_type=EXCLUDED.entry_type, updated_at=now()
                 RETURNING id`,
                [slug, title, entry_type]
            );
            const gameId = gameRes.rows[0].id;

            await db.query(
                `INSERT INTO platform_ids (game_id, platform, namespace, external_id)
                 VALUES ($1, $2, $3, $4) ON CONFLICT (platform, namespace, external_id) DO NOTHING`,
                [gameId, platform, ns, id]
            );

            // Store raw cover URL — cdn_url stays NULL until nightly cron
            if (sourceUrl) {
                await db.query(
                    `INSERT INTO game_images (game_id, source, image_type, url, cdn_url, sort_order)
                     VALUES ($1, $2, 'cover', $3, NULL, 0) ON CONFLICT DO NOTHING`,
                    [gameId, platform, sourceUrl]
                );
                console.log(`[Cover] ✅ raw URL saved ${platform}:${id}`);
            } else {
                console.warn(`[Cover] ⚠️  No cover URL found for ${platform}:${id}`);
            }

            // Epic basic metadata from payload
            if (platform === 'epic' && epic_metadata) {
                try {
                    if (hero_url && !epic_metadata._heroUrl) epic_metadata._heroUrl = hero_url;
                    const epicDev  = epic_metadata.developer || null;
                    const rawDesc  = epic_metadata.description || null;
                    const epicDesc = rawDesc && rawDesc.toLowerCase() !== (title || '').toLowerCase() ? rawDesc : null;
                    const epicYear = epic_metadata.creationDate
                        ? new Date(epic_metadata.creationDate).getFullYear() || null : null;

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
                    if (epicYear) {
                        await db.query(
                            `UPDATE games SET release_year=COALESCE(release_year,$2), updated_at=now() WHERE id=$1`,
                            [gameId, epicYear]
                        );
                    }
                } catch (err) {
                    console.warn(`[Meta] epic_metadata save failed for ${id}:`, err.message);
                }
            }

            _enqueue({ gameId, platform, externalId: id, clientData: item });

        } catch (err) {
            console.error(`[Cover] ❌ ${item.platform}:${item.id} —`, err.message);
        }

        await delay(300); // much shorter — no R2 round-trip to wait for
    }

    _drainQueue();
}

// ─── Enrich Single Game ────────────────────────────────────────────────────────
app.post('/games/enrich/:gameId', async (req, res) => {
    const { gameId } = req.params;
    const background = req.body?.background === true;

    let pidRows;
    try {
        const r = await db.query(
            `SELECT platform, namespace, external_id FROM platform_ids WHERE game_id=$1`, [gameId]
        );
        pidRows = r.rows;
    } catch (err) {
        return res.status(500).json({ status: 'error', message: err.message });
    }

    if (!pidRows.length) {
        return res.status(404).json({ status: 'not_found', message: 'Game not found or has no platform_ids' });
    }

    const pid = pidRows.find(p => p.platform === 'steam') ||
                pidRows.find(p => p.platform === 'epic')  || pidRows[0];
    const { platform, external_id: externalId } = pid;

    if (background) {
        res.status(202).json({ status: 'accepted', message: 'Queued for enrichment', gameId });
        _enqueue({ gameId, platform, externalId, clientData: req.body });
        _drainQueue();
    } else {
        try {
            const result = await enrichGame(gameId, platform, externalId, req.body);
            res.status(200).json({ status: 'success', data: result });
        } catch (err) {
            res.status(500).json({ status: 'error', message: err.message });
        }
    }
});

// ─── Enrich Batch ──────────────────────────────────────────────────────────────
app.post('/games/enrich', async (req, res) => {
    const { gameIds, enrichAll, limit = 100 } = req.body || {};
    let targets = [];

    try {
        if (enrichAll) {
            const r = await db.query(
                `SELECT g.id AS game_id, p.platform, p.external_id
                 FROM games g JOIN platform_ids p ON p.game_id = g.id
                 WHERE p.platform IN ('steam','epic')
                   AND NOT EXISTS (SELECT 1 FROM game_metadata m WHERE m.game_id = g.id AND m.source = 'igdb')
                 ORDER BY g.created_at DESC LIMIT $1`, [limit]
            );
            targets = r.rows.map(row => ({ gameId: row.game_id, platform: row.platform, externalId: row.external_id }));
        } else if (Array.isArray(gameIds) && gameIds.length) {
            const r = await db.query(
                `SELECT g.id AS game_id, p.platform, p.external_id
                 FROM games g JOIN platform_ids p ON p.game_id = g.id
                 WHERE g.id = ANY($1::uuid[]) AND p.platform IN ('steam','epic')`, [gameIds]
            );
            targets = r.rows.map(row => ({ gameId: row.game_id, platform: row.platform, externalId: row.external_id }));
        } else {
            return res.status(400).json({ status: 'error', message: 'Provide gameIds array OR enrichAll:true' });
        }
    } catch (err) {
        return res.status(500).json({ status: 'error', message: err.message });
    }

    res.status(202).json({ status: 'accepted', message: `Queued ${targets.length} games`, count: targets.length });

    for (const t of targets) _enqueue({ gameId: t.gameId, platform: t.platform, externalId: t.externalId, clientData: {} });
    _drainQueue();
});

// ─── Nightly Cron: Promote raw URLs → WebP → R2 ───────────────────────────────
//
// POST /jobs/promote-images
//
// Finds all game_images rows where cdn_url IS NULL,
// converts each to WebP via Sharp, uploads to R2, updates cdn_url.
//
// Trigger this at 00:00 daily from a Railway cron job or an external scheduler
// (e.g. cron-job.org, GitHub Actions, Render cron).
//
// Optional body: { "limit": 500 }  — max images to process per run (default 500)
// Optional header: X-Cron-Secret: <your secret>  — protects the endpoint
//
app.post('/jobs/promote-images', async (req, res) => {
    // Simple secret check — set CRON_SECRET in Railway env vars
    const secret = process.env.CRON_SECRET;
    if (secret && req.headers['x-cron-secret'] !== secret) {
        return res.status(401).json({ status: 'error', message: 'Unauthorized' });
    }

    // Respond immediately — job runs in background
    res.status(202).json({ status: 'accepted', message: 'Image promotion started in background' });

    const limit = Number(req.body?.limit) || 500;

    promotePendingImages(limit)
        .then(result => console.log('[Cron] promote-images done:', result))
        .catch(err   => console.error('[Cron] promote-images error:', err.message));
});

// ─── Promote status check ─────────────────────────────────────────────────────
//
// GET /jobs/promote-images/status
// Returns count of images still pending R2 promotion.
//
app.get('/jobs/promote-images/status', async (req, res) => {
    try {
        const { rows } = await db.query(
            `SELECT COUNT(*) AS pending FROM game_images WHERE cdn_url IS NULL AND url IS NOT NULL`
        );
        res.status(200).json({ status: 'success', pending: Number(rows[0].pending) });
    } catch (err) {
        res.status(500).json({ status: 'error', message: err.message });
    }
});

// ─── Start ─────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`=================================`);
    console.log(`Baddel Server is alive on Port: ${PORT}`);
    console.log(`=================================`);
});
