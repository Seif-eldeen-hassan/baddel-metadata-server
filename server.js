require('dotenv').config();
const express = require('express');
const cors = require('cors');
const db = require('./db');

const app = express();
app.use(cors());
app.use(express.json());

// ─── Health Check ─────────────────────────────────────────────
app.get('/health', async (req, res) => {
    try {
        const result = await db.query('SELECT NOW()');
        res.status(200).json({
            status: 'success',
            message: 'Baddel Metadata Server is running smoothly!',
            database_time: result.rows[0].now
        });
    } catch (error) {
        res.status(500).json({
            status: 'error',
            message: 'Server is running, but Database connection failed.',
            error: error.message
        });
    }
});

// ─── Game Lookup ───────────────────────────────────────────────
// GET /games/lookup?uuid=...
// GET /games/lookup?platform=steam&namespace=app_id&id=292030
// GET /games/lookup?slug=far-cry-6
// GET /games/lookup?title=far cry 6
app.get('/games/lookup', async (req, res) => {
    const { uuid, platform, namespace, id, slug, title } = req.query;

    try {
        let gameId = null;

        if (uuid) {
            const result = await db.query(
                `SELECT id FROM games WHERE id = $1`,
                [uuid]
            );
            if (result.rows.length) gameId = result.rows[0].id;
        }

        else if (platform && namespace && id) {
            const result = await db.query(
                `SELECT game_id FROM platform_ids
                 WHERE platform = $1 AND namespace = $2 AND external_id = $3`,
                [platform, namespace, id]
            );
            if (result.rows.length) gameId = result.rows[0].game_id;
        }

        else if (slug) {
            const result = await db.query(
                `SELECT id FROM games WHERE slug = $1`,
                [slug]
            );
            if (result.rows.length) gameId = result.rows[0].id;
        }

        else if (title) {
            const result = await db.query(
                `SELECT id FROM games WHERE lower(title) = lower($1)`,
                [title]
            );
            if (result.rows.length) gameId = result.rows[0].id;
        }

        else {
            return res.status(400).json({
                status: 'error',
                message: 'Provide at least one of: uuid, platform+namespace+id, slug, title'
            });
        }

        if (!gameId) {
            return res.status(404).json({
                status: 'not_found',
                message: 'Game not found in database'
            });
        }

        const [gameRes, platformRes, imagesRes, mediaRes, ratingsRes, metadataRes, sysreqRes] =
            await Promise.all([
                db.query(`SELECT * FROM games WHERE id = $1`, [gameId]),
                db.query(`SELECT * FROM platform_ids WHERE game_id = $1`, [gameId]),
                db.query(`SELECT * FROM game_images WHERE game_id = $1 ORDER BY image_type, sort_order`, [gameId]),
                db.query(`SELECT * FROM game_media WHERE game_id = $1 ORDER BY media_type, sort_order`, [gameId]),
                db.query(`SELECT * FROM game_ratings WHERE game_id = $1`, [gameId]),
                db.query(`SELECT * FROM game_metadata WHERE game_id = $1`, [gameId]),
                db.query(`SELECT * FROM game_system_requirements WHERE game_id = $1 ORDER BY platform, tier`, [gameId]),
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
            }
        });

    } catch (error) {
        console.error('Lookup error:', error);
        res.status(500).json({
            status: 'error',
            message: 'Internal server error',
            error: error.message
        });
    }
});

// ─── Games Import (Batch) ──────────────────────────────────────
// POST /games/import
// Body: { "platform": "steam", "games": [{ "namespace": "app_id", "id": "292030" }] }
app.post('/games/import', async (req, res) => {
    const { platform, games } = req.body;

    if (!platform || !games || !Array.isArray(games) || games.length === 0) {
        return res.status(400).json({
            status: 'error',
            message: 'Provide platform and a non-empty games array'
        });
    }

    const found = [];
    const pending = [];

    await Promise.all(games.map(async (game) => {
        const { namespace, id } = game;
        if (!namespace || !id) return;

        const result = await db.query(
            `SELECT g.* FROM games g
             JOIN platform_ids p ON p.game_id = g.id
             WHERE p.platform = $1 AND p.namespace = $2 AND p.external_id = $3`,
            [platform, namespace, id]
        );

        if (result.rows.length) {
            found.push(result.rows[0]);
        } else {
            pending.push({ platform, namespace, id });
        }
    }));

    res.status(200).json({
        status: 'success',
        found: found,
        pending: pending,
        summary: {
            total: games.length,
            found: found.length,
            pending: pending.length
        }
    });

    if (pending.length > 0) {
        console.log(`[Import] ${pending.length} games not found — queuing for fetch...`);
        // TODO: fetchAndSaveGames(pending)
    }
});

// ─── Start ─────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`=================================`);
    console.log(`Baddel Server is alive on Port: ${PORT}`);
    console.log(`=================================`);
});