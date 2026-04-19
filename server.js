'use strict';

const { validateEnv } = require('./config/env');
validateEnv();

require('dotenv').config();
const express   = require('express');
const cors      = require('cors');
const rateLimit = require('express-rate-limit');

const db                                       = require('./db');
const log                                      = require('./config/logger');
const { requestId }                            = require('./middleware/requestId');
const { errorHandler }                         = require('./middleware/errorHandler');
const { requireAuth, requireCronSecret }       = require('./middleware/auth');
const { validateImport, validateEnrichBatch,
        validateLookup, validatePromoteImages } = require('./middleware/validate');
const { resolveSteamGame, resolveEpicGame }    = require('./services/coverResolver');
const { enrichGame, promotePendingImages }     = require('./services/enrichGame');
const { enqueue, getJob, getJobsByGame }       = require('./services/jobQueue');
const worker                                   = require('./services/jobWorker');

const allowedOrigins = (process.env.CORS_ORIGIN || '').split(',').map(o => o.trim()).filter(Boolean);
const corsOptions = {
    origin: (origin, cb) => {
        if (!origin && process.env.NODE_ENV !== 'production') return cb(null, true);
        if (!origin || allowedOrigins.includes(origin)) return cb(null, true);
        log.warn({ origin }, 'CORS rejected origin');
        cb(new Error('CORS: origin not allowed'));
    },
    methods: ['GET', 'POST', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-Cron-Secret', 'X-Request-ID'],
};

const delay = (ms) => new Promise(r => setTimeout(r, ms));
const lookupLimiter = rateLimit({ windowMs: 60000, max: 120, standardHeaders: true, legacyHeaders: false, message: { status: 'error', message: 'Too many requests' } });
const adminLimiter  = rateLimit({ windowMs: 60000, max: 30,  standardHeaders: true, legacyHeaders: false, message: { status: 'error', message: 'Too many requests' } });

const app = express();
app.use(cors(corsOptions));
app.use(requestId);
app.use(express.json({ limit: '2mb' }));

let _draining = false;

// ─── Health ────────────────────────────────────────────────────────────────────
app.get('/health', async (req, res) => {
    try {
        const result = await db.query('SELECT NOW()');
        const queueRes = await db.query(`SELECT status, COUNT(*) AS n FROM enrich_jobs GROUP BY status`).catch(() => ({ rows: [] }));
        const queue = Object.fromEntries(queueRes.rows.map(r => [r.status, Number(r.n)]));
        res.status(200).json({ status: 'success', message: 'Baddel Metadata Server is running', database_time: result.rows[0].now, worker: worker.isRunning(), queue });
    } catch {
        res.status(500).json({ status: 'error', message: 'Database connection failed' });
    }
});

app.get('/health/ready', (req, res) => {
    if (_draining) return res.status(503).json({ status: 'draining' });
    res.status(200).json({ status: 'ready' });
});

// ─── Job status endpoints (DB-backed) ─────────────────────────────────────────
app.get('/jobs/enrich/:jobId', requireAuth, adminLimiter, async (req, res, next) => {
    try {
        const job = await getJob(req.params.jobId);
        if (!job) return res.status(404).json({ status: 'not_found', message: 'Job not found' });
        res.status(200).json({ status: 'success', data: job });
    } catch (err) { next(err); }
});

app.get('/jobs/enrich', requireAuth, adminLimiter, async (req, res, next) => {
    const { gameId } = req.query;
    if (!gameId || !/^[0-9a-f-]{36}$/i.test(gameId))
        return res.status(400).json({ status: 'error', message: 'Provide valid gameId UUID' });
    try {
        const jobs = await getJobsByGame(gameId);
        res.status(200).json({ status: 'success', data: jobs });
    } catch (err) { next(err); }
});

// ─── SSE (DB-backed polling — works across multiple instances) ─────────────────
app.get('/games/enrich-stream', requireAuth, async (req, res) => {
    const { gameId } = req.query;
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.flushHeaders();

    const send = (event, data) => { try { res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`); } catch {} };
    const seen = new Map();

    const poll = async () => {
        try {
            const jobs = gameId
                ? await getJobsByGame(gameId, 20)
                : await db.query(`SELECT id, game_id, platform, external_id, status, error FROM enrich_jobs WHERE queued_at > now() - interval '10 minutes' ORDER BY queued_at DESC LIMIT 50`).then(r => r.rows);

            for (const job of jobs) {
                if (seen.get(job.id) === job.status) continue;
                seen.set(job.id, job.status);
                if (job.status === 'succeeded')
                    send('enriched', { gameId: job.game_id, jobId: job.id, externalId: job.external_id });
                else if (job.status === 'failed')
                    send('error', { gameId: job.game_id, jobId: job.id, error: job.error });
                else
                    send('update', { gameId: job.game_id, jobId: job.id, status: job.status });
            }
        } catch { /* client closed */ }
    };

    const pollTimer = setInterval(poll, 2000);
    const heartbeat = setInterval(() => { try { res.write(':ping\n\n'); } catch { clearInterval(heartbeat); } }, 25000);
    req.on('close', () => { clearInterval(pollTimer); clearInterval(heartbeat); });
    await poll();
});

// ─── Game Lookup ───────────────────────────────────────────────────────────────
app.get('/games/lookup', lookupLimiter, validateLookup, async (req, res, next) => {
    const { uuid, platform, namespace, id, slug, title } = req.query;
    try {
        let gameId = null;
        if (uuid) {
            const r = await db.query(`SELECT id FROM games WHERE id = $1`, [uuid]);
            if (r.rows.length) gameId = r.rows[0].id;
        } else if (platform && namespace && id) {
            const r = await db.query(`SELECT game_id FROM platform_ids WHERE platform=$1 AND namespace=$2 AND external_id=$3`, [platform, namespace, id]);
            if (r.rows.length) gameId = r.rows[0].game_id;
        } else if (slug) {
            const r = await db.query(`SELECT id FROM games WHERE slug=$1`, [slug]);
            if (r.rows.length) gameId = r.rows[0].id;
        } else if (title) {
            const r = await db.query(`SELECT id FROM games WHERE lower(title)=lower($1)`, [title]);
            if (r.rows.length) gameId = r.rows[0].id;
        }

        if (!gameId) return res.status(404).json({ status: 'not_found', message: 'Game not found' });

        const [gameRes, platformRes, imagesRes, mediaRes, ratingsRes, metadataRes, sysreqRes] = await Promise.all([
            db.query(`SELECT * FROM games WHERE id=$1`, [gameId]),
            db.query(`SELECT * FROM platform_ids WHERE game_id=$1`, [gameId]),
            db.query(`SELECT * FROM game_images WHERE game_id=$1 ORDER BY image_type, sort_order`, [gameId]),
            db.query(`SELECT * FROM game_media WHERE game_id=$1 ORDER BY media_type, sort_order`, [gameId]),
            db.query(`SELECT * FROM game_ratings WHERE game_id=$1`, [gameId]),
            db.query(`SELECT * FROM game_metadata WHERE game_id=$1`, [gameId]),
            db.query(`SELECT * FROM game_system_requirements WHERE game_id=$1 ORDER BY platform, tier`, [gameId]),
        ]);

        return res.status(200).json({ status: 'success', data: { ...gameRes.rows[0], platform_ids: platformRes.rows, images: imagesRes.rows, media: mediaRes.rows, ratings: ratingsRes.rows, metadata: metadataRes.rows, system_requirements: sysreqRes.rows } });
    } catch (err) { next(err); }
});

// ─── Games Import ──────────────────────────────────────────────────────────────
app.post('/games/import', requireAuth, adminLimiter, validateImport, async (req, res, next) => {
    const { platform, games } = req.body;
    const found = [], pending = [];
    try {
        await Promise.all(games.map(async (game) => {
            if (!game.id) return;
            const ns = platform === 'steam' ? 'app_id' : 'catalog_namespace';
            const result = await db.query(`SELECT g.* FROM games g JOIN platform_ids p ON p.game_id = g.id WHERE p.platform=$1 AND p.namespace=$2 AND p.external_id=$3`, [platform, ns, game.id]);
            result.rows.length ? found.push(result.rows[0]) : pending.push({ ...game, platform });
        }));
    } catch (err) { return next(err); }

    res.status(200).json({ status: 'success', found, pending, summary: { total: games.length, found: found.length, pending: pending.length } });

    if (pending.length > 0) {
        fetchAndSaveGames(pending).catch(err => log.error({ err: err.message }, '[Import] Background error'));
    }
});

async function fetchAndSaveGames(pending) {
    for (const item of pending) {
        try {
            const { platform, id, title: payloadTitle, slug: payloadSlug, cover_url, hero_url, entry_type: payloadType, epic_metadata } = item;
            const ns = platform === 'steam' ? 'app_id' : 'catalog_namespace';
            let title = payloadTitle || null, entry_type = payloadType || 'game', sourceUrl = null;

            if (platform === 'steam') {
                const r = await resolveSteamGame(id);
                if (!title)      title      = r.title;
                if (!entry_type) entry_type = r.entry_type;
                sourceUrl = r.sourceUrl;
            } else {
                const r = await resolveEpicGame(id, cover_url);
                sourceUrl = r.sourceUrl;
            }

            const slugFromTitle = title ? title.toLowerCase().replace(/[^a-z0-9\s]/g, '').trim().replace(/\s+/g, '-') : null;
            const slug = payloadSlug || slugFromTitle || `${platform}-${id}`;
            if (!title) title = slug;

            const client = await db.connect();
            let gameId;
            try {
                await client.query('BEGIN');
                const gameRes = await client.query(`INSERT INTO games (slug, title, entry_type) VALUES ($1,$2,$3) ON CONFLICT (slug) DO UPDATE SET title=EXCLUDED.title, entry_type=EXCLUDED.entry_type, updated_at=now() RETURNING id`, [slug, title, entry_type]);
                gameId = gameRes.rows[0].id;
                await client.query(`INSERT INTO platform_ids (game_id, platform, namespace, external_id) VALUES ($1,$2,$3,$4) ON CONFLICT (platform, namespace, external_id) DO NOTHING`, [gameId, platform, ns, id]);
                if (sourceUrl) await client.query(`INSERT INTO game_images (game_id, source, image_type, url, cdn_url, sort_order) VALUES ($1,$2,'cover',$3,NULL,0) ON CONFLICT DO NOTHING`, [gameId, platform, sourceUrl]);
                if (platform === 'epic' && epic_metadata) {
                    if (hero_url && !epic_metadata._heroUrl) epic_metadata._heroUrl = hero_url;
                    const epicDev = epic_metadata.developer || null;
                    const epicDesc = (() => { const r = epic_metadata.description || null; return r && r.toLowerCase() !== (title||'').toLowerCase() ? r : null; })();
                    const epicYear = epic_metadata.creationDate ? new Date(epic_metadata.creationDate).getFullYear() || null : null;
                    if (epicDev || epicDesc) await client.query(`INSERT INTO game_metadata (game_id, source, description, genres, tags, developer, publisher) VALUES ($1,'epic',$2,'{}','{}', $3,NULL) ON CONFLICT (game_id, source) DO UPDATE SET description=COALESCE(EXCLUDED.description,game_metadata.description), developer=COALESCE(EXCLUDED.developer,game_metadata.developer), fetched_at=now()`, [gameId, epicDesc, epicDev]);
                    if (epicYear) await client.query(`UPDATE games SET release_year=COALESCE(release_year,$2), updated_at=now() WHERE id=$1`, [gameId, epicYear]);
                }
                await client.query('COMMIT');
                log.info({ platform, id, gameId }, '[Import] game saved');
            } catch (err) { await client.query('ROLLBACK'); throw err; }
            finally { client.release(); }

            await enqueue({ gameId, platform, externalId: id, clientData: item });
        } catch (err) {
            log.error({ platform: item.platform, id: item.id, err: err.message }, '[Import] save error');
        }
        await delay(300);
    }
}

// ─── Enrich Single Game ────────────────────────────────────────────────────────
app.post('/games/enrich/:gameId', requireAuth, adminLimiter, async (req, res, next) => {
    const { gameId } = req.params;
    if (!/^[0-9a-f-]{36}$/i.test(gameId)) return res.status(400).json({ status: 'error', message: 'Invalid gameId format' });

    let pidRows;
    try {
        const r = await db.query(`SELECT platform, namespace, external_id FROM platform_ids WHERE game_id=$1`, [gameId]);
        pidRows = r.rows;
    } catch (err) { return next(err); }

    if (!pidRows.length) return res.status(404).json({ status: 'not_found', message: 'Game not found or has no platform_ids' });

    const pid = pidRows.find(p => p.platform === 'steam') || pidRows.find(p => p.platform === 'epic') || pidRows[0];
    const { platform, external_id: externalId } = pid;

    if (req.body?.background === true) {
        try {
            const { id: jobId, created } = await enqueue({ gameId, platform, externalId, clientData: req.body });
            return res.status(202).json({ status: 'accepted', message: created ? 'Queued for enrichment' : 'Already queued', gameId, jobId });
        } catch (err) { return next(err); }
    } else {
        try {
            const result = await enrichGame(gameId, platform, externalId, req.body);
            res.status(200).json({ status: 'success', data: result });
        } catch (err) { next(err); }
    }
});

// ─── Enrich Batch ──────────────────────────────────────────────────────────────
app.post('/games/enrich', requireAuth, adminLimiter, validateEnrichBatch, async (req, res, next) => {
    const { gameIds, enrichAll, limit = 100 } = req.body || {};
    let targets = [];
    try {
        if (enrichAll) {
            const r = await db.query(`SELECT g.id AS game_id, p.platform, p.external_id FROM games g JOIN platform_ids p ON p.game_id = g.id WHERE p.platform IN ('steam','epic') AND NOT EXISTS (SELECT 1 FROM game_metadata m WHERE m.game_id = g.id AND m.source = 'igdb') ORDER BY g.created_at DESC LIMIT $1`, [limit]);
            targets = r.rows.map(row => ({ gameId: row.game_id, platform: row.platform, externalId: row.external_id }));
        } else {
            const r = await db.query(`SELECT g.id AS game_id, p.platform, p.external_id FROM games g JOIN platform_ids p ON p.game_id = g.id WHERE g.id = ANY($1::uuid[]) AND p.platform IN ('steam','epic')`, [gameIds]);
            targets = r.rows.map(row => ({ gameId: row.game_id, platform: row.platform, externalId: row.external_id }));
        }
    } catch (err) { return next(err); }

    const jobs = await Promise.all(targets.map(t => enqueue({ gameId: t.gameId, platform: t.platform, externalId: t.externalId, clientData: {} }).catch(() => null)));
    const jobIds = jobs.filter(Boolean).map(j => j.id);
    res.status(202).json({ status: 'accepted', message: `Queued ${targets.length} games`, count: targets.length, jobIds });
});

// ─── Cron ─────────────────────────────────────────────────────────────────────
app.post('/jobs/promote-images', requireCronSecret, validatePromoteImages, async (req, res) => {
    res.status(202).json({ status: 'accepted', message: 'Image promotion started' });
    const limit = Number(req.body?.limit) || 500;
    promotePendingImages(limit).then(r => log.info(r, '[Cron] done')).catch(err => log.error({ err: err.message }, '[Cron] error'));
});

app.get('/jobs/promote-images/status', requireAuth, adminLimiter, async (req, res, next) => {
    try {
        const { rows } = await db.query(`SELECT COUNT(*) AS pending FROM game_images WHERE cdn_url IS NULL AND url IS NOT NULL`);
        res.status(200).json({ status: 'success', pending: Number(rows[0].pending) });
    } catch (err) { next(err); }
});

app.use(errorHandler);

// ─── Start ────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
const server = app.listen(PORT, () => {
    log.info({ port: PORT }, '=== Baddel Server started ===');
    worker.start();
});

async function shutdown(signal) {
    log.info({ signal }, 'Shutdown signal received');
    _draining = true;
    server.close(async () => {
        await worker.stop(30_000);
        try { await db.end(); } catch {}
        log.info('[Shutdown] complete');
        process.exit(0);
    });
    setTimeout(() => { log.error('[Shutdown] forced'); process.exit(1); }, 35_000);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT',  () => shutdown('SIGINT'));
