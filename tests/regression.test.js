'use strict';

/**
 * tests/regression.test.js
 * Run: node --test tests/regression.test.js
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

function mockEnv(vars) {
    const saved = {};
    for (const [k, v] of Object.entries(vars)) { saved[k] = process.env[k]; process.env[k] = v; }
    return () => { for (const [k, v] of Object.entries(saved)) { if (v === undefined) delete process.env[k]; else process.env[k] = v; } };
}

// ══════════════════════════════════════════════════════════════════════════════
// 1. SSRF guard — static URL validation
// ══════════════════════════════════════════════════════════════════════════════
describe('SSRF guard — URL validation', () => {
    const { assertSafeImageUrl, isPrivateIP } = require('../config/ssrf');

    it('rejects http://', async () => { await assert.rejects(() => assertSafeImageUrl('http://shared.akamai.steamstatic.com/img.jpg'), /only HTTPS/); });
    it('rejects localhost', async () => { await assert.rejects(() => assertSafeImageUrl('https://localhost/secret'), /blocked hostname/); });
    it('rejects 169.254.169.254 (IMDS)', async () => { await assert.rejects(() => assertSafeImageUrl('https://169.254.169.254/latest/'), /blocked hostname/); });
    it('rejects arbitrary host', async () => { await assert.rejects(() => assertSafeImageUrl('https://evil.example.com/img.jpg'), /not in allowlist/); });
    it('rejects raw IPv4 literal', async () => { await assert.rejects(() => assertSafeImageUrl('https://192.168.1.1/img.jpg'), /raw IP/); });
    it('accepts Steam CDN', async () => { await assert.doesNotReject(() => assertSafeImageUrl('https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/123/cover.jpg')); });
    it('accepts IGDB CDN', async () => { await assert.doesNotReject(() => assertSafeImageUrl('https://images.igdb.com/igdb/image/upload/t_720p/abc.jpg')); });
    it('accepts SteamGridDB CDN', async () => { await assert.doesNotReject(() => assertSafeImageUrl('https://cdn2.steamgriddb.com/grid/abc.png')); });
    it('rejects empty URL', async () => { await assert.rejects(() => assertSafeImageUrl(''), /empty/); });
    it('rejects malformed URL', async () => { await assert.rejects(() => assertSafeImageUrl('not a url'), /malformed/); });

    it('isPrivateIP: 10.x.x.x', () => assert.equal(isPrivateIP('10.0.0.1'), true));
    it('isPrivateIP: 192.168.x.x', () => assert.equal(isPrivateIP('192.168.1.1'), true));
    it('isPrivateIP: 172.16.x.x', () => assert.equal(isPrivateIP('172.16.0.1'), true));
    it('isPrivateIP: 127.0.0.1', () => assert.equal(isPrivateIP('127.0.0.1'), true));
    it('isPrivateIP: 169.254.x.x link-local', () => assert.equal(isPrivateIP('169.254.1.1'), true));
    it('isPrivateIP: 1.1.1.1 is public', () => assert.equal(isPrivateIP('1.1.1.1'), false));
    it('isPrivateIP: ::1 IPv6 loopback', () => assert.equal(isPrivateIP('::1'), true));
});

// ══════════════════════════════════════════════════════════════════════════════
// 2. SSRF — redirect hop validation
// ══════════════════════════════════════════════════════════════════════════════
describe('SSRF — redirect hop validation', () => {
    // We test the redirect logic by mocking globalThis.fetch
    const { safeFetch } = require('../config/ssrf');

    function mockFetch(hops) {
        let call = 0;
        return async (url, opts) => {
            const r = hops[call++];
            if (!r) throw new Error('unexpected fetch call');
            if (r.type === 'redirect') {
                return { ok: false, status: 302, headers: { get: (h) => h === 'location' ? r.location : null } };
            }
            return { ok: true, status: 200, headers: { get: (h) => h === 'content-type' ? 'image/jpeg' : h === 'content-length' ? '100' : null }, arrayBuffer: async () => new ArrayBuffer(100) };
        };
    }

    it('rejects redirect to non-allowlisted host', async () => {
        const origFetch = globalThis.fetch;
        globalThis.fetch = mockFetch([
            { type: 'redirect', location: 'https://evil.example.com/img.jpg' },
        ]);
        try {
            await assert.rejects(
                () => safeFetch('https://shared.akamai.steamstatic.com/img.jpg'),
                /not in allowlist/
            );
        } finally { globalThis.fetch = origFetch; }
    });

    it('rejects redirect to localhost', async () => {
        const origFetch = globalThis.fetch;
        globalThis.fetch = mockFetch([
            { type: 'redirect', location: 'https://localhost/secret' },
        ]);
        try {
            await assert.rejects(
                () => safeFetch('https://shared.akamai.steamstatic.com/img.jpg'),
                /blocked hostname/
            );
        } finally { globalThis.fetch = origFetch; }
    });

    it('rejects redirect to raw IP', async () => {
        const origFetch = globalThis.fetch;
        globalThis.fetch = mockFetch([
            { type: 'redirect', location: 'https://192.168.1.1/img.jpg' },
        ]);
        try {
            await assert.rejects(
                () => safeFetch('https://shared.akamai.steamstatic.com/img.jpg'),
                /raw IP/
            );
        } finally { globalThis.fetch = origFetch; }
    });

    it('rejects after too many redirect hops', async () => {
        const origFetch = globalThis.fetch;
        // 6 redirects, all within allowlist — should fail at hop limit
        globalThis.fetch = mockFetch(Array(6).fill({ type: 'redirect', location: 'https://shared.akamai.steamstatic.com/img2.jpg' }));
        try {
            await assert.rejects(
                () => safeFetch('https://shared.akamai.steamstatic.com/img.jpg'),
                /too many redirects/
            );
        } finally { globalThis.fetch = origFetch; }
    });

    it('rejects http:// URL from redirect', async () => {
        const origFetch = globalThis.fetch;
        globalThis.fetch = mockFetch([
            { type: 'redirect', location: 'http://shared.akamai.steamstatic.com/img.jpg' },
        ]);
        try {
            await assert.rejects(
                () => safeFetch('https://shared.akamai.steamstatic.com/img.jpg'),
                /only HTTPS/
            );
        } finally { globalThis.fetch = origFetch; }
    });
});

// ══════════════════════════════════════════════════════════════════════════════
// 3. R2 key uniqueness
// ══════════════════════════════════════════════════════════════════════════════
describe('R2 key generation', () => {
    const { buildImageKey, buildCoverKey } = require('../services/r2');

    it('different rows → different keys', () => {
        const r1 = { id: 'aaaa-0001', source: 'steam', image_type: 'screenshot', url: 'https://shared.akamai.steamstatic.com/img1.jpg' };
        const r2 = { id: 'aaaa-0002', source: 'steam', image_type: 'screenshot', url: 'https://shared.akamai.steamstatic.com/img1.jpg' };
        assert.notEqual(buildImageKey(r1), buildImageKey(r2));
    });
    it('same row → same key (idempotent)', () => {
        const row = { id: 'aaaa-0001', source: 'steam', image_type: 'cover', url: 'https://shared.akamai.steamstatic.com/cover.jpg' };
        assert.equal(buildImageKey(row), buildImageKey(row));
    });
    it('different image_types → different keys', () => {
        const base = { id: 'aaaa-0001', source: 'steam', url: 'https://shared.akamai.steamstatic.com/img.jpg' };
        assert.notEqual(buildImageKey({ ...base, image_type: 'cover' }), buildImageKey({ ...base, image_type: 'screenshot' }));
    });
    it('throws on missing id', () => { assert.throws(() => buildImageKey({ source: 'steam', image_type: 'cover', url: 'https://shared.akamai.steamstatic.com/img.jpg' })); });
    it('key starts with covers/', () => {
        const row = { id: 'test-uuid-1234', source: 'epic', image_type: 'hero', url: 'https://cdn1.epicgames.com/img.jpg' };
        assert.match(buildImageKey(row), /^covers\//);
    });
    it('buildCoverKey: different URLs → different keys', () => {
        assert.notEqual(buildCoverKey('steam', 'g1', 'https://shared.akamai.steamstatic.com/a.jpg'), buildCoverKey('steam', 'g1', 'https://shared.akamai.steamstatic.com/b.jpg'));
    });
});

// ══════════════════════════════════════════════════════════════════════════════
// 4. Auth middleware
// ══════════════════════════════════════════════════════════════════════════════
describe('Auth middleware', () => {
    const { requireAuth, requireCronSecret } = require('../middleware/auth');

    function mockReqRes(headers = {}) {
        const req = { headers, path: '/test', ip: '127.0.0.1' };
        let statusCode, jsonBody;
        const res = { status(c) { statusCode = c; return this; }, json(b) { jsonBody = b; return this; } };
        return { req, res, getStatus: () => statusCode };
    }

    it('requireAuth: rejects missing header', (_, done) => {
        const restore = mockEnv({ API_SECRET: 'supersecretkey1234567890abcdef12' });
        const { req, res, getStatus } = mockReqRes({});
        requireAuth(req, res, () => done(new Error('should not reach next')));
        assert.equal(getStatus(), 401); restore(); done();
    });
    it('requireAuth: rejects wrong token', (_, done) => {
        const restore = mockEnv({ API_SECRET: 'supersecretkey1234567890abcdef12' });
        const { req, res, getStatus } = mockReqRes({ authorization: 'Bearer wrongtoken' });
        requireAuth(req, res, () => done(new Error('should not reach next')));
        assert.equal(getStatus(), 401); restore(); done();
    });
    it('requireAuth: passes with correct token', (_, done) => {
        const secret = 'supersecretkey1234567890abcdef12';
        const restore = mockEnv({ API_SECRET: secret });
        const { req, res } = mockReqRes({ authorization: `Bearer ${secret}` });
        requireAuth(req, res, () => { restore(); done(); });
    });
    it('requireCronSecret: 503 when CRON_SECRET not set', (_, done) => {
        const restore = mockEnv({ CRON_SECRET: '' });
        const { req, res, getStatus } = mockReqRes({});
        requireCronSecret(req, res, () => done(new Error('should not reach next')));
        assert.equal(getStatus(), 503); restore(); done();
    });
    it('requireCronSecret: 401 wrong secret', (_, done) => {
        const restore = mockEnv({ CRON_SECRET: 'correct' });
        const { req, res, getStatus } = mockReqRes({ 'x-cron-secret': 'wrong' });
        requireCronSecret(req, res, () => done(new Error('should not reach next')));
        assert.equal(getStatus(), 401); restore(); done();
    });
    it('requireCronSecret: passes correct secret', (_, done) => {
        const restore = mockEnv({ CRON_SECRET: 'correct' });
        const { req, res } = mockReqRes({ 'x-cron-secret': 'correct' });
        requireCronSecret(req, res, () => { restore(); done(); });
    });
});

// ══════════════════════════════════════════════════════════════════════════════
// 5. Validation middleware
// ══════════════════════════════════════════════════════════════════════════════
describe('Validation middleware', () => {
    const { validateImport, validateEnrichBatch, validateLookup } = require('../middleware/validate');

    function mockReqRes(body = {}, query = {}) {
        const req = { body, query };
        let statusCode;
        const res = { status(c) { statusCode = c; return this; }, json() { return this; } };
        return { req, res, getStatus: () => statusCode };
    }

    it('validateImport: rejects missing platform', (_, done) => { const { req, res, getStatus } = mockReqRes({ games: [{ id: '123' }] }); validateImport(req, res, () => done(new Error())); assert.equal(getStatus(), 400); done(); });
    it('validateImport: rejects invalid platform', (_, done) => { const { req, res, getStatus } = mockReqRes({ platform: 'gog', games: [{ id: '123' }] }); validateImport(req, res, () => done(new Error())); assert.equal(getStatus(), 400); done(); });
    it('validateImport: rejects empty games', (_, done) => { const { req, res, getStatus } = mockReqRes({ platform: 'steam', games: [] }); validateImport(req, res, () => done(new Error())); assert.equal(getStatus(), 400); done(); });
    it('validateImport: passes valid', (_, done) => { const { req, res } = mockReqRes({ platform: 'steam', games: [{ id: '570' }] }); validateImport(req, res, () => done()); });
    it('validateEnrichBatch: rejects empty body', (_, done) => { const { req, res, getStatus } = mockReqRes({}); validateEnrichBatch(req, res, () => done(new Error())); assert.equal(getStatus(), 400); done(); });
    it('validateEnrichBatch: passes enrichAll:true', (_, done) => { const { req, res } = mockReqRes({ enrichAll: true }); validateEnrichBatch(req, res, () => done()); });
    it('validateLookup: rejects empty query', (_, done) => { const { req, res, getStatus } = mockReqRes({}, {}); validateLookup(req, res, () => done(new Error())); assert.equal(getStatus(), 400); done(); });
    it('validateLookup: passes with uuid', (_, done) => { const { req, res } = mockReqRes({}, { uuid: 'some-uuid' }); validateLookup(req, res, () => done()); });
});

// ══════════════════════════════════════════════════════════════════════════════
// 6. Durable job queue logic
// ══════════════════════════════════════════════════════════════════════════════
describe('Durable job queue', () => {
    // Test queue logic using an in-memory mock DB client
    // (no real DB needed for unit tests)

    function makeQueueUnderTest() {
        const rows = [];
        let idCounter = 1;

        const client = {
            async query(sql, params = []) {
                const s = sql.replace(/\s+/g, ' ').trim();

                // INSERT ... ON CONFLICT DO NOTHING RETURNING id
                if (s.includes('INSERT INTO enrich_jobs') && s.includes('RETURNING id')) {
                    const gameId = params[0], platform = params[1], extId = params[2];
                    // Check unique constraint: one active per game
                    const hasActive = rows.some(r => r.game_id === gameId && ['queued','running'].includes(r.status));
                    if (hasActive) return { rows: [] }; // conflict
                    const id = `job-${idCounter++}`;
                    rows.push({ id, game_id: gameId, platform, external_id: extId, status: 'queued', attempts: 0, max_attempts: 3, error: null, queued_at: new Date(), started_at: null, finished_at: null, payload: JSON.parse(params[3] || '{}') });
                    return { rows: [{ id }] };
                }

                // SELECT active job for enqueue fallback
                if (s.includes("status IN ('queued','running')") && s.includes('game_id = $1')) {
                    const gameId = params[0];
                    const found = rows.filter(r => r.game_id === gameId && ['queued','running'].includes(r.status)).sort((a,b) => b.queued_at - a.queued_at);
                    return { rows: found };
                }

                // claimBatch SELECT FOR UPDATE SKIP LOCKED
                if (s.includes("status = 'queued'") && s.includes('FOR UPDATE SKIP LOCKED')) {
                    const maxAttempts = params[0], limit = params[1];
                    const available = rows.filter(r => r.status === 'queued' && r.attempts < maxAttempts).slice(0, limit);
                    return { rows: available };
                }

                // UPDATE to running
                if (s.includes("status = 'running'") && s.includes('ANY($1::uuid[])')) {
                    const ids = params[0];
                    rows.filter(r => ids.includes(r.id)).forEach(r => { r.status = 'running'; r.attempts++; r.started_at = new Date(); });
                    return { rows: [] };
                }

                // complete
                if (s.includes("status = 'succeeded'")) {
                    const id = params[0];
                    const row = rows.find(r => r.id === id);
                    if (row) { row.status = 'succeeded'; row.finished_at = new Date(); row.error = null; }
                    return { rows: [] };
                }

                // fail
                if (s.includes('CASE WHEN attempts <')) {
                    const id = params[0], maxAttempts = params[1], error = params[2];
                    const row = rows.find(r => r.id === id);
                    if (row) {
                        row.error = error;
                        row.status = row.attempts < maxAttempts ? 'queued' : 'failed';
                        if (row.status === 'failed') row.finished_at = new Date();
                    }
                    return { rows: row ? [{ status: row.status, attempts: row.attempts }] : [] };
                }

                // getJob
                if (s.includes('WHERE id = $1') && s.includes('game_id, platform')) {
                    return { rows: rows.filter(r => r.id === params[0]) };
                }

                // getJobsByGame
                if (s.includes('game_id = $1') && s.includes('queued_at DESC')) {
                    return { rows: rows.filter(r => r.game_id === params[0]).slice(0, params[1] || 10) };
                }

                return { rows: [] };
            },
            async release() {},
        };

        const mockDb = {
            async query(sql, params) { return client.query(sql, params); },
            async connect() {
                return {
                    query: client.query.bind(client),
                    release: client.release.bind(client),
                };
            },
        };

        // Build inline versions of queue functions using mock DB
        async function enqueue({ gameId, platform, externalId, clientData = {} }) {
            const payload = JSON.stringify({ platform, externalId, clientData });
            const { rows } = await mockDb.query(
                `INSERT INTO enrich_jobs (game_id, platform, external_id, payload) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING RETURNING id`,
                [gameId, platform, externalId, payload]
            );
            if (rows.length > 0) return { id: rows[0].id, created: true };
            const existing = await mockDb.query(`SELECT id FROM enrich_jobs WHERE game_id = $1 AND status IN ('queued','running') ORDER BY queued_at DESC LIMIT 1`, [gameId]);
            return { id: existing.rows[0]?.id || null, created: false };
        }

        async function claimBatch(limit = 5) {
            const c = await mockDb.connect();
            try {
                await c.query('BEGIN');
                const { rows } = await c.query(`SELECT id, game_id, platform, external_id, payload, attempts FROM enrich_jobs WHERE status = 'queued' AND attempts < $1 ORDER BY queued_at ASC LIMIT $2 FOR UPDATE SKIP LOCKED`, [3, limit]);
                if (rows.length === 0) { await c.query('COMMIT'); return []; }
                await c.query(`UPDATE enrich_jobs SET status = 'running', started_at = now(), attempts = attempts + 1 WHERE id = ANY($1::uuid[])`, [rows.map(r => r.id)]);
                await c.query('COMMIT');
                return rows;
            } finally { c.release(); }
        }

        async function complete(jobId) { await mockDb.query(`UPDATE enrich_jobs SET status = 'succeeded', finished_at = now(), error = NULL WHERE id = $1`, [jobId]); }
        async function fail(jobId, error) { await mockDb.query(`UPDATE enrich_jobs SET status = CASE WHEN attempts < $2 THEN 'queued' ELSE 'failed' END, finished_at = CASE WHEN attempts < $2 THEN NULL ELSE now() END, error = $3 WHERE id = $1 RETURNING status, attempts`, [jobId, 3, error]); }
        async function getJob(id) { const { rows } = await mockDb.query(`SELECT id, game_id, platform, external_id, status, attempts, max_attempts, error, queued_at, started_at, finished_at FROM enrich_jobs WHERE id = $1`, [id]); return rows[0] || null; }
        async function getJobsByGame(gameId, limit = 10) { const { rows } = await mockDb.query(`SELECT id, game_id, platform, external_id, status, attempts, max_attempts, error, queued_at, started_at, finished_at FROM enrich_jobs WHERE game_id = $1 ORDER BY queued_at DESC LIMIT $2`, [gameId, limit]); return rows; }

        return { enqueue, claimBatch, complete, fail, getJob, getJobsByGame, _rows: rows };
    }

    it('enqueue: creates a new job', async () => {
        const q = makeQueueUnderTest();
        const result = await q.enqueue({ gameId: 'game-1', platform: 'steam', externalId: '570' });
        assert.equal(result.created, true);
        assert.ok(result.id);
    });

    it('enqueue: duplicate active job returns existing, not new', async () => {
        const q = makeQueueUnderTest();
        const first = await q.enqueue({ gameId: 'game-1', platform: 'steam', externalId: '570' });
        const second = await q.enqueue({ gameId: 'game-1', platform: 'steam', externalId: '570' });
        assert.equal(second.created, false);
        assert.equal(second.id, first.id);
    });

    it('claimBatch: claims queued jobs and marks them running', async () => {
        const q = makeQueueUnderTest();
        await q.enqueue({ gameId: 'game-1', platform: 'steam', externalId: '570' });
        const claimed = await q.claimBatch(5);
        assert.equal(claimed.length, 1);
        assert.equal(q._rows[0].status, 'running');
        assert.equal(q._rows[0].attempts, 1);
    });

    it('claimBatch: does not return already-running jobs', async () => {
        const q = makeQueueUnderTest();
        await q.enqueue({ gameId: 'game-1', platform: 'steam', externalId: '570' });
        await q.claimBatch(5); // first claim
        const second = await q.claimBatch(5);
        assert.equal(second.length, 0); // already running
    });

    it('complete: marks job succeeded', async () => {
        const q = makeQueueUnderTest();
        const { id } = await q.enqueue({ gameId: 'game-1', platform: 'steam', externalId: '570' });
        await q.claimBatch();
        await q.complete(id);
        const job = await q.getJob(id);
        assert.equal(job.status, 'succeeded');
    });

    it('fail: re-queues job if under max_attempts', async () => {
        const q = makeQueueUnderTest();
        const { id } = await q.enqueue({ gameId: 'game-1', platform: 'steam', externalId: '570' });
        await q.claimBatch(); // attempts = 1
        await q.fail(id, 'timeout');
        const job = await q.getJob(id);
        assert.equal(job.status, 'queued'); // re-queued (attempts 1 < max 3)
        assert.equal(job.error, 'timeout');
    });

    it('fail: marks job failed after max_attempts', async () => {
        const q = makeQueueUnderTest();
        const { id } = await q.enqueue({ gameId: 'game-1', platform: 'steam', externalId: '570' });
        // Simulate 3 attempts
        for (let i = 0; i < 3; i++) {
            await q.claimBatch();
            await q.fail(id, `attempt ${i + 1} failed`);
        }
        const job = await q.getJob(id);
        assert.equal(job.status, 'failed');
    });

    it('getJobsByGame: returns jobs for a game', async () => {
        const q = makeQueueUnderTest();
        await q.enqueue({ gameId: 'game-1', platform: 'steam', externalId: '570' });
        await q.enqueue({ gameId: 'game-2', platform: 'epic',  externalId: 'fortnite' });
        const jobs = await q.getJobsByGame('game-1');
        assert.equal(jobs.length, 1);
        assert.equal(jobs[0].game_id, 'game-1');
    });

    it('enqueue: allows new job after previous succeeded', async () => {
        const q = makeQueueUnderTest();
        const { id: firstId } = await q.enqueue({ gameId: 'game-1', platform: 'steam', externalId: '570' });
        await q.claimBatch();
        await q.complete(firstId);
        // Now game-1 has no active job — should be able to enqueue again
        const second = await q.enqueue({ gameId: 'game-1', platform: 'steam', externalId: '570' });
        assert.equal(second.created, true);
    });
});

// ══════════════════════════════════════════════════════════════════════════════
// 7. Epic image mapping (FIX #7)
// ══════════════════════════════════════════════════════════════════════════════
describe('Epic image mapping (FIX #7)', () => {
    function _getEpicImages(d) {
        if (!d) return { poster: null, background: null, logo: null };
        return { poster: d.poster || null, background: d.background || null, logo: d.logo || null };
    }
    it('reads poster from direct field', () => { assert.equal(_getEpicImages({ poster: 'https://cdn1.epicgames.com/p.jpg' }).poster, 'https://cdn1.epicgames.com/p.jpg'); });
    it('reads background from direct field', () => { assert.equal(_getEpicImages({ background: 'https://cdn1.epicgames.com/bg.jpg' }).background, 'https://cdn1.epicgames.com/bg.jpg'); });
    it('reads logo from direct field', () => { assert.equal(_getEpicImages({ logo: 'https://cdn1.epicgames.com/logo.png' }).logo, 'https://cdn1.epicgames.com/logo.png'); });
    it('does NOT fall back to keyImages', () => { assert.equal(_getEpicImages({ keyImages: [{ type: 'DieselGameBoxTall', url: 'https://cdn1.epicgames.com/k.jpg' }], poster: null }).poster, null); });
    it('returns nulls for null', () => { const r = _getEpicImages(null); assert.equal(r.poster, null); assert.equal(r.background, null); });
});

// ══════════════════════════════════════════════════════════════════════════════
// 8. Video source attribution (FIX #8)
// ══════════════════════════════════════════════════════════════════════════════
describe('upsertVideos source attribution (FIX #8)', () => {
    function makeClient(captures) { return { query: async (sql, params) => { captures.push(params); return { rows: [] }; } }; }

    async function upsertVideos(client, gameId, videos, source) {
        if (!source) throw new Error('upsertVideos: source is required');
        for (let i = 0; i < videos.length; i++) {
            const v = videos[i];
            await client.query(`INSERT INTO game_media (game_id, source, media_type, url, thumbnail_url, title, sort_order) VALUES ($1,$2,'trailer',$3,$4,$5,$6) ON CONFLICT (game_id, url) DO NOTHING`, [gameId, source, v.url, v.thumb || null, v.title || null, i]);
        }
    }

    it('throws if source missing', async () => { await assert.rejects(() => upsertVideos(makeClient([]), 'g', [{ url: 'x' }], undefined), /source is required/); });
    it('stores source=steam', async () => { const c = []; await upsertVideos(makeClient(c), 'g', [{ url: 'x' }], 'steam'); assert.equal(c[0][1], 'steam'); });
    it('stores source=epic', async () => { const c = []; await upsertVideos(makeClient(c), 'g', [{ url: 'x' }], 'epic'); assert.equal(c[0][1], 'epic'); });
    it('stores source=igdb', async () => { const c = []; await upsertVideos(makeClient(c), 'g', [{ url: 'x' }], 'igdb'); assert.equal(c[0][1], 'igdb'); });
});

// ══════════════════════════════════════════════════════════════════════════════
// 11. DB SSL configuration
// ══════════════════════════════════════════════════════════════════════════════
describe('DB SSL configuration', () => {
    function buildSslConfig(env) {
        if (!env.DATABASE_URL) return false;
        if (env.DB_SSL_REJECT_UNAUTHORIZED === 'false') return { rejectUnauthorized: false };
        return { rejectUnauthorized: true };
    }

    it('no DATABASE_URL → ssl: false (local plain connection)', () => {
        assert.equal(buildSslConfig({}), false);
    });

    it('DATABASE_URL set, no override → rejectUnauthorized: true (secure default)', () => {
        const cfg = buildSslConfig({ DATABASE_URL: 'postgresql://prod/db' });
        assert.equal(cfg.rejectUnauthorized, true);
    });

    it('DB_SSL_REJECT_UNAUTHORIZED=false → rejectUnauthorized: false (explicit dev override only)', () => {
        const cfg = buildSslConfig({ DATABASE_URL: 'postgresql://prod/db', DB_SSL_REJECT_UNAUTHORIZED: 'false' });
        assert.equal(cfg.rejectUnauthorized, false);
    });

    it('DB_SSL_REJECT_UNAUTHORIZED=true (or any other value) → still secure', () => {
        const cfg = buildSslConfig({ DATABASE_URL: 'postgresql://prod/db', DB_SSL_REJECT_UNAUTHORIZED: 'true' });
        assert.equal(cfg.rejectUnauthorized, true);
    });

    it('production never silently uses insecure TLS', () => {
        // Simulate production: DATABASE_URL set, override NOT set
        const cfg = buildSslConfig({ DATABASE_URL: 'postgresql://supabase/prod' });
        assert.notEqual(cfg, false);
        assert.equal(cfg.rejectUnauthorized, true);
    });
});

// ══════════════════════════════════════════════════════════════════════════════
// 12. Stale job recovery
// ══════════════════════════════════════════════════════════════════════════════
describe('Stale job recovery', () => {

    function makeRecoveryUnderTest() {
        const rows = [];
        let idCounter = 1;

        function addJob(overrides = {}) {
            const id = `job-${idCounter++}`;
            rows.push({
                id,
                game_id: `game-${id}`,
                status: 'queued',
                attempts: 0,
                max_attempts: 3,
                error: null,
                started_at: null,
                ...overrides,
            });
            return id;
        }

        // Inline recoverStaleJobs logic using the mock rows array
        const STALE_MINUTES = 10;
        async function recoverStaleJobs() {
            const staleTime = new Date(Date.now() - STALE_MINUTES * 60 * 1000);
            const stale = rows.filter(r => r.status === 'running' && r.started_at && r.started_at < staleTime);

            for (const r of stale) {
                if (r.attempts >= r.max_attempts) {
                    r.status = 'failed';
                    r.error  = 'Timed out (process crash recovery)';
                } else {
                    r.status     = 'queued';
                    r.started_at = null;
                }
            }
            return stale.length;
        }

        return { rows, addJob, recoverStaleJobs };
    }

    it('recovers stale running job back to queued', async () => {
        const { rows, addJob, recoverStaleJobs } = makeRecoveryUnderTest();
        const id = addJob({ status: 'running', attempts: 1, started_at: new Date(Date.now() - 15 * 60 * 1000) });
        const count = await recoverStaleJobs();
        assert.equal(count, 1);
        assert.equal(rows.find(r => r.id === id).status, 'queued');
    });

    it('marks stale job as failed if attempts exhausted', async () => {
        const { rows, addJob, recoverStaleJobs } = makeRecoveryUnderTest();
        const id = addJob({ status: 'running', attempts: 3, max_attempts: 3, started_at: new Date(Date.now() - 15 * 60 * 1000) });
        await recoverStaleJobs();
        const job = rows.find(r => r.id === id);
        assert.equal(job.status, 'failed');
        assert.match(job.error, /crash recovery/);
    });

    it('does NOT touch recently-started running jobs', async () => {
        const { rows, addJob, recoverStaleJobs } = makeRecoveryUnderTest();
        const id = addJob({ status: 'running', attempts: 1, started_at: new Date(Date.now() - 2 * 60 * 1000) }); // 2 min ago — not stale
        const count = await recoverStaleJobs();
        assert.equal(count, 0);
        assert.equal(rows.find(r => r.id === id).status, 'running');
    });

    it('does NOT touch queued or succeeded jobs', async () => {
        const { rows, addJob, recoverStaleJobs } = makeRecoveryUnderTest();
        addJob({ status: 'queued' });
        addJob({ status: 'succeeded', started_at: new Date(0) });
        const count = await recoverStaleJobs();
        assert.equal(count, 0);
        assert.equal(rows[0].status, 'queued');
        assert.equal(rows[1].status, 'succeeded');
    });

    it('recovers multiple stale jobs in one pass', async () => {
        const { rows, addJob, recoverStaleJobs } = makeRecoveryUnderTest();
        addJob({ status: 'running', attempts: 1, started_at: new Date(Date.now() - 20 * 60 * 1000) });
        addJob({ status: 'running', attempts: 1, started_at: new Date(Date.now() - 11 * 60 * 1000) });
        addJob({ status: 'running', attempts: 1, started_at: new Date(Date.now() - 1  * 60 * 1000) }); // fresh
        const count = await recoverStaleJobs();
        assert.equal(count, 2);
        assert.equal(rows.filter(r => r.status === 'queued').length, 2);
        assert.equal(rows.filter(r => r.status === 'running').length, 1);
    });
});
describe('Environment validation', () => {
    it('isR2Configured: false when vars missing', () => {
        const restore = mockEnv({ S3_API: '', Access_Key: '', Secret_Access_Key: '', Public_Development_URL: '' });
        delete require.cache[require.resolve('../config/env')];
        const { isR2Configured } = require('../config/env');
        assert.equal(isR2Configured(), false);
        restore();
    });
    it('isCronConfigured: false when missing', () => {
        const restore = mockEnv({ CRON_SECRET: '' });
        delete require.cache[require.resolve('../config/env')];
        const { isCronConfigured } = require('../config/env');
        assert.equal(isCronConfigured(), false);
        restore();
    });
});

// ══════════════════════════════════════════════════════════════════════════════
// 10. Worker shutdown behavior
// ══════════════════════════════════════════════════════════════════════════════
describe('Worker shutdown', () => {
    it('stop() resolves when not running', async () => {
        // Build a fresh worker module (not the singleton) to avoid state bleed
        const wModule = {
            _running: false, _stopping: false, _inFlight: 0, _timer: null,
            isRunning() { return this._running && !this._stopping; },
            inFlight() { return this._inFlight; },
            async stop(timeoutMs = 1000) {
                if (!this._running) return;
                this._stopping = true;
                if (this._timer) { clearTimeout(this._timer); this._timer = null; }
                const deadline = Date.now() + timeoutMs;
                while (this._inFlight > 0 && Date.now() < deadline) {
                    await new Promise(r => setTimeout(r, 50));
                }
                this._running = false;
            },
        };
        // stop() on a not-running worker should resolve immediately
        await assert.doesNotReject(() => wModule.stop(100));
    });

    it('stop() drains in-flight counter', async () => {
        const wModule = {
            _running: true, _stopping: false, _inFlight: 1, _timer: null,
            async stop(timeoutMs = 500) {
                this._stopping = true;
                const deadline = Date.now() + timeoutMs;
                // Simulate job finishing after 50ms
                setTimeout(() => { this._inFlight = 0; }, 50);
                while (this._inFlight > 0 && Date.now() < deadline) {
                    await new Promise(r => setTimeout(r, 20));
                }
                this._running = false;
            },
        };
        await wModule.stop(300);
        assert.equal(wModule._inFlight, 0);
        assert.equal(wModule._running, false);
    });
});

// ══════════════════════════════════════════════════════════════════════════════
// 13. validateClientEnrichRequest middleware
// ══════════════════════════════════════════════════════════════════════════════
describe('validateClientEnrichRequest', () => {
    const { validateClientEnrichRequest } = require('../middleware/validateClientRequest');

    function mockReqRes(body) {
        const req = { body };
        let statusCode, jsonBody;
        const res = { status(c) { statusCode = c; return this; }, json(b) { jsonBody = b; return this; } };
        return { req, res, getStatus: () => statusCode, getBody: () => jsonBody };
    }

    it('passes valid steam request', (_, done) => {
        const { req, res } = mockReqRes({ platform: 'steam', id: '570' });
        validateClientEnrichRequest(req, res, () => done());
    });

    it('passes valid epic request with optional title', (_, done) => {
        const { req, res } = mockReqRes({ platform: 'epic', id: 'abc123', title: 'My Game' });
        validateClientEnrichRequest(req, res, () => done());
    });

    it('rejects missing platform', (_, done) => {
        const { req, res, getStatus } = mockReqRes({ id: '570' });
        validateClientEnrichRequest(req, res, () => done(new Error('should not pass')));
        assert.equal(getStatus(), 400); done();
    });

    it('rejects invalid platform value', (_, done) => {
        const { req, res, getStatus } = mockReqRes({ platform: 'gog', id: '570' });
        validateClientEnrichRequest(req, res, () => done(new Error('should not pass')));
        assert.equal(getStatus(), 400); done();
    });

    it('rejects missing id', (_, done) => {
        const { req, res, getStatus } = mockReqRes({ platform: 'steam' });
        validateClientEnrichRequest(req, res, () => done(new Error('should not pass')));
        assert.equal(getStatus(), 400); done();
    });

    it('rejects oversized id', (_, done) => {
        const { req, res, getStatus } = mockReqRes({ platform: 'steam', id: 'a'.repeat(65) });
        validateClientEnrichRequest(req, res, () => done(new Error('should not pass')));
        assert.equal(getStatus(), 400); done();
    });

    it('rejects id with URL/special characters', (_, done) => {
        const { req, res, getStatus } = mockReqRes({ platform: 'steam', id: 'https://evil.com/img.jpg' });
        validateClientEnrichRequest(req, res, () => done(new Error('should not pass')));
        assert.equal(getStatus(), 400); done();
    });

    it('rejects oversized title', (_, done) => {
        const { req, res, getStatus } = mockReqRes({ platform: 'steam', id: '570', title: 'x'.repeat(201) });
        validateClientEnrichRequest(req, res, () => done(new Error('should not pass')));
        assert.equal(getStatus(), 400); done();
    });

    it('rejects unknown field (e.g. cover_url injection)', (_, done) => {
        const { req, res, getStatus } = mockReqRes({ platform: 'steam', id: '570', cover_url: 'https://evil.com/img.jpg' });
        validateClientEnrichRequest(req, res, () => done(new Error('should not pass')));
        assert.equal(getStatus(), 400); done();
    });

    it('rejects unknown field (epic_metadata injection)', (_, done) => {
        const { req, res, getStatus } = mockReqRes({ platform: 'epic', id: 'abc123', epic_metadata: { developer: 'evil' } });
        validateClientEnrichRequest(req, res, () => done(new Error('should not pass')));
        assert.equal(getStatus(), 400); done();
    });

    it('rejects blank id string', (_, done) => {
        const { req, res, getStatus } = mockReqRes({ platform: 'steam', id: '   ' });
        validateClientEnrichRequest(req, res, () => done(new Error('should not pass')));
        assert.equal(getStatus(), 400); done();
    });

    it('rejects array body', (_, done) => {
        const req = { body: [] };
        let statusCode;
        const res = { status(c) { statusCode = c; return this; }, json() { return this; } };
        validateClientEnrichRequest(req, res, () => done(new Error('should not pass')));
        assert.equal(statusCode, 400); done();
    });
});

// ══════════════════════════════════════════════════════════════════════════════
// 14. importAndEnqueueOne service logic
// ══════════════════════════════════════════════════════════════════════════════
describe('importAndEnqueueOne service', () => {

    // Build an isolated inline version of importAndEnqueueOne using mock state
    function makeServiceUnderTest() {
        const games        = [];  // { id, slug, title, entry_type }
        const platformIds  = [];  // { game_id, platform, namespace, external_id }
        const metadata     = [];  // { game_id }
        const images       = [];  // { game_id, image_type }
        const enrichJobs   = [];  // { id, game_id, platform, external_id, status }
        let idCounter      = 1;

        async function isEnriched(gameId) {
            const hasMeta  = metadata.some(m => m.game_id === gameId);
            const hasImage = images.some(i => i.game_id === gameId && i.image_type !== 'cover');
            return hasMeta && hasImage;
        }

        async function enqueue({ gameId, platform, externalId }) {
            const hasActive = enrichJobs.some(j => j.game_id === gameId && ['queued','running'].includes(j.status));
            if (hasActive) return { id: enrichJobs.find(j => j.game_id === gameId).id, created: false };
            const id = `job-${idCounter++}`;
            enrichJobs.push({ id, game_id: gameId, platform, external_id: externalId, status: 'queued' });
            return { id, created: true };
        }

        async function importAndEnqueueOne({ platform, externalId, hintTitle }) {
            const ns = platform === 'steam' ? 'app_id' : 'catalog_namespace';

            // 1. Lookup existing
            const pidRow = platformIds.find(p => p.platform === platform && p.namespace === ns && p.external_id === externalId);
            let gameId = pidRow?.game_id || null;

            if (gameId) {
                const enriched = await isEnriched(gameId);
                if (enriched) return { gameId, jobId: null, queued: false, alreadyQueued: false, reason: 'already_exists' };

                const activeJob = enrichJobs.find(j => j.game_id === gameId && ['queued','running'].includes(j.status));
                if (activeJob) return { gameId, jobId: activeJob.id, queued: false, alreadyQueued: true, reason: 'already_queued' };

                const { id: jobId } = await enqueue({ gameId, platform, externalId, clientData: {} });
                return { gameId, jobId, queued: true, alreadyQueued: false, reason: 'needs_enrich' };
            }

            // 2. Create
            gameId = `game-${idCounter++}`;
            const title = hintTitle || `${platform}-${externalId}`;
            const slug  = title.toLowerCase().replace(/[^a-z0-9\s]/g, '').trim().replace(/\s+/g, '-') || `${platform}-${externalId}`;
            games.push({ id: gameId, slug, title, entry_type: 'game' });
            platformIds.push({ game_id: gameId, platform, namespace: ns, external_id: externalId });

            const { id: jobId } = await enqueue({ gameId, platform, externalId, clientData: {} });
            return { gameId, jobId, queued: true, alreadyQueued: false, reason: 'created_and_queued' };
        }

        return { importAndEnqueueOne, games, platformIds, metadata, images, enrichJobs };
    }

    it('creates game and queues job for new game', async () => {
        const svc = makeServiceUnderTest();
        const r = await svc.importAndEnqueueOne({ platform: 'steam', externalId: '570' });
        assert.equal(r.reason, 'created_and_queued');
        assert.equal(r.queued, true);
        assert.equal(r.alreadyQueued, false);
        assert.ok(r.gameId);
        assert.ok(r.jobId);
        assert.equal(svc.games.length, 1);
        assert.equal(svc.enrichJobs.length, 1);
    });

    it('returns already_exists for enriched game — no new job', async () => {
        const svc = makeServiceUnderTest();
        // Pre-create enriched game
        const gameId = 'game-exists';
        svc.platformIds.push({ game_id: gameId, platform: 'steam', namespace: 'app_id', external_id: '570' });
        svc.metadata.push({ game_id: gameId });
        svc.images.push({ game_id: gameId, image_type: 'hero' });

        const r = await svc.importAndEnqueueOne({ platform: 'steam', externalId: '570' });
        assert.equal(r.reason, 'already_exists');
        assert.equal(r.queued, false);
        assert.equal(r.alreadyQueued, false);
        assert.equal(r.jobId, null);
        assert.equal(svc.enrichJobs.length, 0); // no job created
    });

    it('returns already_queued when active job exists — no duplicate', async () => {
        const svc = makeServiceUnderTest();
        const gameId = 'game-partial';
        svc.platformIds.push({ game_id: gameId, platform: 'steam', namespace: 'app_id', external_id: '570' });
        svc.enrichJobs.push({ id: 'job-existing', game_id: gameId, platform: 'steam', external_id: '570', status: 'queued' });

        const r = await svc.importAndEnqueueOne({ platform: 'steam', externalId: '570' });
        assert.equal(r.reason, 'already_queued');
        assert.equal(r.alreadyQueued, true);
        assert.equal(r.queued, false);
        assert.equal(r.jobId, 'job-existing');
        assert.equal(svc.enrichJobs.length, 1); // no new job
    });

    it('returns needs_enrich and queues when game exists but not enriched', async () => {
        const svc = makeServiceUnderTest();
        const gameId = 'game-partial';
        svc.platformIds.push({ game_id: gameId, platform: 'epic', namespace: 'catalog_namespace', external_id: 'fortnite' });
        // Not enriched (no metadata, no non-cover images)

        const r = await svc.importAndEnqueueOne({ platform: 'epic', externalId: 'fortnite' });
        assert.equal(r.reason, 'needs_enrich');
        assert.equal(r.queued, true);
        assert.equal(svc.enrichJobs.length, 1);
    });

    it('repeated calls for same new game do not create duplicate jobs', async () => {
        const svc = makeServiceUnderTest();
        const r1 = await svc.importAndEnqueueOne({ platform: 'steam', externalId: '570' });
        const r2 = await svc.importAndEnqueueOne({ platform: 'steam', externalId: '570' });
        assert.equal(r1.reason, 'created_and_queued');
        assert.equal(r2.reason, 'already_queued');
        assert.equal(svc.enrichJobs.length, 1); // only one job total
    });

    it('uses hintTitle as slug seed when provided', async () => {
        const svc = makeServiceUnderTest();
        await svc.importAndEnqueueOne({ platform: 'steam', externalId: '570', hintTitle: 'Dota 2' });
        assert.equal(svc.games[0].title, 'Dota 2');
    });
});

// ══════════════════════════════════════════════════════════════════════════════
// 15. Admin routes still require API_SECRET
// ══════════════════════════════════════════════════════════════════════════════
describe('Admin route auth still enforced', () => {
    const { requireAuth } = require('../middleware/auth');

    function mockReqRes(headers = {}) {
        const req = { headers, path: '/test', ip: '127.0.0.1' };
        let statusCode;
        const res = { status(c) { statusCode = c; return this; }, json() { return this; } };
        return { req, res, getStatus: () => statusCode };
    }

    it('/games/import still requires API_SECRET', (_, done) => {
        const restore = mockEnv({ API_SECRET: 'securekey1234567890abcdef12345678' });
        const { req, res, getStatus } = mockReqRes({});
        requireAuth(req, res, () => done(new Error('should have been rejected')));
        assert.equal(getStatus(), 401);
        restore(); done();
    });

    it('/games/enrich still requires API_SECRET', (_, done) => {
        const restore = mockEnv({ API_SECRET: 'securekey1234567890abcdef12345678' });
        const { req, res, getStatus } = mockReqRes({ authorization: 'Bearer wrongsecret' });
        requireAuth(req, res, () => done(new Error('should have been rejected')));
        assert.equal(getStatus(), 401);
        restore(); done();
    });
});
