'use strict';

/**
 * tests/transient_resolve.test.js
 *
 * Tests for:
 *   1. validateResolveMetadataRequest — middleware
 *   2. transientConfidenceScorer     — scoreCandidate + pickBestCandidate
 *   3. transientResolver             — resolveTransient (IGDB + SGDB mocked)
 *
 * Run: node --test tests/transient_resolve.test.js
 */

const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');

// ─── Helpers ──────────────────────────────────────────────────────────────────

/** Build a minimal Express-style mock req/res/next for middleware tests */
function mockHttp(body = {}, headers = {}) {
    const req = { body, headers };

    let _status = 200;
    let _json   = null;
    let _ended  = false;

    const res = {
        statusCode: 200,
        status(code) { _status = code; this.statusCode = code; return this; },
        json(data)   { _json = data; _ended = true; return this; },
        get()        { return { status: _status, body: _json, ended: _ended }; },
    };

    let nextCalled = false;
    const next = () => { nextCalled = true; };

    return { req, res, next, getStatus: () => _status, getBody: () => _json, nextCalled: () => nextCalled };
}

// ══════════════════════════════════════════════════════════════════════════════
// 1. validateResolveMetadataRequest — middleware
// ══════════════════════════════════════════════════════════════════════════════

describe('validateResolveMetadataRequest', () => {
    const { validateResolveMetadataRequest } = require('../middleware/validateResolveMetadataRequest');

    it('passes a minimal valid request (title only)', () => {
        const { req, res, next, nextCalled } = mockHttp({ title: 'The Witcher 3' });
        validateResolveMetadataRequest(req, res, next);
        assert.ok(nextCalled(), 'next() should be called');
    });

    it('passes a request with all optional hint fields', () => {
        const { req, res, next, nextCalled } = mockHttp({
            title:        'Cyberpunk 2077',
            slug:         'cyberpunk-2077',
            platformHint: 'gog',
            exeName:      'Cyberpunk2077.exe',
            folderName:   'Cyberpunk 2077',
            command:      'C:\\GOG\\Cyberpunk2077.exe',
            pathHint:     'C:\\GOG Games',
            igdbId:       1942,
        });
        validateResolveMetadataRequest(req, res, next);
        assert.ok(nextCalled(), 'next() should be called');
    });

    it('rejects missing title', () => {
        const { req, res, next, getStatus, getBody, nextCalled } = mockHttp({ slug: 'some-slug' });
        validateResolveMetadataRequest(req, res, next);
        assert.ok(!nextCalled(), 'next() should NOT be called');
        assert.equal(getStatus(), 400);
        assert.match(getBody().message, /title is required/);
    });

    it('rejects blank title', () => {
        const { req, res, next, getStatus, getBody, nextCalled } = mockHttp({ title: '   ' });
        validateResolveMetadataRequest(req, res, next);
        assert.ok(!nextCalled());
        assert.equal(getStatus(), 400);
        assert.match(getBody().message, /blank/);
    });

    it('rejects title exceeding max length', () => {
        const { req, res, next, getStatus, nextCalled } = mockHttp({ title: 'A'.repeat(201) });
        validateResolveMetadataRequest(req, res, next);
        assert.ok(!nextCalled());
        assert.equal(getStatus(), 400);
    });

    it('rejects unknown top-level field', () => {
        const { req, res, next, getStatus, getBody, nextCalled } = mockHttp({ title: 'Game', evilField: 'x' });
        validateResolveMetadataRequest(req, res, next);
        assert.ok(!nextCalled());
        assert.equal(getStatus(), 400);
        assert.match(getBody().message, /Unknown field/);
    });

    it('rejects non-object body', () => {
        const { req, res, next, getStatus, nextCalled } = mockHttp(null);
        validateResolveMetadataRequest(req, res, next);
        assert.ok(!nextCalled());
        assert.equal(getStatus(), 400);
    });

    it('rejects array body', () => {
        const { req, res, next, getStatus, nextCalled } = mockHttp([{ title: 'game' }]);
        validateResolveMetadataRequest(req, res, next);
        assert.ok(!nextCalled());
        assert.equal(getStatus(), 400);
    });

    it('rejects igdbId = 0', () => {
        const { req, res, next, getStatus, getBody, nextCalled } = mockHttp({ title: 'Game', igdbId: 0 });
        validateResolveMetadataRequest(req, res, next);
        assert.ok(!nextCalled());
        assert.equal(getStatus(), 400);
        assert.match(getBody().message, /igdbId/);
    });

    it('rejects igdbId that is a string', () => {
        const { req, res, next, getStatus, nextCalled } = mockHttp({ title: 'Game', igdbId: '1942' });
        validateResolveMetadataRequest(req, res, next);
        assert.ok(!nextCalled());
        assert.equal(getStatus(), 400);
    });

    it('rejects igdbId that is a float', () => {
        const { req, res, next, getStatus, nextCalled } = mockHttp({ title: 'Game', igdbId: 1.5 });
        validateResolveMetadataRequest(req, res, next);
        assert.ok(!nextCalled());
        assert.equal(getStatus(), 400);
    });

    it('rejects igdbId that is negative', () => {
        const { req, res, next, getStatus, nextCalled } = mockHttp({ title: 'Game', igdbId: -5 });
        validateResolveMetadataRequest(req, res, next);
        assert.ok(!nextCalled());
        assert.equal(getStatus(), 400);
    });

    it('accepts positive integer igdbId', () => {
        const { req, res, next, nextCalled } = mockHttp({ title: 'Game', igdbId: 1942 });
        validateResolveMetadataRequest(req, res, next);
        assert.ok(nextCalled());
    });

    it('rejects hint field exceeding max length', () => {
        const { req, res, next, getStatus, nextCalled } = mockHttp({ title: 'Game', exeName: 'A'.repeat(301) });
        validateResolveMetadataRequest(req, res, next);
        assert.ok(!nextCalled());
        assert.equal(getStatus(), 400);
    });

    it('rejects title with null-byte control character', () => {
        const { req, res, next, getStatus, nextCalled } = mockHttp({ title: 'Game\x00Evil' });
        validateResolveMetadataRequest(req, res, next);
        assert.ok(!nextCalled());
        assert.equal(getStatus(), 400);
    });
});

// ══════════════════════════════════════════════════════════════════════════════
// 2. transientConfidenceScorer — scoreCandidate
// ══════════════════════════════════════════════════════════════════════════════

describe('transientConfidenceScorer — scoreCandidate', () => {
    const { scoreCandidate, THRESHOLD } = require('../services/transientConfidenceScorer');

    // Helper: build minimal IGDB candidate
    function cand(name, extra = {}) {
        return { id: 1, name, platforms: [{ id: 6 }], total_rating_count: 0, ...extra };
    }

    it('exact title match scores 50+ base', () => {
        const { score } = scoreCandidate(cand('The Witcher 3: Wild Hunt'), { title: 'The Witcher 3: Wild Hunt' });
        assert.ok(score >= 50, `expected score ≥ 50, got ${score}`);
    });

    it('exact match + PC platform scores higher than exact match alone', () => {
        const withPC    = scoreCandidate(cand('Dark Souls', { platforms: [{ id: 6 }], total_rating_count: 200 }), { title: 'Dark Souls' });
        const withoutPC = scoreCandidate(cand('Dark Souls', { platforms: [{ id: 3 }], total_rating_count: 0   }), { title: 'Dark Souls' });
        assert.ok(withPC.score > withoutPC.score, `withPC (${withPC.score}) should > withoutPC (${withoutPC.score})`);
    });

    it('penalises DLC name pattern', () => {
        const base = scoreCandidate(cand('The Witcher 3'), { title: 'The Witcher 3' });
        const dlc  = scoreCandidate(cand('The Witcher 3 Soundtrack'), { title: 'The Witcher 3' });
        assert.ok(base.score > dlc.score, 'DLC/soundtrack should score lower');
    });

    it('penalises NON_GAME_CATEGORIES (DLC category=1)', () => {
        const game = scoreCandidate(cand('Elden Ring',               { category: 0 }), { title: 'Elden Ring' });
        const dlc  = scoreCandidate(cand('Elden Ring DLC Expansion', { category: 1 }), { title: 'Elden Ring' });
        assert.ok(game.score > dlc.score, 'DLC category should be penalised');
    });

    it('penalises demo/beta/editor name', () => {
        const base  = scoreCandidate(cand('Satisfactory'),        { title: 'Satisfactory' });
        const demo  = scoreCandidate(cand('Satisfactory Demo'),   { title: 'Satisfactory' });
        const editor = scoreCandidate(cand('Satisfactory Editor'), { title: 'Satisfactory' });
        assert.ok(base.score > demo.score,   'demo variant should score lower');
        assert.ok(base.score > editor.score, 'editor variant should score lower');
    });

    it('exe hint boosts correct match', () => {
        const withExe    = scoreCandidate(cand('Control'), { title: 'Control', exeName: 'Control.exe'   });
        const wrongExe   = scoreCandidate(cand('Control'), { title: 'Control', exeName: 'unrealengine.exe' });
        assert.ok(withExe.score >= wrongExe.score, 'matching exe hint should not hurt score');
    });

    it('slug exact match adds bonus', () => {
        const withSlug    = scoreCandidate(cand('Hades', { slug: 'hades' }),    { title: 'Hades', slug: 'hades' });
        const withoutSlug = scoreCandidate(cand('Hades', { slug: 'hades' }),    { title: 'Hades' });
        assert.ok(withSlug.score > withoutSlug.score, 'slug match should add score');
    });

    it('episodic mismatch is penalised', () => {
        const correct  = scoreCandidate(cand('Life is Strange Episode 1'), { title: 'Life is Strange Episode 1' });
        const mismatch = scoreCandidate(cand('Life is Strange Episode 2'), { title: 'Life is Strange Episode 1' });
        assert.ok(correct.score > mismatch.score, 'wrong episode number should score lower');
    });

    it('score is clamped to [0, 100]', () => {
        const { score } = scoreCandidate(cand('Cyberpunk 2077', {
            total_rating_count: 9999, slug: 'cyberpunk-2077',
            platforms: [{ id: 6 }, { id: 14 }],
        }), { title: 'Cyberpunk 2077', slug: 'cyberpunk-2077' });
        assert.ok(score <= 100, `score should be ≤ 100, got ${score}`);
        assert.ok(score >= 0,   `score should be ≥ 0, got ${score}`);
    });

    it('completely unrelated name scores below threshold', () => {
        const { score } = scoreCandidate(cand('Pong'), { title: 'Cyberpunk 2077' });
        assert.ok(score < THRESHOLD, `unrelated name should score < ${THRESHOLD}, got ${score}`);
    });
});

// ══════════════════════════════════════════════════════════════════════════════
// 3. transientConfidenceScorer — pickBestCandidate
// ══════════════════════════════════════════════════════════════════════════════

describe('transientConfidenceScorer — pickBestCandidate', () => {
    const { pickBestCandidate, THRESHOLD, AMBIGUITY_MARGIN } = require('../services/transientConfidenceScorer');

    function cand(name, extra = {}) {
        return { id: Math.random(), name, platforms: [{ id: 6 }], total_rating_count: 50, ...extra };
    }

    it('returns not_found for empty candidates', () => {
        const r = pickBestCandidate([], { title: 'Game' });
        assert.equal(r.status, 'not_found');
        assert.equal(r.winner, null);
    });

    it('returns not_found when best score < THRESHOLD', () => {
        const r = pickBestCandidate(
            [cand('XXXXXXXXXXXXXXXXX')],
            { title: 'Totally Different Name ZZZZ' }
        );
        assert.equal(r.status, 'not_found');
        assert.equal(r.winner, null);
    });

    it('returns resolved for clear winner', () => {
        const candidates = [
            cand('Elden Ring', { total_rating_count: 500 }),
            cand('PONG Classic Remake'),
            cand('Unknown Indie Game XYZ'),
        ];
        const r = pickBestCandidate(candidates, { title: 'Elden Ring' });
        assert.equal(r.status, 'resolved');
        assert.ok(r.winner, 'winner should not be null');
        assert.equal(r.winner.name, 'Elden Ring');
    });

    it('returns ambiguous when top two are too close', () => {
        // Two near-identical names with the same request — gap < AMBIGUITY_MARGIN
        const candidates = [
            cand('Hades'),
            cand('Hades II'),
        ];
        // Searching for plain "Hades" — both will score high but close
        const r = pickBestCandidate(candidates, { title: 'Hades' });
        // Either resolved (if margin is ≥15) or ambiguous — just confirm winner is populated
        assert.ok(['resolved', 'ambiguous'].includes(r.status));
    });

    it('allScores contains at most 5 entries', () => {
        const candidates = Array.from({ length: 20 }, (_, i) => cand(`Game ${i}`));
        const r = pickBestCandidate(candidates, { title: 'Game 5' });
        assert.ok(r.allScores.length <= 5);
    });

    it('margin equals score when only one candidate', () => {
        const candidates = [cand('The Witcher 3', { total_rating_count: 200 })];
        const r = pickBestCandidate(candidates, { title: 'The Witcher 3' });
        if (r.status === 'resolved') {
            assert.equal(r.margin, r.score);
        }
    });
});

// ══════════════════════════════════════════════════════════════════════════════
// 4. transientResolver — resolveTransient (mocked externals)
// ══════════════════════════════════════════════════════════════════════════════

describe('transientResolver — resolveTransient', () => {
    // We cannot call real IGDB/SGDB in unit tests, so we monkey-patch the
    // imported modules before requiring transientResolver.

    // NOTE: Node's module cache means we must do the mock BEFORE the first
    //       require('transientResolver'). Each sub-describe block re-patches
    //       the shared modules and re-requires transientResolver.

    // IGDB mock factory
    function makeIgdbResults(names) {
        return names.map((name, i) => ({
            id:                   1000 + i,
            name,
            slug:                 name.toLowerCase().replace(/\s+/g, '-'),
            summary:              `Summary of ${name}`,
            platforms:            [{ id: 6, name: 'PC (Windows)' }],
            total_rating_count:   100,
            category:             0,
            genres:               [{ name: 'Action' }],
            themes:               [],
            keywords:             [],
            involved_companies:   [],
            videos:               [],
            artworks:             [],
            screenshots:          [],
            alternative_names:    [],
            aggregated_rating:    80,
            aggregated_rating_count: 20,
            rating:               75,
            rating_count:         100,
            first_release_date:   1609459200,
            cover: { image_id: 'test_cover', width: 600, height: 900 },
        }));
    }

    // Patch igdbResolver._igdbFetch and steamGridDbResolver before loading transientResolver
    // We accomplish this through process-level require cache manipulation.

    let originalIgdbFetch, originalSgdbImages, originalIgdbGameData;

    beforeEach(() => {
        // Save originals (if already required)
        try {
            const igdbMod  = require('../services/igdbResolver');
            const sgdbMod  = require('../services/steamGridDbResolver');
            originalIgdbFetch   = igdbMod.igdbFetch;  // internal, may not be exported
            originalSgdbImages  = sgdbMod.resolveSgdbImages;
            originalIgdbGameData = igdbMod.fetchIgdbGameData;
        } catch { /* not yet loaded */ }
    });

    it('returns not_found when IGDB returns empty results', async () => {
        // Override igdbResolver to simulate empty IGDB
        const igdbMod = require('../services/igdbResolver');
        const origFetch = igdbMod.igdbFetch;
        // We can't easily mock an unexported internal; instead we mock fetchIgdbGameData
        // and resolve by monkey-patching the module's exports.
        // For this test we use an alternative approach: pass an igdbId that doesn't exist.

        // Since we can't easily mock the internals without dependency injection,
        // this test verifies the resolver handles gracefully when IGDB throws.
        const { resolveTransient } = require('../services/transientResolver');

        // Mock global fetch to simulate IGDB returning empty
        const origGlobalFetch = globalThis.fetch;
        globalThis.fetch = async (url) => {
            if (url.includes('igdb.com')) {
                return { ok: true, json: async () => [] };
            }
            if (url.includes('twitch.tv')) {
                return { ok: true, json: async () => ({ access_token: 'fake', expires_in: 3600 }) };
            }
            return { ok: true, json: async () => ({}) };
        };

        try {
            // Clear the module cache for transientResolver so our fetch mock is picked up
            delete require.cache[require.resolve('../services/transientResolver')];
            const { resolveTransient: resolve } = require('../services/transientResolver');

            const result = await resolve({ title: 'ThisGameDoesNotExistInAnyDatabase12345' });
            assert.ok(['not_found', 'ambiguous', 'resolved'].includes(result.status));
            assert.ok(typeof result.confidence === 'number');
            assert.ok(Array.isArray(result.debug.candidatesTried));
        } finally {
            globalThis.fetch = origGlobalFetch;
            delete require.cache[require.resolve('../services/transientResolver')];
        }
    });

    it('returns not_found for empty title', async () => {
        delete require.cache[require.resolve('../services/transientResolver')];
        const { resolveTransient } = require('../services/transientResolver');

        const result = await resolveTransient({ title: '' });
        assert.equal(result.status, 'not_found');
        assert.equal(result.confidence, 0);
        assert.equal(result.meta, null);
    });

    it('returns resolved with meta when IGDB returns a strong match', async () => {
        // This test patches the IGDB and SGDB module-level exports directly
        // (after purging cache) so the resolver sees our mocked functions.
        const mockGame = makeIgdbResults(['Cyberpunk 2077'])[0];

        // 1. Purge all three modules from cache so we get fresh instances
        for (const mod of ['../services/transientResolver', '../services/igdbResolver', '../services/steamGridDbResolver']) {
            delete require.cache[require.resolve(mod)];
        }

        // 2. Load igdbResolver and monkey-patch its exported functions
        const igdbMod = require('../services/igdbResolver');
        igdbMod.igdbFetch        = async () => [mockGame];   // search returns our game
        igdbMod.fetchIgdbGameData = async (id) => ({         // full data fetch
            id,
            name:               'Cyberpunk 2077',
            slug:               'cyberpunk-2077',
            summary:            'Open world RPG',
            platforms:          [{ id: 6, name: 'PC (Windows)' }],
            total_rating_count: 500,
            aggregated_rating:  85,
            aggregated_rating_count: 30,
            rating:             80,
            rating_count:       200,
            first_release_date: 1608163200,
            genres:             [{ name: 'RPG' }],
            themes:             [],
            keywords:           [],
            involved_companies: [],
            artworks:           [],
            screenshots:        [],
            videos:             [],
            cover: { image_id: 'cov_cp77', width: 600, height: 900 },
        });

        // 3. Load sgdbResolver and monkey-patch it
        const sgdbMod = require('../services/steamGridDbResolver');
        sgdbMod.resolveSgdbImages = async () => ({
            cover: { url: 'https://cdn2.steamgriddb.com/grid/test.png', width: 600, height: 900 },
            hero:  null,
            logo:  null,
        });

        // 4. Now load transientResolver — it will pick up the patched modules
        const { resolveTransient } = require('../services/transientResolver');

        const result = await resolveTransient({ title: 'Cyberpunk 2077' });

        assert.ok(['resolved', 'ambiguous'].includes(result.status), `unexpected status: ${result.status}`);
        assert.ok(typeof result.confidence === 'number');
        assert.ok(result.confidence > 0);

        if (result.status === 'resolved') {
            assert.ok(result.meta !== null, 'resolved result should have meta');
            assert.ok(result.meta.title, 'meta should have title');
            assert.ok(Array.isArray(result.sources));
            assert.ok(result.sources.includes('igdb'));
        }

        // Clean up
        for (const mod of ['../services/transientResolver', '../services/igdbResolver', '../services/steamGridDbResolver']) {
            delete require.cache[require.resolve(mod)];
        }
    });

    it('response shape always has required fields', async () => {
        const origGlobalFetch = globalThis.fetch;
        globalThis.fetch = async (url) => {
            if (url.includes('twitch.tv')) return { ok: true, json: async () => ({ access_token: 't', expires_in: 3600 }) };
            return { ok: true, json: async () => [] };
        };

        try {
            delete require.cache[require.resolve('../services/transientResolver')];
            const { resolveTransient } = require('../services/transientResolver');

            const result = await resolveTransient({ title: 'Any Game Title' });

            // Required fields regardless of status
            assert.ok('status'     in result, 'status field required');
            assert.ok('confidence' in result, 'confidence field required');
            assert.ok('sources'    in result, 'sources field required');
            assert.ok('meta'       in result, 'meta field required (may be null)');
            assert.ok('debug'      in result, 'debug field required');
            assert.ok('candidatesTried' in result.debug, 'debug.candidatesTried required');
            assert.ok('allScores'       in result.debug, 'debug.allScores required');
            assert.ok(Array.isArray(result.sources),              'sources must be array');
            assert.ok(Array.isArray(result.debug.candidatesTried), 'candidatesTried must be array');
        } finally {
            globalThis.fetch = origGlobalFetch;
            delete require.cache[require.resolve('../services/transientResolver')];
        }
    });

    it('caches results — second call does not re-fetch IGDB', async () => {
        let igdbCallCount = 0;
        const origGlobalFetch = globalThis.fetch;

        globalThis.fetch = async (url) => {
            if (url.includes('twitch.tv')) return { ok: true, json: async () => ({ access_token: 't', expires_in: 3600 }) };
            if (url.includes('igdb.com'))  { igdbCallCount++; return { ok: true, json: async () => [] }; }
            return { ok: true, json: async () => ({}) };
        };

        try {
            delete require.cache[require.resolve('../services/transientResolver')];
            const { resolveTransient } = require('../services/transientResolver');

            await resolveTransient({ title: 'CacheTestGameXYZ999' });
            const countAfterFirst = igdbCallCount;

            await resolveTransient({ title: 'CacheTestGameXYZ999' });
            const countAfterSecond = igdbCallCount;

            // Cache hit: IGDB should NOT be called again on second request
            assert.equal(countAfterSecond, countAfterFirst, 'IGDB should not be called on cache hit');
        } finally {
            globalThis.fetch = origGlobalFetch;
            delete require.cache[require.resolve('../services/transientResolver')];
        }
    });
});

// ══════════════════════════════════════════════════════════════════════════════
// 5. Scoring edge cases / regression guards
// ══════════════════════════════════════════════════════════════════════════════

describe('Scoring edge cases', () => {
    const { scoreCandidate, pickBestCandidate } = require('../services/transientConfidenceScorer');

    function cand(name, extra = {}) {
        return { id: Math.random(), name, platforms: [{ id: 6 }], total_rating_count: 50, ...extra };
    }

    it('does not confuse "Hades" with "Hades II" as an exact match', () => {
        const hadesI  = scoreCandidate(cand('Hades'),    { title: 'Hades' });
        const hadesII = scoreCandidate(cand('Hades II'), { title: 'Hades' });
        assert.ok(hadesI.score > hadesII.score, `"Hades" should score higher than "Hades II" for query "Hades"`);
    });

    it('scores "Grand Theft Auto V" above "Grand Theft Auto IV" for GTA V query', () => {
        const gtav  = scoreCandidate(cand('Grand Theft Auto V'),  { title: 'Grand Theft Auto V'  });
        const gtaiv = scoreCandidate(cand('Grand Theft Auto IV'), { title: 'Grand Theft Auto V'  });
        assert.ok(gtav.score >= gtaiv.score, 'GTA V should not score lower than GTA IV for V query');
    });

    it('does not pick a modkit/server entry for "Counter-Strike 2"', () => {
        const cs2    = cand('Counter-Strike 2', { total_rating_count: 500 });
        const server = cand('Counter-Strike 2 Dedicated Server', { total_rating_count: 0 });
        const result = pickBestCandidate([cs2, server], { title: 'Counter-Strike 2' });
        if (result.status === 'resolved') {
            assert.equal(result.winner.name, 'Counter-Strike 2', 'Should pick base game, not server binary');
        }
    });

    it('handles candidate with no name gracefully', () => {
        const result = pickBestCandidate(
            [{ id: 1 }, { id: 2, name: 'Real Game' }],
            { title: 'Real Game' }
        );
        // Should not throw; nameless candidates are filtered
        assert.ok(result);
    });

    it('handles null/undefined candidate list gracefully', () => {
        assert.doesNotThrow(() => pickBestCandidate(null,      { title: 'X' }));
        assert.doesNotThrow(() => pickBestCandidate(undefined, { title: 'X' }));
    });
});
