'use strict';

/**
 * platformSync.batch.test.js
 *
 * Focused unit tests for the batch-sync path inside _importLibraryToServer().
 *
 * Approach:
 *  - We do NOT import platformSync.js directly (it pulls in Electron, steamBridge,
 *    etc. that won't exist in a Node test runner). Instead we extract the pure
 *    logic under test by re-implementing a minimal harness that wires the same
 *    constants, helpers, and the real _importLibraryToServer body via a
 *    controlled module-under-test factory.
 *
 *  - baddelApi is fully mocked. No network calls are made.
 *
 *  - Timers (setTimeout in backoff/inter-page delays) are replaced with a
 *    synchronous stub so tests run in milliseconds.
 *
 * Run with:  node --test platformSync.batch.test.js   (Node 18+)
 *         or: jest platformSync.batch.test.js
 */

// ─── Minimal test runner shim (works with Node --test or Jest) ────────────────
// If running under Jest, `test` and `expect` are globals. If running under
// Node's built-in test runner, import them.
let _test, _assert;
try {
    // Node 18+ built-in test runner
    const nodeTest = require('node:test');
    _test   = nodeTest.test;
    _assert = require('node:assert/strict');
} catch {
    // Jest / other runner — globals already exist
    _test   = global.test;
    _assert = {
        equal:     (a, b, m) => expect(a).toBe(b),
        deepEqual: (a, b, m) => expect(a).toEqual(b),
        ok:        (v, m)    => expect(v).toBeTruthy(),
        fail:      (m)       => { throw new Error(m); },
    };
}

const t  = _test;
const assert = _assert;

// ─── Constants (must mirror platformSync.js) ──────────────────────────────────
const MAX_BATCH_PAGE         = 200;
const MAX_BATCH_RETRIES      = 4;
const MAX_ITEM_ERROR_RETRIES = 2;
const INTER_PAGE_DELAY_MS    = 400;

// ─── Helpers ─────────────────────────────────────────────────────────────────

/**
 * Build a fake game entry as platformSync would receive it from the local cache.
 */
function makeSteamGame(appid, title) {
    return { appName: String(appid), title: title || `Game ${appid}`, id: `steam_${appid}` };
}

function makeEpicGame(namespace, title) {
    return { namespace, title: title || `Game ${namespace}`, id: `epic_${namespace}` };
}

/**
 * Build the minimal deduplicated payload the way _importLibraryToServer does,
 * so tests can assert on it directly without calling the full function.
 */
function buildPayload(platform, games) {
    const seen    = new Set();
    const payload = [];
    for (const g of games) {
        let id, title;
        if (platform === 'steam') {
            id    = g.appName || (g.id ? String(g.id).replace('steam_', '') : null);
            title = g.title || null;
        } else {
            id    = g.namespace || null;
            title = g.title     || null;
        }
        if (!id) continue;
        const key = `${platform}:${id}`;
        if (seen.has(key)) continue;
        seen.add(key);
        payload.push({ id, title });
    }
    return payload;
}

/**
 * Chunk array exactly as _importLibraryToServer does.
 */
function chunk(arr, size) {
    const out = [];
    for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
    return out;
}

/**
 * Build a successful 207-like batch response body.
 * results is an array of { id, status } objects.
 */
function make207Body(results) {
    const counts = { acceptedCount: 0, queuedCount: 0, alreadyQueuedCount: 0,
                     alreadyExistsCount: 0, invalidCount: 0, errorCount: 0,
                     dedupCount: 0, maxBatchSize: 200 };
    for (const r of results) {
        if (r.status === 'created_and_queued') { counts.acceptedCount++; counts.queuedCount++; }
        else if (r.status === 'already_queued')  counts.alreadyQueuedCount++;
        else if (r.status === 'already_exists')  { counts.acceptedCount++; counts.alreadyExistsCount++; }
        else if (r.status === 'needs_enrich')    { counts.acceptedCount++; counts.alreadyExistsCount++; }
        else if (r.status === 'invalid')         counts.invalidCount++;
        else if (r.status === 'error')           counts.errorCount++;
    }
    return { ...counts, results };
}

class ApiError extends Error {
    constructor(status, retryAfter = null) {
        super(`HTTP ${status}`);
        this.status     = status;
        this.retryAfter = retryAfter;
    }
}

/**
 * Minimal re-implementation of _importLibraryToServer's core batch logic,
 * extracted so we can inject a mock baddelApi without touching the real module.
 *
 * The logic here is a faithful copy of the production function's batch section;
 * any divergence would make these tests meaningless. Keep in sync.
 */
async function runBatchSync(platform, games, { requestGameEnrichBatch, lookupGame, normalizeServerData, sleep }) {

    // ── Dedup ──────────────────────────────────────────────────
    const seen    = new Set();
    const payload = [];
    for (const g of games) {
        let id, title;
        if (platform === 'steam') {
            id    = g.appName || (g.id ? String(g.id).replace('steam_', '') : null);
            title = g.title || null;
        } else {
            id    = g.namespace || null;
            title = g.title     || null;
        }
        if (!id) continue;
        const key = `${platform}:${id}`;
        if (seen.has(key)) continue;
        seen.add(key);
        payload.push({ id, title });
    }
    if (payload.length === 0) return null;

    const titleMap = new Map(payload.map(g => [g.id, g.title ?? null]));

    const retryAfterMs = (err, attempt) => {
        if (err?.retryAfter != null) {
            const s = parseInt(err.retryAfter, 10);
            if (!isNaN(s) && s > 0 && s < 600) return s * 1000;
        }
        const base = 10_000;
        const exp  = Math.min(base * Math.pow(2, attempt - 1), 120_000);
        return Math.round(exp * (0.8 + Math.random() * 0.4));
    };

    const chunks = [];
    for (let i = 0; i < payload.length; i += MAX_BATCH_PAGE) {
        chunks.push(payload.slice(i, i + MAX_BATCH_PAGE));
    }

    const totals = {
        submitted: payload.length, localDedupDropped: games.length - payload.length,
        accepted: 0, queued: 0, alreadyQueued: 0, alreadyExists: 0,
        invalid: 0, error: 0, serverDedup: 0,
    };

    const needsLookup = new Set();
    const needsPoll   = new Set();
    const errorItems  = new Set();

    // Track which pages were attempted (for assertions)
    const pagesAttempted = [];
    const itemRetryBatches = [];

    for (let pageIdx = 0; pageIdx < chunks.length; pageIdx++) {
        const chunk_    = chunks[pageIdx];
        let   attempt   = 0;
        let   sent      = false;

        while (!sent && attempt <= MAX_BATCH_RETRIES) {
            pagesAttempted.push({ pageIdx, attempt, ids: chunk_.map(g => g.id) });
            try {
                const body = await requestGameEnrichBatch(platform, chunk_);

                totals.accepted      += body.acceptedCount      || 0;
                totals.queued        += body.queuedCount        || 0;
                totals.alreadyQueued += body.alreadyQueuedCount || 0;
                totals.alreadyExists += body.alreadyExistsCount || 0;
                totals.invalid       += body.invalidCount       || 0;
                totals.error         += body.errorCount         || 0;
                totals.serverDedup   += body.dedupCount         || 0;

                for (const r of (body.results || [])) {
                    if (!r.id) continue;
                    if (r.status === 'already_exists' || r.status === 'needs_enrich') {
                        needsLookup.add(r.id);
                    } else if (r.status === 'created_and_queued' || r.status === 'already_queued') {
                        needsPoll.add(r.id);
                    } else if (r.status === 'error') {
                        errorItems.add(r.id);
                    }
                }

                sent = true;
            } catch (err) {
                const is429 = err?.status === 429;
                attempt++;
                if (!is429) { totals.error += chunk_.length; break; }
                if (attempt > MAX_BATCH_RETRIES) { totals.error += chunk_.length; break; }
                const waitMs = retryAfterMs(err, attempt);
                await sleep(waitMs);
            }
        }

        if (pageIdx < chunks.length - 1) await sleep(INTER_PAGE_DELAY_MS);
    }

    // ── Per-item error retry ────────────────────────────────────
    if (errorItems.size > 0) {
        let remainingErrors = new Set(errorItems);
        let itemRetryPass   = 0;

        while (remainingErrors.size > 0 && itemRetryPass < MAX_ITEM_ERROR_RETRIES) {
            itemRetryPass++;
            const retryBatch = [...remainingErrors].map(id => ({
                id, title: titleMap.get(id) ?? undefined,
            }));

            let retryAttempt = 0;
            let retrySent    = false;

            while (!retrySent && retryAttempt <= MAX_BATCH_RETRIES) {
                itemRetryBatches.push({ pass: itemRetryPass, attempt: retryAttempt, ids: retryBatch.map(g => g.id) });
                try {
                    const retryBody = await requestGameEnrichBatch(platform, retryBatch);
                    const stillError = new Set();

                    for (const r of (retryBody.results || [])) {
                        if (!r.id) continue;
                        remainingErrors.delete(r.id);
                        if (r.status === 'already_exists' || r.status === 'needs_enrich') {
                            needsLookup.add(r.id);
                            totals.error    = Math.max(0, totals.error - 1);
                            totals.alreadyExists++;
                        } else if (r.status === 'created_and_queued' || r.status === 'already_queued') {
                            needsPoll.add(r.id);
                            totals.error = Math.max(0, totals.error - 1);
                            totals.queued++;
                        } else if (r.status === 'error') {
                            stillError.add(r.id);
                        }
                    }

                    remainingErrors = stillError;
                    retrySent = true;
                } catch (retryErr) {
                    const is429 = retryErr?.status === 429;
                    retryAttempt++;
                    if (!is429) break;
                    if (retryAttempt > MAX_BATCH_RETRIES) break;
                    const waitMs = retryAfterMs(retryErr, retryAttempt);
                    await sleep(waitMs);
                }
            }
        }
    }

    return { totals, needsLookup, needsPoll, errorItems, pagesAttempted, itemRetryBatches, payload };
}

// ─── Test suite ──────────────────────────────────────────────────────────────

// Synchronous sleep stub — resolves immediately so tests don't block
const noopSleep = () => Promise.resolve();

// ── 1. Local deduplication ───────────────────────────────────────────────────

t('local dedup: duplicate steam appids produce single payload entry', async () => {
    const games = [
        makeSteamGame('12345', 'Half-Life'),
        makeSteamGame('12345', 'Half-Life'),   // exact duplicate
        makeSteamGame('99999', 'Portal'),
    ];

    let callCount = 0;
    const api = {
        requestGameEnrichBatch: async (platform, batch) => {
            callCount++;
            return make207Body(batch.map(g => ({ id: g.id, status: 'created_and_queued' })));
        },
        lookupGame: async () => null,
        normalizeServerData: () => null,
    };

    const result = await runBatchSync('steam', games, { ...api, sleep: noopSleep });

    assert.equal(result.payload.length, 2, 'dedup should collapse 3 entries → 2 unique');
    assert.equal(result.totals.localDedupDropped, 1, '1 duplicate should be reported');
    assert.equal(callCount, 1, 'should produce exactly 1 batch page');
});

t('local dedup: same namespace from two different epic game objects deduplicates', async () => {
    const games = [
        makeEpicGame('ns-abc', 'Fortnite'),
        makeEpicGame('ns-abc', 'Fortnite'),
        makeEpicGame('ns-xyz', 'Rocket League'),
    ];

    let batchSizes = [];
    const api = {
        requestGameEnrichBatch: async (platform, batch) => {
            batchSizes.push(batch.length);
            return make207Body(batch.map(g => ({ id: g.id, status: 'created_and_queued' })));
        },
        lookupGame: async () => null,
        normalizeServerData: () => null,
    };

    const result = await runBatchSync('epic', games, { ...api, sleep: noopSleep });

    assert.equal(result.payload.length, 2);
    assert.deepEqual(batchSizes, [2], 'only 2 unique items should be sent');
});

// ── 2. Page chunking ─────────────────────────────────────────────────────────

t('chunking: 300 unique games produce exactly 2 batch page calls', async () => {
    const games = Array.from({ length: 300 }, (_, i) => makeSteamGame(1000 + i));

    const pageCalls = [];
    const api = {
        requestGameEnrichBatch: async (platform, batch) => {
            pageCalls.push(batch.length);
            return make207Body(batch.map(g => ({ id: g.id, status: 'created_and_queued' })));
        },
        lookupGame: async () => null,
        normalizeServerData: () => null,
    };

    const result = await runBatchSync('steam', games, { ...api, sleep: noopSleep });

    assert.equal(pageCalls.length, 2, '300 games / 200 per page = 2 pages');
    assert.equal(pageCalls[0], 200, 'first page must be exactly 200');
    assert.equal(pageCalls[1], 100, 'second page must be exactly 100');
    assert.equal(result.payload.length, 300);
});

t('chunking: 200 games produce exactly 1 batch page', async () => {
    const games = Array.from({ length: 200 }, (_, i) => makeSteamGame(2000 + i));
    let pageCount = 0;
    const api = {
        requestGameEnrichBatch: async (_, batch) => {
            pageCount++;
            return make207Body(batch.map(g => ({ id: g.id, status: 'created_and_queued' })));
        },
        lookupGame: async () => null,
        normalizeServerData: () => null,
    };

    await runBatchSync('steam', games, { ...api, sleep: noopSleep });
    assert.equal(pageCount, 1);
});

t('chunking: 201 games produce exactly 2 batch pages', async () => {
    const games = Array.from({ length: 201 }, (_, i) => makeSteamGame(3000 + i));
    let pageCount = 0;
    const api = {
        requestGameEnrichBatch: async (_, batch) => {
            pageCount++;
            return make207Body(batch.map(g => ({ id: g.id, status: 'created_and_queued' })));
        },
        lookupGame: async () => null,
        normalizeServerData: () => null,
    };

    await runBatchSync('steam', games, { ...api, sleep: noopSleep });
    assert.equal(pageCount, 2);
});

// ── 3. HTTP 429 page-level retry ─────────────────────────────────────────────

t('429 on page 1: retries with backoff until success, page 2 still runs', async () => {
    const games = Array.from({ length: 300 }, (_, i) => makeSteamGame(4000 + i));

    let page1Calls = 0;
    let page2Calls = 0;
    const sleepCalls = [];

    const api = {
        requestGameEnrichBatch: async (platform, batch) => {
            // Distinguish page by size: first page is 200, second is 100
            if (batch.length === 200) {
                page1Calls++;
                if (page1Calls === 1) {
                    // First attempt: 429 with Retry-After
                    throw new ApiError(429, '5');
                }
                // Second attempt: success
                return make207Body(batch.map(g => ({ id: g.id, status: 'created_and_queued' })));
            } else {
                page2Calls++;
                return make207Body(batch.map(g => ({ id: g.id, status: 'created_and_queued' })));
            }
        },
        lookupGame: async () => null,
        normalizeServerData: () => null,
    };

    const result = await runBatchSync('steam', games, {
        ...api,
        sleep: (ms) => { sleepCalls.push(ms); return Promise.resolve(); },
    });

    assert.equal(page1Calls, 2, 'page 1 should be attempted twice (1 fail + 1 success)');
    assert.equal(page2Calls, 1, 'page 2 should still run and succeed');
    assert.ok(sleepCalls.some(ms => ms === 5000), 'should have slept for Retry-After=5 → 5000ms');
    assert.equal(result.totals.error, 0, 'no permanent errors after successful retry');
    assert.equal(result.needsPoll.size, 300);
});

t('429 on page 1: respects Retry-After header value over backoff formula', async () => {
    const games = [makeSteamGame('55555')];
    const sleepCalls = [];
    let calls = 0;

    const api = {
        requestGameEnrichBatch: async () => {
            calls++;
            if (calls === 1) throw new ApiError(429, '42'); // Retry-After: 42 seconds
            return make207Body([{ id: '55555', status: 'created_and_queued' }]);
        },
        lookupGame: async () => null,
        normalizeServerData: () => null,
    };

    await runBatchSync('steam', [makeSteamGame('55555')], {
        ...api,
        sleep: (ms) => { sleepCalls.push(ms); return Promise.resolve(); },
    });

    // Should sleep exactly 42000ms for the 429, plus inter-page (irrelevant here)
    assert.ok(sleepCalls.includes(42000), `expected 42000ms sleep, got: ${sleepCalls}`);
});

t('429 exhausted: page gives up after MAX_BATCH_RETRIES, subsequent pages still run', async () => {
    const games = [
        ...Array.from({ length: 200 }, (_, i) => makeSteamGame(5000 + i)),
        makeSteamGame('99991'),
    ];

    let page1Calls = 0;
    let page2Calls = 0;

    const api = {
        requestGameEnrichBatch: async (platform, batch) => {
            if (batch.length === 200) {
                page1Calls++;
                throw new ApiError(429, null); // always 429
            }
            page2Calls++;
            return make207Body(batch.map(g => ({ id: g.id, status: 'created_and_queued' })));
        },
        lookupGame: async () => null,
        normalizeServerData: () => null,
    };

    const result = await runBatchSync('steam', games, { ...api, sleep: noopSleep });

    // page1 should have been tried MAX_BATCH_RETRIES+1 times (initial + retries)
    assert.equal(page1Calls, MAX_BATCH_RETRIES + 1, `page 1 should be tried ${MAX_BATCH_RETRIES + 1} times`);
    assert.equal(page2Calls, 1, 'page 2 should still run despite page 1 giving up');
    assert.ok(result.totals.error > 0, 'failed page items should be counted as errors');
});

// ── 4. HTTP 207 per-item status=error retry ──────────────────────────────────

t('item errors: status=error items are retried, success on retry moves them to needsPoll', async () => {
    const games = [
        makeSteamGame('10001', 'Alpha'),
        makeSteamGame('10002', 'Beta'),
        makeSteamGame('10003', 'Gamma'),
    ];

    let mainBatchCalled = false;
    let retryBatchCalled = false;

    const api = {
        requestGameEnrichBatch: async (platform, batch) => {
            if (!mainBatchCalled) {
                mainBatchCalled = true;
                // Alpha and Gamma succeed; Beta errors
                return make207Body([
                    { id: '10001', status: 'created_and_queued' },
                    { id: '10002', status: 'error' },
                    { id: '10003', status: 'created_and_queued' },
                ]);
            }
            // Retry batch — only Beta should appear
            retryBatchCalled = true;
            assert.equal(batch.length, 1, 'retry batch should contain only the errored item');
            assert.equal(batch[0].id, '10002', 'retry batch should contain Beta');
            return make207Body([{ id: '10002', status: 'created_and_queued' }]);
        },
        lookupGame: async () => null,
        normalizeServerData: () => null,
    };

    const result = await runBatchSync('steam', games, { ...api, sleep: noopSleep });

    assert.ok(mainBatchCalled, 'main batch must have been called');
    assert.ok(retryBatchCalled, 'retry batch must have been called');
    assert.ok(result.needsPoll.has('10001'), 'Alpha → needsPoll');
    assert.ok(result.needsPoll.has('10002'), 'Beta → needsPoll after retry recovery');
    assert.ok(result.needsPoll.has('10003'), 'Gamma → needsPoll');
    assert.equal(result.totals.error, 0, 'no permanent errors after successful item retry');
});

t('item errors: invalid items are NOT retried', async () => {
    const games = [makeSteamGame('20001', 'ValidGame'), makeSteamGame('20002', 'InvalidGame')];
    const batchIds = [];

    const api = {
        requestGameEnrichBatch: async (platform, batch) => {
            batchIds.push(batch.map(g => g.id));
            if (batchIds.length === 1) {
                return make207Body([
                    { id: '20001', status: 'created_and_queued' },
                    { id: '20002', status: 'invalid' },
                ]);
            }
            // If a retry is incorrectly attempted, fail the test
            assert.fail('invalid items must not be retried');
        },
        lookupGame: async () => null,
        normalizeServerData: () => null,
    };

    const result = await runBatchSync('steam', games, { ...api, sleep: noopSleep });

    assert.equal(batchIds.length, 1, 'only one batch call should occur');
    assert.ok(!result.errorItems.has('20002'), 'invalid item must not be in errorItems');
});

t('item errors: already_exists and already_queued items are NOT retried', async () => {
    const games = [
        makeSteamGame('30001'),
        makeSteamGame('30002'),
    ];
    let callCount = 0;

    const api = {
        requestGameEnrichBatch: async (platform, batch) => {
            callCount++;
            return make207Body([
                { id: '30001', status: 'already_exists' },
                { id: '30002', status: 'already_queued' },
            ]);
        },
        lookupGame: async () => null,
        normalizeServerData: () => null,
    };

    await runBatchSync('steam', games, { ...api, sleep: noopSleep });
    assert.equal(callCount, 1, 'no retry batches for already_exists / already_queued');
});

t('item errors: items that keep returning error after MAX_ITEM_ERROR_RETRIES remain in totals.error', async () => {
    const games = [makeSteamGame('40001', 'Stubborn')];
    let callCount = 0;

    const api = {
        requestGameEnrichBatch: async () => {
            callCount++;
            return make207Body([{ id: '40001', status: 'error' }]);
        },
        lookupGame: async () => null,
        normalizeServerData: () => null,
    };

    const result = await runBatchSync('steam', games, { ...api, sleep: noopSleep });

    // 1 main call + MAX_ITEM_ERROR_RETRIES retry calls
    assert.equal(callCount, 1 + MAX_ITEM_ERROR_RETRIES,
        `should be called 1 + ${MAX_ITEM_ERROR_RETRIES} times total`);
    assert.equal(result.totals.error, 1, 'permanently failing item should remain in totals.error');
    assert.ok(result.errorItems.has('40001'), 'stubborn item stays in errorItems set');
});

t('item errors: 429 during item retry is backed off correctly', async () => {
    const games = [makeSteamGame('50001', 'Difficult')];
    const sleepCalls = [];
    let callCount = 0;

    const api = {
        requestGameEnrichBatch: async (platform, batch) => {
            callCount++;
            if (callCount === 1) {
                // Main batch: item errors
                return make207Body([{ id: '50001', status: 'error' }]);
            }
            if (callCount === 2) {
                // First item retry: 429 with Retry-After
                throw new ApiError(429, '3');
            }
            // Second item retry attempt: success
            return make207Body([{ id: '50001', status: 'created_and_queued' }]);
        },
        lookupGame: async () => null,
        normalizeServerData: () => null,
    };

    const result = await runBatchSync('steam', games, {
        ...api,
        sleep: (ms) => { sleepCalls.push(ms); return Promise.resolve(); },
    });

    assert.ok(sleepCalls.includes(3000), 'item retry 429 should respect Retry-After=3 → 3000ms');
    assert.ok(result.needsPoll.has('50001'), 'item should eventually succeed');
    assert.equal(result.totals.error, 0);
});

t('item errors: title hint is preserved through retry batch', async () => {
    const games = [makeSteamGame('60001', 'My Special Game')];
    const retryBatches = [];
    let callCount = 0;

    const api = {
        requestGameEnrichBatch: async (platform, batch) => {
            callCount++;
            if (callCount === 1) {
                return make207Body([{ id: '60001', status: 'error' }]);
            }
            retryBatches.push(batch);
            return make207Body([{ id: '60001', status: 'created_and_queued' }]);
        },
        lookupGame: async () => null,
        normalizeServerData: () => null,
    };

    await runBatchSync('steam', games, { ...api, sleep: noopSleep });

    assert.equal(retryBatches.length, 1);
    assert.equal(retryBatches[0][0].title, 'My Special Game', 'title hint must be preserved in retry');
});

// ── 5. Result routing: needsLookup vs needsPoll ──────────────────────────────

t('routing: already_exists routes to needsLookup', async () => {
    const games = [makeSteamGame('70001')];
    const api = {
        requestGameEnrichBatch: async () =>
            make207Body([{ id: '70001', status: 'already_exists' }]),
        lookupGame: async () => null,
        normalizeServerData: () => null,
    };
    const result = await runBatchSync('steam', games, { ...api, sleep: noopSleep });
    assert.ok(result.needsLookup.has('70001'));
    assert.ok(!result.needsPoll.has('70001'));
});

t('routing: needs_enrich routes to needsLookup', async () => {
    const games = [makeSteamGame('70002')];
    const api = {
        requestGameEnrichBatch: async () =>
            make207Body([{ id: '70002', status: 'needs_enrich' }]),
        lookupGame: async () => null,
        normalizeServerData: () => null,
    };
    const result = await runBatchSync('steam', games, { ...api, sleep: noopSleep });
    assert.ok(result.needsLookup.has('70002'));
    assert.ok(!result.needsPoll.has('70002'));
});

t('routing: created_and_queued routes to needsPoll', async () => {
    const games = [makeSteamGame('70003')];
    const api = {
        requestGameEnrichBatch: async () =>
            make207Body([{ id: '70003', status: 'created_and_queued' }]),
        lookupGame: async () => null,
        normalizeServerData: () => null,
    };
    const result = await runBatchSync('steam', games, { ...api, sleep: noopSleep });
    assert.ok(result.needsPoll.has('70003'));
    assert.ok(!result.needsLookup.has('70003'));
});

t('routing: already_queued routes to needsPoll', async () => {
    const games = [makeSteamGame('70004')];
    const api = {
        requestGameEnrichBatch: async () =>
            make207Body([{ id: '70004', status: 'already_queued' }]),
        lookupGame: async () => null,
        normalizeServerData: () => null,
    };
    const result = await runBatchSync('steam', games, { ...api, sleep: noopSleep });
    assert.ok(result.needsPoll.has('70004'));
    assert.ok(!result.needsLookup.has('70004'));
});

// ── 6. Non-429 page failure isolation ────────────────────────────────────────

t('non-429 failure on page 1 does not abort page 2', async () => {
    const games = [
        ...Array.from({ length: 200 }, (_, i) => makeSteamGame(8000 + i)),
        makeSteamGame('80200'),
    ];

    let page2Called = false;

    const api = {
        requestGameEnrichBatch: async (platform, batch) => {
            if (batch.length === 200) {
                throw new ApiError(500, null); // server error, not rate-limit
            }
            page2Called = true;
            return make207Body(batch.map(g => ({ id: g.id, status: 'created_and_queued' })));
        },
        lookupGame: async () => null,
        normalizeServerData: () => null,
    };

    const result = await runBatchSync('steam', games, { ...api, sleep: noopSleep });

    assert.ok(page2Called, 'page 2 should run even though page 1 had a 500 error');
    assert.ok(result.totals.error > 0, 'page 1 errors should be counted');
    assert.ok(result.needsPoll.has('80200'), 'page 2 items should still be processed');
});

t('non-429 failure is not retried (only 1 attempt per page for non-429)', async () => {
    const games = Array.from({ length: 5 }, (_, i) => makeSteamGame(9000 + i));
    let callCount = 0;

    const api = {
        requestGameEnrichBatch: async () => {
            callCount++;
            throw new ApiError(503, null);
        },
        lookupGame: async () => null,
        normalizeServerData: () => null,
    };

    await runBatchSync('steam', games, { ...api, sleep: noopSleep });
    assert.equal(callCount, 1, 'non-429 errors should not be retried');
});

// ── 7. Edge cases ─────────────────────────────────────────────────────────────

t('empty games array returns null immediately', async () => {
    const api = {
        requestGameEnrichBatch: async () => { assert.fail('should not be called'); },
        lookupGame: async () => null,
        normalizeServerData: () => null,
    };
    const result = await runBatchSync('steam', [], { ...api, sleep: noopSleep });
    assert.equal(result, null);
});

t('games with missing ids are excluded from payload', async () => {
    const games = [
        { appName: null, id: null, title: 'No ID Game' },
        makeSteamGame('11111', 'Valid'),
    ];
    let sentBatch = null;
    const api = {
        requestGameEnrichBatch: async (platform, batch) => {
            sentBatch = batch;
            return make207Body(batch.map(g => ({ id: g.id, status: 'created_and_queued' })));
        },
        lookupGame: async () => null,
        normalizeServerData: () => null,
    };

    const result = await runBatchSync('steam', games, { ...api, sleep: noopSleep });
    assert.equal(result.payload.length, 1, 'only the game with a valid id should be in payload');
    assert.equal(sentBatch.length, 1);
    assert.equal(sentBatch[0].id, '11111');
});

t('mixed statuses in one page: all routes populated correctly', async () => {
    const games = [
        makeSteamGame('A'), makeSteamGame('B'), makeSteamGame('C'),
        makeSteamGame('D'), makeSteamGame('E'),
    ];

    const api = {
        requestGameEnrichBatch: async (platform, batch) => {
            if (batch.length === 5) {
                return make207Body([
                    { id: 'A', status: 'already_exists' },
                    { id: 'B', status: 'needs_enrich' },
                    { id: 'C', status: 'created_and_queued' },
                    { id: 'D', status: 'already_queued' },
                    { id: 'E', status: 'error' },
                ]);
            }
            // Retry batch for E
            return make207Body([{ id: 'E', status: 'created_and_queued' }]);
        },
        lookupGame: async () => null,
        normalizeServerData: () => null,
    };

    const result = await runBatchSync('steam', games, { ...api, sleep: noopSleep });

    assert.ok(result.needsLookup.has('A'),  'A (already_exists) → needsLookup');
    assert.ok(result.needsLookup.has('B'),  'B (needs_enrich)   → needsLookup');
    assert.ok(result.needsPoll.has('C'),    'C (created_queued) → needsPoll');
    assert.ok(result.needsPoll.has('D'),    'D (already_queued) → needsPoll');
    assert.ok(result.needsPoll.has('E'),    'E (error→retry→ok) → needsPoll after recovery');
    assert.equal(result.totals.error, 0,   'no permanent errors');
});

console.log('\n✓ All batch sync tests defined. Run with: node --test platformSync.batch.test.js\n');
