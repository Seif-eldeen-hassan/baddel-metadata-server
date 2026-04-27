'use strict';

const fs = require('fs/promises');
const crypto = require('crypto');

const INPUT = process.argv[2] || 'data/steam_seed_10k.json';
const BASE_URL = (process.argv[3] || process.env.BADDEL_SERVER_URL || '').replace(/\/$/, '');

const PLATFORM = process.env.PLATFORM || 'steam';
const BATCH_SIZE = Number(process.env.BATCH_SIZE || 200);

// مهم بسبب batch rate-limit: 10 requests / 5 min على الـ IP.
// 31 ثانية بين كل batch = آمن.
const DELAY_MS = Number(process.env.DELAY_MS || 31_000);

const INSTALL_ID =
  process.env.INSTALL_ID ||
  'seed-' + crypto.createHash('sha1').update(BASE_URL || 'local').digest('hex').slice(0, 16);

const PROGRESS_FILE = process.env.PROGRESS_FILE || 'seed_progress.json';
const ERROR_FILE = process.env.ERROR_FILE || 'seed_errors.json';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function normalizeGame(item) {
  if (!item) return null;

  if (typeof item === 'string' || typeof item === 'number') {
    return { id: String(item).trim() };
  }

  const id = item.id ?? item.appid ?? item.app_id ?? item.steam_appid;
  const title = item.title ?? item.name ?? item.appName;

  if (!id) return null;

  return {
    id: String(id).trim(),
    ...(title ? { title: String(title).trim() } : {}),
  };
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

async function readJsonSafe(path, fallback) {
  try {
    return JSON.parse(await fs.readFile(path, 'utf8'));
  } catch {
    return fallback;
  }
}

async function writeJson(path, data) {
  await fs.writeFile(path, JSON.stringify(data, null, 2));
}

async function postBatch(games, batchIndex) {
  const url = `${BASE_URL}/client/request-enrich/batch`;

  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Baddel-Install-Id': INSTALL_ID,
      'X-Baddel-App-Version': 'seed-script-1.0',
    },
    body: JSON.stringify({
      platform: PLATFORM,
      games,
    }),
  });

  const text = await res.text();
  let body;

  try {
    body = JSON.parse(text);
  } catch {
    body = { raw: text };
  }

  if (res.status === 429) {
    const retryAfter =
      Number(res.headers.get('retry-after')) ||
      Number(body.retryAfterSeconds) ||
      60;

    console.log(`[rate-limit] batch ${batchIndex} retry after ${retryAfter}s`);
    await sleep((retryAfter + 2) * 1000);
    return postBatch(games, batchIndex);
  }

  if (!res.ok && res.status !== 207) {
    throw new Error(`HTTP ${res.status}: ${text.slice(0, 500)}`);
  }

  return body;
}

async function main() {
  if (!BASE_URL) {
    console.error('Usage: node seed_railway.js <json-file> <https://your-railway-url>');
    process.exit(1);
  }

  const raw = JSON.parse(await fs.readFile(INPUT, 'utf8'));

  const normalized = raw
    .map(normalizeGame)
    .filter(Boolean)
    .filter((g) => g.id && /^[a-zA-Z0-9_-]+$/.test(g.id));

  const seen = new Set();
  const games = [];

  for (const g of normalized) {
    const key = `${PLATFORM}:${g.id}`;
    if (seen.has(key)) continue;
    seen.add(key);
    games.push(g);
  }

  const batches = chunk(games, BATCH_SIZE);
  const progress = await readJsonSafe(PROGRESS_FILE, {
    doneBatches: {},
    totals: {
      submitted: games.length,
      queued: 0,
      alreadyQueued: 0,
      alreadyExists: 0,
      invalid: 0,
      error: 0,
    },
  });

  const allErrors = await readJsonSafe(ERROR_FILE, []);

  console.log(`Loaded ${games.length} unique ${PLATFORM} games`);
  console.log(`Batches: ${batches.length} x ${BATCH_SIZE}`);
  console.log(`Server: ${BASE_URL}`);
  console.log(`Install ID: ${INSTALL_ID}`);

  for (let i = 0; i < batches.length; i++) {
    if (progress.doneBatches[i]) {
      console.log(`[skip] batch ${i + 1}/${batches.length} already done`);
      continue;
    }

    const batch = batches[i];
    console.log(`\n[send] batch ${i + 1}/${batches.length}, games=${batch.length}`);

    try {
      const result = await postBatch(batch, i + 1);

      progress.totals.queued += result.queuedCount || 0;
      progress.totals.alreadyQueued += result.alreadyQueuedCount || 0;
      progress.totals.alreadyExists += result.alreadyExistsCount || 0;
      progress.totals.invalid += result.invalidCount || 0;
      progress.totals.error += result.errorCount || 0;

      const errorItems = (result.results || []).filter(
        (r) => r.status === 'error' || r.status === 'invalid'
      );

      if (errorItems.length) {
        allErrors.push({
          batch: i + 1,
          items: errorItems,
        });
        await writeJson(ERROR_FILE, allErrors);
      }

      progress.doneBatches[i] = {
        at: new Date().toISOString(),
        queuedCount: result.queuedCount || 0,
        alreadyQueuedCount: result.alreadyQueuedCount || 0,
        alreadyExistsCount: result.alreadyExistsCount || 0,
        invalidCount: result.invalidCount || 0,
        errorCount: result.errorCount || 0,
      };

      await writeJson(PROGRESS_FILE, progress);

      console.log(
        `[ok] queued=${result.queuedCount || 0}, alreadyQueued=${result.alreadyQueuedCount || 0}, alreadyExists=${result.alreadyExistsCount || 0}, invalid=${result.invalidCount || 0}, error=${result.errorCount || 0}`
      );
    } catch (err) {
      console.error(`[fail] batch ${i + 1}: ${err.message}`);
      allErrors.push({
        batch: i + 1,
        fatal: err.message,
        games: batch,
      });
      await writeJson(ERROR_FILE, allErrors);
    }

    if (i < batches.length - 1) {
      console.log(`[wait] ${Math.round(DELAY_MS / 1000)}s`);
      await sleep(DELAY_MS);
    }
  }

  console.log('\nDone.');
  console.log(progress.totals);
  console.log(`Progress: ${PROGRESS_FILE}`);
  console.log(`Errors: ${ERROR_FILE}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});