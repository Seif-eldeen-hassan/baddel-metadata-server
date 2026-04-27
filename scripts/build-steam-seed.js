'use strict';

const fs = require('fs/promises');

const PAGES = Number(process.env.PAGES || 10); // 10 pages = ~10k
const OUT = process.env.OUT || 'data/steam_seed_10k.json';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const JUNK_RE =
  /\b(demo|playtest|test server|dedicated server|server|soundtrack|ost|sdk|editor|benchmark|trailer|teaser|dlc|season pass)\b/i;

async function fetchPage(page) {
  const url = `https://steamspy.com/api.php?request=all&page=${page}`;
  const res = await fetch(url, {
    headers: { 'User-Agent': 'BaddelSeedBuilder/1.0' },
  });

  if (!res.ok) {
    throw new Error(`SteamSpy page ${page} failed: HTTP ${res.status}`);
  }

  return Object.values(await res.json());
}

async function main() {
  const all = [];

  for (let page = 0; page < PAGES; page++) {
    console.log(`[seed] fetching page ${page}/${PAGES - 1}`);
    const games = await fetchPage(page);

    for (const g of games) {
      if (!g?.appid || !g?.name) continue;
      if (JUNK_RE.test(g.name)) continue;

      all.push({
        id: String(g.appid),
        title: g.name,
      });
    }

    // SteamSpy all endpoint is rate-limited. Be polite.
    if (page < PAGES - 1) await sleep(65_000);
  }

  const seen = new Set();
  const deduped = [];

  for (const g of all) {
    if (seen.has(g.id)) continue;
    seen.add(g.id);
    deduped.push(g);
  }

  await fs.writeFile(OUT, JSON.stringify(deduped, null, 2));
  console.log(`[seed] wrote ${deduped.length} games to ${OUT}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});