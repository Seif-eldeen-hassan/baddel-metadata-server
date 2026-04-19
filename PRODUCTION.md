# Baddel Metadata Server — Production Runbook

## Quick-start

```bash
cp .env.example .env
# Fill in all required values

npm ci --legacy-peer-deps
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

# Apply DB migrations (run once, in order)
psql $DATABASE_URL -f migrations/001_production_hardening.sql
psql $DATABASE_URL -f migrations/002_job_queue.sql

node server.js
```

## Running Tests

```bash
API_SECRET=supersecretkey1234567890abcdef12 \
CORS_ORIGIN=https://test.baddel.gg \
DATABASE_URL=postgresql://localhost/baddel_test \
IGDB_CLIENT_ID=test IGDB_CLIENT_SECRET=test STEAMGRIDDB_API_KEY=test \
node --test tests/regression.test.js
```

Expected: **64 pass, 0 fail**

---

## Architecture

```
Client / Baddel Electron App
        │
        ▼  Authorization: Bearer <API_SECRET>
  Express server (server.js)
   ├── CORS allowlist          (CORS_ORIGIN env)
   ├── Request ID middleware   (X-Request-ID header)
   ├── Rate limiting           (express-rate-limit)
   ├── Auth: requireAuth       (Authorization: Bearer API_SECRET)
   ├── Auth: requireCronSecret (X-Cron-Secret: CRON_SECRET)
   └── Request validation      (middleware/validate.js)
        │
        ├─ GET  /health                        [public]
        ├─ GET  /health/ready                  [public — 503 during drain]
        ├─ GET  /games/lookup                  [public, rate-limited]
        ├─ POST /client/request-enrich         [public, low-trust, abuse-limited, single-game]
        ├─ POST /client/request-enrich/batch   [public, low-trust, abuse-limited, ≤200 games ← USE THIS FOR LARGE SYNCS]
        ├─ POST /games/import                  [requireAuth — admin only]
        ├─ POST /games/enrich                  [requireAuth] → enrich_jobs table
        ├─ POST /games/enrich/:gameId          [requireAuth] → enrich_jobs table
        ├─ GET  /games/enrich-stream           [requireAuth, SSE, DB-backed]
        ├─ GET  /jobs/enrich/:jobId            [requireAuth] — job status
        ├─ GET  /jobs/enrich?gameId=<uuid>     [requireAuth] — game job history
        ├─ POST /jobs/promote-images           [requireCronSecret]
        └─ GET  /jobs/promote-images/status    [requireAuth]
        │
        ▼
  services/
   ├── jobQueue.js    — enqueue / claimBatch / complete / fail / getJob
   ├── jobWorker.js   — polls enrich_jobs, processes with enrichGame()
   ├── enrichGame.js  — IGDB + SGDB + Steam/Epic Python lookup + DB write
   ├── r2.js          — SSRF-guarded fetch → Sharp WebP → Cloudflare R2
   └── ...
        │
        ▼
  PostgreSQL (Supabase)        Cloudflare R2
  enrich_jobs table            baddel-media bucket
```

---

---

## Client Endpoint: POST /client/request-enrich/batch  ← **Recommended for large syncs**

Use this endpoint when syncing more than ~10 games. The launcher should chunk its library into pages of up to 200 games and send them one-at-a-time (no concurrency needed — the server fans out internally).

### Why batch instead of single-game requests?

| Scenario | Single-game `/client/request-enrich` | Batch `/client/request-enrich/batch` |
|---|---|---|
| 400-game cold sync | 400 HTTP requests, each counted against the rate limiter | 2 HTTP requests (200 + 200), each counted as 1 against the batch limiter |
| Rate-limit headroom | 120 req / 5 min → sync takes ≥ 17 minutes worst-case | 20 batch req / 5 min → sync takes seconds |
| Retry complexity | Launcher must track and retry each game individually | Launcher retries one request; server returns per-item status |
| Response clarity | 400 separate responses to merge | One structured summary with per-game outcome |

### Request

```http
POST /client/request-enrich/batch
Content-Type: application/json
X-Baddel-Install-Id: <launcher-install-uuid>   (optional, for telemetry & rate-limit keying)
X-Baddel-App-Version: 1.2.3                    (optional, for telemetry)
```

```json
{
  "platform": "steam",
  "games": [
    { "id": "570",    "title": "Dota 2"       },
    { "id": "730",    "title": "CS2"          },
    { "id": "440"                             }
  ]
}
```

**Allowed top-level fields:** `platform`, `games` — anything else → 400.  
**Allowed per-game fields:** `id`, `title` (optional) — anything else → that game is marked `invalid` in results.

| Field | Rules |
|---|---|
| `platform` | Required. Must be exactly `"steam"` or `"epic"` |
| `games` | Required. Array of 1–200 items |
| `games[].id` | Required. Alphanumeric + hyphens/underscores only. Max 64 chars |
| `games[].title` | Optional hint. Max 200 chars. Never stored without server-side resolution |

Hard cap: **200 games per request**. For libraries > 200, split into sequential pages.  
Duplicates within a single batch (same platform+id) are silently deduplicated — no error.

### Response (HTTP 207 Multi-Status)

```json
{
  "status": "multi",
  "platform": "steam",
  "acceptedCount": 198,
  "queuedCount": 150,
  "alreadyQueuedCount": 10,
  "alreadyExistsCount": 38,
  "invalidCount": 1,
  "errorCount": 0,
  "dedupCount": 1,
  "maxBatchSize": 200,
  "results": [
    {
      "id": "570",
      "gameId": "<uuid>",
      "jobId": "<job-id>",
      "status": "created_and_queued",
      "queued": true,
      "alreadyQueued": false
    },
    {
      "id": "9999invalid!",
      "index": 2,
      "status": "invalid",
      "reason": "games[2]: id contains invalid characters",
      "queued": false,
      "alreadyQueued": false
    }
  ]
}
```

Per-item `status` values:

| `status` | Meaning |
|---|---|
| `created_and_queued` | Game was new — created and queued for enrichment |
| `needs_enrich` | Game existed but was not enriched — queued now |
| `already_queued` | Active job already exists — no duplicate created |
| `already_exists` | Game is already sufficiently enriched — no work needed |
| `invalid` | Item failed validation — skipped entirely |
| `error` | Unexpected server error for this item — safe to retry |

### Launcher recommended flow for large libraries

```
1. Chunk library into pages of 200 games
2. For each page (sequentially — no concurrency needed):
   a. POST /client/request-enrich/batch
   b. Collect any status='error' items for retry
   c. Wait 500ms between pages to be polite (not required)
3. Poll GET /games/lookup per game as the app needs data
   (enrichment completes in background, typically 10–30s per game)
```

### Batch-specific rate limits

| Tier | Key | Limit |
|---|---|---|
| A (primary) | `X-Baddel-Install-Id` (when valid UUID-like, 8–64 chars) | **20 batch requests per 5-minute window** |
| B (always-on) | IP address | **10 batch requests per 5-minute window** |

A 400-game library needs 2 batch requests (200 + 200). The 20/5-min limit gives 10× headroom for retries, restarts, and re-syncs. Tier B prevents a single IP from cycling fake install IDs to bypass Tier A.

429 responses include `Retry-After` header (seconds) and a JSON body with `retryAfterSeconds`.

### What the batch endpoint does NOT expose

- Admin routes (`/games/enrich`, `/games/import`, `/games/enrich-all`)
- Cron operations (`/jobs/promote-images`)
- Any privilege escalation — per-game idempotency and enrichment safety are identical to the single-game endpoint

---

## Client Endpoint: POST /client/request-enrich

This is the **only** endpoint the Baddel launcher should call for metadata requests. It does not require `API_SECRET`. Do not ship admin secrets in the launcher.

### Request

```http
POST /client/request-enrich
Content-Type: application/json
X-Baddel-Install-Id: <launcher-install-uuid>   (optional, for telemetry)
X-Baddel-App-Version: 1.2.3                    (optional, for telemetry)
```

```json
{
  "platform": "steam",
  "id": "570",
  "title": "Dota 2"
}
```

Allowed fields: `platform`, `id`, `title` (optional). Any other field is rejected with 400.

| Field | Rules |
|---|---|
| `platform` | Required. Must be exactly `"steam"` or `"epic"` |
| `id` | Required. Alphanumeric + hyphens/underscores only. Max 64 chars |
| `title` | Optional hint. Max 200 chars. Never stored without server-side resolution |

### Response

```json
{
  "status": "accepted",
  "queued": true,
  "alreadyQueued": false,
  "gameId": "<uuid>",
  "jobId": "<job-id>",
  "reason": "created_and_queued"
}
```

| `reason` | Meaning |
|---|---|
| `created_and_queued` | Game was new — created and queued for enrichment |
| `needs_enrich` | Game existed but was not enriched — queued now |
| `already_queued` | Active job already exists — no duplicate created |
| `already_exists` | Game is already sufficiently enriched — no work needed |

### Launcher flow

1. Call `GET /games/lookup?platform=steam&namespace=app_id&id=570`
2. If 404 or incomplete data → call `POST /client/request-enrich`
3. Poll `GET /games/lookup` again until data appears (typically 10–30s)

### Abuse protection

Two-tier rate limit — both tiers must pass:

| Tier | Key | Limit |
|---|---|---|
| A (primary) | `X-Baddel-Install-Id` when header is present and valid (UUID-like, 8–64 alphanum chars) | **120 requests per 5-minute window** |
| B (always-on) | IP address | **60 requests per 5-minute window** |

Rationale: a legitimate 100-game library sync at concurrency-2 takes 2–3 minutes and generates ≤ 100 distinct requests. Tier A gives 120/5 min = 24 req/min average headroom. Tier B prevents a single IP from bypassing the install-id limit by cycling fake identifiers.

If the install-id header is absent or invalid, Tier A falls back to IP as well — effectively 60/5 min for unknown clients.

Every 429 response includes:
- Standard `RateLimit-*` and `Retry-After` headers (seconds)
- JSON body with `retryAfterSeconds` for launcher consumption

Every rate-limit hit is logged with: `event`, `key`, `ip`, `installId`, `platform`, `externalId`, `retryAfterSeconds`.

Other protections unchanged:
- Strict field allowlist — no URL injection, no metadata blobs, no cover images
- `X-Baddel-Install-Id` and `X-Baddel-App-Version` are **not authentication** — client-controlled signals only
- No privilege escalation — cannot trigger batch enrich, enrich-all, cron, or admin operations

---

The enrichment queue is backed by PostgreSQL — **not in-memory**.

### How it works

1. `POST /games/import` or `POST /games/enrich` calls `enqueue()` which inserts a row into `enrich_jobs` with `status='queued'`
2. A **unique partial index** on `(game_id) WHERE status IN ('queued','running')` prevents duplicate active jobs per game
3. `jobWorker.js` polls every 2 seconds, claims up to 5 jobs atomically using `SELECT ... FOR UPDATE SKIP LOCKED`
4. Claimed jobs are immediately set to `status='running'` in the same transaction
5. On success → `status='succeeded'`; on failure → re-queued if `attempts < max_attempts (3)`, else `status='failed'`
6. All state (attempts, timestamps, error message) is persisted in the DB

### Multi-instance safety

`FOR UPDATE SKIP LOCKED` ensures two worker instances never claim the same job. Safe to run multiple Railway/Render instances simultaneously.

### Surviving restarts and crash recovery

Jobs in `status='queued'` survive restarts — the worker picks them up on next boot.

Jobs that were `running` when the process died are automatically recovered: on every startup and every 5 minutes during operation, `recoverStaleJobs()` resets any job stuck in `running` for more than 10 minutes back to `queued` (or `failed` if attempts are exhausted). **No manual SQL intervention required.**

---

## Job Status API

All endpoints require `Authorization: Bearer <API_SECRET>`.

| Endpoint | Description |
|---|---|
| `GET /jobs/enrich/:jobId` | Get one job by ID |
| `GET /jobs/enrich?gameId=<uuid>` | List recent jobs for a game |

`POST /games/enrich` and `POST /games/enrich/:gameId?background=true` both return `jobId` in the response so clients can track status.

### SSE stream

`GET /games/enrich-stream` (optionally `?gameId=<uuid>`) streams job state changes as Server-Sent Events. Unlike the old implementation, this is **backed by DB polling** (every 2s) — it works correctly across multiple instances. Clients that connect to any instance will see the same state.

Events emitted: `enriched`, `error`, `update`

---

## Security Controls

| Control | Implementation |
|---|---|
| CORS | `CORS_ORIGIN` env allowlist, no wildcard |
| Auth | `Authorization: Bearer API_SECRET` on all mutating routes |
| Cron auth | `X-Cron-Secret` header — **503** if `CRON_SECRET` not set (fail closed) |
| Rate limiting | 120/min on lookups, 30/min on admin routes; client-enrich: 120/5-min per install-id + 60/5-min per IP; client-enrich/batch: 20/5-min per install-id + 10/5-min per IP (per request, not per game) |
| Input validation | `middleware/validate.js` — type, length, enum checks |
| SSRF | HTTPS-only, 14-host allowlist, DNS/private-IP check on every request, manual hop-by-hop redirect validation, raw IP literal rejection, 10MB limit, image content-type check, 15s timeout |
| R2 keys | `buildImageKey(row)` uses DB UUID + URL hash — zero collision possible |
| DB TLS | `rejectUnauthorized: true` by default — cert verification enforced in production |
| Error leakage | Centralized error handler never exposes stack traces to clients |
| Startup validation | `validateEnv()` exits before serving if required vars missing |
| Graceful shutdown | SIGTERM/SIGINT → stop worker → drain in-flight → close DB → exit |
| Stale job recovery | Worker resets crashed `running` jobs to `queued` on startup and every 5 minutes |

---

## Required Environment Variables

| Variable | Description |
|---|---|
| `DATABASE_URL` | PostgreSQL connection string (Supabase) |
| `DB_SSL_REJECT_UNAUTHORIZED` | Set to `false` **only** for local dev with self-signed certs. Never set in production. |
| `API_SECRET` | Bearer token for admin routes — `openssl rand -hex 32` |
| `CORS_ORIGIN` | Comma-separated allowed origins |
| `IGDB_CLIENT_ID` | Twitch/IGDB OAuth client ID |
| `IGDB_CLIENT_SECRET` | Twitch/IGDB OAuth client secret |
| `STEAMGRIDDB_API_KEY` | SteamGridDB API key |
| `CRON_SECRET` | Secret for `/jobs/promote-images` — `openssl rand -hex 32` |
| `S3_API` | Cloudflare R2 S3-compatible endpoint |
| `Access_Key` | R2 access key |
| `Secret_Access_Key` | R2 secret key |
| `Public_Development_URL` | R2 public CDN base URL |

---

## Migrations

| File | When to run |
|---|---|
| `migrations/001_production_hardening.sql` | **Run now** — unique indexes for safe upserts |
| `migrations/002_job_queue.sql` | **Run now** — creates `enrich_jobs` table |

Both are idempotent (`IF NOT EXISTS`) — safe to re-run.

---

## Cron Setup

```bash
# Daily at 00:00 UTC — promotes raw image URLs to R2
curl -X POST https://your-server/jobs/promote-images \
     -H "X-Cron-Secret: $CRON_SECRET" \
     -H "Content-Type: application/json" \
     -d '{"limit": 500}'
```

---

## Deployment Checklist

- [ ] All required env vars set
- [ ] `API_SECRET` is 32+ chars random (`openssl rand -hex 32`)
- [ ] `CRON_SECRET` is 32+ chars random
- [ ] `CORS_ORIGIN` lists only your actual app domains
- [ ] `migrations/001_production_hardening.sql` applied
- [ ] `migrations/002_job_queue.sql` applied
- [ ] Deployed on Node 22 LTS
- [ ] R2 bucket configured with public domain in `Public_Development_URL`
- [ ] Baddel Electron app sends `Authorization: Bearer <API_SECRET>` header
- [ ] Health check: `GET /health` (liveness) + `GET /health/ready` (readiness)
- [ ] Log aggregation wired to structured JSON stdout

---

## Known Remaining Limitations

### R2 env var naming
`Access_Key`, `Secret_Access_Key`, `Public_Development_URL` are non-standard casing (legacy). They work but can be renamed to `R2_ACCESS_KEY`, `R2_SECRET_KEY`, `R2_PUBLIC_URL` in a future migration.
