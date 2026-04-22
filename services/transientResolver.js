'use strict';

/**
 * services/transientResolver.js
 *
 * Resolves IGDB + SteamGridDB metadata for any game title WITHOUT touching
 * the canonical DB (no games/platform_ids/game_metadata/game_images writes).
 *
 * Used by POST /client/resolve-metadata for non-Steam/non-Epic games
 * (Xbox, EA App, Ubisoft Connect, Riot, Rockstar, manual installs, etc.).
 *
 * Flow:
 *   1. Build name candidates (strip Demo / Prologue / Trial / Chapter suffixes)
 *   2. For each candidate, igdbFetch /games with all needed fields + alternative_names
 *   3. Score every result with transientConfidenceScorer
 *   4. If resolved: fetch SteamGridDB images by IGDB ID
 *   5. Assemble and return normalized metadata payload
 *
 * In-process LRU cache (keyed by normalized title + hints hash):
 *   - TTL: 30 minutes
 *   - Max entries: 500
 *   - Avoids hammering IGDB/SGDB for the same game across launcher restarts
 */

const { igdbFetch: _igdbFetch, fetchIgdbGameData } = require('./igdbResolver');
const { resolveSgdbImages }                        = require('./steamGridDbResolver');
const { pickBestCandidate }                        = require('./transientConfidenceScorer');
const log                                          = require('../config/logger');
const { randomUUID }                               = require('crypto');

// ─── Minimal in-process cache ─────────────────────────────────────────────────

const CACHE_TTL_MS  = 30 * 60 * 1000;  // 30 minutes
const CACHE_MAX     = 500;

const _cache = new Map(); // key → { value, expiresAt }

function _cacheGet(key) {
    const entry = _cache.get(key);
    if (!entry) return null;
    if (Date.now() > entry.expiresAt) { _cache.delete(key); return null; }
    return entry.value;
}

function _cacheSet(key, value) {
    // Evict oldest if at capacity
    if (_cache.size >= CACHE_MAX) {
        const oldest = _cache.keys().next().value;
        _cache.delete(oldest);
    }
    _cache.set(key, { value, expiresAt: Date.now() + CACHE_TTL_MS });
}

function _cacheKey(hints) {
    return [
        (hints.title || '').toLowerCase().replace(/\s+/g, ' ').trim(),
        (hints.slug || '').toLowerCase(),
        (hints.exeName || '').toLowerCase(),
        (hints.folderName || '').toLowerCase(),
        (hints.platformHint || '').toLowerCase(),
        String(hints.igdbId || ''),
        String(hints.releaseYear || ''),
        (hints.developer || '').toLowerCase().replace(/\s+/g, ' ').trim(),
        (hints.publisher || '').toLowerCase().replace(/\s+/g, ' ').trim(),
    ].join('|');
}

// ─── IGDB fields for transient search (includes alternative_names + category) ─

const TRANSIENT_FIELDS = `
    name, slug, summary, storyline,
    first_release_date,
    genres.name, themes.name, keywords.name,
    involved_companies.company.name, involved_companies.developer, involved_companies.publisher,
    aggregated_rating, aggregated_rating_count,
    rating, rating_count, total_rating_count,
    cover.url, cover.image_id, cover.width, cover.height,
    artworks.url, artworks.image_id, artworks.width, artworks.height,
    screenshots.url, screenshots.image_id, screenshots.width, screenshots.height,
    videos.video_id, videos.name,
    platforms.id, platforms.name,
    game_modes.name,
    alternative_names.name,
    category
`.trim();

// ─── Name candidate builder (mirrors igdbResolver's logic) ───────────────────

function buildCandidates(rawTitle) {
    const push = (arr, val) => {
        const v = val?.trim();
        if (v && !arr.includes(v)) arr.push(v);
    };

    const candidates = [rawTitle];

    const noDemo = rawTitle.replace(/\s*[\(-]\s*demo\s*\)?$/i, '').replace(/\s+demo$/i, '').trim();
    push(candidates, noDemo);
    const base = noDemo || rawTitle;

    push(candidates, base.replace(/\s*[–\-]?\s*\(?\bprologue\b\)?$/i, '').trim());
    push(candidates, base.replace(/\s*[–\-]?\s*\(?\bfree\s+trial\b\)?$/i, '').replace(/\s*[–\-]\s*\(?\btrial\b\)?$/i, '').trim());
    push(candidates, base.replace(/\s*[–-]?\s*chapter[:\s]\S.*$/i, '').trim());

    return [...new Set(candidates)].filter(Boolean);
}

// ─── IGDB search ──────────────────────────────────────────────────────────────

/**
 * Search IGDB by name and return up to 50 raw candidates.
 * Uses _igdbFetch directly (already rate-limited in igdbResolver).
 */
async function _igdbSearch(name) {
    const safe = name.replace(/"/g, '').trim();
    if (!safe) return [];
    try {
        const results = await _igdbFetch(
            'games',
            `fields ${TRANSIENT_FIELDS}; search "${safe}"; limit 50;`
        );
        return Array.isArray(results) ? results.filter(g => g?.name) : [];
    } catch (err) {
        log.warn({ name, err: err.message }, '[TransientResolver] IGDB search failed');
        return [];
    }
}

// ─── Image URL builder ────────────────────────────────────────────────────────

function _imgUrl(imageId, size = '720p') {
    return imageId ? `https://images.igdb.com/igdb/image/upload/t_${size}/${imageId}.jpg` : null;
}

// ─── Payload quality guards ───────────────────────────────────────────────────

/**
 * Returns true when a normalized meta payload has at least one useful text
 * signal from a trusted metadata source (IGDB).
 *
 * SGDB alone provides cover/hero/logo — that is art, not metadata.
 * A payload missing ALL of the following is art-only and MUST NOT be
 * returned as `resolved`:
 *   • description or storyline
 *   • developer or publisher (from IGDB involved_companies)
 *   • at least one IGDB screenshot (trusted media from a metadata source)
 *
 * @param {object} meta — result of _normalizeMeta()
 * @returns {boolean}
 */
function _hasUsableTextMeta(meta) {
    if (!meta) return false;
    const hasDescription = !!(meta.description || meta.storyline);
    const hasCompany     = !!(meta.developer || meta.publisher);
    // screenshots come from IGDB — they are a metadata-source signal
    const hasIgdbScreenshots = Array.isArray(meta.images?.screenshots) && meta.images.screenshots.length > 0;
    return hasDescription || hasCompany || hasIgdbScreenshots;
}

/**
 * Sanitize a normalized meta payload before returning it to callers.
 * Mutates and returns the same object.
 *
 * Rules applied:
 *   - title must never be null  → fall back to igdbSlug, then ''
 *   - genres: drop null/empty entries
 *   - videos: drop any entry whose URL contains 'undefined' or is falsy
 *   - if no valid videos remain, delete the videos array entirely
 *
 * @param {object} meta — result of _normalizeMeta()
 * @param {string} [fallbackTitle] — original request title, used if meta.title is null
 * @returns {object}
 */
function _sanitizePayload(meta, fallbackTitle) {
    if (!meta) return meta;

    // Title must never be null
    if (!meta.title) {
        meta.title = meta.slug || fallbackTitle || '';
        log.warn({ title: meta.title, igdbId: meta.igdbId }, '[TransientResolver] sanitize: meta.title was null — using fallback');
    }

    // Genres: filter out null/empty
    if (Array.isArray(meta.genres)) {
        meta.genres = meta.genres.filter(g => g && typeof g === 'string' && g.trim().length > 0);
    }

    // Videos: reject any entry with a missing/undefined video_id in the URL
    if (Array.isArray(meta.videos)) {
        const validVideos = meta.videos.filter(v => {
            if (!v?.url) return false;
            // Reject URLs that contain the literal string 'undefined'
            if (v.url.includes('undefined')) {
                log.warn({ url: v.url, igdbId: meta.igdbId }, '[TransientResolver] sanitize: dropping trailer with undefined in URL');
                return false;
            }
            return true;
        });
        if (validVideos.length === 0) {
            // Omit trailer fields entirely rather than returning empty array
            delete meta.videos;
        } else {
            meta.videos = validVideos;
        }
    }

    return meta;
}

// ─── Normalize IGDB game object → response meta ───────────────────────────────

function _normalizeMeta(igdbData, sgdbImages) {
    const g = igdbData;

    const developers = (g.involved_companies || []).filter(ic => ic.developer).map(ic => ic.company?.name).filter(Boolean);
    const publishers = (g.involved_companies || []).filter(ic => ic.publisher).map(ic => ic.company?.name).filter(Boolean);

    const genres = (g.genres   || []).map(x => x.name);
    const tags   = [...(g.themes   || []).map(x => x.name), ...(g.keywords || []).map(x => x.name)];

    const releaseYear = g.first_release_date
        ? new Date(g.first_release_date * 1000).getFullYear()
        : null;

    const cover = (() => {
        // Prefer SteamGridDB cover (600x900 portrait) over IGDB
        if (sgdbImages?.cover?.url) return sgdbImages.cover;
        if (g.cover?.image_id) return { url: _imgUrl(g.cover.image_id, '720p'), width: g.cover.width || null, height: g.cover.height || null };
        return null;
    })();

    const hero = (() => {
        if (sgdbImages?.hero?.url) return sgdbImages.hero;
        const art = g.artworks?.[0];
        if (art?.image_id) return { url: _imgUrl(art.image_id, '1080p'), width: art.width || null, height: art.height || null };
        return null;
    })();

    const logo = sgdbImages?.logo || null;

    const artworks = (g.artworks || []).map(a => ({
        url: _imgUrl(a.image_id, '1080p'), width: a.width || null, height: a.height || null,
    })).filter(a => a.url);

    const screenshots = (g.screenshots || []).map(s => ({
        url: _imgUrl(s.image_id, '1080p'), width: s.width || null, height: s.height || null,
    })).filter(s => s.url);

    const videos = (g.videos || []).map(v => ({
        url:   `https://www.youtube-nocookie.com/embed/${v.video_id}?autoplay=0&rel=0`,
        thumb: `https://img.youtube.com/vi/${v.video_id}/mqdefault.jpg`,
        title: v.name || 'Trailer',
    }));

    const ratings = [];
    if (g.aggregated_rating) {
        ratings.push({
            score:  Number((g.aggregated_rating / 10).toFixed(1)),
            count:  g.aggregated_rating_count || 0,
            source: 'igdb_critics',
        });
    }
    if (g.rating) {
        ratings.push({
            score:  Number((g.rating / 10).toFixed(1)),
            count:  g.rating_count || 0,
            source: 'igdb_users',
        });
    }

    return {
        igdbId:      g.id,
        title:       g.name      || null,
        slug:        g.slug      || null,
        releaseYear,
        description: g.summary   || null,
        storyline:   g.storyline || null,
        genres,
        tags,
        developer:   developers[0] || null,
        publisher:   publishers[0] || null,
        ratings,
        images: {
            cover,
            hero,
            logo,
            artworks,
            screenshots,
        },
        videos,
        platforms: (g.platforms || []).map(p => p.name).filter(Boolean),
    };
}

// ─── Merge already-normalized fetchIgdbGameData() result with SGDB images ────
//
// fetchIgdbGameData() in igdbResolver.js already normalizes the raw IGDB wire
// object into the same schema that _normalizeMeta() would produce.  The only
// thing left to do is overlay the SteamGridDB images (cover/hero/logo) exactly
// as _normalizeMeta() does, without re-reading IGDB raw fields that no longer
// exist on the object.
//
// Input  (fullGameData from fetchIgdbGameData):
//   { igdbId, title, slug, releaseYear, description, storyline,
//     genres[], tags[], developer, publisher, ratings[],
//     images: { cover, logo, artworks[], screenshots[] },
//     videos[{ url, thumb, title }], platforms[] }
//
// Output: same shape — ready for _sanitizePayload() and _hasUsableTextMeta().

function _mergeNormalizedWithSgdb(normalizedData, sgdbImages) {
    // Deep-clone images so we do not mutate fetchIgdbGameData's return value
    const images = {
        cover:       normalizedData.images?.cover       || null,
        logo:        normalizedData.images?.logo        || null,
        hero:        null,
        artworks:    normalizedData.images?.artworks    || [],
        screenshots: normalizedData.images?.screenshots || [],
    };

    // Prefer SteamGridDB art over IGDB equivalents (same priority as _normalizeMeta)
    if (sgdbImages?.cover?.url) images.cover = sgdbImages.cover;
    if (sgdbImages?.hero?.url)  images.hero  = sgdbImages.hero;
    else {
        // Fall back to first IGDB artwork as hero if SGDB has none
        const firstArtwork = normalizedData.images?.artworks?.[0];
        if (firstArtwork?.url) images.hero = firstArtwork;
    }
    if (sgdbImages?.logo?.url)  images.logo  = sgdbImages.logo;

    return {
        igdbId:      normalizedData.igdbId,
        title:       normalizedData.title,
        slug:        normalizedData.slug,
        releaseYear: normalizedData.releaseYear,
        description: normalizedData.description,
        storyline:   normalizedData.storyline,
        genres:      normalizedData.genres      || [],
        tags:        normalizedData.tags        || [],
        developer:   normalizedData.developer   || null,
        publisher:   normalizedData.publisher   || null,
        ratings:     normalizedData.ratings     || [],
        images,
        videos:      normalizedData.videos      || [],
        platforms:   normalizedData.platforms   || [],
    };
}

// ─── Public resolver ──────────────────────────────────────────────────────────

/**
 * Resolve metadata for a non-Steam/non-Epic game by title + hints.
 * No DB reads or writes. Results are cached in-process.
 *
 * @param {{
 *   title:        string,
 *   slug?:        string,
 *   platformHint?: string,
 *   exeName?:     string,
 *   folderName?:  string,
 *   command?:     string,
 *   pathHint?:    string,
 *   igdbId?:      number,
 * }} hints
 *
 * @returns {Promise<{
 *   status:     'resolved'|'ambiguous'|'not_found',
 *   confidence: number,
 *   sources:    string[],
 *   meta:       object|null,
 *   debug: {
 *     selectedCandidate: string|null,
 *     margin:            number,
 *     scoreBreakdown:    object|null,
 *     allScores:         object[],
 *     candidatesTried:   string[],
 *   },
 * }>}
 */
async function resolveTransient(hints) {
    const title = (hints.title || '').trim();
    const requestId = randomUUID();

    // ── Structured request log ───────────────────────────────────────────────
    log.info({
        requestId,
        title,
        slug:         hints.slug         || null,
        platformHint: hints.platformHint || null,
        igdbId:       hints.igdbId       || null,
        releaseYear:  hints.releaseYear  || null,
        developer:    hints.developer    || null,
        publisher:    hints.publisher    || null,
    }, '[TransientResolver] request START');

    if (!title) {
        return {
            status:     'not_found',
            confidence: 0,
            sources:    [],
            meta:       null,
            debug:      { selectedCandidate: null, margin: 0, scoreBreakdown: null, allScores: [], candidatesTried: [] },
        };
    }

    // ── Cache check ──────────────────────────────────────────────────────────
    const ck = _cacheKey(hints);
    const cached = _cacheGet(ck);
    if (cached) {
        log.info({ requestId, title, status: cached.status }, '[TransientResolver] cache hit');
        return cached;
    }

    const candidatesTried = [];

    // ── If caller already has an IGDB ID, skip search ────────────────────────
    if (hints.igdbId) {
        log.info({ requestId, title, igdbId: hints.igdbId }, '[TransientResolver] direct IGDB ID path');
        try {
            const gameData = await fetchIgdbGameData(hints.igdbId);
            if (gameData) {
                // ── SCHEMA GUARD ──────────────────────────────────────────────────────
                // fetchIgdbGameData() returns an ALREADY-NORMALIZED object (not raw IGDB
                // wire format). Passing it into _normalizeMeta() produces silent nulls:
                //   g.name        → undefined (it's gameData.title)
                //   g.summary     → undefined (it's gameData.description)
                //   g.involved_companies → undefined (it's gameData.publisher/developer)
                //   v.video_id    → undefined (videos are already {url, thumb, title})
                // Fix: use _mergeNormalizedWithSgdb() which understands the normalized schema.
                log.info({
                    requestId,
                    igdbId:                 hints.igdbId,
                    gameData_keys:          Object.keys(gameData),
                    has_name:               'name'               in gameData,
                    has_title:              'title'              in gameData,
                    has_summary:            'summary'            in gameData,
                    has_description:        'description'        in gameData,
                    has_involved_companies: 'involved_companies' in gameData,
                    has_publisher:          'publisher'          in gameData,
                    videos_0_keys:          gameData.videos?.[0] ? Object.keys(gameData.videos[0]) : [],
                    has_video_id_in_v0:     'video_id' in (gameData.videos?.[0] ?? {}),
                    has_url_in_v0:          'url'      in (gameData.videos?.[0] ?? {}),
                }, '[TransientResolver] DEBUG schema inspection (direct-ID path)');

                const sgdbImages = await resolveSgdbImages({ igdbId: hints.igdbId, title }).catch(() => ({ cover: null, hero: null, logo: null }));
                // fetchIgdbGameData already normalized — merge SGDB art, do NOT re-normalize
                const meta       = _sanitizePayload(_mergeNormalizedWithSgdb(gameData, sgdbImages), title);
                const hasSgdb    = !!(sgdbImages?.cover || sgdbImages?.hero || sgdbImages?.logo);
                const hasText    = _hasUsableTextMeta(meta);

                log.info({
                    requestId,
                    title,
                    igdbId:          hints.igdbId,
                    matchedTitle:    meta.title,
                    igdbFetchOk:     true,
                    hasText,
                    isSgdbOnly:      !hasText && hasSgdb,
                    hasSgdbArt:      hasSgdb,
                }, '[TransientResolver] direct-ID detail');

                // Direct ID path: still enforce text-metadata requirement
                if (!hasText) {
                    log.warn({ requestId, title, igdbId: hints.igdbId }, '[TransientResolver] direct-ID result is art-only — returning art_only');
                    const result = {
                        status:     'art_only',
                        confidence: 50,
                        sources:    hasSgdb ? ['igdb', 'steamgriddb'] : ['igdb'],
                        meta:       null,
                        debug: {
                            selectedCandidate: gameData.title || null,  // normalized: .title not .name
                            margin:            50,
                            scoreBreakdown:    { directIdMatch: 50 },
                            allScores:         [],
                            candidatesTried:   [],
                            artOnlyReason:     'no_text_meta_from_igdb',
                        },
                    };
                    _cacheSet(ck, result);
                    return result;
                }

                const result = {
                    status:     'resolved',
                    confidence: 95, // direct ID match
                    sources:    ['igdb', hasSgdb ? 'steamgriddb' : null].filter(Boolean),
                    meta,
                    debug: {
                        selectedCandidate: gameData.title || null,  // normalized: .title not .name
                        margin:            95,
                        scoreBreakdown:    { directIdMatch: 95 },
                        allScores:         [],
                        candidatesTried:   [],
                    },
                };
                _cacheSet(ck, result);
                log.info({ requestId, title, igdbId: hints.igdbId, matchedTitle: meta.title }, '[TransientResolver] direct-ID resolved');
                return result;
            }
        } catch (err) {
            log.warn({ requestId, title, igdbId: hints.igdbId, err: err.message }, '[TransientResolver] direct IGDB fetch failed, falling back to search');
        }
    }

    // ── Build name candidates ─────────────────────────────────────────────────
    const raw        = title.replace(/['"®™©]/g, '').trim();
    const candidates = buildCandidates(raw);

    let bestResult = null;

    for (const candidate of candidates) {
        candidatesTried.push(candidate);
        log.info({ requestId, candidate }, '[TransientResolver] searching IGDB');

        const results = await _igdbSearch(candidate);
        log.info({
            requestId,
            candidate,
            igdbCandidateCount: results.length,
        }, '[TransientResolver] IGDB search result count');

        if (!results.length) continue;

        const picked = pickBestCandidate(results, { ...hints, title: candidate });

        log.info({
            requestId,
            candidate,
            pickerStatus:    picked.status,
            topScore:        picked.allScores[0]?.score ?? null,
            topCandidateName: picked.allScores[0]?.name ?? null,
            threshold:       45,   // mirrors THRESHOLD constant in transientConfidenceScorer
            margin:          picked.margin,
        }, '[TransientResolver] pickBestCandidate result');

        if (picked.status === 'resolved') {
            bestResult = { picked, candidateUsed: candidate };
            break;
        }

        // Track best ambiguous result so far
        if (picked.status === 'ambiguous' && (!bestResult || picked.score > bestResult.picked.score)) {
            bestResult = { picked, candidateUsed: candidate };
        }

        // Also try short name (pre-colon) for non-episodic titles
        const isEpisodic = /\bep\s*\d+\b|\bepisode\s*\d+\b|season\s*\d+/i.test(candidate);
        if (candidate.includes(':') && !isEpisodic) {
            const short = candidate.split(':')[0].trim();
            if (short.length >= 4 && !candidatesTried.includes(short)) {
                candidatesTried.push(short);
                const shortResults = await _igdbSearch(short);
                if (shortResults.length) {
                    const shortPicked = pickBestCandidate(shortResults, { ...hints, title: short });
                    if (shortPicked.status === 'resolved') {
                        bestResult = { picked: shortPicked, candidateUsed: short };
                        break;
                    }
                    if (shortPicked.status === 'ambiguous' && (!bestResult || shortPicked.score > bestResult.picked.score)) {
                        bestResult = { picked: shortPicked, candidateUsed: short };
                    }
                }
            }
        }
    }

    // ── Build response ────────────────────────────────────────────────────────

    if (!bestResult || bestResult.picked.status === 'not_found') {
        const topScore = bestResult?.picked?.allScores?.[0];
        log.info({
            requestId,
            title,
            candidatesTried,
            topCandidateName: topScore?.name ?? null,
            topScore:         topScore?.score ?? null,
            threshold:        45,
            reason:           bestResult ? 'below_threshold' : 'no_igdb_results',
        }, '[TransientResolver] not_found');

        const result = {
            status:     'not_found',
            confidence: bestResult?.picked?.score || 0,
            sources:    [],
            meta:       null,
            debug: {
                selectedCandidate: null,
                margin:            bestResult?.picked?.margin || 0,
                scoreBreakdown:    null,
                allScores:         bestResult?.picked?.allScores || [],
                candidatesTried,
            },
        };
        _cacheSet(ck, result);
        return result;
    }

    const { picked, candidateUsed } = bestResult;
    const winnerRaw = picked.winner;
    const topScore  = picked.allScores[0];

    // Fetch full IGDB data for the winner (it may have fewer fields from search)
    let fullGameData = null;
    let igdbDetailFetchOk = false;
    try {
        fullGameData = await fetchIgdbGameData(winnerRaw.id);
        igdbDetailFetchOk = !!fullGameData;
    } catch (err) {
        log.warn({ requestId, igdbId: winnerRaw.id, err: err.message }, '[TransientResolver] fetchIgdbGameData failed, using search result');
    }

    // ── DEBUG LOG 1: raw field values from both the search object and the
    //   detail fetch, printed as actual values so we can see exactly what
    //   IGDB returned vs what fetchIgdbGameData shaped it into.
    //
    //   winnerRaw  = raw IGDB game object from _igdbSearch() — real wire format,
    //                field names match the IGDB API directly (summary, storyline,
    //                involved_companies, screenshots, videos, aggregated_rating…)
    //
    //   fullGameData = PRE-SHAPED return value of fetchIgdbGameData().
    //                Field mapping inside fetchIgdbGameData (igdbResolver.js):
    //                  g.name           → fullGameData.title          (NOT .name)
    //                  g.summary        → fullGameData.description     (NOT .summary)
    //                  g.storyline      → fullGameData.storyline
    //                  involved_companies[0].company.name (developer:true)
    //                                   → fullGameData.developer
    //                  involved_companies[0].company.name (publisher:true)
    //                                   → fullGameData.publisher
    //                  g.genres[].name  → fullGameData.genres[]        (string array)
    //                  g.aggregated_rating / g.rating
    //                                   → fullGameData.ratings[]       (shaped array)
    //                  g.screenshots[]  → fullGameData.images.screenshots[]
    //                  g.videos[]       → fullGameData.videos[]
    //
    //   Key diagnostic:
    //     If winnerRaw.summary != null  AND  fullGameData.description == null
    //     → field-mapping bug in fetchIgdbGameData (Case B).
    //     If both are null
    //     → IGDB genuinely has no summary for this title (Case A).
    //     If fullGameData.title != winnerRaw.name
    //     → fetchIgdbGameData fetched a DIFFERENT record (Case C).
    log.info({
        requestId,
        // ── winnerRaw: raw IGDB search object ────────────────────────────────
        winnerRaw_id:                     winnerRaw.id,
        winnerRaw_name:                   winnerRaw.name,
        winnerRaw_summary:                winnerRaw.summary                                                ?? null,
        winnerRaw_storyline:              winnerRaw.storyline                                             ?? null,
        winnerRaw_involved_companies_len: (winnerRaw.involved_companies || []).length,
        winnerRaw_involved_companies:     (winnerRaw.involved_companies || []).map(ic => ({
                                              name:      ic.company?.name ?? null,
                                              developer: ic.developer     ?? false,
                                              publisher: ic.publisher     ?? false,
                                          })),
        winnerRaw_screenshots_len:        (winnerRaw.screenshots || []).length,
        winnerRaw_videos_len:             (winnerRaw.videos || []).length,
        winnerRaw_videos_ids:             (winnerRaw.videos || []).map(v => v.video_id ?? null),
        winnerRaw_aggregated_rating:      winnerRaw.aggregated_rating                                     ?? null,
        winnerRaw_rating:                 winnerRaw.rating                                                ?? null,
        winnerRaw_genres:                 (winnerRaw.genres || []).map(g => g?.name ?? g),
        winnerRaw_category:               winnerRaw.category                                              ?? null,
        // ── fullGameData: shaped return value of fetchIgdbGameData() ─────────
        fetchOk:                          igdbDetailFetchOk,
        fetch_title:                      fullGameData?.title                                             ?? null,
        fetch_description:                fullGameData?.description                                       ?? null,
        fetch_storyline:                  fullGameData?.storyline                                         ?? null,
        fetch_developer:                  fullGameData?.developer                                         ?? null,
        fetch_publisher:                  fullGameData?.publisher                                         ?? null,
        fetch_genres:                     fullGameData?.genres                                            ?? null,
        fetch_ratings:                    fullGameData?.ratings                                           ?? null,
        fetch_screenshots_len:            (fullGameData?.images?.screenshots || []).length,
        fetch_videos_len:                 (fullGameData?.videos || []).length,
        fetch_videos_urls:                (fullGameData?.videos || []).map(v => v.url ?? null),
        // ── Diagnostic flags ─────────────────────────────────────────────────
        // Case B: IGDB search had summary but detail fetch lost it
        DIAG_summary_lost_in_fetch:       !!(winnerRaw.summary && !fullGameData?.description),
        // Case C: detail fetch returned a different game than the search winner
        DIAG_record_mismatch:             igdbDetailFetchOk && fullGameData?.title !== winnerRaw.name,
        DIAG_fetch_name_vs_search_name:   `"${fullGameData?.title ?? 'N/A'}" vs "${winnerRaw.name}"`,
    }, '[TransientResolver] DEBUG raw IGDB detail dump');

    // ── SCHEMA-AWARE META ASSEMBLY ─────────────────────────────────────────────
    // fetchIgdbGameData() returns an already-normalized object — field names differ
    // from raw IGDB wire format. Passing fullGameData into _normalizeMeta() causes
    // every field lookup to silently return null/undefined:
    //   _normalizeMeta reads:  g.name, g.summary, g.involved_companies, v.video_id
    //   fullGameData has:      .title, .description, .developer/.publisher, v.url
    //
    // Contract enforced here:
    //   fullGameData available → _mergeNormalizedWithSgdb()  (already normalized)
    //   fullGameData missing   → _normalizeMeta(winnerRaw)   (raw IGDB wire object)
    log.info({
        requestId,
        igdbId:                 winnerRaw.id,
        fullGameData_available: igdbDetailFetchOk,
        fullGameData_keys:      fullGameData ? Object.keys(fullGameData) : [],
        has_name:               fullGameData ? ('name'               in fullGameData) : null,
        has_title:              fullGameData ? ('title'              in fullGameData) : null,
        has_summary:            fullGameData ? ('summary'            in fullGameData) : null,
        has_description:        fullGameData ? ('description'        in fullGameData) : null,
        has_involved_companies: fullGameData ? ('involved_companies' in fullGameData) : null,
        has_publisher:          fullGameData ? ('publisher'          in fullGameData) : null,
        videos_0_keys:          fullGameData?.videos?.[0] ? Object.keys(fullGameData.videos[0]) : [],
        has_video_id_in_v0:     fullGameData ? ('video_id' in (fullGameData.videos?.[0] ?? {})) : null,
        has_url_in_v0:          fullGameData ? ('url'      in (fullGameData.videos?.[0] ?? {})) : null,
        path_chosen:            fullGameData ? '_mergeNormalizedWithSgdb(fullGameData)' : '_normalizeMeta(winnerRaw)',
    }, '[TransientResolver] DEBUG schema inspection (search path)');

    // Fetch SteamGridDB images
    const sgdbImages = await resolveSgdbImages({
        igdbId: winnerRaw.id,
        title:  winnerRaw.name,
    }).catch(() => ({ cover: null, hero: null, logo: null }));

    const meta = _sanitizePayload(
        fullGameData
            ? _mergeNormalizedWithSgdb(fullGameData, sgdbImages)  // already normalized by fetchIgdbGameData
            : _normalizeMeta(winnerRaw, sgdbImages),              // raw wire object from _igdbSearch
        title
    );

    // ── DEBUG LOG 2: normalized meta values BEFORE the quality gate runs.
    //   These are the exact values _hasUsableTextMeta() will inspect.
    //   Also logs every field the gate currently IGNORES so you can decide
    //   whether the gate is too strict.
    const _hasTextDebug = _hasUsableTextMeta(meta);
    log.info({
        requestId,
        igdbId:               winnerRaw.id,
        // ── Fields _hasUsableTextMeta() CHECKS (any truthy → accepted) ───────
        meta_title:           meta.title                                    ?? null,
        meta_description:     meta.description                              ?? null,
        meta_storyline:       meta.storyline                                ?? null,
        meta_developer:       meta.developer                                ?? null,
        meta_publisher:       meta.publisher                                ?? null,
        meta_screenshots_len: (meta.images?.screenshots || []).length,
        meta_videos_len:      (meta.videos || []).length,
        hasUsableTextMeta:    _hasTextDebug,
        // ── Fields the gate currently IGNORES ────────────────────────────────
        meta_videos_urls:     (meta.videos || []).map(v => v.url ?? null),
        meta_genres:          meta.genres                                   ?? null,
        meta_ratings:         meta.ratings                                  ?? null,
        meta_has_cover:       !!(meta.images?.cover?.url),
        meta_has_hero:        !!(meta.images?.hero?.url),
        meta_has_logo:        !!(meta.images?.logo?.url),
        // ── One-line verdict ─────────────────────────────────────────────────
        DIAG_non_text_igdb_signal_exists:
            (meta.videos  || []).length > 0
            || (meta.ratings || []).length > 0
            || (meta.genres  || []).length > 0,
    }, '[TransientResolver] DEBUG normalized meta pre-quality-gate');
    const hasSgdb = !!(sgdbImages?.cover || sgdbImages?.hero || sgdbImages?.logo);
    const hasText = _hasUsableTextMeta(meta);

    // ── CRITICAL QUALITY GATE ────────────────────────────────────────────────
    // An IGDB match that returned no usable text metadata means IGDB has very
    // sparse data for this title (common for Microsoft store exclusives such as
    // Microsoft Jigsaw, Microsoft Solitaire, etc.).
    // SGDB may still have matched art by name.  That is art-only, NOT resolved.
    // Return 'art_only' so the launcher can decide whether to show partial art
    // without persisting it as a resolved game.
    const isSgdbOnly = !hasText && hasSgdb;

    log.info({
        requestId,
        title,
        matchedTitle:       meta.title,
        igdbId:             winnerRaw.id,
        igdbScore:          picked.score,
        igdbMargin:         picked.margin,
        threshold:          45,
        igdbDetailFetchOk,
        hasDescription:     !!(meta.description || meta.storyline),
        hasDeveloper:       !!meta.developer,
        hasPublisher:       !!meta.publisher,
        hasIgdbScreenshots: (meta.images?.screenshots?.length ?? 0) > 0,
        hasText,
        hasSgdbArt:         hasSgdb,
        isSgdbOnly,
        pickerStatus:       picked.status,
    }, '[TransientResolver] quality assessment');

    if (!hasText) {
        log.warn({
            requestId,
            title,
            igdbId:      winnerRaw.id,
            matchedTitle: meta.title,
            isSgdbOnly,
        }, '[TransientResolver] result is art-only (no text metadata from IGDB) — returning art_only instead of resolved');

        const result = {
            status:     'art_only',
            confidence: picked.score,
            sources:    ['igdb', hasSgdb ? 'steamgriddb' : null].filter(Boolean),
            meta:       null,   // never expose art-only payloads as `meta` — callers expect text
            debug: {
                selectedCandidate: winnerRaw.name,
                candidateUsed,
                margin:            picked.margin,
                scoreBreakdown:    topScore?.breakdown || null,
                allScores:         picked.allScores,
                candidatesTried,
                artOnlyReason:     igdbDetailFetchOk ? 'igdb_text_empty' : 'igdb_detail_fetch_failed',
            },
        };
        _cacheSet(ck, result);
        return result;
    }

    const sources = ['igdb'];
    if (hasSgdb) sources.push('steamgriddb');

    // For ambiguous results: only expose meta if it has text (already guaranteed above)
    const result = {
        status:     picked.status,   // 'resolved' or 'ambiguous'
        confidence: picked.score,
        sources,
        meta:       picked.status === 'resolved' ? meta : null,
        // For ambiguous: provide a hint so the launcher can show a picker or fallback
        ambiguousHint: picked.status === 'ambiguous' ? {
            igdbId:   winnerRaw.id,
            title:    winnerRaw.name,
            score:    picked.score,
            margin:   picked.margin,
        } : undefined,
        debug: {
            selectedCandidate: winnerRaw.name,
            candidateUsed,
            margin:            picked.margin,
            scoreBreakdown:    topScore?.breakdown || null,
            allScores:         picked.allScores,
            candidatesTried,
        },
    };

    _cacheSet(ck, result);

    log.info({
        requestId,
        title,
        status:     result.status,
        confidence: result.confidence,
        matched:    winnerRaw.name,
        igdbId:     winnerRaw.id,
        margin:     picked.margin,
        hasText,
        hasSgdbArt: hasSgdb,
    }, '[TransientResolver] resolved');

    return result;
}

module.exports = { resolveTransient };