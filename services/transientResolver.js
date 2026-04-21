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
        log.info({ title }, '[TransientResolver] cache hit');
        return cached;
    }

    const candidatesTried = [];

    // ── If caller already has an IGDB ID, skip search ────────────────────────
    if (hints.igdbId) {
        log.info({ title, igdbId: hints.igdbId }, '[TransientResolver] direct IGDB ID path');
        try {
            const gameData = await fetchIgdbGameData(hints.igdbId);
            if (gameData) {
                const sgdbImages = await resolveSgdbImages({ igdbId: hints.igdbId, title }).catch(() => ({ cover: null, hero: null, logo: null }));
                const meta       = _normalizeMeta(gameData, sgdbImages);
                const result     = {
                    status:     'resolved',
                    confidence: 95, // direct ID match
                    sources:    ['igdb', sgdbImages?.cover ? 'steamgriddb' : null].filter(Boolean),
                    meta,
                    debug: {
                        selectedCandidate: gameData.name || null,
                        margin:            95,
                        scoreBreakdown:    { directIdMatch: 95 },
                        allScores:         [],
                        candidatesTried:   [],
                    },
                };
                _cacheSet(ck, result);
                return result;
            }
        } catch (err) {
            log.warn({ title, igdbId: hints.igdbId, err: err.message }, '[TransientResolver] direct IGDB fetch failed, falling back to search');
        }
    }

    // ── Build name candidates ─────────────────────────────────────────────────
    const raw        = title.replace(/['"®™©]/g, '').trim();
    const candidates = buildCandidates(raw);

    let bestResult = null;

    for (const candidate of candidates) {
        candidatesTried.push(candidate);
        log.info({ candidate }, '[TransientResolver] searching IGDB');

        const results = await _igdbSearch(candidate);
        if (!results.length) continue;

        const picked = pickBestCandidate(results, { ...hints, title: candidate });

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
        log.info({ title, candidatesTried }, '[TransientResolver] not_found');
        return result;
    }

    const { picked, candidateUsed } = bestResult;
    const winnerRaw = picked.winner;
    const topScore  = picked.allScores[0];

    // Fetch full IGDB data for the winner (it may have fewer fields from search)
    let fullGameData = null;
    try {
        fullGameData = await fetchIgdbGameData(winnerRaw.id);
    } catch (err) {
        log.warn({ igdbId: winnerRaw.id, err: err.message }, '[TransientResolver] fetchIgdbGameData failed, using search result');
    }

    const gameDataToUse = fullGameData || winnerRaw;

    // Fetch SteamGridDB images
    const sgdbImages = await resolveSgdbImages({
        igdbId: winnerRaw.id,
        title:  winnerRaw.name,
    }).catch(() => ({ cover: null, hero: null, logo: null }));

    const meta    = _normalizeMeta(gameDataToUse, sgdbImages);
    const sources = ['igdb'];
    if (sgdbImages?.cover || sgdbImages?.hero || sgdbImages?.logo) sources.push('steamgriddb');

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
        title,
        status:     result.status,
        confidence: result.confidence,
        matched:    winnerRaw.name,
        igdbId:     winnerRaw.id,
        margin:     picked.margin,
    }, '[TransientResolver] resolved');

    return result;
}

module.exports = { resolveTransient };
