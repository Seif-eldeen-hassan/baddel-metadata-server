'use strict';

/**
 * services/transientConfidenceScorer.js
 *
 * Scores IGDB game candidates against a transient resolve request.
 *
 * Scoring components (all additive, some are penalties):
 *
 *   Title similarity (0–50 pts)
 *     +50  exact normalized match
 *     +40  starts-with or contained-by (with length penalty)
 *     +30  high Jaccard (≥0.8)
 *     +20  moderate Jaccard (≥0.55)
 *     +10  bigram similarity (≥0.55)
 *
 *   Slug match (0–15 pts)
 *     +15  exact slug match
 *     +8   slug starts-with or contains
 *
 *   Alternate names (0–12 pts)
 *     +12  any alt name exact match
 *     +6   alt name partial
 *
 *   Platform consistency (0–12 pts, −10 penalty)
 *     +12  platformHint maps to IGDB platform family and candidate is in it
 *     +8   no hint; candidate is on PC (Windows/Mac)
 *     +5   hint given but no family overlap; candidate still on PC
 *     −10  hint given and candidate is NOT on that platform family
 *
 *   Exe/folder hints (0–10 pts)
 *     +10  exe stem exactly matches normalised title
 *     +7   folder stem matches
 *     +4   either contains the game title
 *
 *   Release year hint (0–8 pts, −8 penalty)
 *     +8   candidate release year exactly matches hints.releaseYear
 *     +4   within ±1 year (early access / regional release offset)
 *     −8   more than 3 years apart
 *
 *   Developer / publisher hints (0–10 pts)
 *     +10  developer name exact match
 *     +8   publisher name exact match
 *     +6   developer partial match
 *     +4   publisher partial match
 *
 *   Popularity tiebreak (0–5 pts)
 *     +5   popular (total_rating_count ≥ 100) + PC
 *     +3   any popularity signal
 *
 *   Penalties
 *     −25  entry category is DLC / soundtrack / mod / season pass / edition
 *     −20  name contains demo / beta / test / editor / server / dedicated
 *     −15  large title-length mismatch (ratio < 0.4)
 *     −10  episodic mismatch (target has ep number, candidate has different one)
 *
 * Final score is clamped to [0, 100].
 * Minimum acceptance threshold: 45 / 100.
 * Ambiguity margin: best score must lead second-best by ≥ 15 to be "resolved".
 */

const THRESHOLD       = 45;
const AMBIGUITY_MARGIN = 15;

// ─── String utils (intentionally self-contained; no dep on igdbResolver) ──────

function cleanStr(s) {
    return String(s || '')
        .toLowerCase()
        .replace(/['"®™©]/g, '')
        .replace(/[^a-z0-9\s]/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

function tokenSet(s) { return new Set(cleanStr(s).split(/\s+/).filter(Boolean)); }

function jaccardSim(a, b) {
    const setA = tokenSet(a);
    const setB = tokenSet(b);
    const intersect = [...setA].filter(t => setB.has(t)).length;
    const union     = new Set([...setA, ...setB]).size;
    return union === 0 ? 0 : intersect / union;
}

function bigramSet(s) {
    const clean = cleanStr(s).replace(/\s/g, '');
    const set   = new Set();
    for (let i = 0; i < clean.length - 1; i++) set.add(clean.slice(i, i + 2));
    return set;
}

function bigramSim(a, b) {
    const ba = bigramSet(a);
    const bb = bigramSet(b);
    const intersect = [...ba].filter(g => bb.has(g)).length;
    const union     = new Set([...ba, ...bb]).size;
    return union === 0 ? 0 : intersect / union;
}

function epNumber(s) {
    const m = String(s).match(/\b(?:ep|episode|season)\s*(\d+)\b/i);
    return m ? m[1] : null;
}

// ─── Penalty keyword lists ────────────────────────────────────────────────────

const JUNK_PATTERNS = [
    /\bdemo\b/, /\bbeta\b/, /\btest\b/, /\beditor\b/, /\bserver\b/,
    /\bdedicated\b/, /\bmod\s*kit\b/, /\btool\b/, /\bsdk\b/, /\bplaytest\b/,
];

const DLC_PATTERNS = [
    /\bdlc\b/, /\bsoundtrack\b/, /\bseason\s*pass\b/, /\bbundle\b/,
    /\bedition\b/, /\bexpansion\b/, /\bpack\b/,
];

// IGDB category IDs: 1=dlc, 2=expansion, 3=bundle, 4=standalone dlc,
// 5=mod, 6=episode, 7=season, 8=remake, 9=remaster, 10=expanded game
const NON_GAME_CATEGORIES = new Set([1, 2, 3, 4, 5]);

// ─── Main scorer ──────────────────────────────────────────────────────────────

// ─── Platform hint → IGDB platform ID mapping ─────────────────────────────────
//
// Covers the launchers that reach the transient lane (non-Steam/non-Epic).
// Extend as needed; unrecognised hints fall through to the generic bonus block.
//
const PLATFORM_HINT_MAP = {
    // PC launchers / storefronts
    gog:             [6],          // Windows
    'gog galaxy':    [6],
    ubisoft:         [6],
    'ubisoft connect':[6],
    'ea app':        [6],
    'ea desktop':    [6],
    origin:          [6],
    battlenet:       [6],
    'battle.net':    [6],
    riot:            [6],
    rockstar:        [6],
    'rockstar games':[6],
    xbox:            [6, 11],      // Windows + Xbox
    'xbox game pass':[6, 11],
    'microsoft store':[6, 11],
    gamepass:        [6, 11],
    // Console families
    playstation:     [48, 167, 9, 38],  // PS4, PS5, PS3, PSP
    ps4:             [48],
    ps5:             [167],
    ps3:             [9],
    nintendo:        [130, 20, 21, 22], // Switch, NES, SNES, N64
    switch:          [130],
    'nintendo switch':[130],
    mac:             [14],
    linux:           [3],
};

// Map a raw platformHint string to a Set of IGDB platform IDs.
function _platformHintIds(rawHint) {
    if (!rawHint) return new Set();
    const key = String(rawHint).toLowerCase().trim();
    // Try exact key first, then partial match
    const exact = PLATFORM_HINT_MAP[key];
    if (exact) return new Set(exact);
    for (const [k, ids] of Object.entries(PLATFORM_HINT_MAP)) {
        if (key.includes(k) || k.includes(key)) return new Set(ids);
    }
    return new Set();
}

/**
 * Score a single IGDB candidate against the request hints.
 *
 * @param {object} candidate   — raw IGDB game object
 * @param {object} hints       — { title, slug, platformHint, exeName, folderName, releaseYear, developer, publisher }
 * @returns {{ score: number, breakdown: object }}
 */
function scoreCandidate(candidate, hints) {
    const breakdown = {};
    let score       = 0;

    const reqTitle  = hints.title  || '';
    const candName  = candidate.name || '';

    // ── 1. Title similarity ──────────────────────────────────────────────────
    const cleanReq  = cleanStr(reqTitle);
    const cleanCand = cleanStr(candName);

    if (cleanReq === cleanCand) {
        breakdown.titleExact = 50;
        score += 50;
    } else {
        const lenRatio = cleanReq.length > 0 && cleanCand.length > 0
            ? Math.min(cleanReq.length, cleanCand.length) / Math.max(cleanReq.length, cleanCand.length)
            : 0;

        if (cleanCand.startsWith(cleanReq) || cleanReq.startsWith(cleanCand)) {
            const pts = Math.round(40 * lenRatio);
            breakdown.titleStartsWith = pts;
            score += pts;
        } else {
            const jacc = jaccardSim(reqTitle, candName);
            if (jacc >= 0.8) {
                breakdown.titleJaccardHigh = 30;
                score += 30;
            } else if (jacc >= 0.55) {
                breakdown.titleJaccardMid = 20;
                score += 20;
            } else {
                const bsim = bigramSim(reqTitle, candName);
                if (bsim >= 0.55) {
                    breakdown.titleBigram = 10;
                    score += 10;
                } else {
                    breakdown.titleNoMatch = 0;
                }
            }
        }

        // Length penalty — fires when ratio is low AND we didn't already get exact/startswith
        if (
            !breakdown.titleExact &&
            !breakdown.titleStartsWith &&
            cleanReq.length > 0 && cleanCand.length > 0
        ) {
            const ratio = Math.min(cleanReq.length, cleanCand.length) / Math.max(cleanReq.length, cleanCand.length);
            if (ratio < 0.4) {
                breakdown.titleLengthPenalty = -15;
                score -= 15;
            }
        }
    }

    // ── 2. Slug match ────────────────────────────────────────────────────────
    if (hints.slug && candidate.slug) {
        const cleanSlug     = cleanStr(hints.slug);
        const cleanCandSlug = cleanStr(candidate.slug);
        if (cleanSlug === cleanCandSlug) {
            breakdown.slugExact = 15;
            score += 15;
        } else if (cleanCandSlug.includes(cleanSlug) || cleanSlug.includes(cleanCandSlug)) {
            breakdown.slugPartial = 8;
            score += 8;
        }
    }

    // ── 3. Alt name check ────────────────────────────────────────────────────
    const altNames = (candidate.alternative_names || []).map(a => a.name || a).filter(Boolean);
    if (altNames.length > 0 && reqTitle) {
        const cleanReqForAlt = cleanStr(reqTitle);
        let altBonus = 0;
        for (const alt of altNames) {
            const cleanAlt = cleanStr(alt);
            if (cleanAlt === cleanReqForAlt) { altBonus = 12; break; }
            if (cleanAlt.includes(cleanReqForAlt) || cleanReqForAlt.includes(cleanAlt)) {
                altBonus = Math.max(altBonus, 6);
            }
        }
        if (altBonus > 0) {
            breakdown.altNameMatch = altBonus;
            score += altBonus;
        }
    }

    // ── 4. Platform consistency (enhanced) ──────────────────────────────────
    const PC_IDS = new Set([6, 14]); // Windows=6, Mac=14
    const candPlatformIds = new Set((candidate.platforms || []).map(p => typeof p === 'object' ? p.id : p));

    if (candPlatformIds.size > 0) {
        const hasPc = [...PC_IDS].some(id => candPlatformIds.has(id));
        const hintIds = _platformHintIds(hints.platformHint);

        if (hintIds.size > 0) {
            const hintOverlap = [...hintIds].filter(id => candPlatformIds.has(id)).length;
            if (hintOverlap > 0) {
                breakdown.platformHintMatch = 12;
                score += 12;
            } else if (hasPc) {
                breakdown.platformHintPC = 5;
                score += 5;
            } else {
                breakdown.platformHintMismatch = -10;
                score -= 10;
            }
        } else {
            if (hasPc) {
                breakdown.platformPC = 8;
                score += 8;
            } else if (candPlatformIds.size >= 1) {
                breakdown.platformAny = 5;
                score += 5;
            }
        }
    }

    // ── 5. Exe / folder hints ────────────────────────────────────────────────
    const exeStem    = hints.exeName    ? cleanStr(hints.exeName.replace(/\.exe$/i, ''))    : null;
    const folderStem = hints.folderName ? cleanStr(hints.folderName)                         : null;

    if (exeStem && cleanCand) {
        if (exeStem === cleanCand || cleanCand.includes(exeStem) || exeStem.includes(cleanCand)) {
            breakdown.exeHint = exeStem === cleanCand ? 10 : 4;
            score += breakdown.exeHint;
        }
    } else if (folderStem && cleanCand) {
        if (folderStem === cleanCand || cleanCand.includes(folderStem) || folderStem.includes(cleanCand)) {
            breakdown.folderHint = folderStem === cleanCand ? 7 : 4;
            score += breakdown.folderHint;
        }
    }

    // ── 6. Release year hint (0–8 pts) ──────────────────────────────────────
    //
    // hints.releaseYear: number — caller may supply year from file metadata,
    // Steam store page, or any other out-of-band signal.
    // candidate.first_release_date: Unix timestamp (seconds).
    if (hints.releaseYear && candidate.first_release_date) {
        const candYear = new Date(candidate.first_release_date * 1000).getFullYear();
        if (candYear === hints.releaseYear) {
            breakdown.releaseYearExact = 8;
            score += 8;
        } else if (Math.abs(candYear - hints.releaseYear) === 1) {
            // Allow ±1 year tolerance (international release offset, early-access dates)
            breakdown.releaseYearClose = 4;
            score += 4;
        } else if (Math.abs(candYear - hints.releaseYear) <= 3) {
            // Distant mismatch still within remaster/anniversary window — neutral
        } else {
            // Far-off year is a meaningful mismatch signal
            breakdown.releaseYearMismatch = -8;
            score -= 8;
        }
    }

    // ── 7. Developer / publisher hints (0–10 pts) ────────────────────────────
    //
    // hints.developer / hints.publisher: strings from launcher manifest,
    // install folder metadata, or Steam store data passed through by the caller.
    //
    // IGDB's involved_companies is a nested array; we normalise to plain strings.
    if (hints.developer || hints.publisher) {
        const candDevs = (candidate.involved_companies || [])
            .filter(ic => ic.developer)
            .map(ic => cleanStr(ic.company?.name || ic.company || ''))
            .filter(Boolean);
        const candPubs = (candidate.involved_companies || [])
            .filter(ic => ic.publisher)
            .map(ic => cleanStr(ic.company?.name || ic.company || ''))
            .filter(Boolean);

        const devHint = hints.developer ? cleanStr(hints.developer) : null;
        const pubHint = hints.publisher ? cleanStr(hints.publisher) : null;

        let companyBonus = 0;

        if (devHint && candDevs.length > 0) {
            if (candDevs.includes(devHint)) {
                companyBonus = Math.max(companyBonus, 10);
            } else if (candDevs.some(d => d.includes(devHint) || devHint.includes(d))) {
                companyBonus = Math.max(companyBonus, 6);
            }
        }

        if (pubHint && candPubs.length > 0) {
            if (candPubs.includes(pubHint)) {
                companyBonus = Math.max(companyBonus, 8);
            } else if (candPubs.some(p => p.includes(pubHint) || pubHint.includes(p))) {
                companyBonus = Math.max(companyBonus, 4);
            }
        }

        if (companyBonus > 0) {
            breakdown.companyHint = companyBonus;
            score += companyBonus;
        }
    }

    // ── 8. Popularity tiebreak (0–5 pts) ─────────────────────────────────────
    const ratingCount = candidate.total_rating_count || 0;
    const candHasPC   = [...PC_IDS].some(id => candPlatformIds.has(id));
    if (ratingCount >= 100 && candHasPC) {
        breakdown.popularPC = 5;
        score += 5;
    } else if (ratingCount > 0) {
        breakdown.popularAny = 3;
        score += 3;
    }

    // ── 9. Penalties ─────────────────────────────────────────────────────────

    // DLC / non-game category
    if (candidate.category !== undefined && NON_GAME_CATEGORIES.has(candidate.category)) {
        breakdown.categoryPenalty = -25;
        score -= 25;
    } else {
        // Name-based DLC check (if category is missing)
        const lowerCand = (candName || '').toLowerCase();
        if (DLC_PATTERNS.some(p => p.test(lowerCand))) {
            breakdown.dlcNamePenalty = -25;
            score -= 25;
        }
    }

    // Junk / dev entries
    const lowerCand = (candName || '').toLowerCase();
    if (JUNK_PATTERNS.some(p => p.test(lowerCand))) {
        breakdown.junkPenalty = -20;
        score -= 20;
    }

    // Episodic mismatch
    const reqEp  = epNumber(reqTitle);
    const candEp = epNumber(candName);
    if (reqEp && candEp && reqEp !== candEp) {
        breakdown.episodicMismatch = -10;
        score -= 10;
    }

    const finalScore = Math.max(0, Math.min(100, score));
    return { score: finalScore, breakdown };
}

/**
 * Pick the best candidate from an array of IGDB results.
 * Returns null if nothing meets the threshold.
 *
 * @param {object[]} candidates  — raw IGDB game objects
 * @param {object}   hints       — same hints object passed to scoreCandidate
 * @returns {{
 *   winner:   object|null,
 *   score:    number,
 *   margin:   number,
 *   status:   'resolved'|'ambiguous'|'not_found',
 *   allScores: Array<{id, name, score, breakdown}>,
 * }}
 */
function pickBestCandidate(candidates, hints) {
    if (!candidates?.length) {
        return { winner: null, score: 0, margin: 0, status: 'not_found', allScores: [] };
    }

    const scored = candidates
        .filter(c => c && c.name)
        .map(c => {
            const { score, breakdown } = scoreCandidate(c, hints);
            return { id: c.id, name: c.name, score, breakdown, raw: c };
        })
        .sort((a, b) => b.score - a.score);

    if (!scored.length) {
        return { winner: null, score: 0, margin: 0, status: 'not_found', allScores: [] };
    }

    const best   = scored[0];
    const second = scored[1];
    const margin = second ? best.score - second.score : best.score;

    // Expose top 5 for debugging
    const allScores = scored.slice(0, 5).map(({ id, name, score, breakdown }) => ({ id, name, score, breakdown }));

    if (best.score < THRESHOLD) {
        return { winner: null, score: best.score, margin, status: 'not_found', allScores };
    }

    if (margin < AMBIGUITY_MARGIN) {
        // Two candidates are too close — return ambiguous with the better one as a hint
        return { winner: best.raw, score: best.score, margin, status: 'ambiguous', allScores };
    }

    return { winner: best.raw, score: best.score, margin, status: 'resolved', allScores };
}

module.exports = { scoreCandidate, pickBestCandidate, THRESHOLD, AMBIGUITY_MARGIN };
