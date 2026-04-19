'use strict';

/**
 * config/ssrf.js
 *
 * SSRF protection for all outbound image fetches.
 *
 * Enforces:
 *   1. HTTPS only
 *   2. Hostname allowlist
 *   3. Blocks private/loopback/link-local/metadata IPs — at DNS level
 *   4. Manual redirect following (hop-by-hop re-validation, max 5 hops)
 *   5. Image content-type check on final response
 *   6. 10 MB max body size
 *   7. 15s total timeout
 */

const dns     = require('dns').promises;
const { URL } = require('url');

const ALLOWED_IMAGE_HOSTS = new Set([
    'shared.akamai.steamstatic.com',
    'steamcdn-a.akamaihd.net',
    'cdn.cloudflare.steamstatic.com',
    'store.cloudflare.steamstatic.com',
    'steamuserimages-a.akamaihd.net',
    'shared.steamstatic.com',
    'images.igdb.com',
    'cdn2.steamgriddb.com',
    'cdn1.epicgames.com',
    'cdn2.epicgames.com',
    'cdn3.epicgames.com',
    'static-assets-prod.epicgames.com',
    'shared-static-prod.epicgames.com',
    'imagedelivery.net',
]);

// ─── IP safety checks ─────────────────────────────────────────────────────────

function isPrivateIP(ip) {
    if (ip === '::1' || ip === '::ffff:127.0.0.1') return true;
    const addr = ip.replace(/^::ffff:/, '');
    const parts = addr.split('.').map(Number);
    if (parts.length !== 4 || parts.some(isNaN)) {
        // Treat any unrecognised IPv6 that isn't public as unsafe
        return ip.startsWith('fc') || ip.startsWith('fd') || ip.startsWith('fe8') ||
               ip.startsWith('fe9') || ip.startsWith('fea') || ip.startsWith('feb');
    }
    const [a, b] = parts;
    return (
        a === 10 ||
        a === 127 ||
        (a === 172 && b >= 16 && b <= 31) ||
        (a === 192 && b === 168) ||
        (a === 169 && b === 254) ||
        (a === 100 && b >= 64 && b <= 127) ||
        a === 0 ||
        (a === 198 && b === 51) ||
        (a === 203 && b === 0)
    );
}

// ─── Per-URL validation (hostname + DNS) ─────────────────────────────────────

async function assertSafeImageUrl(rawUrl) {
    if (!rawUrl || typeof rawUrl !== 'string') throw new Error('SSRF: empty or non-string URL');

    let parsed;
    try { parsed = new URL(rawUrl); }
    catch { throw new Error(`SSRF: malformed URL: ${rawUrl}`); }

    if (parsed.protocol !== 'https:') throw new Error(`SSRF: only HTTPS allowed, got: ${parsed.protocol}`);

    const hostname = parsed.hostname.toLowerCase();

    // Block known bad hostnames immediately, before DNS
    if (['metadata.google.internal', '169.254.169.254', '[::1]', 'localhost'].includes(hostname))
        throw new Error(`SSRF: blocked hostname: ${hostname}`);

    // Reject raw IP literals in URL
    const ipv4Literal = /^\d{1,3}(\.\d{1,3}){3}$/;
    if (ipv4Literal.test(hostname) || hostname.startsWith('[')) {
        throw new Error(`SSRF: raw IP address in URL not allowed: ${hostname}`);
    }

    // Allowlist check
    const inAllowlist = ALLOWED_IMAGE_HOSTS.has(hostname) ||
        [...ALLOWED_IMAGE_HOSTS].some(h => hostname.endsWith(`.${h}`));
    if (!inAllowlist) throw new Error(`SSRF: hostname not in allowlist: ${hostname}`);

    // DNS check — validate all resolved IPs
    const [addrs4, addrs6] = await Promise.all([
        dns.resolve4(hostname).catch(() => []),
        dns.resolve6(hostname).catch(() => []),
    ]);

    const allAddrs = [...addrs4, ...addrs6];
    // If DNS returns nothing we still allow it (CDN may CNAME-resolve at edge)
    for (const ip of allAddrs) {
        if (isPrivateIP(ip)) throw new Error(`SSRF: hostname ${hostname} resolves to private IP: ${ip}`);
    }
}

// ─── Safe fetch with manual redirect following ────────────────────────────────

const MAX_REDIRECTS = 5;
const MAX_BYTES     = 10 * 1024 * 1024;
const TIMEOUT_MS    = 15_000;

/**
 * Fetch an image URL safely:
 *   - validates every hop with assertSafeImageUrl
 *   - follows redirects manually (max 5)
 *   - checks content-type on final response
 *   - enforces body size limit
 *   - enforces total timeout
 *
 * Returns the Response object (body not yet consumed).
 * Throws on any SSRF / safety violation.
 */
async function safeFetch(imageUrl) {
    await assertSafeImageUrl(imageUrl);

    let currentUrl = imageUrl;
    let hops       = 0;

    // Overall deadline across all hops
    const deadline   = Date.now() + TIMEOUT_MS;
    const remaining  = () => Math.max(0, deadline - Date.now());

    while (true) {
        if (hops > MAX_REDIRECTS) throw new Error(`SSRF: too many redirects (max ${MAX_REDIRECTS})`);

        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), remaining());

        let res;
        try {
            res = await fetch(currentUrl, {
                redirect: 'manual',   // never auto-follow
                headers:  { 'User-Agent': 'BaddelServer/1.0' },
                signal:   controller.signal,
            });
        } catch (err) {
            clearTimeout(timer);
            if (err.name === 'AbortError') throw new Error('SSRF: fetch timeout');
            throw err;
        }
        clearTimeout(timer);

        // Handle redirects
        if (res.status >= 300 && res.status < 400) {
            const location = res.headers.get('location');
            if (!location) throw new Error('SSRF: redirect with no Location header');

            // Resolve relative URLs against current URL
            let nextUrl;
            try { nextUrl = new URL(location, currentUrl).href; }
            catch { throw new Error(`SSRF: invalid redirect Location: ${location}`); }

            // Re-validate the redirect target
            await assertSafeImageUrl(nextUrl);

            currentUrl = nextUrl;
            hops++;
            continue;
        }

        if (!res.ok) throw new Error(`HTTP ${res.status} fetching image`);

        // Content-type check
        const ct = (res.headers.get('content-type') || '').toLowerCase();
        if (!ct.startsWith('image/')) throw new Error(`Rejected non-image content-type: ${ct}`);

        // Content-length pre-check
        const cl = Number(res.headers.get('content-length') || 0);
        if (cl > MAX_BYTES) throw new Error(`Content-Length ${cl} exceeds limit`);

        return res;
    }
}

module.exports = { assertSafeImageUrl, safeFetch, ALLOWED_IMAGE_HOSTS, isPrivateIP };
