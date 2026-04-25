'use strict';

/**
 * config/env.js
 *
 * Fail-fast environment validation.
 * Call validateEnv() once at startup before anything else.
 *
 * R2 variable migration (legacy → standard):
 *   Access_Key            → R2_ACCESS_KEY
 *   Secret_Access_Key     → R2_SECRET_KEY
 *   Public_Development_URL → R2_PUBLIC_URL
 *
 * Both old and new names are accepted during the transition period.
 * A deprecation warning is logged when the legacy names are detected so you
 * know exactly which secrets to rename in your Railway / CI dashboard.
 */

const REQUIRED = [
    'DATABASE_URL',
    'IGDB_CLIENT_ID',
    'IGDB_CLIENT_SECRET',
    'STEAMGRIDDB_API_KEY',
    'API_SECRET',   // Bearer token for admin/mutating routes
    'CORS_ORIGIN',  // explicit allowlist — never *
];

// ─── R2 normalization ─────────────────────────────────────────────────────────
//
// Canonical names (what the rest of the codebase reads after this runs):
//   S3_API, R2_ACCESS_KEY, R2_SECRET_KEY, R2_PUBLIC_URL
//
// Legacy aliases (still accepted, but trigger a deprecation warning):
//   Access_Key → R2_ACCESS_KEY
//   Secret_Access_Key → R2_SECRET_KEY
//   Public_Development_URL → R2_PUBLIC_URL

const R2_LEGACY_MAP = {
    'Access_Key':             'R2_ACCESS_KEY',
    'Secret_Access_Key':      'R2_SECRET_KEY',
    'Public_Development_URL': 'R2_PUBLIC_URL',
};

function _normalizeR2Vars() {
    const deprecated = [];
    for (const [legacy, canonical] of Object.entries(R2_LEGACY_MAP)) {
        if (process.env[legacy] && !process.env[canonical]) {
            process.env[canonical] = process.env[legacy];
            deprecated.push(`${legacy} → ${canonical}`);
        }
    }
    if (deprecated.length) {
        console.warn(
            '⚠️  Deprecated R2 env var names detected. Please rename in your secret store:\n' +
            deprecated.map(m => `   ${m}`).join('\n') + '\n' +
            '   The old names will be removed in a future release.'
        );
    }
}

const REQUIRED_FOR_R2 = ['S3_API', 'R2_ACCESS_KEY', 'R2_SECRET_KEY', 'R2_PUBLIC_URL'];

// ─── Main validation ──────────────────────────────────────────────────────────

function validateEnv() {
    // Normalize legacy R2 names FIRST so the checks below see the canonical names
    _normalizeR2Vars();

    const missing = REQUIRED.filter(k => !process.env[k]);

    if (missing.length > 0) {
        console.error('❌ FATAL: Missing required environment variables:');
        missing.forEach(k => console.error(`   - ${k}`));
        process.exit(1);
    }

    const r2Missing = REQUIRED_FOR_R2.filter(k => !process.env[k]);

    if (r2Missing.length > 0 && r2Missing.length < REQUIRED_FOR_R2.length) {
        // Partial R2 config is worse than no R2 config — fail hard
        console.error('❌ FATAL: Partial R2 configuration — all four vars required or none:');
        r2Missing.forEach(k => console.error(`   - ${k}`));
        process.exit(1);
    }

    if (r2Missing.length === REQUIRED_FOR_R2.length) {
        console.warn('⚠️  R2 not configured — image promotion (cdn_url) will be disabled.');
    }

    if (!process.env.CRON_SECRET) {
        console.warn('⚠️  CRON_SECRET not set — /jobs/promote-images will be disabled (fail closed).');
    }

    if (process.env.API_SECRET && process.env.API_SECRET.length < 32) {
        console.warn('⚠️  API_SECRET is shorter than 32 characters. Use a strong random secret (openssl rand -hex 32).');
    }
}

function isR2Configured() {
    return REQUIRED_FOR_R2.every(k => !!process.env[k]);
}

function isCronConfigured() {
    return !!process.env.CRON_SECRET;
}

module.exports = { validateEnv, isR2Configured, isCronConfigured };
