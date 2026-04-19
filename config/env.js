'use strict';

/**
 * config/env.js
 *
 * Fail-fast environment validation.
 * Call validateEnv() once at startup before anything else.
 * Groups secrets by severity so we can warn on optional vs hard-fail on critical.
 */

const REQUIRED = [
    'DATABASE_URL',
    'IGDB_CLIENT_ID',
    'IGDB_CLIENT_SECRET',
    'STEAMGRIDDB_API_KEY',
    'API_SECRET',          // auth for admin/mutating routes
    'CORS_ORIGIN',         // explicit allowlist, not *
];

const REQUIRED_FOR_R2 = [
    'S3_API',
    'Access_Key',
    'Secret_Access_Key',
    'Public_Development_URL',
];

const CRON_WARN = ['CRON_SECRET'];

function validateEnv() {
    const missing = [];

    for (const key of REQUIRED) {
        if (!process.env[key]) missing.push(key);
    }

    const r2Missing = REQUIRED_FOR_R2.filter(k => !process.env[k]);

    if (missing.length > 0) {
        console.error('❌ FATAL: Missing required environment variables:');
        missing.forEach(k => console.error(`   - ${k}`));
        process.exit(1);
    }

    if (r2Missing.length > 0 && r2Missing.length < REQUIRED_FOR_R2.length) {
        // partial R2 config is worse than no R2 config
        console.error('❌ FATAL: Partial R2 configuration (all or none required):');
        r2Missing.forEach(k => console.error(`   - ${k}`));
        process.exit(1);
    }

    if (r2Missing.length === REQUIRED_FOR_R2.length) {
        console.warn('⚠️  R2 not configured — image promotion (cdn_url) will be disabled.');
    }

    if (!process.env.CRON_SECRET) {
        // Fail closed: cron route will be disabled, not open
        console.warn('⚠️  CRON_SECRET not set — /jobs/promote-images will be disabled.');
    }

    // Warn on obviously weak API_SECRET
    if (process.env.API_SECRET && process.env.API_SECRET.length < 32) {
        console.warn('⚠️  API_SECRET is shorter than 32 characters. Use a strong random secret.');
    }
}

function isR2Configured() {
    return REQUIRED_FOR_R2.every(k => !!process.env[k]);
}

function isCronConfigured() {
    return !!process.env.CRON_SECRET;
}

module.exports = { validateEnv, isR2Configured, isCronConfigured };
