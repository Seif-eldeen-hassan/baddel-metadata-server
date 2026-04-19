'use strict';
const { Pool } = require('pg');
require('dotenv').config();

/**
 * SSL configuration:
 *
 * Production (DATABASE_URL set, DB_SSL_REJECT_UNAUTHORIZED not 'false'):
 *   - Full TLS with certificate verification (rejectUnauthorized: true)
 *   - This is the correct setting for Supabase and any hosted Postgres
 *
 * Local dev override (DB_SSL_REJECT_UNAUTHORIZED=false):
 *   - Disables cert verification — only for local self-signed certs
 *   - Never set this in production
 *
 * No DATABASE_URL (local dev, no SSL):
 *   - ssl: false — plain local connection
 */
function buildSslConfig() {
    if (!process.env.DATABASE_URL) return false;          // local plain connection
    if (process.env.DB_SSL_REJECT_UNAUTHORIZED === 'false') {
        return { rejectUnauthorized: false };              // explicit dev override only
    }
    return { rejectUnauthorized: true };                  // secure default for production
}

const pool = new Pool({
    connectionString: process.env.DATABASE_URL || 'postgresql://localhost/baddel_test',
    ssl: buildSslConfig(),
});

pool.on('error', (err) => {
    console.error('[DB] Unexpected pool error:', err.message);
});

module.exports = pool;