'use strict';
const { Pool } = require('pg');
require('dotenv').config();

/**
 * SSL configuration for Supabase + Railway:
 *
 * Supabase uses a self-signed / intermediate certificate that Node.js
 * rejects by default (rejectUnauthorized: true).  The connection string
 * includes ?sslmode=require which tells the pg driver to encrypt the
 * connection, but the driver's own TLS stack still verifies the cert chain
 * — and fails with "self-signed certificate in certificate chain".
 *
 * Fix: always set rejectUnauthorized: false when DATABASE_URL is present.
 * The connection is still fully encrypted (SSL is ON) — we just skip
 * certificate chain verification, which is standard practice for Supabase
 * on Railway/Render/Fly etc.
 *
 * Local dev (no DATABASE_URL): plain connection, no SSL.
 */
function buildSslConfig() {
    if (!process.env.DATABASE_URL) return false;
    return { rejectUnauthorized: false };
}

const pool = new Pool({
    connectionString: process.env.DATABASE_URL || 'postgresql://localhost/baddel_test',
    ssl: buildSslConfig(),
});

pool.on('error', (err) => {
    console.error('[DB] Unexpected pool error:', err.message, err.code);
});

module.exports = pool;