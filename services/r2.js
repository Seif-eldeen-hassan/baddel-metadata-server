'use strict';

/**
 * services/r2.js
 * Upload images to Cloudflare R2.
 * All fetching goes through safeFetch() which enforces:
 *   HTTPS-only, allowlist, DNS/private-IP check, hop-by-hop redirect validation,
 *   image content-type, 10MB size, 15s timeout.
 */

const { S3Client, PutObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const { createHash } = require('crypto');
const { safeFetch }      = require('../config/ssrf');
const { isR2Configured } = require('../config/env');
const log                = require('../config/logger');

let sharp;
try { sharp = require('sharp'); sharp.cache(false); sharp.concurrency(1); }
catch { sharp = null; }

let _r2 = null;
function getR2() {
    if (_r2) return _r2;
    if (!isR2Configured()) throw new Error('R2 not configured');
    _r2 = new S3Client({
        region: 'auto', endpoint: process.env.S3_API,
        credentials: { accessKeyId: process.env.Access_Key, secretAccessKey: process.env.Secret_Access_Key },
    });
    return _r2;
}

const BUCKET      = 'baddel-media';
const PUBLIC_BASE = () => process.env.Public_Development_URL;
const MAX_BYTES   = 10 * 1024 * 1024;

// ─── Key generation ───────────────────────────────────────────────────────────

function buildImageKey(row) {
    if (!row?.id || !row.url) throw new Error('buildImageKey: row must have id and url');
    const urlHash = createHash('sha256').update(row.url).digest('hex').slice(0, 8);
    const type    = (row.image_type || 'img').replace(/[^a-z0-9_-]/gi, '');
    const source  = (row.source     || 'unk').replace(/[^a-z0-9_-]/gi, '');
    return `covers/${source}_${type}_${urlHash}_${row.id}.webp`;
}

function buildCoverKey(platform, id, imageUrl) {
    const urlHash = createHash('sha256').update(imageUrl || '').digest('hex').slice(0, 8);
    return `covers/${platform}_${id}_${urlHash}.webp`;
}

async function existsInR2(key) {
    if (!isR2Configured()) return false;
    try { await getR2().send(new HeadObjectCommand({ Bucket: BUCKET, Key: key })); return true; }
    catch { return false; }
}

// ─── Core upload ─────────────────────────────────────────────────────────────

async function uploadImageToR2(imageUrl, key) {
    if (!imageUrl || !key) return null;
    if (!isR2Configured()) { log.warn({ key }, '[R2] skipped — not configured'); return null; }
    if (!sharp)             { log.warn({ key }, '[R2] skipped — sharp not installed'); return null; }
    if (await existsInR2(key)) return `${PUBLIC_BASE()}/${key}`;

    // safeFetch handles: SSRF guard, DNS check, redirect validation, content-type, size, timeout
    let res;
    try {
        res = await safeFetch(imageUrl);
    } catch (err) {
        log.warn({ key, url: imageUrl, err: err.message }, '[R2] safeFetch rejected');
        return null;
    }

    let originalBuffer;
    try {
        originalBuffer = Buffer.from(await res.arrayBuffer());
        if (originalBuffer.byteLength > MAX_BYTES)
            throw new Error(`Body size ${originalBuffer.byteLength} exceeds limit`);
    } catch (err) {
        log.warn({ key, err: err.message }, '[R2] body read failed');
        return null;
    }

    let webpBuffer;
    try {
        webpBuffer = await sharp(originalBuffer).webp({ quality: 80 }).toBuffer();
    } catch (err) {
        log.warn({ key, err: err.message }, '[R2] sharp conversion failed');
        return null;
    } finally { originalBuffer = null; }

    try {
        await getR2().send(new PutObjectCommand({ Bucket: BUCKET, Key: key, Body: webpBuffer, ContentType: 'image/webp' }));
        webpBuffer = null;
        return `${PUBLIC_BASE()}/${key}`;
    } catch (err) {
        log.error({ key, err: err.message }, '[R2] PutObject failed');
        return null;
    }
}

module.exports = { uploadImageToR2, buildCoverKey, buildImageKey, existsInR2 };
