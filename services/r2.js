'use strict';

/**
 * services/r2.js
 * Upload images to Cloudflare R2 and return the public CDN URL.
 * Uses S3-compatible API (AWS SDK v3).
 */

const { S3Client, PutObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');

const r2 = new S3Client({
    region: 'auto',
    endpoint: process.env.S3_API,
    credentials: {
        accessKeyId:     process.env.Access_Key,
        secretAccessKey: process.env.Secret_Access_Key,
    },
});

const BUCKET      = 'baddel-images';
const PUBLIC_BASE = process.env.Public_Development_URL; // e.g. https://pub-xxx.r2.dev

/**
 * Check if a key already exists in R2 (avoid re-uploading same image).
 */
async function existsInR2(key) {
    try {
        await r2.send(new HeadObjectCommand({ Bucket: BUCKET, Key: key }));
        return true;
    } catch {
        return false;
    }
}

/**
 * Fetch a remote image URL and upload it to R2 as WebP key.
 * Returns the public CDN URL, or null on failure.
 *
 * @param {string} imageUrl   - Remote image URL (Steam CDN / Epic CDN)
 * @param {string} key        - R2 object key, e.g. "covers/steam_292030.webp"
 */
async function uploadImageToR2(imageUrl, key) {
    if (!imageUrl || !key) return null;

    // Skip if already uploaded
    if (await existsInR2(key)) {
        return `${PUBLIC_BASE}/${key}`;
    }

    try {
        // Fetch the image buffer
        const res = await fetch(imageUrl, {
            headers: { 'User-Agent': 'BaddelServer/1.0' },
            redirect: 'follow',
        });

        if (!res.ok) throw new Error(`HTTP ${res.status} fetching image`);

        const buffer      = Buffer.from(await res.arrayBuffer());
        const contentType = res.headers.get('content-type') || 'image/jpeg';

        await r2.send(new PutObjectCommand({
            Bucket:      BUCKET,
            Key:         key,
            Body:        buffer,
            ContentType: contentType,
        }));

        return `${PUBLIC_BASE}/${key}`;
    } catch (err) {
        console.error(`[R2] Upload failed for key "${key}":`, err.message);
        return null;
    }
}

/**
 * Build a consistent R2 key for a game cover image.
 * e.g. covers/steam_292030.jpg  or  covers/epic_fn.jpg
 *
 * @param {string} platform  - "steam" | "epic"
 * @param {string} id        - Steam appId or Epic namespace
 * @param {string} imageUrl  - Original URL (used to detect extension)
 */
function buildCoverKey(platform, id, imageUrl) {
    const ext = (imageUrl || '').split('?')[0].match(/\.(webp|jpg|jpeg|png)$/i)?.[1] || 'jpg';
    return `covers/${platform}_${id}.${ext}`;
}

module.exports = { uploadImageToR2, buildCoverKey, existsInR2 };
