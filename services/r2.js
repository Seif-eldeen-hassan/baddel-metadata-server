'use strict';

/**
 * services/r2.js
 * Upload images to Cloudflare R2 and return the public CDN URL.
 * Uses S3-compatible API (AWS SDK v3) with Sharp for WebP compression.
 */

const { S3Client, PutObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const sharp = require('sharp'); 
sharp.cache(false);     
sharp.concurrency(1);

const r2 = new S3Client({
    region: 'auto',
    endpoint: process.env.S3_API,
    credentials: {
        accessKeyId:     process.env.Access_Key,
        secretAccessKey: process.env.Secret_Access_Key,
    },
});

const BUCKET      = 'baddel-media';
const PUBLIC_BASE = process.env.Public_Development_URL;

async function existsInR2(key) {
    try {
        await r2.send(new HeadObjectCommand({ Bucket: BUCKET, Key: key }));
        return true;
    } catch {
        return false;
    }
}

async function uploadImageToR2(imageUrl, key) {
    if (!imageUrl || !key) return null;

    if (await existsInR2(key)) {
        return `${PUBLIC_BASE}/${key}`;
    }

    try {
        const res = await fetch(imageUrl, {
            headers: { 'User-Agent': 'BaddelServer/1.0' },
            redirect: 'follow',
        });

        if (!res.ok) throw new Error(`HTTP ${res.status} fetching image`);

        const originalBuffer = Buffer.from(await res.arrayBuffer());
        const webpBuffer = await sharp(originalBuffer)
            .webp({ quality: 80 }) 
            .toBuffer();

        await r2.send(new PutObjectCommand({
            Bucket:      BUCKET,
            Key:         key,
            Body:        webpBuffer,
            ContentType: 'image/webp',
        }));

        return `${PUBLIC_BASE}/${key}`;
    } catch (err) {
        console.error(`[R2] Upload failed for key "${key}":`, err.message);
        return null;
    }
}

function buildCoverKey(platform, id, imageUrl) {
    return `covers/${platform}_${id}.webp`;
}

module.exports = { uploadImageToR2, buildCoverKey, existsInR2 };