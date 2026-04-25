/**
 * empty-bucket.js — DESTRUCTIVE MAINTENANCE SCRIPT
 *
 * Deletes every object in the R2 bucket.
 * ⚠️  This is irreversible. Run only when you intend to wipe all stored images.
 *
 * Usage (must pass the explicit --confirm flag):
 *   node empty-bucket.js --confirm
 *
 * The --confirm flag is a deliberate friction point so the script can NEVER be
 * run accidentally (e.g. via `node .`, a misconfigured cron, or a typo).
 */

'use strict';

// ─── Safety gate — must be the very first thing that runs ────────────────────
if (!process.argv.includes('--confirm')) {
    console.error('');
    console.error('⛔  SAFETY GATE: This script deletes the entire R2 bucket.');
    console.error('    You must pass --confirm to proceed:');
    console.error('');
    console.error('      node empty-bucket.js --confirm');
    console.error('');
    process.exit(1);
}

require('dotenv').config();

const { S3Client, ListObjectsV2Command, DeleteObjectsCommand } = require('@aws-sdk/client-s3');

// ─── Validate required env vars before doing anything ────────────────────────
const REQUIRED = ['S3_API', 'R2_ACCESS_KEY', 'R2_SECRET_KEY'];
const missing  = REQUIRED.filter(k => !process.env[k]);
if (missing.length) {
    console.error('❌  Missing env vars:', missing.join(', '));
    process.exit(1);
}

const r2 = new S3Client({
    region:      'auto',
    endpoint:    process.env.S3_API,
    credentials: {
        accessKeyId:     process.env.R2_ACCESS_KEY,
        secretAccessKey: process.env.R2_SECRET_KEY,
    },
});

const BUCKET = 'baddel-media';

async function emptyBucket() {
    let isTruncated       = true;
    let continuationToken = undefined;
    let totalDeleted      = 0;

    console.log(`\n🗑️  Emptying bucket: ${BUCKET} …\n`);

    while (isTruncated) {
        const listRes = await r2.send(new ListObjectsV2Command({
            Bucket: BUCKET,
            ContinuationToken: continuationToken,
        }));

        if (!listRes.Contents?.length) break;

        const objectsToDelete = listRes.Contents.map(obj => ({ Key: obj.Key }));

        await r2.send(new DeleteObjectsCommand({
            Bucket: BUCKET,
            Delete: { Objects: objectsToDelete },
        }));

        totalDeleted += objectsToDelete.length;
        console.log(`  ✅  Deleted ${totalDeleted} objects so far …`);

        isTruncated       = listRes.IsTruncated;
        continuationToken = listRes.NextContinuationToken;
    }

    console.log(`\n🎉  Done. ${totalDeleted} objects deleted from "${BUCKET}".\n`);
}

emptyBucket().catch((err) => {
    console.error('❌  Fatal error:', err.message);
    process.exit(1);
});
