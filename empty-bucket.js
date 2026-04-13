require('dotenv').config();
const { S3Client, ListObjectsV2Command, DeleteObjectsCommand } = require('@aws-sdk/client-s3');

const r2 = new S3Client({
    region: 'auto',
    endpoint: process.env.S3_API,
    credentials: {
        accessKeyId: process.env.Access_Key,
        secretAccessKey: process.env.Secret_Access_Key,
    },
});

const BUCKET = 'baddel-media';

async function emptyBucket() {
    let isTruncated = true;
    let continuationToken = undefined;
    let totalDeleted = 0;

    console.log(`🗑️ جاري تفريغ الـ Bucket: ${BUCKET}...`);

    while (isTruncated) {
        // 1. جلب دفعة من الملفات (أقصى حاجة 1000 في الطلب الواحد)
        const listRes = await r2.send(new ListObjectsV2Command({
            Bucket: BUCKET,
            ContinuationToken: continuationToken,
        }));

        if (!listRes.Contents || listRes.Contents.length === 0) {
            break;
        }

        // 2. تجهيز الملفات للمسح
        const objectsToDelete = listRes.Contents.map(obj => ({ Key: obj.Key }));

        // 3. مسح الدفعة
        await r2.send(new DeleteObjectsCommand({
            Bucket: BUCKET,
            Delete: { Objects: objectsToDelete }
        }));

        totalDeleted += objectsToDelete.length;
        console.log(`✅ تم مسح ${totalDeleted} ملف...`);

        // 4. التحقق إذا كان في ملفات تانية لسه متمسحتش
        isTruncated = listRes.IsTruncated;
        continuationToken = listRes.NextContinuationToken;
    }

    console.log('🎉 الـ Bucket بقى فاضي تماماً وتقدر تمسحه من الداشبورد دلوقتي لو عايز (أو تسيبه زي ما هو وتستخدمه من جديد).');
}

emptyBucket().catch(console.error);