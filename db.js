const { Pool } = require('pg');
require('dotenv').config();

// إعداد الاتصال بقاعدة البيانات
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: {
        rejectUnauthorized: false // السطر ده مهم جداً عشان Supabase بيطلب اتصال مشفر
    }
});

// اختبار سريع للاتصال أول ما السيرفر يشتغل
pool.connect((err, client, release) => {
    if (err) {
        return console.error('❌ Error acquiring client', err.stack);
    }
    console.log('✅ Successfully connected to Supabase Database!');
    release(); // تحرير الاتصال بعد النجاح
});

module.exports = pool;