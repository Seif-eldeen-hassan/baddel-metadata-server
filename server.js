// استدعاء المكتبات الأساسية
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const db = require('./db');
// تشغيل تطبيق الإكسبريس
const app = express();

// إعدادات الحماية والـ JSON
app.use(cors()); // بيسمح للانشر بتاعك (اللي شغال على بورت مختلف) يكلم السيرفر
app.use(express.json()); // بيخلي السيرفر يفهم الداتا اللي جاية بصيغة JSON

// مسار اختبار (Test Endpoint) عشان نتأكد إن السيرفر شغال
app.get('/health', async (req, res) => {
    try {
        // بنسأل الداتا بيز عن الوقت الحالي كاختبار
        const result = await db.query('SELECT NOW()');
        res.status(200).json({ 
            status: "success", 
            message: "Baddel Metadata Server is running smoothly! 🚀",
            database_time: result.rows[0].now
        });
    } catch (error) {
        res.status(500).json({
            status: "error",
            message: "Server is running, but Database connection failed.",
            error: error.message
        });
    }
});

// تحديد البورت (المنفذ) اللي السيرفر هيشتغل عليه
const PORT = process.env.PORT || 3000;

// تشغيل السيرفر
app.listen(PORT, () => {
    console.log(`=================================`);
    console.log(`Baddel Server is alive on Port: ${PORT}`);
    console.log(`=================================`);
});