"""
إرسال إعلانات تلقائياً كل 60 ثانية (دقيقة واحدة) إلى مجموعات محددة
"""

from telethon import TelegramClient
import asyncio
import logging

# إعداد السجل
logging.basicConfig(format='[%(levelname)s] %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# بيانات الدخول
api_id = 22043994
api_hash = '56f64582b363d367280db96586b97801'
phone_number = '+967772997043'

# المجموعات المستهدفة فقط
target_groups = [
    "https://t.me/nasdygsnnz",
    "https://t.me/ab12342030",
    "https://t.me/DigitalSAMAA",
    "https://t.me/RASF91",
    "https://t.me/+D3PKvv5Yvew4M2Jk",
    "https://t.me/+7j-xqCFEiYE5NTc0",
    "https://t.me/sultanu1999",
    "https://t.me/salla_pool",
    "https://t.me/Taif64",
    "https://t.me/groupIAU",
    "https://t.me/universty_taif11",
    "https://t.me/Maths_genius2",
    "https://t.me/ksucpy",
    "https://t.me/Tu_English2",
    "https://t.me/uiifidii",
    "https://t.me/mtager545",
    "https://t.me/bdydbeu",
    "https://t.me/sdgghjklv",
    "https://t.me/httpsLjsIIb3S3nIwMzVk"
]

# نص الإعلان
ad_message = """مركز سرعة انجاز 📚للخدمات الطلابية والاكاديمية.   نقدم لكم الخدمات التالية 
ونضمن لكم الدرجة الكاملة 

*خدمات التقنية والبرمجة*
كل ما يخص التقنية وعلوم الحاسوب ولغات وأدوات البرمجة.
برمجة وتطوير مواقع الويب باستخدام HTML, CSS, JavaScript, PHP native, Laravel, MySQL, ASP.NET, SQL Server.
التعامل مع التقنيات الحديثة مثل Bootstrap5, JSON, AJAX, jQuery, Firebase, وAPI Firebase.
تطوير مواقع باستخدام Wordpress.
تصميم وتطوير مواقع إلكترونية للشركات أو المؤسسات أو للأعمال التجارية.
برمجة أنظمة إدارة المهام بين الموظفين وتحسين الإنتاجية.
أنظمة لإدارة الهيكل التنظيمي ومتابعة الأداء.
لوحات تحكم لمتابعة التقارير والطلبات.
برمجة متاجر إلكترونية باحترافية مع تحسين تجربة المستخدم.
رفع المواقع على استضافة موثوقة وعرضها بسرعة وكفاءة.
تحسين ظهور المواقع في نتائج البحث على جوجل (SEO).
تقديم استشارات برمجية وتقنية.
دعم فني وصيانة للمواقع والتطبيقات.
*خدمات طلابية متكاملة*
حل الواجبات الجامعية بجودة عالية.
حل اختبارات كويز، ميد، فاينل مع ضمان العلامة الكاملة.
حل الأسايمنت والبروجكت والتقارير والتكاليف.
تلخيص المقررات وتقديمها بأسلوب منظم.
تصميم عروض بوربوينت متميزة.
كتابة بحوث متكاملة باللغتين مع تنسيق أكاديمي رائع.
طرح عناوين وتجهيز مقترحات رسائل الماجستير والدكتوراه وبحوث الترقية.
إجراء التحليل الإحصائي لبيانات الرسائل.
إعداد عروض PowerPoint احترافية للأبحاث والمشاريع.
*خدمات خاصة إضافية*
حل سيسكو (Cisco).
حل منصة ألف (Alef) التعليمية.
حل واجبات LMS بجودة واحترافية.
تفريغ المحتوى اليدوي إلى ملفات Word أو PDF.
إنشاء الخرائط الذهنية للمقررات والدورات.
🚨 سكليفك الطبي… أسرع مما تتخيل! 🚨
📍 مركز سرعة إنجاز – خبرة وأمانة وسرعة في خدمتك

🩺 سواء كنت عسكري – مدني – طالب…
📄 نوفر لك خدمة استخراج سكليف صحتي بكل احترافية وفي وقت قياسي، بدون عناء أو تأخير!

✨ مميزات خدمتنا:
✅ سرعة إنجاز غير مسبوقة ⏱
✅ دقة ومطابقة للمواصفات المطلوبة 📋
✅ تعامل سري وآمن 100% 🔒
✅ خدمة في جميع مناطق المملكة 🇸🇦

📞 تواصل معنا الآن:
📲 https://wa.me/+966510349663
*بعض أعمالي السابقة وردود الطلاب عن اعمالنا تجدونها علي الرابط التالي*.  https://surraenjazblog.wordpress.com/
"""

# إنشاء العميل
client = TelegramClient("session_name", api_id, api_hash)

async def send_ads():
    await client.start(phone_number)
    logger.info("✅ تم تسجيل الدخول... بدأ الإرسال")

    while True:
        for link in target_groups:
            try:
                entity = await client.get_entity(link)
                await client.send_message(entity, ad_message, link_preview=False)
                logger.info(f"📨 تم الإرسال إلى: {link}")
            except Exception as e:
                logger.error(f"❌ خطأ عند الإرسال إلى {link}: {e}")

        logger.info("⏳ سيتم الإرسال مرة أخرى بعد 60 ثانية")
        await asyncio.sleep(60)  # 60 ثانية = دقيقة واحدة

if __name__ == "__main__":
    asyncio.run(send_ads())

