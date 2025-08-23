# auto_ad.py
from telethon import TelegramClient
import asyncio
import logging

# إعداد التسجيل (logs)
logging.basicConfig(format='[%(levelname)s] %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# بياناتك من my.telegram.org
api_id = 22043994
api_hash = '56f64582b363d367280db96586b97801'
phone_number = '+967772997043'

# ملف المجموعات
file_name = "group_ids.txt"

# روابط المجموعات المحددة
target_usernames = [
    'sultanu1999',
    'salla_pool', 
    'Taif64',
    'groupIAU',
    'universty_taif11',
    'Maths_genius2',
    'ksucpy',
    'Tu_English2',
    'uiifidii',
    'mtager545',
    'bdydbeu',
    'sdgghjklv',
    'httpsLjsIIb3S3nIwMzVk',

    # المجموعات الجديدة
    'nasdygsnnz',
    'ab12342030',
    'DigitalSAMAA',
    'RASF91',
    'https://t.me/+D3PKvv5Yvew4M2Jk'
]

# رابط المجموعة الخاصة
private_group_link = 'https://t.me/+7j-xqCFEiYE5NTc0'

# مجموعة برسالة مختلفة
special_group = 'IATC2'

# نص الإعلان اللي راح يتكرر
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

# رسالة خاصة لمجموعة IATC2
special_message = """🚨 سكليفك الطبي… أسرع مما تتخيل! 🚨
📍 مركز سرعة إنجاز – خبرة وأمانة وسرعة في خدمتك

🩺 سواء كنت عسكري – مدني – طالب…
📄 نوفر لك خدمة استخراج سكليف صحتي بكل احترافية وفي وقت قياسي، بدون عناء أو تأخير!

✨ مميزات خدمتنا:
✅ سرعة إنجاز غير مسبوقة ⏱
✅ دقة ومطابقة للمواصفات المطلوبة 📋
✅ تعامل سري وآمن 100% 🔒
✅ خدمة في جميع مناطق المملكة 🇸🇦

📞 تواصل معنا الآن:
📲 واتساب الرقم 510349663"""

# إنشاء عميل
client = TelegramClient('session_name', api_id, api_hash)

# تحميل قائمة المجموعات من الملف
def load_group_ids():
    try:
        with open(file_name, "r", encoding="utf-8") as f:
            return [int(line.strip()) for line in f if line.strip()]
    except Exception as e:
        logger.error(f"⚠️ خطأ عند قراءة الملف: {e}")
        return []

async def send_ads():
    await client.start(phone_number)
    logger.info("✅ البوت بدأ العمل...")

    while True:
        group_ids = load_group_ids()
        if not group_ids:
            logger.warning("⚠️ لا توجد مجموعات في الملف!")
        # إرسال إلى المجموعات من الملف (إن وجدت)
        for group_id in group_ids:
            try:
                await client.send_message(group_id, ad_message, link_preview=False)
                logger.info(f"📨 تم إرسال الإعلان إلى المجموعة: {group_id}")
            except Exception as e:
                logger.error(f"❌ خطأ عند إرسال الإعلان للمجموعة {group_id}: {e}")
        
        # إرسال إلى المجموعات المحددة بالأسماء
        for username in target_usernames:
            try:
                await client.send_message(username, ad_message, link_preview=False)
                logger.info(f"📨 تم إرسال الإعلان إلى {username}")
            except Exception as e:
                logger.error(f"❌ خطأ عند إرسال الإعلان لـ {username}: {e}")
        
        # إرسال إلى المجموعة الخاصة
        try:
            entity = await client.get_entity(private_group_link)
            await client.send_message(entity, ad_message, link_preview=False)
            logger.info(f"📨 تم إرسال الإعلان إلى المجموعة الخاصة")
        except Exception as e:
            logger.error(f"❌ خطأ عند إرسال الإعلان للمجموعة الخاصة: {e}")
        
        # إرسال إلى المجموعة الخاصة برسالة مختلفة
        try:
            await client.send_message(special_group, special_message, link_preview=False)
            logger.info(f"📨 تم إرسال الرسالة الخاصة إلى @{special_group}")
        except Exception as e:
            logger.error(f"❌ خطأ عند إرسال الرسالة الخاصة لـ @{special_group}: {e}")
        
        logger.info("⏳ سيعاد الإرسال بعد دقيقة واحدة...")
        await asyncio.sleep(60)  # 60 ثانية = دقيقة واحدة

if __name__ == "__main__":
    asyncio.run(send_ads())
