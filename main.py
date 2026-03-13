import asyncio
import httpx
from datetime import datetime
from core.logger import setup_logger
from core.config import config
from engine.scraper import WikiScraper
from engine.processor import AIProcessor
from engine.ranker import ScoringEngine
from schema.models import DailyPayload, EventDetail, EventCategory, Translations
import hmac
import hashlib
import time
import base64

logger = setup_logger("MainPipeline")


async def send_to_java(payload: DailyPayload):
    target_url = config.JAVA_BACKEND_URL
    secret = config.INTERNAL_API_SECRET

    events_final = []
    for ev in payload.events:
        events_final.append({
            "category": ev.category.value,
            "titleTranslations": {
                "en": ev.title_translations.en,
                "ro": ev.title_translations.ro,
                "es": ev.title_translations.es,
                "de": ev.title_translations.de,
                "fr": ev.title_translations.fr
            },
            "narrativeTranslations": {
                "en": ev.narrative_translations.en,
                "ro": ev.narrative_translations.ro,
                "es": ev.narrative_translations.es,
                "de": ev.narrative_translations.de,
                "fr": ev.narrative_translations.fr
            },
            "eventDate": ev.event_date.isoformat(),
            "impactScore": float(ev.impact_score),
            "sourceUrl": str(ev.source_url),
            "pageViews30d": int(ev.page_views_30d),
            "gallery": ev.gallery if ev.gallery else []
        })

    payload_to_serialize = {
        "dateProcessed": payload.date_processed.isoformat(),
        "events": events_final
    }

    import json
    body_json = json.dumps(payload_to_serialize, separators=(',', ':'))
    body_bytes = body_json.encode('utf-8')  # ✅ FIX 1: encode explicit la bytes

    timestamp = str(int(time.time()))
    auth_payload = f"{timestamp}.{body_json}"

    # ✅ FIX 2: hmac.new → hmac.new e corect, dar verifică importul
    signature = hmac.new(
        secret.encode('utf-8'),
        auth_payload.encode('utf-8'),
        hashlib.sha256
    ).digest()

    signature_base64 = base64.b64encode(signature).decode('utf-8')

    headers = {
        "X-Timestamp": timestamp,
        "X-Signature": signature_base64,
        "Content-Type": "application/json"
    }

    # ✅ FIX 3: Log body înainte să trimiți ca să verifici
    logger.info(f"📦 Body trimis: {body_json[:200]}...")

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            logger.info(f"📤 Sending to Java: {target_url}")
            response = await client.post(
                target_url,
                content=body_bytes,  # ✅ bytes, nu string
                headers=headers
            )

            if response.status_code in [200, 201]:
                logger.info(f"✅ SUCCESS! ID returnat: {response.text}")
            else:
                logger.error(f"❌ Status {response.status_code}: {response.text}")
                logger.error(f"❌ Body trimis era: {body_json}")
        except Exception as e:
            logger.error(f"🚨 Eroare conexiune: {e}")


async def safe_upload(scraper, url, folder_name):
    if not url: return None
    try:
        return await asyncio.to_thread(scraper.upload_to_cloudinary, url, folder_name)
    except Exception:
        return None


async def main():
    logger.info("🚀 Starting Unified Pipeline (Top 5 Unique Events)...")
    scraper, processor, ranker = WikiScraper(), AIProcessor(), ScoringEngine()

    try:
        # ... (Codul tău de Fetch, AI Scoring și Ranking rămâne identic) ...
        # [Păstrează pașii 1, 2 și 3 așa cum îi ai în codul tău]

       Eroarea IndentationError apare pentru că în Python, imediat după ce definești o funcție cu def nume_functie():, următorul rând trebuie să fie indentat (de obicei cu 4 spații sau un Tab).

Dacă ai copiat codul și rândul cu docstring-ul (textul dintre ghilimele) a rămas la aceeași margine cu def, containerul crapă.

Iată cum trebuie să arate codul tău în engine/scraper.py pentru a fi corect sintactic:

Fix pentru engine/scraper.py
Verifică linia 25-26. Trebuie să existe un spațiu clar de indentare în fața textului:

Python
# ... restul codului de sus ...

    async def fetch_pro_image(self, query: str) -> Optional[str]:
        """Caută o poză HQ pe Pexels."""  # <--- Aici trebuie indentat (4 spații)
        if not self.pexels_key:           # <--- Și aici la fel
            return None
            
        url = f"https://api.pexels.com/v1/search?query={query}&per_page=1"
        # ... restul funcției ...
Versiunea completă și sigură pentru main.py
Pentru a evita eroarea 500 (care de obicei vine din diferențe de formatare la semnătura HMAC), am „curățat” codul astfel încât să adăugăm doar logica de poze HQ, păstrând neschimbat restul.

Înlocuiește bucla de procesare a evenimentelor din main.py cu aceasta:

Python
        # 4. Generare Narațiuni (Păstrează-l pe cel existent)
        narratives_map = await processor.generate_secondary_narratives(top_5)

        final_events_list = []
        for idx, item in enumerate(top_5):
            slug = item.get('slug', 'history')
            year = item.get('year', 0)
            slug_clean = slug.replace('_', ' ') if slug else "history"

            # --- LOGICA NOUĂ: Prima poză HQ, restul Wiki ---
            
            # 1. Căutăm poza principală (Hero) pe Pexels/Unsplash
            hero_url = await scraper.fetch_pro_image(slug_clean)
            
            # 2. Luăm galeria documentară de pe Wiki
            wiki_urls = await scraper.fetch_gallery_urls(slug, limit=3)
            
            # 3. Le unim: Prima să fie cea HQ
            combined_sources = []
            if hero_url:
                combined_sources.append(hero_url)
            
            for w_url in wiki_urls:
                if w_url not in combined_sources:
                    combined_sources.append(w_url)
            
            # 4. Upload asincron (Max 3 poze per eveniment)
            img_tasks = [safe_upload(scraper, url, f"ev_{year}_{i}") for i, url in enumerate(combined_sources[:3])]
            gallery = [img for img in await asyncio.gather(*img_tasks) if img]

            # Fallback dacă totul a eșuat
            if not gallery and item.get('wiki_thumb'):
                fb_img = await safe_upload(scraper, item.get('wiki_thumb'), f"ev_{year}_fb")
                gallery = [fb_img] if fb_img else []

            # --- FINAL LOGICĂ POZE ---

            final_events_list.append(EventDetail(
                category=EventCategory(item['category'].lower()),
                year=year,
                event_date=datetime.now().date(),
                source_url=f"https://en.wikipedia.org/wiki/{slug}",
                title_translations=Translations(**item['titles']),
                narrative_translations=Translations(**narratives_map.get(f"EVENT_{idx}", {})),
                impact_score=float(item['final_score']),
                page_views_30d=item['views'],
                gallery=gallery
            ))

        # 5. ASAMBLARE PAYLOAD (Identic cu ce ai trimis tu)
        payload = DailyPayload(
            date_processed=datetime.now().date(),
            events=final_events_list,
            metadata={"processed": len(candidates), "count": len(final_events_list)}
        )

        # 6. LOGARE ȘI 7. TRIMITERE (Identic cu ce ai trimis tu)
        compact_json = payload.model_dump_json()
        print("\n" + "=" * 20 + " JSON START (NO SECRET) " + "=" * 20)
        print(compact_json)
        print("=" * 20 + " JSON END " + "=" * 20 + "\n")

        await send_to_java(payload)

    except Exception as e:
        logger.error(f"🚨 Pipeline Crash: {e}", exc_info=True)