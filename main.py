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

       # 4. Generare Narațiuni (Identic cu ce ai deja)
        narratives_map = await processor.generate_secondary_narratives(top_5)

        final_events_list = []
        for idx, item in enumerate(top_5):
            slug = item.get('slug', 'history')
            year = item.get('year', 0)
            # Curățăm slug-ul pentru o căutare mai bună pe Unsplash (ex: "French_Revolution" -> "French Revolution")
            search_query = slug.replace('_', ' ')

            # --- LOGICA NOUĂ PENTRU IMAGINI HIBRIDE ---
            
            # Pas A: Încercăm să luăm o poză High-Quality de pe Unsplash/Pexels
            hero_img_url = await scraper.fetch_pro_image(search_query)
            
            # Pas B: Luăm restul pozelor de pe Wiki
            wiki_imgs = await scraper.fetch_gallery_urls(slug, limit=3)
            
            # Pas C: Combinăm - Prima e cea Pro, restul sunt Wiki
            # Folosim un set pentru a evita duplicatele dacă Wiki dă aceeași poză
            combined_urls = []
            if hero_img_url:
                combined_urls.append(hero_img_url)
            
            for w_url in wiki_imgs:
                if w_url not in combined_urls:
                    combined_urls.append(w_url)

            # Limităm galeria totală la 3-4 poze ca să nu încărcăm baza de date
            final_urls = combined_urls[:3]

            # Pas D: Upload pe Cloudinary (asincron)
            img_tasks = [safe_upload(scraper, url, f"ev_{year}_{i}") for i, url in enumerate(final_urls)]
            gallery = [img for img in await asyncio.gather(*img_tasks) if img]
            
            # Dacă galeria e goală (n-a mers nimic), punem un placeholder sau thumb-ul de la wiki
            if not gallery and item.get('wiki_thumb'):
                thumb = await safe_upload(scraper, item.get('wiki_thumb'), f"ev_{year}_fb")
                gallery = [thumb] if thumb else []

            # --- SFÂRȘIT LOGICĂ IMAGINI ---

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