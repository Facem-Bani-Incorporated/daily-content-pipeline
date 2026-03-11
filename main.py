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

    # 1. Reconstruim lista de evenimente exact cum apar în EventDTO.java
    events_final = []
    for ev in payload.events:
        events_final.append({
            "category": ev.category.value,  # "war_conflict" etc.
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
            "eventDate": ev.event_date.isoformat(),  # "2026-03-11"
            "impactScore": float(ev.impact_score),
            "sourceUrl": str(ev.source_url),
            "pageViews30d": int(ev.page_views_30d),
            "gallery": ev.gallery if ev.gallery else []
        })

    # 2. DailyContentDTO.java vrea dateProcessed și events
    payload_to_serialize = {
        "dateProcessed": payload.date_processed.isoformat(),
        "events": events_final
    }

    import json
    # Important: separators=(',', ':') elimină spațiile pentru ca HMAC-ul să fie identic cu ce vede Java
    body_json = json.dumps(payload_to_serialize, separators=(',', ':'))

    # 3. Logica HMAC (care deja funcționează)
    timestamp = str(int(time.time()))
    auth_payload = f"{timestamp}.{body_json}"

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

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            logger.info(f"📤 Final Attempt - Sending to Java: {target_url}")
            response = await client.post(target_url, content=body_json, headers=headers)

            if response.status_code in [200, 201]:
                logger.info("✅ SUCCESS! Legătura Python -> Java este oficial funcțională.")
            else:
                # Dacă încă dă 400, înseamnă că un câmp are nume greșit (ex: eventDate vs event_date)
                logger.error(f"❌ Status {response.status_code}: {response.text}")
        except Exception as e:
            logger.error(f"🚨 Eroare: {e}")


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
        # 1. Fetch & Heuristic Rank
        raw_events = await scraper.fetch_today()
        for item in raw_events:
            item['h_score'] = ranker.heuristic_score(item)

        candidates = sorted(raw_events, key=lambda x: x['h_score'], reverse=True)[:config.MAX_CANDIDATES_FOR_AI]

        # 2. AI Scoring & Views
        ai_data = await processor.batch_score_and_categorize(candidates)
        ai_results = ai_data.get('results', {})
        view_tasks = [scraper.fetch_page_views(c.get('slug')) for c in candidates]
        views = await asyncio.gather(*view_tasks)

        # 3. Merging & Final Ranking
        for idx, item in enumerate(candidates):
            res = ai_results.get(f"ID_{idx}", {})
            item.update({
                'views': views[idx] if isinstance(views[idx], int) else 0,
                'category': res.get('category', 'culture_arts'),
                'score': res.get('score', 50),
                'titles': res.get('titles', {l: "History Event" for l in ["en", "ro", "es", "de", "fr"]})
            })
            item['final_score'] = ranker.calculate_final_score(item['h_score'], item['score'], item['views'])

        # SORTARE FINALĂ ȘI LIMITARE LA 5 EVENIMENTE DIFERITE
        candidates.sort(key=lambda x: x.get('final_score', 0), reverse=True)
        top_5 = candidates[:5]

        # 4. Generare Narațiuni (Toate odată)
        narratives_map = await processor.generate_secondary_narratives(top_5)

        final_events_list = []
        for idx, item in enumerate(top_5):
            slug = item.get('slug', 'history')
            year = item.get('year', 0)

            # Primul primește galerie, restul 1 poză
            if idx == 0:
                wiki_imgs = await scraper.fetch_gallery_urls(slug, limit=3)
                img_tasks = [safe_upload(scraper, url, f"ev_{year}_{i}") for i, url in enumerate(wiki_imgs)]
                gallery = [img for img in await asyncio.gather(*img_tasks) if img]
            else:
                thumb = await safe_upload(scraper, item.get('wiki_thumb'), f"ev_{year}")
                gallery = [thumb] if thumb else []

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

        # --- AICI ERA GREȘEALA: Codul de mai jos trebuie să fie ÎN AFARA buclei for ---

        # 5. ASAMBLARE PAYLOAD (O singură dată, cu toate cele 5)
        payload = DailyPayload(
            date_processed=datetime.now().date(),
            events=final_events_list,
            metadata={"processed": len(candidates), "count": len(final_events_list)}
        )

        # 6. LOGARE CURATĂ (Compactă pentru a evita timestamp pe fiecare linie în Railway)
        compact_json = payload.model_dump_json()
        print("\n" + "=" * 20 + " JSON START (NO SECRET) " + "=" * 20)
        print(compact_json)
        print("=" * 20 + " JSON END " + "=" * 20 + "\n")

        # 7. TRIMITERE FINALĂ
        await send_to_java(payload)

    except Exception as e:
        logger.error(f"🚨 Pipeline Crash: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())