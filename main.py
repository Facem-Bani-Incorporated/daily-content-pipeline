import asyncio
import json
import httpx
from datetime import datetime
from core.logger import setup_logger
from core.config import config
from engine.scraper import WikiScraper
from engine.processor import AIProcessor
from engine.ranker import ScoringEngine
from schema.models import DailyPayload, MainEvent, SecondaryEvent, EventCategory, Translations
from tenacity import retry, stop_after_attempt, wait_fixed

logger = setup_logger("MainPipeline")


# --- 1. FUNCTII DE UTILITATE ---

@retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
async def send_to_java(payload: DailyPayload):
    """Trimite payload-ul către backend-ul Java cu mecanism de retry."""
    headers = {
        "X-Internal-Api-Key": config.INTERNAL_API_SECRET,
        "Content-Type": "application/json"
    }
    # mode='json' asigură conversia tipurilor complexe (date, enum) în string/number
    payload_json = payload.model_dump(mode='json')

    async with httpx.AsyncClient(timeout=180.0) as client:
        logger.info(f"📤 Ingesting to Java: {config.JAVA_BACKEND_URL}")
        response = await client.post(config.JAVA_BACKEND_URL, json=payload_json, headers=headers)
        response.raise_for_status()
        return response.status_code


async def safe_upload(scraper, url, folder_name):
    """Upload securizat către Cloudinary (non-blocking)."""
    try:
        return await asyncio.to_thread(scraper.upload_to_cloudinary, url, folder_name)
    except Exception as e:
        logger.warning(f"⚠️ Cloudinary Upload Failed for {url}: {e}")
        return None


# --- 2. PIPELINE-UL PRINCIPAL ---

async def main():
    logger.info("🚀 Starting Elite Daily Pipeline (Debug Mode)...")
    # Text de siguranță în caz că AI-ul ratează o traducere
    default_langs = {l: "Translation in progress..." for l in ["en", "ro", "es", "de", "fr"]}

    scraper = WikiScraper()
    processor = AIProcessor()
    ranker = ScoringEngine()

    try:
        # STEP 1: FETCH EVENIMENTE DE PE WIKIPEDIA
        raw_events = await scraper.fetch_today()
        if not raw_events:
            logger.error("❌ No events found on Wikipedia!")
            return

        # STEP 2: PRE-SCORING (Heuristics)
        for item in raw_events:
            item['h_score'] = ranker.heuristic_score(item)

        candidates = sorted(raw_events, key=lambda x: x['h_score'], reverse=True)[:config.MAX_CANDIDATES_FOR_AI]

        # STEP 3: AI BATCH CATEGORIZATION & TITLES
        logger.info(f"🤖 AI analyzing {len(candidates)} candidates for ranking...")
        ai_data = await processor.batch_score_and_categorize(candidates)
        results = ai_data.get('results', {})

        # STEP 4: FETCH PAGE VIEWS (Paralel)
        view_tasks = [scraper.fetch_page_views(c.get('slug')) for c in candidates]
        views = await asyncio.gather(*view_tasks)

        # STEP 5: CALCUL FINAL SCORE & MAPPING
        for idx, item in enumerate(candidates):
            res = results.get(f"ID_{idx}", {})
            item['views'] = views[idx] if isinstance(views[idx], int) else 0
            item['category'] = res.get('category', 'culture')
            item['ai_impact'] = res.get('score', 50)
            item['titles'] = res.get('titles', default_langs)
            item['final_score'] = ranker.calculate_final_score(item['h_score'], item['ai_impact'], item['views'])

        # Sortăm elitele: Locul 0 = Main, Locurile 1-5 = Secondary
        candidates.sort(key=lambda x: x.get('final_score', 0), reverse=True)

        # STEP 6: MAIN EVENT - GENERARE NARAȚIUNE PREMIUM (400 CUVINTE)
        top_data = candidates[0]
        logger.info(f"✨ Generating Premium Content for: {top_data.get('titles', {}).get('en')}")

        main_content_task = processor.generate_multilingual_main_event(top_data)

        # Galerie Main
        slug_main = top_data.get('slug', 'history')
        wiki_imgs = await scraper.fetch_gallery_urls(slug_main, limit=3)
        img_tasks = [safe_upload(scraper, url, f"main_{top_data['year']}_{i}") for i, url in enumerate(wiki_imgs)]

        # Așteptăm AI-ul și Upload-urile simultan
        main_content, *main_gallery_results = await asyncio.gather(main_content_task, *img_tasks)
        main_gallery = [img for img in main_gallery_results if img]

        # STEP 7: SECONDARY EVENTS - GENERARE NARAȚIUNI MEDII (200 CUVINTE)
        secondary_pool = candidates[1:6]
        logger.info(f"✍️ Generating secondary narratives for {len(secondary_pool)} events...")
        sec_narratives_map = await processor.generate_secondary_narratives(secondary_pool)

        secondary_objs = []
        for idx, item in enumerate(secondary_pool):
            # Mapare cheie EVENT_0, EVENT_1... (trebuie să bată cu processor.py)
            key = f"EVENT_{idx}"
            narratives = sec_narratives_map.get(key, default_langs)

            # Imagine eveniment secundar
            raw_thumb = item.get('wiki_thumb') or "https://images.unsplash.com/photo-1447069387593-a5de0862481e?w=400"
            thumb_url = await safe_upload(scraper, raw_thumb, f"sec_{item['year']}_{idx}")

            secondary_objs.append(SecondaryEvent(
                title_translations=Translations(**item.get('titles', default_langs)),
                year=item['year'],
                source_url=f"https://en.wikipedia.org/wiki/{item.get('slug', '')}",
                thumbnail_url=thumb_url,
                ai_relevance_score=float(item.get('final_score', 0)),
                narrative_translations=Translations(**narratives)
            ))

        # STEP 8: ASAMBLARE PAYLOAD FINAL CONFORM SCHEMEI
        payload = DailyPayload(
            date_processed=datetime.now().date(),
            api_secret=config.INTERNAL_API_SECRET,
            main_event=MainEvent(
                category=EventCategory(top_data['category'].lower()),
                page_views_30d=top_data.get('views', 0),
                title_translations=Translations(**main_content.get('titles', default_langs)),
                year=top_data['year'],
                source_url=f"https://en.wikipedia.org/wiki/{slug_main}",
                event_date=datetime.now().date(),
                narrative_translations=Translations(**main_content.get('narratives', default_langs)),
                impact_score=float(top_data['final_score']),
                gallery=main_gallery
            ),
            secondary_events=secondary_objs
        )

        # STEP 9: DEBUG OUTPUT (Vezi tot JSON-ul în log-uri înainte de trimitere)
        final_json_data = payload.model_dump(mode='json')

        print("\n" + "🔥" * 5 + " DEBUG FULL JSON PAYLOAD START " + "🔥" * 5)
        print(json.dumps(final_json_data, indent=4, ensure_ascii=False))
        print("🔥" * 5 + " DEBUG FULL JSON PAYLOAD END " + "🔥" * 5 + "\n")

        with open("last_payload_debug.json", "w", encoding="utf-8") as f:
            json.dump(final_json_data, f, indent=4, ensure_ascii=False)

        # STEP 10: TRIMITERE CĂTRE JAVA
        try:
            status = await send_to_java(payload)
            logger.info(f"✅ SUCCESS: Java received data (Status {status})")
        except Exception as e:
            logger.warning(f"⚠️ Transmission failed (Expected if link is dummy). Error: {e}")

    except Exception as e:
        logger.error(f"🚨 Pipeline Crash: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())