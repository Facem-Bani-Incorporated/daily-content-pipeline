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


@retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
async def send_to_java(payload: DailyPayload):
    headers = {
        "X-Internal-Api-Key": config.INTERNAL_API_SECRET,
        "Content-Type": "application/json"
    }
    payload_json = payload.model_dump(mode='json')
    async with httpx.AsyncClient(timeout=150.0) as client:
        logger.info(f"📤 Ingesting to Java: {config.JAVA_BACKEND_URL}")
        response = await client.post(config.JAVA_BACKEND_URL, json=payload_json, headers=headers)
        response.raise_for_status()
        return response.status_code


async def main():
    logger.info("🚀 Starting Elite Pipeline (Robust Mode)...")

    empty_translations = {"en": "", "ro": "", "es": "", "de": "", "fr": ""}

    try:
        scraper = WikiScraper()
        processor = AIProcessor()
        ranker = ScoringEngine()

        # STEP 1: FETCH
        raw_events = await scraper.fetch_today()
        if not raw_events:
            raise ValueError("Wikipedia returned no events.")

        # STEP 2: PRE-RANKING (Heuristics)
        for item in raw_events:
            item['h_score'] = ranker.heuristic_score(item)

        candidates = sorted(raw_events, key=lambda x: x['h_score'], reverse=True)[:config.MAX_CANDIDATES_FOR_AI]

        # STEP 3: PAGE VIEWS
        logger.info(f"📊 Fetching popularity data for top {len(candidates)} candidates...")
        view_tasks = [scraper.fetch_page_views(item.get('slug')) for item in candidates]
        views_results = await asyncio.gather(*view_tasks, return_exceptions=True)

        for item, result in zip(candidates, views_results):
            item['views'] = result if isinstance(result, int) else 0

        # STEP 4: AI SELECTION & BATCH CATEGORIZATION
        logger.info("🤖 AI is analyzing candidates...")
        ai_data = await processor.batch_score_and_categorize(candidates)
        results = ai_data.get('results', {})

        # STEP 5: HYBRID SCORING
        for idx, item in enumerate(candidates):
            res = results.get(f"ID_{idx}", {"score": 50, "category": "culture"})
            try:
                item['category'] = EventCategory(res.get('category', 'culture').lower())
            except ValueError:
                item['category'] = EventCategory.CULTURE

            item['ai_impact'] = res.get('score', 50)
            item['titles'] = res.get('titles', empty_translations)
            item['final_score'] = ranker.calculate_final_score(item['h_score'], item['ai_impact'], item.get('views', 0))

        # Sortăm final: Primul e Main, următoarele 5 sunt Secondary
        candidates.sort(key=lambda x: x.get('final_score', 0), reverse=True)

        # --- STEP 6: MAIN EVENT GENERATION ---
        top_data = candidates[0]
        logger.info(f"✨ Generating Premium Main Event for {top_data['year']}...")
        main_content = await processor.generate_multilingual_main_event(top_data)

        # Upload Main Gallery
        slug_main = top_data.get('slug') or "history"
        wiki_imgs = await scraper.fetch_gallery_urls(slug_main, limit=3)
        main_gallery_tasks = [
            asyncio.to_thread(scraper.upload_to_cloudinary, url, f"main_{top_data['year']}_{i}")
            for i, url in enumerate(wiki_imgs)
        ]
        main_gallery = [img for img in await asyncio.gather(*main_gallery_tasks) if img]

        # --- STEP 7: SECONDARY EVENTS NARRATIVES (BATCH) ---
        secondary_pool = candidates[1:6]
        logger.info(f"✍️ Generating narratives for {len(secondary_pool)} secondary events...")
        sec_narratives_map = await processor.generate_secondary_narratives(secondary_pool)

        # Assembly Secondary Objects
        secondary_objs = []
        for idx, item in enumerate(secondary_pool):
            key = f"EVENT_{idx}"
            narratives = sec_narratives_map.get(key, empty_translations)

            # Imagine pentru secundar
            raw_thumb = item.get('wiki_thumb') or "https://images.unsplash.com/photo-1447069387593-a5de0862481e?w=400"
            thumb_url = await asyncio.to_thread(scraper.upload_to_cloudinary, raw_thumb, f"sec_{item['year']}_{idx}")

            sec_event = SecondaryEvent(
                title_translations=Translations(**item.get('titles', empty_translations)),
                year=item['year'],
                source_url=f"https://en.wikipedia.org/wiki/{item.get('slug', '')}",
                thumbnail_url=thumb_url,
                ai_relevance_score=item.get('final_score', 0),
                narrative_translations=Translations(**narratives)
            )
            secondary_objs.append(sec_event)

        # --- STEP 8: PAYLOAD ASSEMBLY ---
        payload = DailyPayload(
            date_processed=datetime.now().date(),
            api_secret=config.INTERNAL_API_SECRET,
            main_event=MainEvent(
                category=top_data['category'],
                page_views_30d=top_data.get('views', 0),
                title_translations=Translations(**main_content.get('titles', empty_translations)),
                year=top_data['year'],
                source_url=f"https://en.wikipedia.org/wiki/{slug_main}",
                event_date=datetime.now().date(),
                narrative_translations=Translations(**main_content.get('narratives', empty_translations)),
                impact_score=top_data['final_score'],
                gallery=main_gallery
            ),
            secondary_events=secondary_objs
        )

        # --- STEP 9: PRETTY LOGGING & SEND ---
        print_payload_summary(payload)

        status = await send_to_java(payload)
        logger.info(f"✅ PIPELINE FINISHED. Java Response: {status}")

    except Exception as e:
        logger.error(f"🚨 Pipeline Crash: {e}", exc_info=True)


def print_payload_summary(payload: DailyPayload):
    """Afișează datele frumos în consolă pentru verificare rapidă."""
    print("\n" + "=" * 50)
    print(f"📅 DATA PROCESĂRII: {payload.date_processed}")
    print(f"🌟 MAIN EVENT ({payload.main_event.year}): {payload.main_event.title_translations.en}")
    print(f"📊 Scor Impact: {payload.main_event.impact_score} | Vizualizări: {payload.main_event.page_views_30d}")
    print(f"🖼️ Imagini în Galerie: {len(payload.main_event.gallery)}")
    print("-" * 50)
    print(f"🔗 EVENIMENTE SECUNDARE ({len(payload.secondary_events)}):")
    for idx, sec in enumerate(payload.secondary_events):
        print(f"  {idx + 1}. [{sec.year}] {sec.title_translations.en} (Scor: {sec.ai_relevance_score:.2f})")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    asyncio.run(main())