import asyncio
import json
import httpx
import os
from datetime import datetime
from core.logger import setup_logger
from core.config import config
from engine.scraper import WikiScraper
from engine.processor import AIProcessor
from engine.ranker import ScoringEngine
from schema.models import DailyPayload, MainEvent, SecondaryEvent, EventCategory
from tenacity import retry, stop_after_attempt, wait_fixed

logger = setup_logger("MainPipeline")


# --- 1. BRIDGE FUNCTIONS ---

async def save_event_content_safe(payload: DailyPayload):
    if not config.DATABASE_URL:
        return
    try:
        from core.database import AsyncSessionLocal, ProcessedEvent
        async with AsyncSessionLocal() as session:
            async with session.begin():
                main = payload.main_event
                new_entry = ProcessedEvent(
                    event_date=payload.date_processed,
                    year=main.year,
                    titles=main.title_translations.model_dump(),
                    narrative=main.narrative_translations.model_dump(),
                    image_url=main.gallery[0] if main.gallery else None,
                    impact_score=main.impact_score,
                    source_url=main.source_url
                )
                session.add(new_entry)
            await session.commit()
        logger.info(f"🏛️ Local archive saved for year {main.year}.")
    except Exception as e:
        logger.warning(f"⚠️ Local DB backup skipped/failed: {e}")


@retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
async def send_to_java(payload: DailyPayload):
    headers = {
        "X-Internal-Api-Key": config.INTERNAL_API_SECRET,
        "Content-Type": "application/json"
    }
    payload_json = payload.model_dump(mode='json')
    async with httpx.AsyncClient(timeout=120.0) as client:
        logger.info(f"📤 Ingesting to Java: {config.JAVA_BACKEND_URL}")
        response = await client.post(config.JAVA_BACKEND_URL, json=payload_json, headers=headers)
        response.raise_for_status()
        return response.status_code


# --- 2. MAIN PIPELINE ---

async def main():
    logger.info("🚀 Starting Elite Pipeline (Robust Mode)...")

    # Initializări pentru a preveni NameError
    empty_translations = {"en": "", "ro": "", "es": "", "de": "", "fr": ""}
    main_content = {"titles": empty_translations, "narratives": empty_translations}
    top_data = {}
    main_gallery = []

    try:
        scraper = WikiScraper()
        processor = AIProcessor()
        ranker = ScoringEngine()

        # STEP 1: FETCH
        raw_events = await scraper.fetch_today()
        if not raw_events:
            raise ValueError("Wikipedia returned no events.")

        # STEP 2: PRE-RANKING
        for item in raw_events:
            item['h_score'] = ranker.heuristic_score(item)

        candidates = sorted(raw_events, key=lambda x: x['h_score'], reverse=True)[:config.MAX_CANDIDATES_FOR_AI]

        # STEP 3: PAGE VIEWS
        logger.info(f"📊 Fetching popularity data for top {len(candidates)} candidates...")
        view_tasks = [scraper.fetch_page_views(item.get('slug')) for item in candidates]
        views_results = await asyncio.gather(*view_tasks, return_exceptions=True)

        for item, result in zip(candidates, views_results):
            item['views'] = result if isinstance(result, int) else 0

        # STEP 4: AI SELECTION
        logger.info("🤖 AI is analyzing candidates...")
        ai_data = await processor.batch_score_and_categorize(candidates)
        results = ai_data.get('results', {})

        # STEP 5: HYBRID SCORING
        for idx, item in enumerate(candidates):
            res = results.get(f"ID_{idx}", {"score": 50, "category": "culture"})

            raw_cat = res.get('category', 'culture').lower()
            try:
                item['category'] = EventCategory(raw_cat)
            except ValueError:
                item['category'] = EventCategory.CULTURE

            item['ai_impact'] = res.get('score', 50)
            item['titles'] = res.get('titles', empty_translations)
            item['summaries'] = res.get('summaries', empty_translations)
            item['final_score'] = ranker.calculate_final_score(item['h_score'], item['ai_impact'], item.get('views', 0))

        candidates.sort(key=lambda x: x.get('final_score', 0), reverse=True)
        top_data = candidates[0]
        slug_main = top_data.get('slug') or "history"

        # STEP 6: PREMIUM CONTENT GENERATION (Main Event)
        logger.info(f"✨ Generating Premium Content for {top_data['year']}...")
        main_content = await processor.generate_multilingual_main_event(top_data)

        # Main Gallery
        wiki_imgs = await scraper.fetch_gallery_urls(slug_main, limit=3)
        main_gallery_tasks = [
            asyncio.to_thread(scraper.upload_to_cloudinary, url, f"main_{top_data['year']}_{i}")
            for i, url in enumerate(wiki_imgs)
        ]
        main_gallery_results = await asyncio.gather(*main_gallery_tasks)
        main_gallery = [img for img in main_gallery_results if img]

        # STEP 7: SECONDARY EVENTS & SUMMARIES
        secondary_objs = []
        secondary_summaries_metadata = {}

        for idx, item in enumerate(candidates[1:6]):
            slug_sec = item.get('slug')
            raw_img_url = item.get('wiki_thumb') or "https://images.unsplash.com/photo-1447069387593-a5de0862481e?w=400"

            thumb = await asyncio.to_thread(
                scraper.upload_to_cloudinary,
                raw_img_url,
                f"sec_{item['year']}_{idx}"
            )

            sec_event = SecondaryEvent(
                title_translations=item.get('titles', empty_translations),
                year=item['year'],
                source_url=f"https://en.wikipedia.org/wiki/{slug_sec}" if slug_sec else "https://en.wikipedia.org",
                ai_relevance_score=item.get('final_score', 0),
                thumbnail_url=thumb
            )
            secondary_objs.append(sec_event)
            secondary_summaries_metadata[f"year_{item['year']}"] = item.get('summaries', empty_translations)

        # STEP 8: PAYLOAD ASSEMBLY
        payload = DailyPayload(
            date_processed=datetime.now().date(),
            api_secret=config.INTERNAL_API_SECRET,
            main_event=MainEvent(
                category=top_data['category'],
                page_views_30d=top_data.get('views', 0),
                title_translations=main_content.get('titles', empty_translations),
                year=top_data['year'],
                source_url=f"https://en.wikipedia.org/wiki/{slug_main}",
                event_date=datetime.now().date(),
                narrative_translations=main_content.get('narratives', empty_translations),
                impact_score=top_data['final_score'],
                gallery=main_gallery
            ),
            secondary_events=secondary_objs,
            metadata={"secondary_summaries": secondary_summaries_metadata}
        )

        # STEP 9: LOGGING & TRANSMIT
        full_payload_json = json.dumps(payload.model_dump(mode='json'), ensure_ascii=False)
        print(f"\n🚀 FULL PAYLOAD:\n{full_payload_json}\n")

        await save_event_content_safe(payload)
        status = await send_to_java(payload)
        if status in [200, 201]:
            logger.info(f"✅ SUCCESS: Status {status}")

    except Exception as e:
        logger.error(f"🚨 Pipeline Crash: {e}", exc_info=True)
    finally:
        if config.DATABASE_URL:
            try:
                from core.database import engine
                await engine.dispose()
            except:
                pass


if __name__ == "__main__":
    asyncio.run(main())