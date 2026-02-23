import asyncio
import json
import httpx
from datetime import datetime
from core.logger import setup_logger
from core.config import config
from core.database import engine, init_db, AsyncSessionLocal, ProcessedEvent
from engine.scraper import WikiScraper
from engine.processor import AIProcessor
from engine.ranker import ScoringEngine
from schema.models import DailyPayload, MainEvent, SecondaryEvent
from tenacity import retry, stop_after_attempt, wait_fixed

logger = setup_logger("MainPipeline")


# --- 1. BRIDGE FUNCTIONS (Zero Circular Imports) ---

async def save_event_content_safe(payload: DailyPayload):
    """Saves backup locally only if DB is configured."""
    if not config.DATABASE_URL:
        logger.info("ℹ️ Database Optional: Skipping local backup.")
        return

    try:
        async with AsyncSessionLocal() as session:
            async with session.begin():
                main = payload.main_event
                new_entry = ProcessedEvent(
                    event_date=payload.date_processed,
                    year=main.year,
                    titles=dict(main.title_translations),
                    narrative=dict(main.narrative_translations),
                    image_url=main.gallery[0] if main.gallery else None,
                    impact_score=main.impact_score,
                    source_url=main.source_url
                )
                session.add(new_entry)
            await session.commit()
        logger.info(f"🏛️ Local archive saved for year {main.year}.")
    except Exception as e:
        logger.warning(f"⚠️ Local backup failed (non-critical): {e}")


@retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
async def send_to_java(payload: DailyPayload):
    """Sends payload to Sergiu's Spring Boot backend."""
    headers = {
        "X-Internal-Api-Key": config.INTERNAL_API_SECRET,
        "Content-Type": "application/json"
    }
    # Important: model_dump(mode='json') ensures dates and enums are stringified correctly
    payload_json = payload.model_dump(mode='json')

    async with httpx.AsyncClient(timeout=120.0) as client:
        logger.info(f"📤 Ingesting to Java: {config.JAVA_BACKEND_URL}")
        response = await client.post(
            config.JAVA_BACKEND_URL,
            json=payload_json,
            headers=headers
        )
        response.raise_for_status()
        return response.status_code


# --- 2. MAIN PIPELINE (Elite Performance) ---

async def main():
    logger.info("🚀 Starting Elite Pipeline (Async Parallel Mode)...")

    if config.DATABASE_URL:
        try:
            await init_db()
        except Exception as e:
            logger.warning(f"⚠️ Database init failed: {e}")

    try:
        scraper = WikiScraper()
        processor = AIProcessor()
        ranker = ScoringEngine()

        # STEP 1 & 2: FETCH & PRE-SCREEN
        raw_events = await scraper.fetch_today()
        if not raw_events:
            raise ValueError("Wikipedia returned no events for today.")

        for item in raw_events:
            item['h_score'] = ranker.heuristic_score(item)

        candidates = sorted(raw_events, key=lambda x: x['h_score'], reverse=True)[:config.MAX_CANDIDATES_FOR_AI]

        # STEP 3: PARALLEL ENRICHMENT
        logger.info(f"📊 Fetching page views for {len(candidates)} candidates...")
        view_tasks = []
        for item in candidates:
            p = item.get("pages", [])
            slug = p[0].get("titles", {}).get("canonical") if p else None
            item['slug'] = slug
            view_tasks.append(scraper.fetch_page_views(slug))

        views_results = await asyncio.gather(*view_tasks, return_exceptions=True)
        for item, result in zip(candidates, views_results):
            item['views'] = result if isinstance(result, int) else 0

        # STEP 4: AI BATCH ANALYSIS
        logger.info("🤖 AI Categorizing and Scoring...")
        ai_data = await processor.batch_score_and_categorize(candidates)
        results = ai_data.get('results', {})

        # STEP 5: HYBRID RANKING
        for idx, item in enumerate(candidates):
            res = results.get(f"ID_{idx}", {"score": 50, "category": "politics", "titles": {}})
            item['category'] = res.get('category', 'politics')
            item['ai_impact'] = res.get('score', 50)
            item['titles'] = res.get('titles', {})
            item['final_score'] = ranker.calculate_final_score(item['h_score'], item['ai_impact'], item.get('views', 0))

        candidates.sort(key=lambda x: x.get('final_score', 0), reverse=True)
        top_data = candidates[0]

        # STEP 6: PREMIUM CONTENT
        logger.info(f"✨ Generating Narrative for {top_data['year']} ({top_data['category']})")
        main_content = await processor.generate_multilingual_main_event(top_data)

        # Media uploads
        slug_main = top_data.get('slug') or "history"
        wiki_imgs = await scraper.fetch_gallery_urls(slug_main, limit=3)
        main_gallery_tasks = [
            asyncio.to_thread(scraper.upload_to_cloudinary, url, f"main_{top_data['year']}_{i}")
            for i, url in enumerate(wiki_imgs)
        ]
        main_gallery = await asyncio.gather(*main_gallery_tasks)

        secondary_objs = []
        for idx, item in enumerate(candidates[1:6]):
            slug_sec = item.get('slug')
            thumb = None
            if slug_sec:
                imgs_sec = await scraper.fetch_gallery_urls(slug_sec, limit=1)
                if imgs_sec:
                    thumb = await asyncio.to_thread(scraper.upload_to_cloudinary, imgs_sec[0],
                                                    f"sec_{item['year']}_{idx}")

            secondary_objs.append(SecondaryEvent(
                title_translations=item.get('titles', {}),
                year=item['year'],
                source_url=f"https://en.wikipedia.org/wiki/{slug_sec}" if slug_sec else "",
                ai_relevance_score=item.get('final_score', 0),
                thumbnail_url=thumb
            ))

            # --- STEP 7: PAYLOAD ASSEMBLY ---
            payload = DailyPayload(
                date_processed=datetime.now().date(),
                api_secret=config.INTERNAL_API_SECRET,
                main_event=MainEvent(
                    title_translations=main_content['titles'],
                    year=top_data['year'],
                    category=top_data['category'],
                    source_url=f"https://en.wikipedia.org/wiki/{slug_main}",
                    event_date=datetime.now().date(),
                    narrative_translations=main_content['narratives'],
                    impact_score=top_data['final_score'],
                    gallery=[img for img in main_gallery if img]
                ),
                secondary_events=secondary_objs
            )

            # --- MINIMALIST INSPECTOR & METRICS ---
            display_payload = payload.model_dump(mode='json')
            payload_size_kb = len(json.dumps(display_payload)) / 1024

            display_payload['api_secret'] = "********HIDDEN********"
            for lang in display_payload['main_event']['narrative_translations']:
                text = display_payload['main_event']['narrative_translations'][lang]
                display_payload['main_event']['narrative_translations'][lang] = text[:100] + "..." if len(
                    text) > 100 else text

            print("\n" + "═" * 60)
            print(f"🔍 PAYLOAD INSPECTOR | Size: {payload_size_kb:.2f} KB")
            print(f"📊 Main Category: {top_data['category'].upper()}")
            print("═" * 60)
            print(json.dumps(display_payload, indent=4, ensure_ascii=False))
            print("═" * 60 + "\n")

            # --- STEP 8: SAVE & TRANSMIT ---

            # 8a. Local JSON Backup (In caz că pică Ingestia sau DB-ul)
            backup_filename = f"backup_{payload.date_processed}.json"
            with open(backup_filename, "w", encoding="utf-8") as f:
                json.dump(payload.model_dump(mode='json'), f, ensure_ascii=False, indent=4)
            logger.info(f"💾 Emergency backup saved to {backup_filename}")

            # 8b. Database Save
            await save_event_content_safe(payload)

            # 8c. Java Ingestion
            try:
                status_code = await send_to_java(payload)
                if status_code in [200, 201]:
                    logger.info(f"✅ SUCCESS: Ingested to Java (Status {status_code})")
                    # Daca a reusit, putem sterge backup-ul local sa nu ocupe spatiu
                    import os
                    os.remove(backup_filename)
            except Exception as e:
                logger.error(f"❌ Java Ingestion Failed: {e}")
                logger.warning(f"⚠️ Action Required: Manual ingestion needed for {backup_filename}")