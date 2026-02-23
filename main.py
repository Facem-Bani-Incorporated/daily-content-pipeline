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
from schema.models import DailyPayload, MainEvent, SecondaryEvent
from tenacity import retry, stop_after_attempt, wait_fixed

logger = setup_logger("MainPipeline")


# --- 1. BRIDGE FUNCTIONS (Fail-safe & Lazy Load) ---

async def save_event_content_safe(payload: DailyPayload):
    """Salvează în DB locală doar dacă este configurată."""
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
        logger.warning(f"⚠️ Local DB backup skipped/failed: {e}")


@retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
async def send_to_java(payload: DailyPayload):
    """Trimite datele către backend-ul Spring Boot."""
    headers = {
        "X-Internal-Api-Key": config.INTERNAL_API_SECRET,
        "Content-Type": "application/json"
    }
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


# --- 2. MAIN PIPELINE ---

async def main():
    logger.info("🚀 Starting Elite Pipeline (DB-Optional Mode)...")

    # Inițializăm DB doar dacă avem string-ul de conexiune
    if config.DATABASE_URL:
        try:
            from core.database import init_db
            await init_db()
            logger.info("✅ Database connection initialized.")
        except Exception as e:
            logger.error(f"❌ DB Init Error: {e}")
    else:
        logger.info("ℹ️ Running in DB-less mode (DATABASE_URL missing).")

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

        # STEP 3: PAGE VIEWS (Parallel)
        logger.info(f"📊 Fetching popularity data for top {len(candidates)} candidates...")
        view_tasks = [scraper.fetch_page_views(item.get('slug')) for item in candidates]
        views_results = await asyncio.gather(*view_tasks, return_exceptions=True)

        for item, result in zip(candidates, views_results):
            item['views'] = result if isinstance(result, int) else 0

        # STEP 4: AI SELECTION & CATEGORIZATION
        logger.info("🤖 AI is analyzing and choosing the best content...")
        ai_data = await processor.batch_score_and_categorize(candidates)
        results = ai_data.get('results', {})

        # STEP 5: HYBRID SCORING
        for idx, item in enumerate(candidates):
            res = results.get(f"ID_{idx}", {"score": 50, "category": "culture", "titles": {}})
            item['category'] = res.get('category', 'culture')
            item['ai_impact'] = res.get('score', 50)
            item['titles'] = res.get('titles', {})
            item['final_score'] = ranker.calculate_final_score(item['h_score'], item['ai_impact'], item.get('views', 0))

        candidates.sort(key=lambda x: x.get('final_score', 0), reverse=True)
        top_data = candidates[0]

        # STEP 6: PREMIUM CONTENT GENERATION
        logger.info(f"✨ Generating Premium Content for {top_data['year']} ({top_data['category']})")
        main_content = await processor.generate_multilingual_main_event(top_data)

        # Main Gallery Uploads
        slug_main = top_data.get('slug') or "history"
        wiki_imgs = await scraper.fetch_gallery_urls(slug_main, limit=3)
        main_gallery_tasks = [
            asyncio.to_thread(scraper.upload_to_cloudinary, url, f"main_{top_data['year']}_{i}")
            for i, url in enumerate(wiki_imgs)
        ]
        main_gallery = await asyncio.gather(*main_gallery_tasks)

        # Secondary Events (Top 5)
        secondary_objs = []
        for idx, item in enumerate(candidates[1:6]):
            slug_sec = item.get('slug')
            # Placeholder or actual thumb if available
            thumb = await asyncio.to_thread(scraper.upload_to_cloudinary, "https://via.placeholder.com/300",
                                            f"sec_{idx}")

            secondary_objs.append(SecondaryEvent(
                title_translations=item.get('titles', {}),
                year=item['year'],
                source_url=f"https://en.wikipedia.org/wiki/{slug_sec}" if slug_sec else "",
                ai_relevance_score=item.get('final_score', 0),
                thumbnail_url=thumb
            ))

        # STEP 7: PAYLOAD ASSEMBLY
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

        # --- STEP 8: EXPORT FULL REPORT TO TXT ---
        report_file = f"full_output_{payload.date_processed}.txt"

        # Extragem dicționarele din obiectele Pydantic pentru a putea folosi .items()
        narratives_dict = payload.main_event.narrative_translations.model_dump()
        titles_dict = payload.main_event.title_translations.model_dump()

        with open(report_file, "w", encoding="utf-8") as f:
            f.write("═" * 60 + "\n")
            f.write(f"🚀 PIPELINE FULL REPORT: {payload.date_processed}\n")
            f.write("═" * 60 + "\n\n")
            f.write(f"WINNER: {payload.main_event.year} - {payload.main_event.category.upper()}\n")
            f.write(f"TOTAL SCORE: {payload.main_event.impact_score}\n\n")

            f.write("--- FULL NARRATIVES (All Languages) ---\n")
            # ACUM folosim variabila transformată în dict
            for lang, text in narratives_dict.items():
                f.write(f"\n[{lang.upper()}]:\n{text}\n")

            f.write("\n" + "--- SECONDARY EVENTS ---\n")
            for sec in payload.secondary_events:
                # Și aici transformăm title_translations dacă e model Pydantic
                sec_titles = sec.title_translations.model_dump() if hasattr(sec.title_translations,
                                                                            'model_dump') else sec.title_translations
                f.write(f"• {sec.year}: {sec_titles.get('en')} (Score: {sec.ai_relevance_score})\n")

            f.write("\n" + "═" * 60 + "\n")
            f.write("RAW JSON PAYLOAD:\n")
            f.write(json.dumps(payload.model_dump(mode='json'), indent=4, ensure_ascii=False))

        logger.info(f"📄 Full report saved: {report_file}")

        # --- STEP 9: MINIMALIST CONSOLE LOG ---
        display = payload.model_dump(mode='json')
        display['api_secret'] = "********"

        # Pydantic model_dump(mode='json') returnează deja dict, deci aici e safe:
        main_ev = display.get('main_event', {})
        narratives = main_ev.get('narrative_translations', {})

        for lang in narratives:
            val = narratives[lang]
            if isinstance(val, str):
                narratives[lang] = val[:70] + "..."

        print("\n" + "═" * 60)
        print(f"🔍 CONSOLE PREVIEW (Full data available in {report_file})")
        print(json.dumps(display, indent=4, ensure_ascii=False))
        print("═" * 60 + "\n")

        # --- STEP 10: TRANSMIT ---
        await save_event_content_safe(payload)

        try:
            status = await send_to_java(payload)
            if status in [200, 201]:
                logger.info(f"✅ SUCCESS: Data ingested by Java (Status {status}).")
        except Exception as e:
            logger.error(f"❌ Java Ingestion Failed: {e}")

    except Exception as e:
        logger.error(f"🚨 Pipeline Crash: {e}", exc_info=True)
    finally:
        # Cleanup final
        if config.DATABASE_URL:
            try:
                from core.database import engine
                await engine.dispose()
                logger.info("🔌 Database connection closed.")
            except:
                pass


if __name__ == "__main__":
    asyncio.run(main())