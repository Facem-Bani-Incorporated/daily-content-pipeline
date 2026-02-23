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


# --- 1. FUNCȚII DE BRIDGE (Direct aici pentru a evita erorile de import) ---

async def save_event_content_safe(payload: DailyPayload):
    """Salvează backup local doar dacă DATABASE_URL este configurat."""
    if not config.DATABASE_URL:
        logger.info("ℹ️ Database opțional: Skip local backup (DATABASE_URL lipsește).")
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
        logger.info(f"🏛️ Conținut arhivat local pentru anul {main.year}.")
    except Exception as e:
        logger.warning(f"⚠️ Backup local eșuat, dar continuăm: {e}")


@retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
async def send_to_java(payload: DailyPayload):
    """Trimite payload-ul către backend-ul de Java al lui Sergiu."""
    headers = {
        "X-Internal-Api-Key": config.INTERNAL_API_SECRET,
        "Content-Type": "application/json"
    }
    payload_json = payload.model_dump(mode='json')

    async with httpx.AsyncClient(timeout=120.0) as client:
        logger.info(f"📤 Trimitere date către Java la: {config.JAVA_BACKEND_URL}")
        response = await client.post(
            config.JAVA_BACKEND_URL,
            json=payload_json,
            headers=headers
        )
        response.raise_for_status()
        return response.status_code


# --- 2. PIPELINE-UL PRINCIPAL ---

async def main():
    logger.info("🚀 Pornire Pipeline Elite (Mode: Database Optional)...")

    # 0. Inițializare DB (Dacă există URL-ul)
    if config.DATABASE_URL:
        try:
            await init_db()
        except Exception as e:
            logger.warning(f"⚠️ Init DB failed: {e}")

    try:
        scraper = WikiScraper()
        processor = AIProcessor()
        ranker = ScoringEngine()

        # 1. FETCH & PRE-SCREENING
        raw_events = await scraper.fetch_today()
        if not raw_events:
            raise ValueError("Wikipedia API a returnat zero evenimente.")

        for item in raw_events:
            item['h_score'] = ranker.heuristic_score(item)

        candidates = sorted(raw_events, key=lambda x: x['h_score'], reverse=True)[:config.MAX_CANDIDATES_FOR_AI]

        # 2. ENRICHMENT (Popularitate & AI Scoring)
        logger.info(f"📊 Procesare {len(candidates)} candidați...")
        for item in candidates:
            p = item.get("pages", [])
            slug = p[0].get("titles", {}).get("canonical") if p else None
            item['views'] = await scraper.fetch_page_views(slug)

        ai_data = await processor.batch_score_and_categorize(candidates)
        results = ai_data.get('results', {})

        # 3. RANKING FINAL
        for idx, item in enumerate(candidates):
            res = results.get(f"ID_{idx}", {"score": 50, "category": "POLITICS", "titles": {}})
            item['category'] = res.get('category', 'POLITICS')
            item['ai_impact'] = res.get('score', 50)
            item['titles'] = res.get('titles', {})
            item['final_score'] = ranker.calculate_final_score(item['h_score'], item['ai_impact'], item.get('views', 0))

        candidates.sort(key=lambda x: x.get('final_score', 0), reverse=True)
        top_data = candidates[0]

        # 4. GENERARE MEDIA & CONTINUT
        main_content = await processor.generate_multilingual_main_event(top_data)

        # Imagini Main
        p_main = top_data.get("pages", [])
        slug_main = p_main[0].get("titles", {}).get("canonical") if p_main else "history"
        wiki_imgs = await scraper.fetch_gallery_urls(slug_main, limit=3)
        main_gallery = [scraper.upload_to_cloudinary(url, f"main_{top_data['year']}_{i}") for i, url in
                        enumerate(wiki_imgs)]

        # Evenimente Secundare (Top 5)
        secondary_objs = []
        for idx, item in enumerate(candidates[1:6]):
            p_sec = item.get("pages", [])
            slug_sec = p_sec[0].get("titles", {}).get("canonical") if p_sec else ""
            thumb = None
            if slug_sec:
                imgs_sec = await scraper.fetch_gallery_urls(slug_sec, limit=1)
                if imgs_sec:
                    thumb = scraper.upload_to_cloudinary(imgs_sec[0], f"sec_{item['year']}_{idx}")

            secondary_objs.append(SecondaryEvent(
                title_translations=item.get('titles', {}),
                year=item['year'],
                source_url=f"https://en.wikipedia.org/wiki/{slug_sec}",
                ai_relevance_score=item.get('final_score', 0),
                thumbnail_url=thumb
            ))

        # 5. CONSTRUCȚIE PAYLOAD
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

        # --- INSPECTOR JSON (Log-ul pe care îl cauți în Railway) ---
        print("\n" + "═" * 60)
        print("🔍 PAYLOAD INSPECTOR (Ready for Java):")
        print("═" * 60)
        print(json.dumps(payload.model_dump(mode='json'), indent=4, ensure_ascii=False))
        print("═" * 60 + "\n")

        # 6. TRANSMISIE
        await save_event_content_safe(payload)
        status_code = await send_to_java(payload)

        if status_code in [200, 201]:
            logger.info(f"✅ SUCCES: Datele au fost transmise către Spring Boot.")

    except Exception as e:
        logger.error(f"🚨 Pipeline Crash: {e}", exc_info=True)
    finally:
        if engine and config.DATABASE_URL:
            await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())