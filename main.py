import asyncio
import json
from datetime import datetime
from core.logger import setup_logger
from core.config import config
from core.database import engine, init_db
from engine.scraper import WikiScraper
from engine.processor import AIProcessor
from engine.ranker import ScoringEngine
from schema.models import DailyPayload, MainEvent, SecondaryEvent
# Importăm funcțiile de transmisie și salvare locală
from main import save_event_content, send_to_java

logger = setup_logger("MainPipeline")


async def main():
    logger.info("🚀 Pornire Pipeline Elite (Popularity + Category Enabled)...")

    # 0. Inițializare DB local (Backup)
    try:
        await init_db()
    except Exception as e:
        logger.warning(f"⚠️ Local DB not available: {e}")

    try:
        scraper = WikiScraper()
        processor = AIProcessor()
        ranker = ScoringEngine()

        # 1. FETCH INITIAL
        raw_events = await scraper.fetch_today()
        if not raw_events:
            raise ValueError("Wikipedia API returnat gol.")

        # 2. PRE-SCREENING (Heuristic)
        for item in raw_events:
            item['h_score'] = ranker.heuristic_score(item)

        candidates = sorted(raw_events, key=lambda x: x['h_score'], reverse=True)[:config.MAX_CANDIDATES_FOR_AI]

        # 3. ENRICHMENT: PAGE VIEWS
        logger.info(f"📊 Colectare vizualizări pentru {len(candidates)} candidați...")
        for item in candidates:
            p = item.get("pages", [])
            slug = p[0].get("titles", {}).get("canonical") if p else None
            item['views'] = await scraper.fetch_page_views(slug)

        # 4. AI ANALYSIS: CATEGORY & IMPACT
        ai_data = await processor.batch_score_and_categorize(candidates)
        results = ai_data.get('results', {})

        # 5. FINAL RANKING
        for idx, item in enumerate(candidates):
            res = results.get(f"ID_{idx}", {"score": 50, "category": "politics", "titles": {}})
            item['category'] = res.get('category', 'politics')
            item['ai_impact'] = res.get('score', 50)
            item['titles'] = res.get('titles', {})

            item['final_score'] = ranker.calculate_final_score(
                item['h_score'],
                item['ai_impact'],
                item.get('views', 0)
            )

        candidates.sort(key=lambda x: x.get('final_score', 0), reverse=True)
        top_data = candidates[0]

        # 6. GENERARE CONȚINUT PREMIUM & MEDIA
        main_content = await processor.generate_multilingual_main_event(top_data)

        p_main = top_data.get("pages", [])
        slug_main = p_main[0].get("titles", {}).get("canonical") if p_main else "history"
        wiki_imgs = await scraper.fetch_gallery_urls(slug_main, limit=3)
        main_gallery = [scraper.upload_to_cloudinary(url, f"main_{top_data['year']}_{i}") for i, url in
                        enumerate(wiki_imgs)]

        # Evenimente Secundare
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

        # 7. CONSTRUCȚIE PAYLOAD FINAL
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

        # --- 8. INSPECTOR DE PAYLOAD (Relevant pentru debugging) ---
        # Folosim model_dump(mode='json') pentru a simula serializarea HTTP reală
        final_json_output = payload.model_dump(mode='json')

        print("\n" + "═" * 60)
        print("🔍 PAYLOAD INSPECTOR - READY FOR SPRING BOOT")
        print("═" * 60)
        print(json.dumps(final_json_output, indent=4, ensure_ascii=False))
        print("═" * 60 + "\n")

        # 9. TRANSMISIE & BACKUP
        # await save_event_content(payload) # Doar dacă vrei backup local în Postgres-ul Python
        status_code = await send_to_java(payload)

        if status_code in [200, 201]:
            logger.info(f"🚀 Succes Total! Datele au fost transmise către Java.")
        else:
            logger.error(f"❌ Transmisie eșuată. Status: {status_code}")

    except Exception as e:
        logger.error(f"🚨 Pipeline Crash: {e}", exc_info=True)
    finally:
        if engine:
            await engine.dispose()
            logger.info("🔌 Conexiuni închise.")


if __name__ == "__main__":
    asyncio.run(main())