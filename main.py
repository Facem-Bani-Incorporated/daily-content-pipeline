import asyncio
import json
import httpx
from datetime import datetime
from core.logger import setup_logger
from core.config import config
from engine.scraper import WikiScraper
from engine.processor import AIProcessor
from engine.ranker import ScoringEngine
from schema.models import DailyPayload, EventDetail, EventCategory, Translations

logger = setup_logger("MainPipeline")


# --- 1. FUNCTIE DE INGESTIE TOLERANTĂ LA ERORI ---

async def send_to_java(payload: DailyPayload):
    """Încearcă trimiterea, dar NU oprește pipeline-ul dacă backend-ul e offline."""
    headers = {
        "X-Internal-Api-Key": config.INTERNAL_API_SECRET,
        "Content-Type": "application/json"
    }
    # mode='json' e critic pentru a converti obiectele Pydantic în JSON valid
    payload_json = payload.model_dump(mode='json')

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            logger.info(f"📤 Ingesting to Java: {config.JAVA_BACKEND_URL}")
            response = await client.post(config.JAVA_BACKEND_URL, json=payload_json, headers=headers)

            if response.status_code in [200, 201]:
                logger.info("✅ Success! Java Backend a primit datele.")
            else:
                logger.warning(f"⚠️ Backend-ul a răspuns cu STATUS {response.status_code}. Posibil endpoint greșit.")
        except Exception as e:
            logger.error(f"❌ Conexiunea la Java eșuată (Offline/404). Detalii: {e}")
            logger.info("💡 Sfat CTO: Verifică logurile lui Sergiu sau URL-ul din .env")


async def safe_upload(scraper, url, folder_name):
    """Upload către Cloudinary (non-blocking)."""
    try:
        return await asyncio.to_thread(scraper.upload_to_cloudinary, url, folder_name)
    except Exception as e:
        logger.warning(f"⚠️ Cloudinary Failed: {e}")
        return None


# --- 2. PIPELINE-UL PRINCIPAL ---

async def main():
    logger.info("🚀 Starting Elite Daily Pipeline (Payload-First Mode)...")
    default_langs = {l: "Data pending..." for l in ["en", "ro", "es", "de", "fr"]}

    scraper = WikiScraper()
    processor = AIProcessor()
    ranker = ScoringEngine()

    try:
        # 1. Fetch & Rank (Heuristics)
        raw_events = await scraper.fetch_today()
        for item in raw_events:
            item['h_score'] = ranker.heuristic_score(item)
        candidates = sorted(raw_events, key=lambda x: x['h_score'], reverse=True)[:config.MAX_CANDIDATES_FOR_AI]

        # 2. AI Scoring & Categorization (Batch)
        ai_data = await processor.batch_score_and_categorize(candidates)
        ai_results = ai_data.get('results', {})

        # 3. Parallel Views Fetch
        view_tasks = [scraper.fetch_page_views(c.get('slug')) for c in candidates]
        views = await asyncio.gather(*view_tasks)

        for idx, item in enumerate(candidates):
            res = ai_results.get(f"ID_{idx}", {})
            item['views'] = views[idx] if isinstance(views[idx], int) else 0
            item['category'] = res.get('category', 'culture_arts')
            item['ai_impact'] = res.get('score', 50)
            item['titles'] = res.get('titles', default_langs)
            item['final_score'] = ranker.calculate_final_score(item['h_score'], item['ai_impact'], item['views'])

        candidates.sort(key=lambda x: x.get('final_score', 0), reverse=True)

        # 4. Main Event Generation
        top_data = candidates[0]
        main_content = await processor.generate_multilingual_main_event(top_data)

        slug_main = top_data.get('slug', 'history')
        wiki_imgs = await scraper.fetch_gallery_urls(slug_main, limit=3)
        img_tasks = [safe_upload(scraper, url, f"main_{top_data['year']}_{i}") for i, url in enumerate(wiki_imgs)]
        main_gallery = [img for img in await asyncio.gather(*img_tasks) if img]

        main_event_obj = EventDetail(
            category=EventCategory(top_data['category'].lower()),
            year=top_data['year'],
            event_date=datetime.now().date(),
            source_url=f"https://en.wikipedia.org/wiki/{slug_main}",
            title_translations=Translations(**main_content.get('titles', top_data['titles'])),
            narrative_translations=Translations(**main_content.get('narratives', default_langs)),
            impact_score=float(top_data['final_score']),
            page_views_30d=top_data.get('views', 0),
            gallery=main_gallery
        )

        # 5. Secondary Events Generation
        secondary_pool = candidates[1:6]
        sec_narratives_map = await processor.generate_secondary_narratives(secondary_pool)
        secondary_objs = []

        for idx, item in enumerate(secondary_pool):
            key = f"EVENT_{idx}"
            narratives = sec_narratives_map.get(key, default_langs)
            raw_thumb = item.get('wiki_thumb') or "https://images.unsplash.com/photo-1447069387593-a5de0862481e?w=400"
            thumb_url = await safe_upload(scraper, raw_thumb, f"sec_{item['year']}_{idx}")

            secondary_objs.append(EventDetail(
                category=EventCategory(item.get('category', 'culture_arts').lower()),
                year=item['year'],
                event_date=datetime.now().date(),
                source_url=f"https://en.wikipedia.org/wiki/{item.get('slug', '')}",
                title_translations=Translations(**item.get('titles', default_langs)),
                narrative_translations=Translations(**narratives),
                impact_score=float(item.get('final_score', 0)),
                page_views_30d=item.get('views', 0),
                gallery=[thumb_url] if thumb_url else []
            ))

        # 6. ASSEMBLE FINAL PAYLOAD
        payload = DailyPayload(
            date_processed=datetime.now().date(),
            api_secret=config.INTERNAL_API_SECRET,
            main_event=main_event_obj,
            secondary_events=secondary_objs,
            metadata={"processed_candidates": len(candidates)}
        )

        # 7. VIZUALIZARE PAYLOAD (CHIAR DACĂ BACKENDUL E PICAT)
        print("\n" + "💎" * 15)
        print("💎 FINAL GENERATED PAYLOAD 💎")
        print("💎" * 15)
        print(payload.model_dump_json(indent=4))
        print("💎" * 15 + "\n")

        # 8. Dispatch (Fără să crape main())
        await send_to_java(payload)

    except Exception as e:
        logger.error(f"🚨 Pipeline Crash: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())