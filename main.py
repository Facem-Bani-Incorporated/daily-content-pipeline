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
from tenacity import retry, stop_after_attempt, wait_fixed

logger = setup_logger("MainPipeline")


@retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
async def send_to_java(payload: DailyPayload):
    headers = {
        "X-Internal-Api-Key": config.INTERNAL_API_SECRET,
        "Content-Type": "application/json"
    }
    payload_json = payload.model_dump(mode='json')
    async with httpx.AsyncClient(timeout=180.0) as client:
        logger.info(f"📤 Ingesting to Java: {config.JAVA_BACKEND_URL}")
        response = await client.post(config.JAVA_BACKEND_URL, json=payload_json, headers=headers)
        response.raise_for_status()
        return response.status_code


async def safe_upload(scraper, url, folder_name):
    try:
        return await asyncio.to_thread(scraper.upload_to_cloudinary, url, folder_name)
    except Exception as e:
        logger.warning(f"⚠️ Cloudinary Failed: {e}")
        return None


async def main():
    logger.info("🚀 Starting Elite Daily Pipeline (Uniform Mode)...")
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

        # Merge data & final ranking
        for idx, item in enumerate(candidates):
            res = ai_results.get(f"ID_{idx}", {})
            item['views'] = views[idx] if isinstance(views[idx], int) else 0
            item['category'] = res.get('category', 'culture_arts')
            item['ai_impact'] = res.get('score', 50)
            item['titles'] = res.get('titles', default_langs)
            item['final_score'] = ranker.calculate_final_score(item['h_score'], item['ai_impact'], item['views'])

        candidates.sort(key=lambda x: x.get('final_score', 0), reverse=True)

        # 4. Main Event - Deep Narrative & Gallery
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

        # 5. Secondary Events - Uniform Generation
        secondary_pool = candidates[1:6]
        sec_narratives_map = await processor.generate_secondary_narratives(secondary_pool)
        secondary_objs = []

        for idx, item in enumerate(secondary_pool):
            key = f"EVENT_{idx}"
            narratives = sec_narratives_map.get(key, default_langs)

            # Thumbnail logic (Treat secondary as a single-image gallery)
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

        # 6. Assemble & Dispatch
        payload = DailyPayload(
            date_processed=datetime.now().date(),
            api_secret=config.INTERNAL_API_SECRET,
            main_event=main_event_obj,
            secondary_events=secondary_objs,
            metadata={"processed_candidates": len(candidates)}
        )

        logger.info(f"✅ Payload Ready. Main Event: {main_event_obj.year}")
        await send_to_java(payload)

    except Exception as e:
        logger.error(f"🚨 Pipeline Crash: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())