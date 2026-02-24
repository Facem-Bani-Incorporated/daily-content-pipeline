import asyncio
import json
import httpx
from datetime import datetime
from core.logger import setup_logger
from core.config import config
from engine.scraper import WikiScraper
from engine.processor import AIProcessor
from engine.ranker import ScoringEngine
# DailyPayload acum nu mai cere api_secret în definiție conform discuției noastre pe models.py
from schema.models import DailyPayload, EventDetail, EventCategory, Translations

logger = setup_logger("MainPipeline")


async def send_to_java(payload: DailyPayload):
    """Trimitere securizată: Secretul este mutat în Header."""
    headers = {
        "X-Internal-Api-Key": config.INTERNAL_API_SECRET, # Secretul pleacă aici
        "Content-Type": "application/json"
    }
    # Payload-ul JSON va conține acum doar datele, fără cheia secretă
    payload_json = payload.model_dump(mode='json')

    async with httpx.AsyncClient(timeout=20.0) as client:
        try:
            logger.info(f"📤 Ingesting to Java: {config.JAVA_BACKEND_URL}")
            response = await client.post(config.JAVA_BACKEND_URL, json=payload_json, headers=headers)
            if response.status_code in [200, 201]:
                logger.info("✅ Success! Java Backend a acceptat cheia din Header.")
            else:
                logger.warning(f"⚠️ Backend Status {response.status_code}. Verifică dacă Sergiu citește corect Header-ul.")
        except Exception as e:
            logger.error(f"❌ Backend Offline sau eroare rețea: {e}")


async def safe_upload(scraper, url, folder_name):
    """Upload sigur în Cloudinary."""
    if not url: return None
    try:
        return await asyncio.to_thread(scraper.upload_to_cloudinary, url, folder_name)
    except Exception as e:
        logger.warning(f"⚠️ Cloudinary Failed: {e}")
        return None


async def main():
    logger.info("🚀 Starting Unified Pipeline (Header Auth Mode)...")
    scraper, processor, ranker = WikiScraper(), AIProcessor(), ScoringEngine()

    try:
        # 1. Fetch & Rank (Heuristics)
        raw_events = await scraper.fetch_today()
        for item in raw_events:
            item['h_score'] = ranker.heuristic_score(item)
        candidates = sorted(raw_events, key=lambda x: x['h_score'], reverse=True)[:config.MAX_CANDIDATES_FOR_AI]

        # 2. AI & Views
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
                'titles': res.get('titles', {l: "Event" for l in ["en", "ro", "es", "de", "fr"]})
            })
            item['final_score'] = ranker.calculate_final_score(item['h_score'], item['score'], item['views'])

        candidates.sort(key=lambda x: x.get('final_score', 0), reverse=True)
        top_candidates = candidates[:6]  # Luăm primele 6

        # 4. Multilingual Generation (Parallel)
        narratives_map = await processor.generate_secondary_narratives(top_candidates)

        final_events_list = []
        for idx, item in enumerate(top_candidates):
            slug = item.get('slug', 'history')
            year = item.get('year', 0)

            # Primul primește galerie (3 poze), restul 1 poză
            if idx == 0:
                wiki_imgs = await scraper.fetch_gallery_urls(slug, limit=3)
                img_tasks = [safe_upload(scraper, url, f"ev_{year}_{i}") for i, url in enumerate(wiki_imgs)]
                gallery = [img for img in await asyncio.gather(*img_tasks) if img]
            else:
                thumb = await safe_upload(scraper, item.get('wiki_thumb'), f"ev_{year}")
                gallery = [thumb] if thumb else []

            # Construim obiectul EventDetail
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

        # 5. ASSEMBLE PAYLOAD (Fără api_secret în constructor)
        payload = DailyPayload(
            date_processed=datetime.now().date(),
            events=final_events_list,
            metadata={"processed_candidates": len(candidates), "count": len(final_events_list)}
        )

        # 6. LOGARE CURATĂ (Fără date sensibile în logurile Railway)
        clean_json = payload.model_dump_json(indent=4)
        logger.info(f"\n{'=' * 20} JSON START (NO SECRET) {'=' * 20}\n{clean_json}\n{'=' * 20} JSON END {'=' * 20}\n")

        await asyncio.sleep(1)

        # 7. Dispatch (Secretul pleacă prin Header în funcția de mai jos)
        await send_to_java(payload)

    except Exception as e:
        logger.error(f"🚨 Pipeline Crash: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())