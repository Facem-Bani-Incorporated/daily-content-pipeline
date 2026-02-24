import asyncio
from datetime import datetime
from core.logger import setup_logger
from core.config import config
from engine.scraper import WikiScraper
from engine.processor import AIProcessor
from engine.ranker import ScoringEngine
from schema.models import DailyPayload, EventDetail, EventCategory, Translations

logger = setup_logger("MainPipeline")


async def send_to_java(payload: DailyPayload):
    """Trimitere securizată către backend Java."""
    headers = {
        "X-Internal-Api-Key": config.INTERNAL_API_SECRET,
        "Content-Type": "application/json"
    }
    payload_json = payload.model_dump(mode='json')

    async with httpx.AsyncClient(timeout=20.0) as client:
        try:
            logger.info(f"📤 Ingesting to Java: {config.JAVA_BACKEND_URL}")
            response = await client.post(config.JAVA_BACKEND_URL, json=payload_json, headers=headers)
            if response.status_code in [200, 201]:
                logger.info("✅ Success! Datele au fost salvate în DB.")
            else:
                logger.warning(f"⚠️ Backend Response Status: {response.status_code}")
        except Exception as e:
            logger.error(f"❌ Connection failed: {e}")


async def safe_upload(scraper, url, folder_name):
    """Upload sigur în Cloudinary."""
    if not url: return None
    try:
        return await asyncio.to_thread(scraper.upload_to_cloudinary, url, folder_name)
    except Exception:
        return None


async def main():
    logger.info("🚀 Starting Unified Pipeline...")
    scraper, processor, ranker = WikiScraper(), AIProcessor(), ScoringEngine()

    try:
        # 1. Colectare și Score Euristic
        raw_events = await scraper.fetch_today()
        for item in raw_events:
            item['h_score'] = ranker.heuristic_score(item)

        # Selectăm top pentru AI
        candidates = sorted(raw_events, key=lambda x: x['h_score'], reverse=True)[:config.MAX_CANDIDATES_FOR_AI]

        # 2. AI Categorization & Views (Parallel)
        ai_data = await processor.batch_score_and_categorize(candidates)
        ai_results = ai_data.get('results', {})
        view_tasks = [scraper.fetch_page_views(c.get('slug')) for c in candidates]
        views = await asyncio.gather(*view_tasks)

        # 3. Merge & Final Sorting
        for idx, item in enumerate(candidates):
            res = ai_results.get(f"ID_{idx}", {})
            item.update({
                'v_count': views[idx] if isinstance(views[idx], int) else 0,
                'cat': res.get('category', 'culture_arts'),
                'a_score': res.get('score', 50),
                'titles': res.get('titles', {l: "History" for l in ["en", "ro", "es", "de", "fr"]})
            })
            item['f_score'] = ranker.calculate_final_score(item['h_score'], item['a_score'], item['v_count'])

        # Luăm top 6 final
        candidates.sort(key=lambda x: x.get('f_score', 0), reverse=True)
        top_6 = candidates[:6]

        # 4. Generare Narațiuni în Batch (Parallel)
        # processor.generate_secondary_narratives scoate deja dict-ul de limbi pentru fiecare
        narratives_map = await processor.generate_secondary_narratives(top_6)

        final_events_list = []
        for idx, item in enumerate(top_6):
            slug = item.get('slug', 'history')
            year = item.get('year', 0)

            # Gestionare Imagini: Primul primește galerie (3 poze), restul 1 poză
            if idx == 0:
                wiki_imgs = await scraper.fetch_gallery_urls(slug, limit=3)
                img_tasks = [safe_upload(scraper, url, f"ev_{year}_{i}") for i, url in enumerate(wiki_imgs)]
                image_urls = [img for img in await asyncio.gather(*img_tasks) if img]
            else:
                thumb = await safe_upload(scraper, item.get('wiki_thumb'), f"ev_{year}")
                image_urls = [thumb] if thumb else []

            # Creare Obiect EventDetail curat
            final_events_list.append(EventDetail(
                category=EventCategory(item['cat'].lower()),
                year=year,
                event_date=datetime.now().date(),
                source_url=f"https://en.wikipedia.org/wiki/{slug}",
                title_translations=Translations(**item['titles']),
                narrative_translations=Translations(**narratives_map.get(f"EVENT_{idx}", {})),
                impact_score=float(item['f_score']),
                page_views_30d=item['v_count'],
                gallery=image_urls
            ))

        # 5. ASAMBLARE PAYLOAD UNIC
        payload = DailyPayload(
            date_processed=datetime.now().date(),
            api_secret=config.INTERNAL_API_SECRET,
            events=final_events_list,
            metadata={"processed": len(candidates), "delivered": len(final_events_list)}
        )

        # 6. LOGARE FINALĂ ȘI TRIMITERE
        clean_json = payload.model_dump_json(indent=4)

        # Folosim un singur print lung pentru a evita intercalarea logurilor
        logger.info(f"\n{'=' * 20} JSON START {'=' * 20}\n{clean_json}\n{'=' * 20} JSON END {'=' * 20}\n")

        await asyncio.sleep(1)  # Pauză scurtă pentru buffer-ul de consolă
        await send_to_java(payload)

    except Exception as e:
        logger.error(f"🚨 Pipeline Crash: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())