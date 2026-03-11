import asyncio
import httpx
from datetime import datetime
from core.logger import setup_logger
from core.config import config
from engine.scraper import WikiScraper
from engine.processor import AIProcessor
from engine.ranker import ScoringEngine
from schema.models import DailyPayload, EventDetail, EventCategory, Translations

logger = setup_logger("MainPipeline")


async def send_to_java(payload: DailyPayload):
    target_url = config.JAVA_BACKEND_URL
    secret = config.INTERNAL_API_SECRET

    # 1. Pregătire Header-e
    headers = {
        "X-Internal-Api-Key": secret,
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "DailyHistory-Python-Pipeline/1.0"
    }

    payload_json = payload.model_dump(
        mode='json',
        by_alias=True,
        exclude={'metadata': True, 'events': {'__all__': {'year'}}}
    )

    # 2. Configurare Retry (3 încercări)
    # Folositor dacă Sergiu face deploy în același timp
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        for attempt in range(1, 4):
            try:
                logger.info(f"📤 [Attempt {attempt}/3] Ingesting to: {target_url}")

                response = await client.post(target_url, json=payload_json, headers=headers)

                # Verificăm statusul
                if response.status_code in [200, 201]:
                    logger.info(f"✅ SUCCESS! Java accepted data (ID: {response.text})")
                    return  # Ieșim din funcție dacă e bine

                # ERROR HANDLING SPECIFIC
                if response.status_code == 401:
                    logger.error(
                        "🚫 ERROR 401: Secret mismatch sau Spring Security blochează header-ul X-Internal-Api-Key.")
                    logger.error(
                        f"DEBUG: Verifică dacă INTERNAL_API_SECRET din Railway Python este IDENTIC cu cel din Java.")
                    break  # Nu mai încercăm, e problemă de credențiale

                if response.status_code == 404:
                    logger.error(f"❓ ERROR 404: Endpoint-ul nu există. Ești sigur că URL-ul e {target_url}?")
                    break

                if response.status_code == 400:
                    logger.error(f"🧱 ERROR 400: Payload invalid. Java zice: {response.text}")
                    break

                logger.warning(f"⚠️ Status neașteptat: {response.status_code}. Reîncercăm...")

            except httpx.ConnectError:
                logger.error(f"🔌 [Attempt {attempt}] Java Offline sau URL Greșit (.railway.internal nu răspunde).")
            except httpx.TimeoutException:
                logger.error(f"⏳ [Attempt {attempt}] Java se mișcă prea greu (Timeout).")
            except Exception as e:
                logger.error(f"🚨 Eroare neprevăzută: {type(e).__name__}: {e}")

            if attempt < 3:
                await asyncio.sleep(5)  # Așteptăm 5 secunde înainte de retry

        logger.critical("💀 Pipeline failed after all attempts.")


async def safe_upload(scraper, url, folder_name):
    if not url: return None
    try:
        return await asyncio.to_thread(scraper.upload_to_cloudinary, url, folder_name)
    except Exception:
        return None


async def main():
    logger.info("🚀 Starting Unified Pipeline (Top 5 Unique Events)...")
    scraper, processor, ranker = WikiScraper(), AIProcessor(), ScoringEngine()

    try:
        # 1. Fetch & Heuristic Rank
        raw_events = await scraper.fetch_today()
        for item in raw_events:
            item['h_score'] = ranker.heuristic_score(item)

        candidates = sorted(raw_events, key=lambda x: x['h_score'], reverse=True)[:config.MAX_CANDIDATES_FOR_AI]

        # 2. AI Scoring & Views
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
                'titles': res.get('titles', {l: "History Event" for l in ["en", "ro", "es", "de", "fr"]})
            })
            item['final_score'] = ranker.calculate_final_score(item['h_score'], item['score'], item['views'])

        # SORTARE FINALĂ ȘI LIMITARE LA 5 EVENIMENTE DIFERITE
        candidates.sort(key=lambda x: x.get('final_score', 0), reverse=True)
        top_5 = candidates[:5]

        # 4. Generare Narațiuni (Toate odată)
        narratives_map = await processor.generate_secondary_narratives(top_5)

        final_events_list = []
        for idx, item in enumerate(top_5):
            slug = item.get('slug', 'history')
            year = item.get('year', 0)

            # Primul primește galerie, restul 1 poză
            if idx == 0:
                wiki_imgs = await scraper.fetch_gallery_urls(slug, limit=3)
                img_tasks = [safe_upload(scraper, url, f"ev_{year}_{i}") for i, url in enumerate(wiki_imgs)]
                gallery = [img for img in await asyncio.gather(*img_tasks) if img]
            else:
                thumb = await safe_upload(scraper, item.get('wiki_thumb'), f"ev_{year}")
                gallery = [thumb] if thumb else []

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

        # --- AICI ERA GREȘEALA: Codul de mai jos trebuie să fie ÎN AFARA buclei for ---

        # 5. ASAMBLARE PAYLOAD (O singură dată, cu toate cele 5)
        payload = DailyPayload(
            date_processed=datetime.now().date(),
            events=final_events_list,
            metadata={"processed": len(candidates), "count": len(final_events_list)}
        )

        # 6. LOGARE CURATĂ (Compactă pentru a evita timestamp pe fiecare linie în Railway)
        compact_json = payload.model_dump_json()
        print("\n" + "=" * 20 + " JSON START (NO SECRET) " + "=" * 20)
        print(compact_json)
        print("=" * 20 + " JSON END " + "=" * 20 + "\n")

        # 7. TRIMITERE FINALĂ
        await send_to_java(payload)

    except Exception as e:
        logger.error(f"🚨 Pipeline Crash: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())