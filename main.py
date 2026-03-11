import asyncio
import httpx
from datetime import datetime
from core.logger import setup_logger
from core.config import config
from engine.scraper import WikiScraper
from engine.processor import AIProcessor
from engine.ranker import ScoringEngine
from schema.models import DailyPayload, EventDetail, EventCategory, Translations
import hmac
import hashlib
import time

logger = setup_logger("MainPipeline")


async def send_to_java(payload: DailyPayload):
    target_url = config.JAVA_BACKEND_URL
    secret = config.INTERNAL_API_SECRET

    # 1. GENERARE TIMESTAMP ȘI DATE REQUEST
    timestamp = str(int(time.time()))
    method = "POST"
    # Folosim path-ul exact așa cum e în requestMatchers din Java
    path = "/api/daily-content"

    # 2. CONSTRUCȚIE MESAJ (Standard HMAC Auth)
    # Dacă Sergiu a urmat un tutorial standard, mesajul arată așa:
    message = f"{timestamp}:{method}:{path}"

    signature = hmac.new(
        secret.encode('utf-8'),
        message.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

    # 3. HEADERELE (Am pus cele mai comune nume folosite în Java)
    headers = {
        "X-Pipeline-Timestamp": timestamp,
        "X-Pipeline-Signature": signature,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    payload_json = payload.model_dump(
        mode='json',
        by_alias=True,
        exclude={'metadata': True, 'events': {'__all__': {'year'}}}
    )

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            logger.info(f"🔐 Attempting HMAC Auth to: {target_url}")
            logger.info(f"DEBUG: Msg used for sig: {message}")

            response = await client.post(target_url, json=payload_json, headers=headers)

            if response.status_code in [200, 201]:
                logger.info("✅ SUCCESS! Datele au fost acceptate.")
            else:
                logger.error(f"❌ Refuzat ({response.status_code}).")
                # Dacă e tot 401, cere-i lui Sergiu fisierul PipelineHmacAuthFilter.java
                if response.status_code == 401:
                    logger.warning(
                        "Sfat: Cere-i lui Sergiu fisierul 'PipelineHmacAuthFilter.java' ca sa vedem formatul exact al semnaturii.")
        except Exception as e:
            logger.error(f"🚨 Eroare: {e}")


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