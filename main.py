import asyncio
import httpx
import hmac
import hashlib
import time
import base64
import json
from datetime import datetime

from core.logger import setup_logger
from core.config import config
from engine.scraper import WikiScraper
from engine.processor import AIProcessor
from engine.ranker import ScoringEngine
from schema.models import DailyPayload, EventDetail, EventCategory, Translations

logger = setup_logger("MainPipeline")

async def send_to_java(payload: DailyPayload):
    """Trimite datele către Backend-ul Java folosind securitate HMAC-SHA256."""
    target_url = config.JAVA_BACKEND_URL
    secret = config.INTERNAL_API_SECRET

    # Serializăm payload-ul folosind Pydantic (mult mai rapid și sigur)
    body_json = payload.model_dump_json()
    body_bytes = body_json.encode('utf-8')

    # Generăm semnătura de securitate
    timestamp = str(int(time.time()))
    auth_payload = f"{timestamp}.{body_json}"
    
    signature = hmac.new(
        secret.encode('utf-8'),
        auth_payload.encode('utf-8'),
        hashlib.sha256
    ).digest()

    signature_base64 = base64.b64encode(signature).decode('utf-8')

    headers = {
        "X-Timestamp": timestamp,
        "X-Signature": signature_base64,
        "Content-Type": "application/json",
        "User-Agent": "HistoryApp-Python-Pipeline/1.0"
    }

    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            logger.info(f"📤 Sending payload to Java ({len(payload.events)} events)...")
            response = await client.post(target_url, content=body_bytes, headers=headers)
            
            if response.status_code in [200, 201]:
                logger.info(f"✅ SUCCESS! Backend Response: {response.text}")
            else:
                logger.error(f"❌ Java Error {response.status_code}: {response.text}")
        except Exception as e:
            logger.error(f"🚨 Connection failed: {e}")

async def safe_upload(scraper, url, public_id):
    """Helper pentru upload asincron pe Cloudinary fără a bloca pipeline-ul."""
    if not url: return None
    try:
        # Folosim to_thread deoarece librăria cloudinary este sincronă (blocking)
        return await asyncio.to_thread(scraper.upload_to_cloudinary, url, public_id)
    except Exception as e:
        logger.warning(f"⚠️ Upload failed for {public_id}: {e}")
        return None

async def main():
    logger.info("🚀 Starting Unified Pipeline (Cinematic Edition)...")
    scraper, processor, ranker = WikiScraper(), AIProcessor(), ScoringEngine()

    try:
        # 1. FETCH & INITIAL RANKING
        raw_events = await scraper.fetch_today()
        if not raw_events:
            logger.error("❌ No events fetched from Wiki. Aborting.")
            return

        for item in raw_events:
            item['h_score'] = ranker.heuristic_score(item)

        # Limităm candidații pentru procesarea AI (economie de tokeni)
        candidates = sorted(raw_events, key=lambda x: x['h_score'], reverse=True)[:config.MAX_CANDIDATES_FOR_AI]

        # 2. AI SCORING & POPULARITY METRICS
        logger.info(f"🧠 Processing {len(candidates)} candidates with AI & Metrics...")
        ai_data = await processor.batch_score_and_categorize(candidates)
        ai_results = ai_data.get('results', {})
        
        # Luăm view-urile în paralel
        view_tasks = [scraper.fetch_page_views(c.get('slug')) for c in candidates]
        views = await asyncio.gather(*view_tasks)

        # 3. MERGING & FINAL SELECTION (Top 5 Unique)
        for idx, item in enumerate(candidates):
            res = ai_results.get(f"ID_{idx}", {})
            item.update({
                'views': views[idx] if isinstance(views[idx], int) else 0,
                'category': res.get('category', 'culture_arts'),
                'score': res.get('score', 50),
                'titles': res.get('titles', {l: "History Event" for l in ["en", "ro", "es", "de", "fr"]})
            })
            item['final_score'] = ranker.calculate_final_score(item['h_score'], item['score'], item['views'])

        candidates.sort(key=lambda x: x.get('final_score', 0), reverse=True)
        top_5 = candidates[:5]

        # 4. NARRATIVE GENERATION (All 5 at once)
        narratives_map = await processor.generate_secondary_narratives(top_5)

        # 5. CINEMATIC IMAGE PROCESSING
        final_events_list = []
        for idx, item in enumerate(top_5):
            slug = item.get('slug', 'history')
            year = item.get('year', 0)
            logger.info(f"📸 Processing visuals for: {slug} ({year})")

            # Strategie: Pentru evenimentul #1 căutăm pe Pexels + Galerie Wiki
            # Pentru restul, încercăm Pexels, apoi Fallback pe Wiki Thumb
            if idx == 0:
                # fetch_gallery_urls acum caută automat pe Pexels pentru prima poziție
                wiki_imgs = await scraper.fetch_gallery_urls(slug, limit=3)
                img_tasks = [safe_upload(scraper, url, f"ev_{year}_main_{i}") for i, url in enumerate(wiki_imgs)]
                gallery = [img for img in await asyncio.gather(*img_tasks) if img]
            else:
                # Căutăm o imagine Pro pe Pexels pentru claritate
                pro_img = await scraper.fetch_pro_image(slug.replace('_', ' '))
                source = pro_img if pro_img else item.get('wiki_thumb')
                
                thumb = await safe_upload(scraper, source, f"ev_{year}_{idx}")
                gallery = [thumb] if thumb else []

            # Adăugăm în lista finală
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

        # 6. ASAMBLARE PAYLOAD & SEND
        payload = DailyPayload(
            date_processed=datetime.now().date(),
            events=final_events_list,
            metadata={"processed": len(candidates), "source": "Hybrid-Wiki-Pexels"}
        )

        # Debugging clean (Fără secrete în log-uri)
        print("\n" + "═"*30 + " PAYLOAD READY " + "═"*30)
        logger.info(f"Ready to send {len(final_events_list)} events to Java.")
        
        await send_to_java(payload)

    except Exception as e:
        logger.error(f"🚨 Pipeline Crash: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())