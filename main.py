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
    """Trimite payload-ul către backend-ul Java folosind serializarea automată Pydantic."""
    target_url = config.JAVA_BACKEND_URL
    secret = config.INTERNAL_API_SECRET

    # Folosim by_alias=True pentru a transforma snake_case în camelCase (ex: event_date -> eventDate)
    body_json = payload.model_dump_json(by_alias=True)
    body_bytes = body_json.encode("utf-8")
    
    timestamp = str(int(time.time()))
    auth_payload = f"{timestamp}.{body_json}"

    signature = hmac.new(
        secret.encode("utf-8"),
        auth_payload.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    
    headers = {
        "X-Timestamp": timestamp,
        "X-Signature": base64.b64encode(signature).decode("utf-8"),
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            logger.info(f"📤 Trimitere către Java: {target_url}")
            response = await client.post(target_url, content=body_bytes, headers=headers)
            if response.status_code in [200, 201]:
                logger.info(f"✅ SUCCES! Payload acceptat pentru data: {payload.date_processed}")
            else:
                logger.error(f"❌ Eroare Java (Status {response.status_code}): {response.text}")
        except Exception as e:
            logger.error(f"🚨 Eroare conexiune HTTP: {e}")

async def safe_upload(scraper: WikiScraper, url: str, public_id: str):
    """Helper pentru upload asincron în Cloudinary."""
    if not url: return None
    try:
        return await asyncio.to_thread(scraper.upload_to_cloudinary, url, public_id)
    except Exception: return None

async def main():
    logger.info("🚀 Pornire Pipeline AI-Driven (60 → 15 → 5)...")
    
    scraper = WikiScraper()
    processor = AIProcessor()
    ranker = ScoringEngine()
    today = datetime.now()

    try:
        # ─────────────────────────────────────────────────────────
        # STEP 1: AI Discovery (60 evenimente)
        # ─────────────────────────────────────────────────────────
        logger.info(f"🌐 PASS 1 — Descoperire 60 evenimente pentru {today.strftime('%B %d')}...")
        all_events = await processor.discover_events(today)
        if not all_events:
            logger.error("❌ AI-ul nu a returnat evenimente. Oprire.")
            return

        # ─────────────────────────────────────────────────────────
        # STEP 2: Deep Rank (Selecție 15)
        # ─────────────────────────────────────────────────────────
        logger.info("🔬 PASS 2 — Deep ranking: selectăm top 15 cele mai de impact...")
        top15 = await processor.deep_rank_and_select(all_events, today)
        if not top15:
            logger.warning("⚠️ Deep rank eșuat, fallback pe scorurile brute.")
            top15 = sorted(all_events, key=lambda x: x.get("ai_score", 0), reverse=True)[:15]

        # ─────────────────────────────────────────────────────────
        # STEP 3: Pageviews (Validare reală via Wikipedia)
        # ─────────────────────────────────────────────────────────
        logger.info("📊 Preluare vizualizări Wikipedia pentru top 15...")
        view_tasks = [scraper.fetch_page_views(item.get("slug", "")) for item in top15]
        views = await asyncio.gather(*view_tasks)

        # ─────────────────────────────────────────────────────────
        # STEP 4: Scorul Final & Top 5
        # ─────────────────────────────────────────────────────────
        for idx, item in enumerate(top15):
            item["views"] = views[idx] if isinstance(views[idx], int) else 0
            item["final_score"] = ranker.calculate_final_score(
                ai_score=item.get("deep_score", item.get("ai_score", 50)),
                views=item["views"],
                category=item.get("category", "culture_arts"),
                year=item.get("year", 0),
            )

        top15.sort(key=lambda x: x.get("final_score", 0), reverse=True)
        top5 = top15[:5]

        # ─────────────────────────────────────────────────────────
        # STEP 5: Multilingual Narratives (Top 5)
        # ─────────────────────────────────────────────────────────
        logger.info("✍️ Generare narative multilingve pentru top 5...")
        narratives_map = await processor.generate_secondary_narratives(top5, today)

        # ─────────────────────────────────────────────────────────
        # STEP 6: Media & Cloudinary
        # ─────────────────────────────────────────────────────────
        final_events_list = []
        for idx, item in enumerate(top5):
            slug = item.get("slug", "")
            year = item.get("year", 0)
            logger.info(f"🖼️ Procesare media pentru: {slug}")

            # Luăm imagini din Wikipedia gallery
            wiki_urls = await scraper.fetch_gallery_urls(slug, limit=3)
            
            gallery = []
            for i, url in enumerate(wiki_urls):
                img_url = await safe_upload(scraper, url, f"ev_{year}_{slug[:20]}_{i}")
                if img_url:
                    gallery.append(img_url)

            # Fallback la thumbnail-ul Wikipedia dacă galeria e goală
            if not gallery and item.get("thumbnail"):
                fb = await safe_upload(scraper, item["thumbnail"], f"ev_{year}_{slug[:20]}_fb")
                if fb: gallery = [fb]

            # ─────────────────────────────────────────────────────
            # STEP 7: Construire Obiect Valid (Respectă Schema)
            # ─────────────────────────────────────────────────────
            narrative_data = narratives_map.get(f"EVENT_{idx}", {})
            
            # Asigurăm că limbile sunt prezente (prevenim linia 111 de eroare)
            def fix_langs(d):
                return {l: d.get(l, "Data pending...") for l in ["en", "ro", "es", "de", "fr"]}

            final_events_list.append(
                EventDetail(
                    category=EventCategory(item["category"].lower()),
                    year=year,
                    event_date=today.date(),
                    source_url=f"https://en.wikipedia.org/wiki/{slug}",
                    title_translations=Translations(**fix_langs(item.get("titles", {}))),
                    narrative_translations=Translations(**fix_langs(narrative_data)),
                    impact_score=float(item["final_score"]),
                    page_views_30d=item["views"],
                    gallery=gallery,
                )
            )

        # ─────────────────────────────────────────────────────────
        # STEP 8: Payload & Trimitere
        # ─────────────────────────────────────────────────────────
        payload = DailyPayload(
            date_processed=today.date(),
            events=final_events_list,
            metadata={
                "total_discovered": len(all_events),
                "pipeline": "ai_60_15_5_v3_perfect"
            }
        )

        await send_to_java(payload)
        logger.info("🏁 Pipeline finalizat cu succes!")

    except Exception as e:
        logger.error(f"🚨 Pipeline Crash: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())