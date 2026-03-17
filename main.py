import asyncio
import httpx
import hmac
import hashlib
import time
import base64
import json
from datetime import datetime, timedelta

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

    events_final = []
    for ev in payload.events:
        events_final.append({
            "category": ev.category.value,
            "titleTranslations": ev.title_translations.model_dump(),
            "narrativeTranslations": ev.narrative_translations.model_dump(),
            "eventDate": ev.event_date.isoformat(),
            "impactScore": float(ev.impact_score),
            "sourceUrl": str(ev.source_url),
            "pageViews30d": int(ev.page_views_30d),
            "gallery": ev.gallery if ev.gallery else [],
        })

    payload_to_serialize = {
        "dateProcessed": payload.date_processed.isoformat(),
        "events": events_final,
    }

    body_json = json.dumps(payload_to_serialize, separators=(",", ":"))
    body_bytes = body_json.encode("utf-8")
    timestamp = str(int(time.time()))
    auth_payload = f"{timestamp}.{body_json}"

    signature = hmac.new(
        secret.encode("utf-8"),
        auth_payload.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    signature_base64 = base64.b64encode(signature).decode("utf-8")

    headers = {
        "X-Timestamp": timestamp,
        "X-Signature": signature_base64,
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            logger.info(f"📤 Sending to Java: {target_url}")
            response = await client.post(target_url, content=body_bytes, headers=headers)
            if response.status_code in [200, 201]:
                logger.info(f"✅ SUCCESS! Payload accepted for date: {payload.date_processed}")
            else:
                logger.error(f"❌ Status {response.status_code}: {response.text}")
        except Exception as e:
            logger.error(f"🚨 Connection error: {e}")


async def safe_upload(scraper: WikiScraper, url: str, public_id: str):
    if not url:
        return None
    try:
        return await asyncio.to_thread(scraper.upload_to_cloudinary, url, public_id)
    except Exception:
        return None


async def main():
    logger.info("🚀 Starting Pipeline for TODAY's events (AI-driven, 60→15→5)...")

    scraper = WikiScraper()
    processor = AIProcessor()
    ranker = ScoringEngine()

    today = datetime.now()

    try:
        # ─────────────────────────────────────────────────────────
        # STEP 1: AI casts a wide net
        # ─────────────────────────────────────────────────────────
        logger.info(f"🌐 PASS 1 — Discovering 60 events for {today.strftime('%B %d')}...")
        all_events = await processor.discover_events(today)
        logger.info(f"📋 Got {len(all_events)} validated events from AI")

        if not all_events:
            logger.error("❌ AI returned no events. Aborting.")
            return

        # ─────────────────────────────────────────────────────────
        # STEP 1.5: MEGA STRICT INTEGRITY CHECK (Adăugat)
        # ─────────────────────────────────────────────────────────
        logger.info("🛡️ PASS 1.5 — Verifying dates integrity (Anti-hallucination)...")
        verified_events = await processor.verify_events_integrity(all_events, today)
        logger.info(f"✅ {len(verified_events)} events passed the brutal date check")
        
        if not verified_events:
            logger.error("❌ All events failed integrity check. Aborting to avoid sending wrong data.")
            return

        # ─────────────────────────────────────────────────────────
        # STEP 2: Deep Ranking (Folosim lista verificată)
        # ─────────────────────────────────────────────────────────
        logger.info("🔬 PASS 2 — Deep ranking: selecting top 15 most impactful...")
        top15 = await processor.deep_rank_and_select(verified_events, today)

        if not top15:
            logger.warning("⚠️ Deep rank failed, falling back to raw AI scores")
            top15 = sorted(verified_events, key=lambda x: x.get("ai_score", 0), reverse=True)[:15]

        # ─────────────────────────────────────────────────────────
        # STEP 3: Fetch Wikipedia pageviews
        # ─────────────────────────────────────────────────────────
        logger.info("📊 Fetching Wikipedia pageviews for top 15...")
        view_tasks = [scraper.fetch_page_views(item.get("slug", "")) for item in top15]
        views = await asyncio.gather(*view_tasks)

        # ─────────────────────────────────────────────────────────
        # STEP 4: Scoring and Top 5
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

        # Logare Top 5 pentru debug
        for i, ev in enumerate(top5):
            logger.info(f" 🏆 #{i+1} [{ev['year']}] Score: {ev['final_score']} - {ev['slug']}")

        # ─────────────────────────────────────────────────────────
        # STEP 5: Narratives
        # ─────────────────────────────────────────────────────────
        logger.info("✍️ Generating multilingual narratives...")
        narratives_map = await processor.generate_secondary_narratives(top5, today)

        # ─────────────────────────────────────────────────────────
        # STEP 6: Media Processing
        # ─────────────────────────────────────────────────────────
        final_events_list = []
        for idx, item in enumerate(top5):
            slug = item.get("slug", "")
            year = item.get("year", 0)
            
            # Curățăm titlurile și narativele de posibile valori None (pentru Pydantic)
            narrative_dict = narratives_map.get(f"EVENT_{idx}", {})
            for lang in ["en", "ro", "es", "de", "fr"]:
                if lang not in narrative_dict: narrative_dict[lang] = "Narrative pending..."
            
            titles_dict = item.get("titles", {})
            for lang in ["en", "ro", "es", "de", "fr"]:
                if lang not in titles_dict: titles_dict[lang] = "Event"

            # Media logic...
            wiki_urls = await scraper.fetch_gallery_urls(slug, limit=3)
            gallery = []
            for i, url in enumerate(wiki_urls):
                up_url = await safe_upload(scraper, url, f"ev_{year}_{slug[:15]}_{i}")
                if up_url: gallery.append(up_url)

            final_events_list.append(
                EventDetail(
                    category=EventCategory(item["category"].lower()),
                    year=year,
                    event_date=today.date(),
                    source_url=f"https://en.wikipedia.org/wiki/{slug}",
                    title_translations=Translations(**titles_dict),
                    narrative_translations=Translations(**narrative_dict),
                    impact_score=float(item["final_score"]),
                    page_views_30d=item["views"],
                    gallery=gallery,
                )
            )

        # ─────────────────────────────────────────────────────────
        # STEP 8: Final Payload
        # ─────────────────────────────────────────────────────────
        payload = DailyPayload(
            date_processed=today.date(),
            events=final_events_list,
            metadata={
                "target_date": str(today.date()),
                "pipeline_ver": "1.5_strict"
            }
        )

        await send_to_java(payload)

    except Exception as e:
        logger.error(f"🚨 Pipeline Crash: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())