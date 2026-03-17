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
from engine.scraper import SmartWikiScraper
from engine.processor import AIProcessor
from engine.ranker import ScoringEngine
from schema.models import DailyPayload, EventDetail, EventCategory, Translations

logger = setup_logger("MainPipeline")


# ─────────────────────────────────────────────
# SEND TO JAVA (UNCHANGED)
# ─────────────────────────────────────────────
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


# ─────────────────────────────────────────────
# SAFE UPLOAD
# ─────────────────────────────────────────────
async def safe_upload(scraper, url: str, public_id: str):
    if not url:
        return None
    try:
        return await asyncio.to_thread(scraper.upload_to_cloudinary, url, public_id)
    except Exception:
        return None


# ─────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────
async def main():
    logger.info("🚀 Starting Pipeline (Wikipedia-driven + AI ranking)...")

    scraper = SmartWikiScraper()
    processor = AIProcessor()
    ranker = ScoringEngine()

    today = datetime.now()

    try:
        # ─────────────────────────────────────────
        # STEP 1: REAL EVENTS (NO AI)
        # ─────────────────────────────────────────
        logger.info(f"🌐 Fetching real events for {today.strftime('%B %d')}...")

        all_events = await scraper.get_today_interesting_events(limit=60)

        if not all_events:
            logger.error("❌ No events found. Aborting.")
            return

        # ─────────────────────────────────────────
        # STEP 2: AI DEEP RANK (ONLY RANKING)
        # ─────────────────────────────────────────
        logger.info("🔬 AI ranking top events...")

        try:
            top15 = await processor.deep_rank_and_select(all_events, today)
        except Exception as e:
            logger.warning(f"⚠️ AI failed: {e}")
            top15 = all_events[:15]

        if not top15:
            top15 = all_events[:15]

        # ─────────────────────────────────────────
        # STEP 3: PAGEVIEWS (REAL SIGNAL)
        # ─────────────────────────────────────────
        logger.info("📊 Fetching Wikipedia pageviews...")

        view_tasks = [
            scraper.fetch_page_views(item.get("slug", ""))
            for item in top15
        ]

        views = await asyncio.gather(*view_tasks)

        # ─────────────────────────────────────────
        # STEP 4: FINAL SCORE
        # ─────────────────────────────────────────
        for idx, item in enumerate(top15):
            item["views"] = views[idx] if isinstance(views[idx], int) else 0

            item["final_score"] = ranker.calculate_final_score(
                ai_score=item.get("deep_score", item.get("interest_score", 50)),
                views=item["views"],
                category=item.get("category", "culture_arts"),
                year=item.get("year", 0),
            )

        top15.sort(key=lambda x: x.get("final_score", 0), reverse=True)
        top5 = top15[:5]

        logger.info("🏆 FINAL TOP 5:")
        for i, ev in enumerate(top5):
            logger.info(
                f"{i+1}. [{ev['year']}] {ev['text'][:80]} → score={ev['final_score']}"
            )

        # ─────────────────────────────────────────
        # STEP 5: AI NARRATIVES
        # ─────────────────────────────────────────
        logger.info("✍️ Generating narratives...")

        try:
            narratives_map = await processor.generate_secondary_narratives(top5, today)
        except Exception:
            narratives_map = {}

        # ─────────────────────────────────────────
        # STEP 6: IMAGES
        # ─────────────────────────────────────────
        final_events_list = []

        for idx, item in enumerate(top5):
            slug = item.get("slug", "")
            year = item.get("year", 0)

            logger.info(f"🖼️ Images for: {slug}")

            # Hero
            hero_url = None
            try:
                hero_url = await scraper.fetch_pro_image(slug.replace("_", " "))
            except Exception:
                pass

            # Wiki gallery
            try:
                wiki_urls = await scraper.fetch_gallery_urls(slug, limit=3)
            except Exception:
                wiki_urls = []

            combined = []
            seen = set()

            if hero_url:
                combined.append(hero_url)
                seen.add(hero_url)

            for u in wiki_urls:
                if len(combined) >= 3:
                    break
                if u not in seen and ".gif" not in u.lower():
                    combined.append(u)
                    seen.add(u)

            gallery = []
            for i, url in enumerate(combined):
                img = await safe_upload(scraper, url, f"ev_{year}_{slug[:20]}_{i}")
                if img:
                    gallery.append(img)

            # fallback
            if not gallery and item.get("wiki_thumb"):
                fb = await safe_upload(scraper, item["wiki_thumb"], f"fb_{slug}")
                if fb:
                    gallery = [fb]

            # ─────────────────────────────────────
            # BUILD EVENT
            # ─────────────────────────────────────
            narrative_data = narratives_map.get(f"EVENT_{idx}", {})

            titles = item.get(
                "titles",
                {lang: item.get("text", "Historical Event") for lang in ["en", "ro", "es", "de", "fr"]}
            )

            final_events_list.append(
                EventDetail(
                    category=EventCategory(item.get("category", "culture_arts").lower()),
                    year=year,
                    event_date=today.date(),
                    source_url=f"https://en.wikipedia.org/wiki/{slug}",
                    title_translations=Translations(**titles),
                    narrative_translations=Translations(**narrative_data),
                    impact_score=float(item.get("final_score", 50)),
                    page_views_30d=item.get("views", 0),
                    gallery=gallery,
                )
            )

        # ─────────────────────────────────────────
        # STEP 7: SEND
        # ─────────────────────────────────────────
        payload = DailyPayload(
            date_processed=today.date(),
            events=final_events_list,
            metadata={
                "total_discovered": len(all_events),
                "after_ai_rank": len(top15),
                "final_selected": len(final_events_list),
                "pipeline": "wiki_real_v4",
            },
        )

        await send_to_java(payload)

    except Exception as e:
        logger.error(f"🚨 Pipeline Crash: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())