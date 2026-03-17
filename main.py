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
from engine.ranker import ScoringEngine
from schema.models import DailyPayload, EventDetail, EventCategory, Translations

logger = setup_logger("MainPipeline")


# ─────────────────────────────────────────
# SEND TO JAVA
# ─────────────────────────────────────────
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
    timestamp = str(int(time.time()))
    auth_payload = f"{timestamp}.{body_json}"

    signature = hmac.new(
        secret.encode(),
        auth_payload.encode(),
        hashlib.sha256,
    ).digest()

    signature_base64 = base64.b64encode(signature).decode()

    headers = {
        "X-Timestamp": timestamp,
        "X-Signature": signature_base64,
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            logger.info(f"📤 Sending to Java: {target_url}")
            res = await client.post(target_url, content=body_json, headers=headers)

            if res.status_code in [200, 201]:
                logger.info("✅ SUCCESS!")
            else:
                logger.error(f"❌ {res.status_code}: {res.text}")

        except Exception as e:
            logger.error(f"🚨 Connection error: {e}")


# ─────────────────────────────────────────
# SAFE UPLOAD
# ─────────────────────────────────────────
async def safe_upload(scraper, url: str, public_id: str):
    if not url:
        return None
    try:
        return await asyncio.to_thread(scraper.upload_to_cloudinary, url, public_id)
    except Exception:
        return None


# ─────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────
async def main():
    logger.info("🚀 Smart Pipeline Started")

    scraper = SmartWikiScraper()
    ranker = ScoringEngine()

    today = datetime.now()

    try:
        # ─────────────────────────────────────
        # STEP 1: GET EVENTS (SMART SCRAPER)
        # ─────────────────────────────────────
        logger.info("🌐 Fetching smart events...")
        events = await scraper.get_today_interesting_events(limit=50)

        if not events:
            logger.error("❌ No events found")
            return

        # ─────────────────────────────────────
        # STEP 2: PAGE VIEWS
        # ─────────────────────────────────────
        logger.info("📊 Fetching page views...")
        tasks = [
            scraper.fetch_page_views(ev.get("slug", ""))
            for ev in events
        ]

        views = await asyncio.gather(*tasks)

        # ─────────────────────────────────────
        # STEP 3: FINAL SCORING
        # ─────────────────────────────────────
        for i, ev in enumerate(events):
            ev["views"] = views[i] if isinstance(views[i], int) else 0

            ev["final_score"] = ranker.calculate_final_score(
                ai_score=ev.get("interest_score", 50),
                views=ev["views"],
                category="history",
                year=ev.get("year", 0),
            )

        events.sort(key=lambda x: x["final_score"], reverse=True)
        top5 = events[:5]

        logger.info("🏆 FINAL TOP 5:")
        for i, ev in enumerate(top5):
            logger.info(f"{i+1}. [{ev['year']}] {ev['text'][:80]}")

        # ─────────────────────────────────────
        # STEP 4: BUILD FINAL OBJECTS
        # ─────────────────────────────────────
        final_events = []

        for ev in top5:
            slug = ev.get("slug", "")
            year = ev.get("year", 0)

            # images
            hero = await scraper.fetch_pro_image(slug)
            gallery_urls = await scraper.fetch_gallery_urls(slug)

            combined = []
            if hero:
                combined.append(hero)

            combined.extend(gallery_urls[:2])

            gallery = []
            for i, url in enumerate(combined):
                uploaded = await safe_upload(scraper, url, f"{slug}_{i}")
                if uploaded:
                    gallery.append(uploaded)

            # simple translations fallback
            title = ev["text"][:60]

            final_events.append(
                EventDetail(
                    category=EventCategory("culture_arts"),
                    year=year,
                    event_date=today.date(),
                    source_url=f"https://en.wikipedia.org/wiki/{slug}",
                    title_translations=Translations(
                        en=title,
                        ro=title,
                        es=title,
                        de=title,
                        fr=title,
                    ),
                    narrative_translations=Translations(
                        en=ev["text"],
                        ro=ev["text"],
                        es=ev["text"],
                        de=ev["text"],
                        fr=ev["text"],
                    ),
                    impact_score=float(ev["final_score"]),
                    page_views_30d=ev["views"],
                    gallery=gallery,
                )
            )

        # ─────────────────────────────────────
        # STEP 5: SEND
        # ─────────────────────────────────────
        payload = DailyPayload(
            date_processed=today.date(),
            events=final_events,
            metadata={
                "total": len(events),
                "final": len(final_events),
                "pipeline": "smart_scraper_v1",
            },
        )

        await send_to_java(payload)

    except Exception as e:
        logger.error(f"🚨 Pipeline crash: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())