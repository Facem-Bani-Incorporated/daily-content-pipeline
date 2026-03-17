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
    logger.info("🚀 Starting Pipeline for TOMORROW's events (AI-driven, 60→15→5)...")

    scraper = WikiScraper()
    processor = AIProcessor()
    ranker = ScoringEngine()

    today = datetime.now()

    try:
        # ─────────────────────────────────────────────────────────
        # STEP 1: AI casts a wide net — 60 events for today's date
        # ─────────────────────────────────────────────────────────
        logger.info(f"🌐 PASS 1 — Discovering 60 events for {today.strftime('%B %d')}...")
        all_events = await processor.discover_events(today)
        logger.info(f"📋 Got {len(all_events)} validated events from AI")

        if not all_events:
            logger.error("❌ AI returned no events. Aborting.")
            return

        # ─────────────────────────────────────────────────────────
        # STEP 2: AI deep-ranks and selects top 15 globally important
        # ─────────────────────────────────────────────────────────
        logger.info("🔬 PASS 2 — Deep ranking: selecting top 15 most globally impactful...")
        top15 = await processor.deep_rank_and_select(all_events, today)
        logger.info(f"🏅 Deep rank returned {len(top15)} enriched events")

        if not top15:
            logger.warning("⚠️ Deep rank failed, falling back to raw AI scores")
            top15 = sorted(all_events, key=lambda x: x.get("ai_score", 0), reverse=True)[:15]

        # ─────────────────────────────────────────────────────────
        # STEP 3: Fetch Wikipedia pageviews in parallel for top 15
        # Views act as a real-world validation signal
        # ─────────────────────────────────────────────────────────
        logger.info("📊 Fetching Wikipedia pageviews for top 15...")
        view_tasks = [scraper.fetch_page_views(item.get("slug", "")) for item in top15]
        views = await asyncio.gather(*view_tasks)

        # ─────────────────────────────────────────────────────────
        # STEP 4: Calculate final scores and select top 5
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

        logger.info("🏆 FINAL TOP 5:")
        for i, ev in enumerate(top5):
            breakdown = ev.get("score_breakdown", {})
            logger.info(
                f"  {i+1}. [{ev['year']}] {ev['text'][:80]}\n"
                f"      → final={ev['final_score']} | deep={ev.get('deep_score','?')} | "
                f"views={ev['views']} | breakdown={breakdown}"
            )

        # ─────────────────────────────────────────────────────────
        # STEP 5: Generate multilingual narratives for top 5
        # ─────────────────────────────────────────────────────────
        logger.info("✍️ Generating multilingual narratives for top 5...")
        narratives_map = await processor.generate_secondary_narratives(top5, today)

        # ─────────────────────────────────────────────────────────
        # STEP 6: Fetch & upload images using AI-provided wiki slugs
        # ─────────────────────────────────────────────────────────
        final_events_list = []

        for idx, item in enumerate(top5):
            slug = item.get("slug", "")
            year = item.get("year", 0)
            slug_display = slug.replace("_", " ")

            logger.info(f"🖼️ Fetching images for: {slug_display}")

            # Hero image from Pexels
            hero_url = await scraper.fetch_pro_image(slug_display)

            # Gallery from the exact Wikipedia article AI specified
            wiki_urls = await scraper.fetch_gallery_urls(slug, limit=3)

            # Merge: hero first, then wiki, no duplicates, no GIFs
            combined_sources = []
            seen_urls: set = set()

            if hero_url:
                combined_sources.append(hero_url)
                seen_urls.add(hero_url)

            for w_url in wiki_urls:
                if len(combined_sources) >= 3:
                    break
                if w_url not in seen_urls and ".gif" not in w_url.lower():
                    combined_sources.append(w_url)
                    seen_urls.add(w_url)

            # Upload to Cloudinary
            gallery = []
            for i, url in enumerate(combined_sources):
                img_url = await safe_upload(scraper, url, f"ev_{year}_{slug[:20]}_{i}")
                if img_url:
                    gallery.append(img_url)
                await asyncio.sleep(0.5)

            # Fallback to wiki_thumb if gallery is empty
            if not gallery and item.get("wiki_thumb"):
                fb = await safe_upload(scraper, item["wiki_thumb"], f"ev_{year}_{slug[:20]}_fb")
                gallery = [fb] if fb else []

            # ─────────────────────────────────────────────────────
            # STEP 7: Assemble EventDetail
            # ─────────────────────────────────────────────────────
            narrative_data = narratives_map.get(f"EVENT_{idx}", {})
            titles = item.get("titles", {lang: "Historical Event" for lang in ["en", "ro", "es", "de", "fr"]})

            final_events_list.append(
                EventDetail(
                    category=EventCategory(item["category"].lower()),
                    year=year,
                    event_date=today.date(),
                    source_url=f"https://en.wikipedia.org/wiki/{slug}",
                    title_translations=Translations(**titles),
                    narrative_translations=Translations(**narrative_data),
                    impact_score=float(item["final_score"]),
                    page_views_30d=item["views"],
                    gallery=gallery,
                )
            )

        # ─────────────────────────────────────────────────────────
        # STEP 8: Send final payload to Java backend
        # ─────────────────────────────────────────────────────────
        payload = DailyPayload(
            date_processed=today.date(),
            events=final_events_list,
            metadata={
                "total_discovered": len(all_events),
                "after_deep_rank": len(top15),
                "final_selected": len(final_events_list),
                "target_date": str(today.date()),
                "pipeline": "ai_driven_60_to_5_v3",
            },
        )

        await send_to_java(payload)

    except Exception as e:
        logger.error(f"🚨 Pipeline Crash: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())