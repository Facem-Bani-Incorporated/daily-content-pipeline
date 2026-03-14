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
    logger.info("🚀 Starting Pipeline for TOMORROW's events (AI-driven)...")

    scraper = WikiScraper()
    processor = AIProcessor()
    ranker = ScoringEngine()

    tomorrow = datetime.now() + timedelta(days=1)

    try:
        # ─────────────────────────────────────────────
        # STEP 1: AI discovers the best events for tomorrow's date
        # No Wikipedia feed — AI decides based on its knowledge
        # ─────────────────────────────────────────────
        logger.info(f"🤖 AI discovering events for {tomorrow.strftime('%B %d')}...")
        raw_events = await processor.discover_events(tomorrow)
        logger.info(f"📋 AI returned {len(raw_events)} candidate events")

        if not raw_events:
            logger.error("❌ AI returned no events. Aborting.")
            return

        # ─────────────────────────────────────────────
        # STEP 2: Enrich top 10 candidates with titles + refined AI scores
        # ─────────────────────────────────────────────
        top_candidates = raw_events[:10]
        ai_data = await processor.batch_score_and_categorize(top_candidates, tomorrow)
        ai_results = ai_data.get("results", {})

        # ─────────────────────────────────────────────
        # STEP 3: Fetch Wikipedia pageviews in parallel (validates real interest)
        # ─────────────────────────────────────────────
        view_tasks = [scraper.fetch_page_views(item.get("slug", "")) for item in top_candidates]
        views = await asyncio.gather(*view_tasks)

        # ─────────────────────────────────────────────
        # STEP 4: Merge scores and rank
        # ─────────────────────────────────────────────
        for idx, item in enumerate(top_candidates):
            refined = ai_results.get(f"ID_{idx}", {})
            item["views"] = views[idx] if isinstance(views[idx], int) else 0
            item["titles"] = refined.get("titles", {lang: "Historical Event" for lang in ["en", "ro", "es", "de", "fr"]})

            # Use refined AI score if available, otherwise fall back to discovery score
            item["refined_score"] = refined.get("score", item.get("ai_score", 50))

            item["final_score"] = ranker.calculate_final_score(
                ai_score=item["refined_score"],
                views=item["views"],
                category=item.get("category", "culture_arts"),
                year=item.get("year", 0),
            )

        top_candidates.sort(key=lambda x: x.get("final_score", 0), reverse=True)
        top_5 = top_candidates[:5]

        logger.info("🏆 Top 5 events selected:")
        for i, ev in enumerate(top_5):
            logger.info(f"  {i+1}. [{ev['year']}] {ev['text'][:80]} — score: {ev['final_score']}")

        # ─────────────────────────────────────────────
        # STEP 5: Generate multilingual narratives for top 5
        # ─────────────────────────────────────────────
        narratives_map = await processor.generate_secondary_narratives(top_5, tomorrow)

        # ─────────────────────────────────────────────
        # STEP 6: Fetch & upload images
        # AI provides the Wikipedia slug → scraper fetches gallery from that article
        # ─────────────────────────────────────────────
        final_events_list = []

        for idx, item in enumerate(top_5):
            slug = item.get("slug", "")
            year = item.get("year", 0)
            slug_display = slug.replace("_", " ")

            # 6a. Try Pexels for a high-quality hero image
            hero_url = await scraper.fetch_pro_image(slug_display)

            # 6b. Fetch up to 3 images from the Wikipedia article the AI specified
            wiki_urls = await scraper.fetch_gallery_urls(slug, limit=3)

            # 6c. Merge sources (hero first, then wiki, no duplicates)
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

            # 6d. Upload to Cloudinary
            gallery = []
            for i, url in enumerate(combined_sources):
                img_url = await safe_upload(scraper, url, f"ev_{year}_{slug[:20]}_{i}")
                if img_url:
                    gallery.append(img_url)
                await asyncio.sleep(0.5)

            # 6e. Fallback: use Wikipedia thumbnail if gallery is empty
            if not gallery and item.get("wiki_thumb"):
                fb = await safe_upload(scraper, item["wiki_thumb"], f"ev_{year}_{slug[:20]}_fb")
                gallery = [fb] if fb else []

            # ─────────────────────────────────────────────
            # STEP 7: Assemble EventDetail
            # ─────────────────────────────────────────────
            narrative_data = narratives_map.get(f"EVENT_{idx}", {})

            final_events_list.append(
                EventDetail(
                    category=EventCategory(item["category"].lower()),
                    year=year,
                    event_date=tomorrow.date(),
                    source_url=f"https://en.wikipedia.org/wiki/{slug}",
                    title_translations=Translations(**item["titles"]),
                    narrative_translations=Translations(**narrative_data),
                    impact_score=float(item["final_score"]),
                    page_views_30d=item["views"],
                    gallery=gallery,
                )
            )

        # ─────────────────────────────────────────────
        # STEP 8: Send payload to Java backend
        # ─────────────────────────────────────────────
        payload = DailyPayload(
            date_processed=tomorrow.date(),
            events=final_events_list,
            metadata={
                "processed": len(top_candidates),
                "target_date": str(tomorrow.date()),
                "pipeline": "ai_driven_v2",
            },
        )

        await send_to_java(payload)

    except Exception as e:
        logger.error(f"🚨 Pipeline Crash: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())