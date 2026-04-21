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
from engine.quiz_generator import QuizGenerator
from engine.ranker import ScoringEngine
from engine.social_agent import SocialMediaAgent
from schema.models import DailyPayload, EventDetail, EventCategory, Translations

logger = setup_logger("MainPipeline")


async def send_to_java(payload: DailyPayload):
    target_url = config.JAVA_BACKEND_URL
    secret = config.INTERNAL_API_SECRET

    events_final = []
    for ev in payload.events:
        # NOTE: Backend Java does NOT know about 'quiz' field yet — do NOT send it.
        # Only send fields that EventDTO.java actually has.
        ev_dict = {
            "category": ev.category.value,
            "titleTranslations": ev.title_translations.model_dump(),
            "narrativeTranslations": ev.narrative_translations.model_dump(),
            "eventDate": ev.event_date.isoformat(),
            "impactScore": float(ev.impact_score),
            "sourceUrl": str(ev.source_url),
            "pageViews30d": int(ev.page_views_30d),
            "gallery": ev.gallery if ev.gallery else [],
            "isPro": bool(ev.is_pro),
            "location": ev.location,
        }
        events_final.append(ev_dict)

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
            logger.info(f"📦 Payload: {len(events_final)} events "
                        f"({sum(1 for e in events_final if e['isPro'])} PRO, "
                        f"{sum(1 for e in events_final if not e['isPro'])} FREE)")
            response = await client.post(target_url, content=body_bytes, headers=headers)
            if response.status_code in [200, 201]:
                logger.info(f"✅ SUCCESS! Payload accepted for date: {payload.date_processed}")
            else:
                logger.error(f"❌ Status {response.status_code}: {response.text}")
                # DEBUG: dump the exact JSON we sent so we can see what Java choked on
                logger.error(f"🧪 JSON sent (first 2000 chars):\n{body_json[:2000]}")
                if len(body_json) > 2000:
                    logger.error(f"🧪 JSON sent (last 1000 chars):\n{body_json[-1000:]}")
        except Exception as e:
            logger.error(f"🚨 Connection error: {e}")


async def safe_upload(scraper: WikiScraper, url: str, public_id: str):
    if not url:
        return None
    try:
        return await asyncio.to_thread(scraper.upload_to_cloudinary, url, public_id)
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════
# FREE PIPELINE — Existing flow: 60 → top 15 → top 5
# ══════════════════════════════════════════════════════════════════
async def run_free_pipeline(
    today: datetime,
    processor: AIProcessor,
    scraper: WikiScraper,
    ranker: ScoringEngine,
    quiz_gen: QuizGenerator,
) -> tuple:
    """Returns (final_events_list, metadata_dict)."""
    logger.info(f"🆓 FREE — Discovering events for {today.strftime('%B %d')}...")
    all_events = await processor.discover_events(today)
    logger.info(f"📋 FREE got {len(all_events)} validated events")

    if not all_events:
        logger.error("❌ FREE: AI returned no events")
        return [], {"free_discovered": 0}

    logger.info("🔬 FREE — Deep ranking to top 15...")
    top15 = await processor.deep_rank_and_select(all_events, today)
    if not top15:
        top15 = sorted(all_events, key=lambda x: x.get("ai_score", 0), reverse=True)[:15]

    logger.info("📊 FREE — Fetching pageviews for top 15...")
    view_tasks = [scraper.fetch_page_views(item.get("slug", "")) for item in top15]
    views = await asyncio.gather(*view_tasks)

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

    logger.info("🏆 FREE TOP 5:")
    for i, ev in enumerate(top5):
        logger.info(f"  {i+1}. [{ev['year']}] {ev['text'][:80]} → {ev['final_score']}")

    logger.info("✍️ FREE — Generating narratives...")
    narratives_map = await processor.generate_secondary_narratives(top5, today)

    logger.info("🧠 FREE — Generating quizzes...")
    quizzes = await quiz_gen.generate_quizzes(top5, narratives_map)

    final_events_list = await _build_event_details(
        top5, narratives_map, quizzes, today, scraper, is_pro=False
    )

    return final_events_list, {
        "free_discovered": len(all_events),
        "free_after_rank": len(top15),
        "free_final": len(final_events_list),
        "free_quizzes_ok": sum(1 for q in quizzes if q is not None),
    }


# ══════════════════════════════════════════════════════════════════
# PRO PIPELINE — New flow: personalities + media + sport (1 each)
# ══════════════════════════════════════════════════════════════════
async def run_pro_pipeline(
    today: datetime,
    processor: AIProcessor,
    scraper: WikiScraper,
    ranker: ScoringEngine,
    quiz_gen: QuizGenerator,
) -> tuple:
    """Returns (pro_events_list, metadata_dict)."""
    logger.info(f"💎 PRO — Discovering personalities/media/sport for {today.strftime('%B %d')}...")

    pro_candidates = await processor.discover_pro_events(today)
    logger.info(f"📋 PRO got {len(pro_candidates)} validated candidates")

    if not pro_candidates:
        logger.warning("⚠️ PRO: no candidates — skipping PRO pipeline")
        return [], {"pro_discovered": 0}

    logger.info("🔬 PRO — Selecting best event per category...")
    pro_selected = await processor.deep_rank_pro_per_category(pro_candidates, today)

    if not pro_selected:
        logger.warning("⚠️ PRO: no events selected — skipping")
        return [], {"pro_discovered": len(pro_candidates), "pro_final": 0}

    logger.info("📊 PRO — Fetching pageviews...")
    view_tasks = [scraper.fetch_page_views(item.get("slug", "")) for item in pro_selected]
    views = await asyncio.gather(*view_tasks)

    for idx, item in enumerate(pro_selected):
        item["views"] = views[idx] if isinstance(views[idx], int) else 0
        item["final_score"] = ranker.calculate_final_score(
            ai_score=item.get("deep_score", item.get("ai_score", 50)),
            views=item["views"],
            category=item.get("category", "personalities"),
            year=item.get("year", 0),
        )
        item["is_pro"] = True

    logger.info("🏆 PRO SELECTED:")
    for i, ev in enumerate(pro_selected):
        logger.info(
            f"  {i+1}. [{ev['category']}] [{ev['year']}] "
            f"{ev['text'][:70]} → {ev['final_score']}"
        )

    logger.info("✍️ PRO — Generating narratives...")
    narratives_map = await processor.generate_secondary_narratives(pro_selected, today)

    logger.info("🧠 PRO — Generating quizzes...")
    quizzes = await quiz_gen.generate_quizzes(pro_selected, narratives_map)

    final_pro_list = await _build_event_details(
        pro_selected, narratives_map, quizzes, today, scraper, is_pro=True
    )

    return final_pro_list, {
        "pro_discovered": len(pro_candidates),
        "pro_selected": len(pro_selected),
        "pro_final": len(final_pro_list),
        "pro_quizzes_ok": sum(1 for q in quizzes if q is not None),
    }


# ══════════════════════════════════════════════════════════════════
# SHARED — Build EventDetail objects (used by both free & pro)
# ══════════════════════════════════════════════════════════════════
async def _build_event_details(
    selected_items: list,
    narratives_map: dict,
    quizzes: list,
    today: datetime,
    scraper: WikiScraper,
    is_pro: bool,
) -> list:
    """Fetch images + assemble EventDetail objects."""
    tier_tag = "pro" if is_pro else "free"
    final_list = []

    for idx, item in enumerate(selected_items):
        slug = item.get("slug", "")
        year = item.get("year", 0)
        slug_display = slug.replace("_", " ")

        logger.info(f"🖼️ [{tier_tag.upper()}] Fetching images for: {slug_display}")

        hero_url = await scraper.fetch_pro_image(slug_display)
        wiki_urls = await scraper.fetch_gallery_urls(slug, limit=3)

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

        gallery = []
        for i, url in enumerate(combined_sources):
            img_url = await safe_upload(
                scraper, url, f"{tier_tag}_ev_{year}_{slug[:20]}_{i}"
            )
            if img_url:
                gallery.append(img_url)
            await asyncio.sleep(0.5)

        # Safely build the event_date, handling historical years < 1 AD
        try:
            ev_date = today.date().replace(year=year) if year > 0 else today.date()
        except ValueError:
            ev_date = today.date()

        narrative_data = narratives_map.get(f"EVENT_{idx}", {})
        titles = item.get("titles", {lang: "Historical Event" for lang in ["en", "ro", "es", "de", "fr"]})
        event_quiz = quizzes[idx] if idx < len(quizzes) else None

        try:
            category_enum = EventCategory(item["category"].lower())
        except ValueError:
            logger.error(f"⚠️ Unknown category '{item['category']}' — defaulting to culture_arts")
            category_enum = EventCategory.CULTURE_ARTS

        final_list.append(
            EventDetail(
                category=category_enum,
                year=year,
                event_date=ev_date,
                source_url=f"https://en.wikipedia.org/wiki/{slug}",
                title_translations=Translations(**titles),
                narrative_translations=Translations(**narrative_data),
                impact_score=float(item["final_score"]),
                page_views_30d=item["views"],
                gallery=gallery,
                quiz=event_quiz,
                is_pro=is_pro,
                location=item.get("location"),
            )
        )

    return final_list


# ══════════════════════════════════════════════════════════════════
# MAIN — orchestrate free + pro pipelines in parallel
# ══════════════════════════════════════════════════════════════════
async def main():
    logger.info("🚀 Starting DailyHistory Pipeline (FREE + PRO)...")

    scraper = WikiScraper()
    processor = AIProcessor()
    quiz_gen = QuizGenerator()
    ranker = ScoringEngine()

    today = datetime.now()

    try:
        logger.info("⚡ Launching FREE + PRO pipelines in parallel...")
        (free_events, free_meta), (pro_events, pro_meta) = await asyncio.gather(
            run_free_pipeline(today, processor, scraper, ranker, quiz_gen),
            run_pro_pipeline(today, processor, scraper, ranker, quiz_gen),
        )

        all_events = free_events + pro_events

        if not all_events:
            logger.error("❌ No events generated — aborting payload send")
            return

        # ═══════════════════════════════════════════════════════════
        # DEBUG MODE — send FREE only, then PRO only, in 2 separate calls
        # This isolates which one causes the 500
        # ═══════════════════════════════════════════════════════════
        DEBUG_SPLIT_SEND = True
        if DEBUG_SPLIT_SEND:
            logger.info("🧪 DEBUG MODE: splitting payload to isolate issue")

            # First: only FREE events
            if free_events:
                logger.info("━" * 60)
                logger.info("🧪 TEST 1/2: Sending ONLY FREE events...")
                free_payload = DailyPayload(
                    date_processed=today.date(),
                    events=free_events,
                    metadata={"test": "free_only"},
                )
                await send_to_java(free_payload)

            # Then: only PRO events
            if pro_events:
                logger.info("━" * 60)
                logger.info("🧪 TEST 2/2: Sending ONLY PRO events...")
                pro_payload = DailyPayload(
                    date_processed=today.date(),
                    events=pro_events,
                    metadata={"test": "pro_only"},
                )
                # Log the exact JSON for PRO events
                for i, ev in enumerate(pro_events):
                    logger.info(
                        f"🧪 PRO #{i}: category={ev.category.value}, "
                        f"year={ev.year}, is_pro={ev.is_pro}, "
                        f"location={ev.location!r}, "
                        f"gallery_count={len(ev.gallery)}"
                    )
                await send_to_java(pro_payload)

            logger.info("━" * 60)
            logger.info("🧪 DEBUG runs complete")
            return

        combined_metadata = {
            **free_meta,
            **pro_meta,
            "target_date": str(today.date()),
            "pipeline": "ai_driven_with_pro_v5",
            "total_events": len(all_events),
        }

        payload = DailyPayload(
            date_processed=today.date(),
            events=all_events,
            metadata=combined_metadata,
        )

        logger.info("━" * 60)
        logger.info(f"📊 FINAL PAYLOAD: {len(all_events)} events total")
        logger.info(f"   → FREE: {len(free_events)} | PRO: {len(pro_events)}")
        logger.info("━" * 60)
        for i, ev in enumerate(all_events):
            tier = "💎 PRO" if ev.is_pro else "🆓 FREE"
            title = ev.title_translations.en[:55]
            logger.info(
                f"  {i+1}. {tier} [{ev.category.value:20s}] | "
                f"{ev.year} | {title}"
            )
        logger.info("━" * 60)

        await send_to_java(payload)

        logger.info("📱 Running Social Media Agent (FREE events only)...")
        try:
            if free_events:
                social_agent = SocialMediaAgent()
                await social_agent.generate_and_post(free_events, today)
            else:
                logger.warning("⚠️ No FREE events — skipping social agent")
        except Exception as e:
            logger.error(f"⚠️ Social Media Agent failed (non-critical): {e}")

    except Exception as e:
        logger.error(f"🚨 Pipeline Crash: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())