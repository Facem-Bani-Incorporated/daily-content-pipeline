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
from engine.deduplicator import EventDeduplicator
from engine.wiki_date_validator import WikiDateValidator
from schema.models import DailyPayload, EventDetail, EventCategory, Translations

logger = setup_logger("MainPipeline")

# How many events we want at the end of each pipeline
TARGET_FREE_COUNT = 6
TARGET_PRO_COUNT = 4  # 1 personalities + 1 media + 1 sport + 1 extra (best-of-the-rest)

# Minimum acceptable count before we fall back to non-validated events
MIN_FREE_COUNT = 6
MIN_PRO_COUNT = 4

# Minimum final_score (0-100) for a NEW event to be accepted in refresh mode.
# New events below this threshold are considered low-relevance and are replaced
# by the best existing events from the DB.
REFRESH_SCORE_THRESHOLD = 60


def _db_row_to_event_detail(row: dict) -> EventDetail:
    """
    Convert a raw DB row dict (from load_top_events_for_date) into an EventDetail.
    Used in refresh mode to fill slots with existing high-quality events.
    No narrative generation or image uploading — uses stored data as-is.
    """
    def _safe_dict(val, default=None):
        if isinstance(val, dict):
            return val
        return default or {}

    def _safe_list(val):
        if isinstance(val, list):
            return val
        return []

    lang_defaults = {"en": "", "ro": "", "es": "", "de": "", "fr": ""}

    title_data = {**lang_defaults, **_safe_dict(row.get("title_translations"))}
    narrative_data = {**lang_defaults, **_safe_dict(row.get("narrative_translations"))}

    try:
        category_enum = EventCategory(str(row.get("category") or "").lower())
    except ValueError:
        category_enum = EventCategory.CULTURE_ARTS

    event_date = row.get("event_date")
    try:
        year = int(event_date.year) if hasattr(event_date, "year") else 0
    except Exception:
        year = 0

    return EventDetail(
        category=category_enum,
        year=year,
        event_date=event_date,
        source_url=str(row.get("source_url") or ""),
        title_translations=Translations(
            en=str(title_data.get("en") or ""),
            ro=str(title_data.get("ro") or ""),
            es=str(title_data.get("es") or ""),
            de=str(title_data.get("de") or ""),
            fr=str(title_data.get("fr") or ""),
        ),
        narrative_translations=Translations(
            en=str(narrative_data.get("en") or ""),
            ro=str(narrative_data.get("ro") or ""),
            es=str(narrative_data.get("es") or ""),
            de=str(narrative_data.get("de") or ""),
            fr=str(narrative_data.get("fr") or ""),
        ),
        impact_score=float(row.get("impact_score") or 0),
        page_views_30d=int(row.get("page_views_30d") or 0),
        gallery=_safe_list(row.get("gallery")),
        quiz=None,  # quiz preserved in DB; not re-sent to avoid overwrite
        is_pro=bool(row.get("is_pro", False)),
        location=row.get("location"),
    )


def _serialize_quiz(quiz) -> dict | None:
    """
    Serialize QuizTranslations into the JSON structure expected by the Java backend.
    Uses camelCase keys (correctId) to match the Java DTO.
    Returns None if quiz is missing — backend treats null as "no quiz yet".
    """
    if quiz is None:
        return None
    result = {}
    for lang in ["en", "ro", "es", "de", "fr"]:
        questions = getattr(quiz, lang, [])
        result[lang] = [
            {
                "id": q.id,
                "question": q.question,
                "options": [{"id": opt.id, "text": opt.text} for opt in q.options],
                "correctId": q.correct_id,
                "explanation": q.explanation,
            }
            for q in questions
        ]
    return result


def _dedupe_by_slug(events: list, label: str = "") -> list:
    """Remove same-slug duplicates within a single list. Keeps first occurrence."""
    seen_slugs = set()
    result = []
    dropped = 0
    for ev in events:
        slug = (ev.get("slug") or "").strip()
        if not slug:
            continue
        if slug in seen_slugs:
            dropped += 1
            logger.warning(f"🔁 [{label}] In-payload duplicate dropped: {slug}")
            continue
        seen_slugs.add(slug)
        result.append(ev)

    if dropped:
        logger.info(f"🧹 [{label}] In-payload dedup: {len(result)} unique, {dropped} dropped")
    return result


async def send_to_java(payload: DailyPayload):
    target_url = config.JAVA_BACKEND_URL
    secret = config.INTERNAL_API_SECRET

    events_final = []
    for ev in payload.events:
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
            "quiz": _serialize_quiz(ev.quiz),
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
            logger.info(
                f"📦 Payload: {len(events_final)} events "
                f"({sum(1 for e in events_final if e['isPro'])} PRO, "
                f"{sum(1 for e in events_final if not e['isPro'])} FREE)"
            )
            response = await client.post(target_url, content=body_bytes, headers=headers)
            if response.status_code in [200, 201]:
                logger.info(f"✅ SUCCESS! Payload accepted for date: {payload.date_processed}")
            else:
                logger.error(f"❌ Status {response.status_code}: {response.text}")
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


def _is_wikipedia_url(url: str) -> bool:
    if not url:
        return False
    url_lower = url.lower()
    return "wikipedia.org" in url_lower or "wikimedia.org" in url_lower


def _backfill_to_minimum(
    validated: list,
    all_candidates: list,
    minimum: int,
    label: str = "FREE",
) -> list:
    """
    If validator was too aggressive and rejected events we actually need,
    backfill from the original candidate pool (sorted by ai_score) to
    reach at least `minimum` events.

    Backfilled events are ranked by AI confidence score so we still get
    the AI's best picks, just without the wiki-date check.
    """
    if len(validated) >= minimum:
        return validated

    needed = minimum - len(validated)
    used_slugs = {(v.get("slug") or "").lower() for v in validated}

    backfill_pool = [
        c for c in all_candidates
        if (c.get("slug") or "").lower() not in used_slugs
    ]
    backfill_pool.sort(key=lambda x: x.get("ai_score", 0), reverse=True)

    backfilled = backfill_pool[:needed]
    if backfilled:
        logger.warning(
            f"⚠️ [{label}] Validator too strict — backfilling {len(backfilled)} "
            f"events from AI candidate pool to reach min {minimum}"
        )
        for ev in backfilled:
            logger.warning(
                f"  ↩ Backfilled: {ev.get('year')} {ev.get('slug')} "
                f"(ai_score: {ev.get('ai_score', 0)})"
            )

    return validated + backfilled


# ══════════════════════════════════════════════════════════════════
# FREE PIPELINE
# Target: 5 events labeled is_pro=False
# ══════════════════════════════════════════════════════════════════
async def run_free_pipeline(
    today: datetime,
    processor: AIProcessor,
    scraper: WikiScraper,
    ranker: ScoringEngine,
    quiz_gen: QuizGenerator,
    deduper: EventDeduplicator,
    date_validator: WikiDateValidator,
    is_refresh: bool = False,
) -> tuple:
    mode = "REFRESH" if is_refresh else "INITIAL"
    logger.info(f"🆓 FREE [{mode}] — Discovering events for {today.strftime('%B %d')}...")
    all_events = await processor.discover_events(today)
    all_events = _dedupe_by_slug(all_events, "FREE-discover")
    logger.info(f"📋 FREE got {len(all_events)} unique validated events")

    if not all_events:
        logger.error("❌ FREE: AI returned no events")
        return [], {"free_discovered": 0}

    original_pool = list(all_events)

    logger.info("🗓️ FREE — Validating dates against Wikipedia...")
    validated = await date_validator.validate_events(all_events, today, tier="FREE")

    if len(validated) < MIN_FREE_COUNT:
        validated = _backfill_to_minimum(
            validated, original_pool, MIN_FREE_COUNT, label="FREE"
        )

    if not validated:
        logger.error("❌ FREE: no events available even after backfill")
        return [], {"free_discovered": len(all_events), "free_after_validation": 0}

    logger.info(f"🔬 FREE — Deep ranking {len(validated)} events...")
    top_ranked = await processor.deep_rank_and_select(validated, today)
    top_ranked = _dedupe_by_slug(top_ranked, "FREE-rank")

    if not top_ranked:
        top_ranked = sorted(validated, key=lambda x: x.get("ai_score", 0), reverse=True)
        top_ranked = _dedupe_by_slug(top_ranked, "FREE-rank-fallback")

    logger.info("📊 FREE — Fetching pageviews...")
    view_tasks = [scraper.fetch_page_views(item.get("slug", "")) for item in top_ranked]
    views = await asyncio.gather(*view_tasks)

    for idx, item in enumerate(top_ranked):
        item["views"] = views[idx] if isinstance(views[idx], int) else 0
        item["final_score"] = ranker.calculate_final_score(
            ai_score=item.get("deep_score", item.get("ai_score", 50)),
            views=item["views"],
            category=item.get("category", "culture_arts"),
            year=item.get("year", 0),
        )

    top_ranked.sort(key=lambda x: x.get("final_score", 0), reverse=True)

    logger.info("🔍 FREE — Filtering duplicates against existing DB events...")
    deduped = deduper.filter_duplicates(top_ranked, tier="FREE")
    deduped = _dedupe_by_slug(deduped, "FREE-final")

    if not deduped and not is_refresh:
        logger.error("❌ FREE: no non-duplicate events left")
        return [], {
            "free_discovered": len(all_events),
            "free_after_validation": len(validated),
            "free_after_dedup": 0,
            "free_final": 0,
        }

    # ── REFRESH MODE ──────────────────────────────────────────────
    # Keep only new events that score >= threshold.
    # Fill remaining slots with best existing DB events so total = TARGET_FREE_COUNT.
    if is_refresh:
        high_quality = [e for e in deduped if e.get("final_score", 0) >= REFRESH_SCORE_THRESHOLD]
        logger.info(
            f"🔄 FREE REFRESH: {len(high_quality)} new events score≥{REFRESH_SCORE_THRESHOLD} "
            f"(out of {len(deduped)} new candidates)"
        )

        slots_needed = TARGET_FREE_COUNT - len(high_quality)
        filler_details: list = []
        if slots_needed > 0:
            exclude_slugs = {e.get("slug", "").lower() for e in high_quality}
            filler_rows = deduper.load_top_events_for_date(
                today.date(), is_pro=False, limit=slots_needed, exclude_slugs=exclude_slugs
            )
            filler_details = [_db_row_to_event_detail(row) for row in filler_rows]
            logger.info(f"📂 FREE REFRESH: filling {len(filler_details)} slots from DB")

        new_details: list = []
        if high_quality:
            logger.info("✍️ FREE REFRESH — Generating narratives for new events...")
            narratives_map = await processor.generate_secondary_narratives(high_quality, today)
            quizzes = await quiz_gen.generate_quizzes(high_quality, narratives_map)
            new_details = await _build_event_details(
                high_quality, narratives_map, quizzes, today, scraper, is_pro=False
            )

        final_events_list = new_details + filler_details
        logger.info(
            f"🏆 FREE REFRESH TOTAL: {len(new_details)} new + "
            f"{len(filler_details)} from DB = {len(final_events_list)}"
        )
        return final_events_list, {
            "free_discovered": len(all_events),
            "free_after_validation": len(validated),
            "free_new_qualified": len(high_quality),
            "free_filler_from_db": len(filler_details),
            "free_final": len(final_events_list),
        }

    # ── INITIAL MODE ──────────────────────────────────────────────
    selected = deduped[:TARGET_FREE_COUNT]

    if len(deduped) < TARGET_FREE_COUNT:
        logger.warning(
            f"⚠️ FREE: only {len(deduped)} non-dup events available "
            f"(wanted {TARGET_FREE_COUNT})"
        )

    logger.info(f"🏆 FREE TOP {len(selected)} (post-dedup, {len(deduped)} non-dup candidates):")
    for i, ev in enumerate(selected):
        logger.info(f"  {i+1}. [{ev['year']}] {ev['text'][:80]} → {ev['final_score']}")

    logger.info("✍️ FREE — Generating narratives...")
    narratives_map = await processor.generate_secondary_narratives(selected, today)

    logger.info("🧠 FREE — Generating quizzes...")
    quizzes = await quiz_gen.generate_quizzes(selected, narratives_map)

    final_events_list = await _build_event_details(
        selected, narratives_map, quizzes, today, scraper, is_pro=False
    )

    return final_events_list, {
        "free_discovered": len(all_events),
        "free_after_validation": len(validated),
        "free_after_dedup": len(deduped),
        "free_final": len(final_events_list),
        "free_quizzes_ok": sum(1 for q in quizzes if q is not None),
    }


# ══════════════════════════════════════════════════════════════════
# PRO PIPELINE
# Target: 3 events (1 personalities + 1 media + 1 sport), all is_pro=True
# ══════════════════════════════════════════════════════════════════
async def run_pro_pipeline(
    today: datetime,
    processor: AIProcessor,
    scraper: WikiScraper,
    ranker: ScoringEngine,
    quiz_gen: QuizGenerator,
    deduper: EventDeduplicator,
    date_validator: WikiDateValidator,
    is_refresh: bool = False,
) -> tuple:
    mode = "REFRESH" if is_refresh else "INITIAL"
    logger.info(f"💎 PRO [{mode}] — Discovering personalities/media/sport for {today.strftime('%B %d')}...")
    pro_candidates = await processor.discover_pro_events(today)
    pro_candidates = _dedupe_by_slug(pro_candidates, "PRO-discover")
    logger.info(f"📋 PRO got {len(pro_candidates)} unique candidates")

    if not pro_candidates:
        logger.warning("⚠️ PRO: no candidates")
        return [], {"pro_discovered": 0}

    original_pool = list(pro_candidates)

    logger.info("🗓️ PRO — Validating dates against Wikipedia...")
    validated = await date_validator.validate_events(pro_candidates, today, tier="PRO")

    # PRO needs at least 1 per category — backfill per category if validator was too strict
    pro_cats = ["personalities", "media", "sport"]
    by_cat_validated = {c: [] for c in pro_cats}
    for ev in validated:
        cat = ev.get("category", "").lower()
        if cat in by_cat_validated:
            by_cat_validated[cat].append(ev)

    by_cat_original = {c: [] for c in pro_cats}
    for ev in original_pool:
        cat = ev.get("category", "").lower()
        if cat in by_cat_original:
            by_cat_original[cat].append(ev)

    # Backfill missing categories
    final_pool = []
    for cat in pro_cats:
        cat_validated = by_cat_validated[cat]
        if not cat_validated and by_cat_original[cat]:
            backfill = sorted(
                by_cat_original[cat],
                key=lambda x: x.get("ai_score", 0),
                reverse=True,
            )[:3]
            logger.warning(
                f"⚠️ PRO [{cat}]: validator rejected all — backfilling {len(backfill)} candidates"
            )
            final_pool.extend(backfill)
        else:
            final_pool.extend(cat_validated)

    if not final_pool:
        logger.warning("⚠️ PRO: no candidates available even after backfill")
        return [], {"pro_discovered": 0, "pro_after_validation": 0}

    logger.info("🔍 PRO — Filtering candidates against existing DB events...")
    pro_clean = deduper.filter_duplicates(final_pool, tier="PRO")
    pro_clean = _dedupe_by_slug(pro_clean, "PRO-clean")

    if not pro_clean and not is_refresh:
        logger.warning("⚠️ PRO: all candidates were duplicates")
        return [], {
            "pro_discovered": len(pro_candidates),
            "pro_after_dedup": 0,
            "pro_final": 0,
        }

    logger.info("🔬 PRO — Selecting best event per category...")
    pro_selected = await processor.deep_rank_pro_per_category(pro_clean, today) if pro_clean else []
    pro_selected = _dedupe_by_slug(pro_selected, "PRO-rank")

    if not pro_selected and not is_refresh:
        logger.warning("⚠️ PRO: no events selected by ranker")
        return [], {
            "pro_discovered": len(pro_candidates),
            "pro_after_dedup": len(pro_clean),
            "pro_final": 0,
        }

    # Compute pageviews + final_score for all selected events
    if pro_selected:
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

    # ── REFRESH MODE ──────────────────────────────────────────────
    # Keep only new events that score >= threshold.
    # Fill remaining slots with best existing DB events so total = TARGET_PRO_COUNT.
    if is_refresh:
        high_quality = [e for e in pro_selected if e.get("final_score", 0) >= REFRESH_SCORE_THRESHOLD]
        logger.info(
            f"🔄 PRO REFRESH: {len(high_quality)} new events score≥{REFRESH_SCORE_THRESHOLD} "
            f"(out of {len(pro_selected)} new candidates)"
        )

        slots_needed = TARGET_PRO_COUNT - len(high_quality)
        filler_details: list = []
        if slots_needed > 0:
            exclude_slugs = {e.get("slug", "").lower() for e in high_quality}
            filler_rows = deduper.load_top_events_for_date(
                today.date(), is_pro=True, limit=slots_needed, exclude_slugs=exclude_slugs
            )
            filler_details = [_db_row_to_event_detail(row) for row in filler_rows]
            logger.info(f"📂 PRO REFRESH: filling {len(filler_details)} slots from DB")

        new_details: list = []
        if high_quality:
            logger.info("✍️ PRO REFRESH — Generating narratives for new events...")
            narratives_map = await processor.generate_secondary_narratives(high_quality, today)
            quizzes = await quiz_gen.generate_quizzes(high_quality, narratives_map)
            new_details = await _build_event_details(
                high_quality, narratives_map, quizzes, today, scraper, is_pro=True
            )

        final_pro_list = new_details + filler_details
        logger.info(
            f"🏆 PRO REFRESH TOTAL: {len(new_details)} new + "
            f"{len(filler_details)} from DB = {len(final_pro_list)}"
        )
        return final_pro_list, {
            "pro_discovered": len(pro_candidates),
            "pro_after_validation": len(validated),
            "pro_new_qualified": len(high_quality),
            "pro_filler_from_db": len(filler_details),
            "pro_final": len(final_pro_list),
        }

    # ── INITIAL MODE ──────────────────────────────────────────────
    logger.info(f"🏆 PRO SELECTED {len(pro_selected)} events:")
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
        "pro_after_dedup": len(pro_clean),
        "pro_selected": len(pro_selected),
        "pro_final": len(final_pro_list),
        "pro_quizzes_ok": sum(1 for q in quizzes if q is not None),
    }


# ══════════════════════════════════════════════════════════════════
# SHARED — Build EventDetail
# ══════════════════════════════════════════════════════════════════
async def _build_event_details(
    selected_items: list,
    narratives_map: dict,
    quizzes: list,
    today: datetime,
    scraper: WikiScraper,
    is_pro: bool,
) -> list:
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
            if _is_wikipedia_url(url):
                gallery.append(url)
                logger.info(f"  → Wikipedia URL kept directly: {url[:70]}")
            else:
                img_url = await safe_upload(
                    scraper, url, f"{tier_tag}_ev_{year}_{slug[:20]}_{i}"
                )
                if img_url:
                    gallery.append(img_url)
                    logger.info(f"  → Uploaded via Cloudinary: {img_url[:70]}")
                await asyncio.sleep(0.5)

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
# CORE — run FREE + PRO for a single date and send to Java
# ══════════════════════════════════════════════════════════════════
async def run_pipeline_for_date(
    target_date: datetime,
    scraper: WikiScraper,
    processor: AIProcessor,
    quiz_gen: QuizGenerator,
    ranker: ScoringEngine,
    run_social: bool = False,
    refresh_mode: bool = False,
) -> bool:
    """
    Run the full FREE + PRO pipeline for `target_date`.
    refresh_mode=True  → date already has events; keep only new events scoring ≥60,
                         fill remaining slots with best existing DB events.
    refresh_mode=False → fresh date; generate full 6 FREE + 4 PRO.
    Creates fresh deduper/validator so each date gets a clean DB snapshot.
    Returns True if payload was sent successfully.
    """
    deduper = EventDeduplicator(similarity_threshold=0.85)
    date_validator = WikiDateValidator(fuzzy_slug_threshold=0.90)

    mode_label = "REFRESH" if refresh_mode else f"INITIAL ({TARGET_FREE_COUNT}+{TARGET_PRO_COUNT})"

    try:
        logger.info(f"⚡ Launching FREE + PRO pipelines in parallel — {mode_label}...")
        (free_events, free_meta), (pro_events, pro_meta) = await asyncio.gather(
            run_free_pipeline(
                target_date, processor, scraper, ranker, quiz_gen, deduper, date_validator,
                is_refresh=refresh_mode,
            ),
            run_pro_pipeline(
                target_date, processor, scraper, ranker, quiz_gen, deduper, date_validator,
                is_refresh=refresh_mode,
            ),
        )

        all_events = free_events + pro_events

        if not all_events:
            logger.error("❌ No events generated — aborting payload send")
            return False

        # Final cross-tier dedup — catches exact URL matches AND same-year
        # fuzzy title matches (same historical moment, different Wikipedia articles).
        # FREE events come first in all_events so PRO duplicates are dropped.
        all_events = deduper.filter_final_cross_tier(all_events)

        # Sort: FREE events first (by impact_score desc), then PRO (by impact_score desc).
        # Frontend uses events[0] = highest-impact FREE = Main/Home hero.
        free_sorted = sorted(
            [e for e in all_events if not e.is_pro],
            key=lambda e: e.impact_score,
            reverse=True,
        )
        pro_sorted = sorted(
            [e for e in all_events if e.is_pro],
            key=lambda e: e.impact_score,
            reverse=True,
        )
        all_events = free_sorted + pro_sorted

        combined_metadata = {
            **free_meta,
            **pro_meta,
            "target_date": str(target_date.date()),
            "pipeline": "ai_driven_v9_with_backfill",
            "total_events": len(all_events),
        }

        payload = DailyPayload(
            date_processed=target_date.date(),
            events=all_events,
            metadata=combined_metadata,
        )

        free_count = sum(1 for e in all_events if not e.is_pro)
        pro_count = sum(1 for e in all_events if e.is_pro)

        logger.info("━" * 60)
        logger.info(f"📊 FINAL PAYLOAD [{target_date.date()}]: {len(all_events)} events total")
        logger.info(f"   → FREE: {free_count} (target {TARGET_FREE_COUNT}) | "
                    f"PRO: {pro_count} (target {TARGET_PRO_COUNT})")
        logger.info("━" * 60)
        for i, ev in enumerate(all_events):
            tier = "💎 PRO" if ev.is_pro else "🆓 FREE"
            title = ev.title_translations.en[:55]
            logger.info(
                f"  {i+1}. {tier} [{ev.category.value:20s}] | "
                f"{ev.year} | {title}"
            )
        logger.info("━" * 60)

        if free_count < TARGET_FREE_COUNT:
            logger.warning(f"⚠️ FREE count below target: {free_count}/{TARGET_FREE_COUNT}")
        if pro_count < TARGET_PRO_COUNT:
            logger.warning(f"⚠️ PRO count below target: {pro_count}/{TARGET_PRO_COUNT}")

        await send_to_java(payload)

        if run_social:
            logger.info("📱 Running Social Media Agent (FREE events only)...")
            try:
                free_only = [e for e in all_events if not e.is_pro]
                if free_only:
                    social_agent = SocialMediaAgent()
                    await social_agent.generate_and_post(free_only, target_date)
                else:
                    logger.warning("⚠️ No FREE events — skipping social agent")
            except Exception as e:
                logger.error(f"⚠️ Social Media Agent failed (non-critical): {e}")

        return True

    except Exception as e:
        logger.error(f"🚨 Pipeline Crash for {target_date.date()}: {e}", exc_info=True)
        return False


# ══════════════════════════════════════════════════════════════════
# MAIN
# Auto-detects which days need processing:
#   - If today AND tomorrow already have events → only process day+2
#   - Otherwise → process all 3 days (each in REFRESH or INITIAL as needed)
# DAY_OFFSET env var overrides auto-detection (0/1/2 = specific day only).
# ══════════════════════════════════════════════════════════════════
async def main():
    import os

    today = datetime.now()
    day_labels = {0: "TODAY", 1: "TOMORROW", 2: "DAY AFTER"}

    day_offset_env = os.environ.get("DAY_OFFSET")
    if day_offset_env is not None:
        # Manual override via env var
        try:
            offset = int(day_offset_env)
            dates_to_process = [(offset, today + timedelta(days=offset))]
            logger.info(f"🚀 Pipeline — manual override DAY_OFFSET={offset}")
        except ValueError:
            logger.warning(f"⚠️ Invalid DAY_OFFSET='{day_offset_env}' — using auto-detect")
            day_offset_env = None

    if day_offset_env is None:
        # Auto-detect: check if today and tomorrow are already populated
        checker = EventDeduplicator()
        today_populated = checker.has_events_for_date(today.date())
        tomorrow_populated = checker.has_events_for_date((today + timedelta(days=1)).date())

        if today_populated and tomorrow_populated:
            logger.info(
                "🚀 Pipeline — today and tomorrow already have events → processing day+2 only"
            )
            dates_to_process = [(2, today + timedelta(days=2))]
        else:
            logger.info("🚀 Pipeline — running full 3-day window")
            dates_to_process = [(i, today + timedelta(days=i)) for i in range(3)]

    scraper = WikiScraper()
    processor = AIProcessor()
    quiz_gen = QuizGenerator()
    ranker = ScoringEngine()

    for i, target_date in dates_to_process:
        label = day_labels.get(i, f"DAY +{i}")
        logger.info(f"\n{'═' * 60}")
        logger.info(f"📅 Processing {label}: {target_date.date()}")
        logger.info(f"{'═' * 60}")

        checker = EventDeduplicator()
        already_populated = checker.has_events_for_date(target_date.date())
        mode = "REFRESH" if already_populated else "INITIAL"
        logger.info(f"📋 {label} ({target_date.date()}) → {mode} mode")

        await run_pipeline_for_date(
            target_date=target_date,
            scraper=scraper,
            processor=processor,
            quiz_gen=quiz_gen,
            ranker=ranker,
            run_social=(i == 0),
            refresh_mode=already_populated,
        )

    logger.info("\n✅ Pipeline complete.")


if __name__ == "__main__":
    asyncio.run(main())