import asyncio
import math
import time
import hmac
import hashlib
import base64
import json
import httpx
from datetime import datetime

from core.logger import setup_logger
from core.config import config
from engine.scraper import SmartWikiScraper
from engine.processor import AIProcessor  # <--- Asigură-te că ai acest import
from schema.models import DailyPayload, EventDetail, EventCategory, Translations

logger = setup_logger("MainPipeline")

# ─────────────────────────────────────────
# SEND TO JAVA (Rămâne neschimbat, e corect)
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
# SAFE UPLOAD (Cloudinary)
# ─────────────────────────────────────────
async def safe_upload(scraper, url: str, public_id: str):
    if not url: return None
    try:
        # Folosim to_thread pentru că operațiile Cloudinary sunt blocking (sync)
        return await asyncio.to_thread(scraper.upload_to_cloudinary, url, public_id)
    except Exception:
        return None

# ─────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────
async def main():
    logger.info("🚀 Starting Elite History Pipeline")

    scraper = SmartWikiScraper()
    processor = AIProcessor()
    today = datetime.now()
    
    # Log explicit pentru debugging
    logger.info(f"📅 Processing date: {today.strftime('%Y-%m-%d')}")
    logger.info(f"📍 Looking for events from: {today.strftime('%B %d')} (in history)")

    try:
        # STEP 1: Extragem evenimentele din EXACT ziua de azi (dar din istorie)
        candidates = await scraper.get_today_elite_events()
        
        if not candidates:
            logger.error(f"❌ No events found for {today.strftime('%B %d')}")
            return
        
        # Validare suplimentară: confirmăm că toate au luna/ziua corectă
        validated_candidates = []
        for c in candidates:
            if c["month"] == today.month and c["day"] == today.day:
                validated_candidates.append(c)
            else:
                logger.warning(f"⚠️ Skipping event with wrong date: {c['text']}")
        
        candidates = validated_candidates
        logger.info(f"✅ Validated {len(candidates)} events for today")

        # STEP 2: Calculăm popularitatea
        logger.info(f"📊 Analyzing popularity...")
        view_tasks = [scraper.fetch_page_views(c["slug"]) for c in candidates]
        views_results = await asyncio.gather(*view_tasks)

        for i, count in enumerate(views_results):
            candidates[i]["views"] = count

        # STEP 3: Scoring avansat
        for c in candidates:
            year = c["year"]
            views = c["views"]
            
            # Penalizare pentru evenimente prea vechi (pre-1500)
            if year < 1500:
                age_penalty = -10
            else:
                age_penalty = 0
            
            # Bonus pentru evenimente moderne cu media disponibilă
            if year > 1900:
                recency_bonus = 20
            elif year > 1800:
                recency_bonus = 10
            else:
                recency_bonus = 0
            
            # Scor bazat pe popularitate + bonusuri
            c["final_score"] = (math.log10(views + 1) * 15) + recency_bonus + age_penalty
            
            # Log pentru debugging
            logger.debug(f"Event: {year} - {c['slug'][:30]} | Views: {views} | Score: {c['final_score']:.2f}")

        # Sortăm și luăm top 5
        candidates.sort(key=lambda x: x["final_score"], reverse=True)
        top5 = candidates[:5]
        
        logger.info("🏆 Top 5 events selected:")
        for i, ev in enumerate(top5, 1):
            logger.info(f"  {i}. [{ev['year']}] {ev['text'][:60]}... (score: {ev['final_score']:.2f})")

        # STEP 4: Procesare AI
        final_events = []
        for ev in top5:
            logger.info(f"✍️ Processing: {ev['slug']} ({ev['year']})")
            
            ai_data = await processor.polish_event(ev)
            
            # Upload imagine
            hq_image = ev.get("thumbnail")
            cloud_url = await safe_upload(scraper, hq_image, f"ev_{ev['year']}_{ev['slug']}")
            
            # Construim event-ul final cu data ISTORICĂ corectă
            historical_date = datetime(ev['year'], ev['month'], ev['day']).date()
            
            final_events.append(
                EventDetail(
                    category=ai_data["category"],
                    year=ev["year"],
                    event_date=historical_date,  # Data când s-a întâmplat în istorie
                    source_url=f"https://en.wikipedia.org/wiki/{ev['slug']}",
                    title_translations=ai_data["titles"],
                    narrative_translations=ai_data["narratives"],
                    impact_score=float(ev["final_score"]),
                    page_views_30d=ev["views"],
                    gallery=[cloud_url] if cloud_url else []
                )
            )

        # STEP 5: Trimitere
        payload = DailyPayload(
            date_processed=today.date(),  # Când am procesat noi
            events=final_events,
            metadata={
                "source": "wiki_on_this_day",
                "version": "2.1",
                "historical_date": f"{today.strftime('%B %d')}",
                "events_count": len(final_events)
            }
        )

        await send_to_java(payload)
        logger.info("🏁 Pipeline completed!")

    except Exception as e:
        logger.error(f"🚨 Pipeline crash: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())