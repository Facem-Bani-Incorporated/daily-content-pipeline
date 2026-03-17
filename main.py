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
    processor = AIProcessor() # Motorul de rescriere și traducere
    today = datetime.now()

    try:
        # STEP 1: Extragem evenimentele verificate (SELECTED)
        # Noul scraper elimină deja țările/orașele prin blacklist
        candidates = await scraper.get_today_elite_events()
        if not candidates:
            logger.error("❌ No elite events found today")
            return

        # STEP 2: Calculăm Popularitatea (PageViews) în paralel
        logger.info(f"📊 Analyzing popularity for {len(candidates)} candidates...")
        view_tasks = [scraper.fetch_page_views(c["slug"]) for c in candidates]
        views_results = await asyncio.gather(*view_tasks)

        for i, count in enumerate(views_results):
            candidates[i]["views"] = count

        # STEP 3: Scoring Matematic (Importanță Istorică + Popularitate Actuală)
        for c in candidates:
            # Bonus pentru epoca modernă (mai mult conținut media disponibil)
            recency_bonus = 25 if c["year"] > 1800 else 0
            # Formula logaritmică: echilibrează evenimentele virale cu cele de nișă
            c["final_score"] = (math.log10(c["views"] + 1) * 12) + recency_bonus

        # Sortăm și luăm cele mai bune 5
        candidates.sort(key=lambda x: x["final_score"], reverse=True)
        top5 = candidates[:5]

        # STEP 4: Procesare AI (Narațiune, Categorisire, Traducere)
        final_events = []
        for ev in top5:
            logger.info(f"✍️ AI Polishing: {ev['slug']} ({ev['year']})")
            
            # AI-ul transformă textul sec de pe Wiki în poveste
            # Și returnează categorisirea corectă (WAR, SCIENCE, etc.)
            ai_data = await processor.polish_event(ev) 

            # Imagini HQ (Cloudinary)
            # Prioritizăm imaginea mare de la Wiki dacă există
            hq_image = ev.get("thumbnail") 
            cloud_url = await safe_upload(scraper, hq_image, f"ev_{ev['year']}_{ev['slug']}")

            final_events.append(
                EventDetail(
                    category=ai_data["category"], 
                    year=ev["year"],
                    event_date=today.date(),
                    source_url=f"https://en.wikipedia.org/wiki/{ev['slug']}",
                    title_translations=ai_data["titles"],
                    narrative_translations=ai_data["narratives"],
                    impact_score=float(ev["final_score"]),
                    page_views_30d=ev["views"],
                    gallery=[cloud_url] if cloud_url else []
                )
            )

        # STEP 5: Trimitere către Backend
        payload = DailyPayload(
            date_processed=today.date(),
            events=final_events,
            metadata={"source": "wiki_elite_selection", "version": "2.0"}
        )

        await send_to_java(payload)
        logger.info("🏁 Pipeline completed successfully!")

    except Exception as e:
        logger.error(f"🚨 Pipeline crash: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())