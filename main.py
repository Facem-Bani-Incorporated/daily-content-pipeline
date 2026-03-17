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
    scraper = WikiScraper()
    today = datetime.now()
    
    # 1. Luam tot ce s-a intamplat azi (100+ evenimente)
    events = await scraper.fetch_on_this_day_raw(today.month, today.day)
    
    # 2. Calculam popularitatea in paralel (Social Proof)
    tasks = [scraper.get_popularity_score(ev["slug"]) for ev in events]
    scores = await asyncio.gather(*tasks)
    
    for i, score in enumerate(scores):
        events[i]["popularity"] = score

    # 3. Filtram: Pastram doar evenimentele care au > 5000 vizualizari (eliminam "gunoiul")
    # Si sortam dupa popularitate
    filtered_events = [e for e in events if e["popularity"] > 5000]
    filtered_events.sort(key=lambda x: x["popularity"], reverse=True)
    
    # 4. Trimitem top 15 cele mai populare catre AI
    # AI-ul nu mai cauta, ci doar ALEGE cele mai bune 5 (diversitate: politica, tech, arta)
    final_selection = await processor.ai_curation(filtered_events[:15])
    
    # 5. Pentru cele 5 finale, luam imagini HQ si urcam in Cloudinary
    for event in final_selection:
        hq_image = await scraper.get_optimized_image(event["slug"])
        if hq_image:
            event["cloud_url"] = scraper.upload_to_cloudinary(hq_image, f"{event['year']}_{event['slug']}")