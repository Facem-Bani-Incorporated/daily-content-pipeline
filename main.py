import asyncio
import math
import time
import hmac
import hashlib
import base64
import httpx
from datetime import datetime, date

from core.logger import setup_logger
from core.config import config
from engine.scraper import SmartWikiScraper
from engine.processor import AIProcessor 
from schema.models import DailyPayload, EventDetail, EventCategory, Translations

logger = setup_logger("MainPipeline")

async def send_to_java(payload: DailyPayload):
    """Trimitere securizată cu CamelCase automat via Pydantic."""
    target_url = config.JAVA_BACKEND_URL
    secret = config.INTERNAL_API_SECRET

    # Generăm JSON-ul folosind alias-urile (camelCase)
    body_json = payload.model_dump_json(by_alias=True)
    
    timestamp = str(int(time.time()))
    auth_payload = f"{timestamp}.{body_json}"
    signature = hmac.new(secret.encode(), auth_payload.encode(), hashlib.sha256).digest()

    headers = {
        "X-Timestamp": timestamp,
        "X-Signature": base64.b64encode(signature).decode(),
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            logger.info(f"📤 Trimitere către Java...")
            res = await client.post(target_url, content=body_json, headers=headers)
            logger.info(f"✅ Java Response: {res.status_code}")
        except Exception as e:
            logger.error(f"🚨 Eroare de rețea: {e}")

async def main():
    logger.info("🚀 PORNIM PIPELINE-UL DE ELITĂ")
    scraper = SmartWikiScraper()
    processor = AIProcessor()
    today = datetime.now()

    try:
        # 1. Scraping
        candidates = await scraper.get_today_elite_events()
        if not candidates: return

        # 2. Popularitate & Scoring (0-100)
        view_tasks = [scraper.fetch_page_views(c["slug"]) for c in candidates]
        views_results = await asyncio.gather(*view_tasks)

        for i, views in enumerate(views_results):
            c = candidates[i]
            c["views"] = views
            # Formula: Logaritm vizualizări + Bonus "Selected" + Bonus Modernitate
            base_score = math.log10(views + 1) * 14
            bonus = 15 if c.get("is_selected") else 0
            modern_bonus = 10 if c["year"] > 1800 else 0
            c["final_score"] = max(0.0, min(100.0, round(base_score + bonus + modern_bonus, 2)))

        candidates.sort(key=lambda x: x["final_score"], reverse=True)
        top5 = candidates[:5]

        # 3. Procesare AI cu Validare "Dumnezeiască"
        final_events = []
        for ev in top5:
            logger.info(f"✍️ Procesare: {ev['slug']} ({ev['year']})")
            ai_data = await processor.polish_event(ev)
            
            # --- VALIDARE LIMBI (Previne eroarea de la linia 111) ---
            def ensure_langs(d):
                return {l: d.get(l, f"Information about {ev['year']}") for l in ["en", "ro", "es", "de", "fr"]}

            # --- VALIDARE CATEGORIE ---
            try:
                valid_cat = EventCategory(ai_data.get("category"))
            except ValueError:
                valid_cat = EventCategory.CULTURE_ARTS

            # Construire obiect conform schemei
            event_obj = EventDetail(
                category=valid_cat,
                year=ev["year"],
                event_date=date(ev['year'], ev['month'], ev['day']),
                source_url=f"https://en.wikipedia.org/wiki/{ev['slug']}",
                title_translations=Translations(**ensure_langs(ai_data.get("titles", {}))),
                narrative_translations=Translations(**ensure_langs(ai_data.get("narratives", {}))),
                impact_score=ev["final_score"],
                page_views_30d=ev["views"],
                gallery=[] # Adaugă logică upload dacă ai Cloudinary configurat
            )
            final_events.append(event_obj)

        # 4. Payload-ul Final
        payload = DailyPayload(
            date_processed=today.date(),
            events=final_events,
            metadata={"source": "elite_wiki_v2", "historical_day": today.strftime("%d %B")}
        )

        await send_to_java(payload)
        logger.info("🏁 PIPELINE FINALIZAT CU SUCCES!")

    except Exception as e:
        logger.error(f"🚨 CRASH: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())