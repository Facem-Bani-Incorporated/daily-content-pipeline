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
    """Trimite payload-ul perfect validat către backend-ul Java."""
    target_url = config.JAVA_BACKEND_URL
    secret = config.INTERNAL_API_SECRET

    # MAGIA PYDANTIC: Serializare directă în string JSON cu alias-uri (camelCase)
    # Ex: event_date devine eventDate, title_translations devine titleTranslations
    body_json = payload.model_dump_json(by_alias=True)
    
    timestamp = str(int(time.time()))
    auth_payload = f"{timestamp}.{body_json}"

    signature = hmac.new(
        secret.encode(),
        auth_payload.encode(),
        hashlib.sha256,
    ).digest()

    headers = {
        "X-Timestamp": timestamp,
        "X-Signature": base64.b64encode(signature).decode(),
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            logger.info(f"📤 Trimitere către Java: {target_url}")
            res = await client.post(target_url, content=body_json, headers=headers)
            if res.status_code in [200, 201]:
                logger.info("✅ SUCCES! Java a acceptat payload-ul.")
            else:
                logger.error(f"❌ EROARE JAVA {res.status_code}: {res.text}")
        except Exception as e:
            logger.error(f"🚨 Eroare de conexiune HTTP: {e}")

async def safe_upload(scraper, url: str, public_id: str):
    """Upload sigur folosind to_thread (presupunând că ai o funcție sclipitoare de Cloudinary)."""
    if not url: return None
    try:
        if hasattr(scraper, 'upload_to_cloudinary'):
            return await asyncio.to_thread(scraper.upload_to_cloudinary, url, public_id)
        return url # Fallback la URL-ul Wikipedia dacă nu ai Cloudinary
    except Exception:
        return None

async def main():
    logger.info("🚀 PORNIM PIPELINE-UL DE ELITĂ")

    scraper = SmartWikiScraper()
    processor = AIProcessor()
    today = datetime.now()

    try:
        # 1. Extragere
        candidates = await scraper.get_today_elite_events()
        if not candidates:
            logger.error("❌ Nu am găsit evenimente pentru azi. Ieșire.")
            return

        # 2. Analiză Popularitate
        logger.info("📊 Calculăm factorul de impact real...")
        view_tasks = [scraper.fetch_page_views(c["slug"]) for c in candidates]
        views_results = await asyncio.gather(*view_tasks)

        # 3. Formula de Scor "Dumnezeiască" (Strict 0.0 - 100.0)
        for i, count in enumerate(views_results):
            c = candidates[i]
            c["views"] = count
            
            # Baza logaritmică (Ex: 100.000 views -> log10=5 * 14 = 70 puncte)
            base_score = math.log10(count + 1) * 14 
            
            # Bonusuri
            bonus_selected = 15 if c.get("is_selected") else 0
            recency_bonus = 10 if c["year"] > 1850 else 0
            
            raw_score = base_score + bonus_selected + recency_bonus
            
            # Clamping extrem de strict pentru schema Pydantic (ge=0, le=100)
            c["final_score"] = max(0.0, min(100.0, round(raw_score, 2)))

        # 4. Sortare și Selecție Top 5
        candidates.sort(key=lambda x: x["final_score"], reverse=True)
        top5 = candidates[:5]

        logger.info("🏆 TOP 5 EVENIMENTE SELECȚIONATE:")
        for ev in top5:
            logger.info(f" -> [{ev['year']}] {ev['text'][:50]}... (Scor: {ev['final_score']})")

        # 5. Procesare AI & Validare Strictă Schema
        final_events = []
        for ev in top5:
            logger.info(f"✍️ Procesare AI pentru: {ev['slug']}")
            ai_data = await processor.polish_event(ev)
            
            cloud_url = await safe_upload(scraper, ev.get("thumbnail"), f"ev_{ev['year']}_{ev['slug']}")
            
            # Asigurare fallback pentru Enum, în caz că AI-ul halucinează categoria
            try:
                safe_category = EventCategory(ai_data.get("category"))
            except ValueError:
                safe_category = EventCategory.CULTURE_ARTS # Fallback sigur

            # Construire Instanță EventDetail (Validare Pydantic)
            event_obj = EventDetail(
                category=safe_category,
                year=ev["year"],
                event_date=date(ev['year'], ev['month'], ev['day']),
                source_url=f"https://en.wikipedia.org/wiki/{ev['slug']}",
                title_translations=Translations(**ai_data["titles"]),
                narrative_translations=Translations(**ai_data["narratives"]),
                impact_score=ev["final_score"],
                page_views_30d=ev["views"],
                gallery=[cloud_url] if cloud_url else []
            )
            final_events.append(event_obj)

        # 6. Construire Payload Final
        payload = DailyPayload(
            date_processed=today.date(),
            events=final_events,
            metadata={
                "source": "wiki_elite_scraper", 
                "algorithm_version": "v3.perfect",
                "events_count": len(final_events)
            }
        )

        # 7. Trimitere
        await send_to_java(payload)
        logger.info("🏁 Pipeline completat cu succes absolut!")

    except Exception as e:
        logger.error(f"🚨 Pipeline prăbușit fatal: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())