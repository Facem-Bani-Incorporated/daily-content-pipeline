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
            "gallery": ev.gallery if ev.gallery else []
        })

    payload_to_serialize = {
        "dateProcessed": payload.date_processed.isoformat(),
        "events": events_final
    }

    body_json = json.dumps(payload_to_serialize, separators=(',', ':'))
    body_bytes = body_json.encode('utf-8')

    timestamp = str(int(time.time()))
    auth_payload = f"{timestamp}.{body_json}"

    signature = hmac.new(
        secret.encode('utf-8'),
        auth_payload.encode('utf-8'),
        hashlib.sha256
    ).digest()

    signature_base64 = base64.b64encode(signature).decode('utf-8')

    headers = {
        "X-Timestamp": timestamp,
        "X-Signature": signature_base64,
        "Content-Type": "application/json"
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            logger.info(f"📤 Sending to Java: {target_url}")
            response = await client.post(target_url, content=body_bytes, headers=headers)
            if response.status_code in [200, 201]:
                logger.info(f"✅ SUCCESS! ID returnat: {response.text}")
            else:
                logger.error(f"❌ Status {response.status_code}: {response.text}")
        except Exception as e:
            logger.error(f"🚨 Eroare conexiune: {e}")

async def safe_upload(scraper, url, folder_name):
    if not url: return None
    try:
        return await asyncio.to_thread(scraper.upload_to_cloudinary, url, folder_name)
    except Exception:
        return None

async def main():
    logger.info("🚀 Starting Unified Pipeline...")
    scraper, processor, ranker = WikiScraper(), AIProcessor(), ScoringEngine()

    try:
        raw_events = await scraper.fetch_today()
        for item in raw_events:
            item['h_score'] = ranker.heuristic_score(item)

        candidates = sorted(raw_events, key=lambda x: x['h_score'], reverse=True)[:config.MAX_CANDIDATES_FOR_AI]
        
        ai_data = await processor.batch_score_and_categorize(candidates)
        ai_results = ai_data.get('results', {})
        
        view_tasks = [scraper.fetch_page_views(c.get('slug')) for c in candidates]
        views = await asyncio.gather(*view_tasks)

        for idx, item in enumerate(candidates):
            res = ai_results.get(f"ID_{idx}", {})
            item.update({
                'views': views[idx] if isinstance(views[idx], int) else 0,
                'category': res.get('category', 'culture_arts'),
                'score': res.get('score', 50),
                'titles': res.get('titles', {l: "History Event" for l in ["en", "ro", "es", "de", "fr"]})
            })
            item['final_score'] = ranker.calculate_final_score(item['h_score'], item['score'], item['views'])

        candidates.sort(key=lambda x: x.get('final_score', 0), reverse=True)
        top_5 = candidates[:5]

        # ... (partea de sus a main() rămâne neschimbată până la narratives_map) ...
        narratives_map = await processor.generate_secondary_narratives(top_5)

        final_events_list = []
        for idx, item in enumerate(top_5):
            slug = item.get('slug', 'history')
            year = item.get('year', 0)
            slug_clean = slug.replace('_', ' ') if slug else "history"

            # --- LOGICA CORECTĂ PENTRU IMAGINI (Max 3 total) ---
            
            # 1. Încercăm să luăm poza "Hero" (Pexels)
            hero_url = await scraper.fetch_pro_image(slug_clean)
            
            # 2. Luăm galeria de pe Wiki (cerem doar 3 pentru siguranță)
            wiki_urls = await scraper.fetch_gallery_urls(slug, limit=3)
            
            combined_sources = []
            
            # Adăugăm prima poză HQ dacă există
            if hero_url:
                combined_sources.append(hero_url)
            
            # Completăm cu poze de pe Wiki până la MAXIM 3 poze în total
            for w_url in wiki_urls:
                if len(combined_sources) < 3:
                    if w_url not in combined_sources:
                        combined_sources.append(w_url)
                else:
                    break # Ne oprim la 3 surse total

            # 3. Upload pe Cloudinary (folosind safe_upload pentru a evita blocajele)
            # Parametrul folder_name devine prefix pentru public_id
            img_tasks = [
                safe_upload(scraper, url, f"ev_{year}_{i}") 
                for i, url in enumerate(combined_sources)
            ]
            gallery = [img for img in await asyncio.gather(*img_tasks) if img]

            # 4. Fallback: Dacă Cloudinary a respins totul sau sursele au fost goale
            if not gallery and item.get('wiki_thumb'):
                fb_img = await safe_upload(scraper, item.get('wiki_thumb'), f"ev_{year}_fb")
                gallery = [fb_img] if fb_img else []

            # --- ASAMBLARE EVENIMENT ---
            final_events_list.append(EventDetail(
                category=EventCategory(item['category'].lower()),
                year=year,
                event_date=datetime.now().date(),
                source_url=f"https://en.wikipedia.org/wiki/{slug}",
                title_translations=Translations(**item['titles']),
                narrative_translations=Translations(**narratives_map.get(f"EVENT_{idx}", {})),
                impact_score=float(item['final_score']),
                page_views_30d=item['views'],
                gallery=gallery
            ))

        # 5. ASAMBLARE ȘI TRIMITERE PAYLOAD
        payload = DailyPayload(
            date_processed=datetime.now().date(),
            events=final_events_list,
            metadata={"processed": len(candidates), "count": len(final_events_list)}
        )

        await send_to_java(payload)

    except Exception as e:
        logger.error(f"🚨 Pipeline Crash: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())