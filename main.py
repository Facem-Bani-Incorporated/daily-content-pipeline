import asyncio
import json
import httpx
from datetime import datetime
from core.logger import setup_logger
from core.config import config
from engine.scraper import WikiScraper
from engine.processor import AIProcessor
from engine.ranker import ScoringEngine
from schema.models import DailyPayload, MainEvent, SecondaryEvent, EventCategory, Translations
from tenacity import retry, stop_after_attempt, wait_fixed

logger = setup_logger("MainPipeline")


# --- 1. BRIDGE FUNCTIONS ---

@retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
async def send_to_java(payload: DailyPayload):
    headers = {
        "X-Internal-Api-Key": config.INTERNAL_API_SECRET,
        "Content-Type": "application/json"
    }
    # mode='json' transformă totul (date, enums) în formate primitive compatibile JSON
    payload_json = payload.model_dump(mode='json')

    async with httpx.AsyncClient(timeout=180.0) as client:
        logger.info(f"📤 Ingesting to Java: {config.JAVA_BACKEND_URL}")
        response = await client.post(config.JAVA_BACKEND_URL, json=payload_json, headers=headers)
        response.raise_for_status()
        return response.status_code


def print_full_inspection(payload: DailyPayload):
    """Afișează absolut tot conținutul JSON-ului pentru inspecție vizuală."""
    print("\n" + "🔍" + " VALIDARE PAYLOAD COMPLET " + "=" * 40)

    # Main Event info
    me = payload.main_event
    print(f"\n🌟 MAIN EVENT [{me.year}]")
    print(f"   📂 Categorie: {me.category.value.upper()}")
    print(f"   🎯 Scor Impact: {me.impact_score}")
    print(f"   📈 Views (30d): {me.page_views_30d}")
    print(f"   📝 Titlu RO: {me.title_translations.ro}")
    print(f"   📖 Narativ RO (primele 150 ch): {me.narrative_translations.ro[:150]}...")
    print(f"   🖼️ Galerie: {me.gallery}")

    print(f"\n🔗 EVENIMENTE SECUNDARE ({len(payload.secondary_events)}):")
    for idx, sec in enumerate(payload.secondary_events):
        print(f"   {idx + 1}. [{sec.year}] {sec.title_translations.ro}")
        print(f"      📊 Scor: {sec.ai_relevance_score}")
        print(f"      📜 Narativ RO (150-250 cuv): {sec.narrative_translations.ro[:120]}...")
        print(f"      🔗 URL: {sec.source_url}")
        print(f"      🖼️ Thumb: {sec.thumbnail_url}")

    print("\n" + "=" * 65 + "\n")


# --- 2. MAIN PIPELINE ---

async def main():
    logger.info("🚀 Starting Daily Pipeline...")
    # Fallback default pentru a preveni erorile de tip None/Empty
    default_langs = {l: "Information pending translation..." for l in ["en", "ro", "es", "de", "fr"]}

    try:
        scraper = WikiScraper()
        processor = AIProcessor()
        ranker = ScoringEngine()

        # 1. FETCH & INITIAL RANKING
        raw_events = await scraper.fetch_today()
        for item in raw_events:
            item['h_score'] = ranker.heuristic_score(item)

        candidates = sorted(raw_events, key=lambda x: x['h_score'], reverse=True)[:config.MAX_CANDIDATES_FOR_AI]

        # 2. AI BATCH (Titles & Categories)
        ai_data = await processor.batch_score_and_categorize(candidates)
        results = ai_data.get('results', {})

        for idx, item in enumerate(candidates):
            res = results.get(f"ID_{idx}", {})
            item['category'] = res.get('category', 'culture')
            item['ai_impact'] = res.get('score', 50)
            item['titles'] = res.get('titles', default_langs)
            item['final_score'] = ranker.calculate_final_score(item['h_score'], item['ai_impact'], 0)

        candidates.sort(key=lambda x: x.get('final_score', 0), reverse=True)

        # 3. MAIN EVENT GENERATION
        top_data = candidates[0]
        main_content = await processor.generate_multilingual_main_event(top_data)

        # Galerie Main
        slug_main = top_data.get('slug', 'history')
        wiki_imgs = await scraper.fetch_gallery_urls(slug_main, limit=3)
        main_gallery = []
        for i, url in enumerate(wiki_imgs):
            up_url = await asyncio.to_thread(scraper.upload_to_cloudinary, url, f"main_{top_data['year']}_{i}")
            if up_url: main_gallery.append(up_url)

        # 4. SECONDARY EVENTS GENERATION
        secondary_pool = candidates[1:6]
        sec_narratives_map = await processor.generate_secondary_narratives(secondary_pool)

        secondary_objs = []
        for idx, item in enumerate(secondary_pool):
            # Mapare sigură: AI folosește EVENT_0, EVENT_1...
            key = f"EVENT_{idx}"
            narratives = sec_narratives_map.get(key, default_langs)

            # Imagine Secundară
            raw_thumb = item.get('wiki_thumb') or "https://images.unsplash.com/photo-1447069387593-a5de0862481e?w=400"
            thumb_url = await asyncio.to_thread(scraper.upload_to_cloudinary, raw_thumb, f"sec_{item['year']}_{idx}")

            secondary_objs.append(SecondaryEvent(
                title_translations=Translations(**item.get('titles', default_langs)),
                year=item['year'],
                source_url=f"https://en.wikipedia.org/wiki/{item.get('slug', '')}",
                thumbnail_url=thumb_url,
                ai_relevance_score=float(item.get('final_score', 0)),
                narrative_translations=Translations(**narratives)
            ))

        # 5. ASAMBLARE PAYLOAD (Respectă perfect modelul tău)
        payload = DailyPayload(
            date_processed=datetime.now().date(),
            api_secret=config.INTERNAL_API_SECRET,
            main_event=MainEvent(
                category=EventCategory(top_data['category'].lower()),
                page_views_30d=top_data.get('views', 0),
                title_translations=Translations(**main_content.get('titles', default_langs)),
                year=top_data['year'],
                source_url=f"https://en.wikipedia.org/wiki/{slug_main}",
                event_date=datetime.now().date(),
                narrative_translations=Translations(**main_content.get('narratives', default_langs)),
                impact_score=float(top_data['final_score']),
                gallery=main_gallery
            ),
            secondary_events=secondary_objs
        )

        # 6. SEND TO JAVA
        logger.info("📡 Sending Final Validated Payload to Backend...")
        status = await send_to_java(payload)
        logger.info(f"✅ Pipeline Completed Successfully (Status {status})")

    except Exception as e:
        logger.error(f"🚨 Pipeline Crash: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())