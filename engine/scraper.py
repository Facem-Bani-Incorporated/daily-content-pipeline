import httpx
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict
from core.logger import setup_logger

logger = setup_logger("EliteScraper")

class SmartWikiScraper:
    def __init__(self):
        # User-Agent curat, fără dependențe externe
        self.headers = {"User-Agent": "HistoryEliteBot/1.0 (Contact: admin@local)"}
        self.blacklist_terms = [
            "footballer", "cricketer", "baseball player", "politician from",
            "village", "municipality", "census-designated", "district", "county"
        ]

    async def fetch_page_views(self, slug: str) -> int:
        """Calculăm popularitatea pe 30 de zile."""
        now = datetime.now()
        today_str = now.strftime("%Y%m%d")
        last_month_str = (now - timedelta(days=30)).strftime("%Y%m%d")
        
        url = (f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
               f"en.wikipedia/all-access/user/{slug}/daily/{last_month_str}/{today_str}")
        
        async with httpx.AsyncClient(headers=self.headers, timeout=10.0) as client:
            try:
                res = await client.get(url)
                if res.status_code == 200:
                    data = res.json()
                    return sum(item["views"] for item in data.get("items", []))
            except Exception:
                return 0
        return 0

    async def get_today_elite_events(self) -> List[Dict]:
        """Extrage evenimentele istorice premium pentru ziua curentă."""
        now = datetime.now()
        month, day = now.month, now.day
        
        url = f"https://en.wikipedia.org/api/rest_v1/feed/onthisday/all/{month:02d}/{day:02d}"
        logger.info(f"🔎 Căutăm evenimente de top pentru {day:02d}/{month:02d}...")

        async with httpx.AsyncClient(headers=self.headers, timeout=20.0) as client:
            try:
                res = await client.get(url)
                res.raise_for_status()
                data = res.json()

                # Prioritate pe evenimentele "selected" (cele mai importante)
                raw_list = data.get("selected", [])
                if len(raw_list) < 12:
                    raw_list.extend(data.get("events", [])[:15])

                clean_events = []
                seen_slugs = set()
                
                for ev in raw_list:
                    year = ev.get("year")
                    text = ev.get("text", "").strip()
                    pages = ev.get("pages", [])
                    
                    if not year or not text or not pages:
                        continue
                        
                    main_page = pages[0]
                    slug = main_page.get("titles", {}).get("canonical", "")
                    
                    if slug in seen_slugs or not slug:
                        continue
                    
                    # Filtrare conținut slab
                    desc = main_page.get("description", "").lower()
                    if any(term in desc for term in self.blacklist_terms):
                        continue
                    
                    seen_slugs.add(slug)
                    clean_events.append({
                        "year": int(year),
                        "text": text,
                        "slug": slug,
                        "description": desc,
                        "extract": main_page.get("extract", "")[:600],
                        "thumbnail": main_page.get("thumbnail", {}).get("source") if main_page.get("thumbnail") else None,
                        "month": month,
                        "day": day,
                        "is_selected": ev in data.get("selected", [])
                    })
                
                return clean_events
            except Exception as e:
                logger.error(f"❌ Scraper Error: {e}")
                return []